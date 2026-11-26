import assert from 'node:assert/strict'
import fc from 'fast-check'
import type { Task, TaskEvent, TaskState } from '../src/store.ts'

export const AGENTS = ['agent:worker-0', 'agent:worker-1', 'agent:worker-2']
const KEYS = 4
const TERMINAL = new Set<TaskState>(['completed', 'failed', 'canceled'])
const memoValue = fc.constantFrom(null, false, true, 'reviewed')

type Op =
  | { t: 'ingest'; key: number; retry: 'safe' | 'manual'; max: number }
  | { t: 'claim'; agent: number; lease: number }
  | { t: 'renew' | 'complete' | 'release' | 'block'; agent: number; key: number; stale: boolean }
  | { t: 'fail'; agent: number; key: number; stale: boolean; retry: boolean }
  | { t: 'unblock' | 'requeue' | 'cancel'; key: number }
  | { t: 'log'; agent: number; key: number }
  | { t: 'memo'; agent: number; key: number; value: null | boolean | string }
  | { t: 'advance'; ms: number }
  | { t: 'repeat' }
  | { t: 'reuse' }
  | { t: 'other-actor' }

const agent = fc.integer({ min: 0, max: AGENTS.length - 1 })
const key = fc.integer({ min: 0, max: KEYS - 1 })
const held = (t: 'renew' | 'complete' | 'release' | 'block') =>
  fc.record({ t: fc.constant(t), agent, key, stale: fc.boolean() })
export const operation: fc.Arbitrary<Op> = fc.oneof(
  fc.record({
    t: fc.constant('ingest' as const),
    key,
    retry: fc.constantFrom('safe' as const, 'manual' as const),
    max: fc.integer({ min: 1, max: 3 }),
  }),
  {
    arbitrary: fc.record({ t: fc.constant('claim' as const), agent, lease: fc.integer({ min: 10_000, max: 50_000 }) }),
    weight: 3,
  },
  held('renew'),
  held('complete'),
  held('release'),
  held('block'),
  fc.record({ t: fc.constant('fail' as const), agent, key, stale: fc.boolean(), retry: fc.boolean() }),
  fc.record({ t: fc.constantFrom('unblock' as const, 'requeue' as const, 'cancel' as const), key }),
  fc.record({ t: fc.constant('log' as const), agent, key }),
  { arbitrary: fc.record({ t: fc.constant('memo' as const), agent, key, value: memoValue }), weight: 2 },
  { arbitrary: fc.record({ t: fc.constant('advance' as const), ms: fc.integer({ min: 0, max: 60_000 }) }), weight: 2 },
  fc.constant({ t: 'repeat' as const }),
  fc.constant({ t: 'reuse' as const }),
  fc.constant({ t: 'other-actor' as const }),
)

export interface Call {
  t: Exclude<Op['t'], 'ingest' | 'advance' | 'repeat' | 'reuse' | 'other-actor'>
  actor: string
  id: number
  epoch: number
  lease: number
  retry: boolean
  requestId: string
  text: string
  value: unknown
}

export type Outcome =
  | { ok: { id: number; state: TaskState; epoch: number } | { text: string } | { value: unknown } | null }
  | { code: string }
export type TaskView = Pick<
  Task,
  'id' | 'state' | 'holder' | 'epoch' | 'attempts' | 'maxAttempts' | 'retry' | 'leaseExpiresAt' | 'readyAt'
> & {
  memos: Record<string, unknown>
}

interface ModelTask {
  state: TaskState
  holder: string | null
  epoch: number
  attempts: number
  max: number
  retry: 'safe' | 'manual'
  lease: number | null
  readyAt: number | null
  memos: Record<string, unknown>
}

export const MEMO_KEY = 'decision'

function writeOf(call: Call): string {
  const { t, id, epoch, lease, retry } = call
  switch (t) {
    case 'claim':
      return JSON.stringify({ t, lease })
    case 'renew':
      return JSON.stringify({ t, id, epoch, lease })
    case 'fail':
      return JSON.stringify({ t, id, epoch, retry })
    case 'complete':
    case 'release':
    case 'block':
      return JSON.stringify({ t, id, epoch })
    case 'unblock':
    case 'requeue':
    case 'cancel':
      return JSON.stringify({ t, id })
    case 'log':
      return JSON.stringify({ t, id, text: call.text })
    case 'memo':
      return JSON.stringify({ t, id, value: call.value })
  }
}

function changeOneField(call: Call, leaseUnit: number): Call {
  switch (call.t) {
    case 'claim':
    case 'renew':
      return { ...call, lease: call.lease + leaseUnit }
    case 'fail':
      return { ...call, retry: !call.retry }
    case 'complete':
    case 'release':
    case 'block':
      return { ...call, epoch: call.epoch + 1 }
    case 'unblock':
      return { ...call, t: 'requeue' }
    case 'requeue':
      return { ...call, t: 'cancel' }
    case 'cancel':
      return { ...call, t: 'unblock' }
    case 'log':
      return { ...call, text: `${call.text} again` }
    case 'memo':
      return { ...call, value: { different: true } }
  }
}

export class Model {
  readonly tasks: ModelTask[] = []
  #ids = new Map<number, number>()
  #requests = new Map<string, { write: string; outcome: Outcome }>()
  #remembered = new Map<string, number>()
  #last: Call | undefined

  call(step: Exclude<Op, { t: 'ingest' | 'advance' }>, index: number, leaseUnit = 1): Call {
    const fallback: Call = {
      t: 'claim',
      actor: AGENTS[0],
      id: 0,
      epoch: 1,
      lease: 10_000,
      retry: false,
      requestId: `r${index}`,
      text: `note ${index}`,
      value: null,
    }
    if (step.t === 'repeat') return this.#last ?? fallback
    if (step.t === 'reuse') return this.#last ? changeOneField(this.#last, leaseUnit) : fallback
    if (step.t === 'other-actor') {
      if (!this.#last) return fallback
      return { ...this.#last, actor: AGENTS[(AGENTS.indexOf(this.#last.actor) + 1) % AGENTS.length] }
    }
    const actor = AGENTS['agent' in step ? step.agent : 0]
    const id = 'key' in step ? (this.#ids.get(step.key) ?? 99) : 0
    const remembered = this.#remembered.get(`${actor}/${id}`) ?? 1
    return (this.#last = {
      ...fallback,
      t: step.t,
      actor,
      id,
      epoch: 'stale' in step && step.stale ? (remembered > 1 ? remembered - 1 : 2) : remembered,
      lease: 'lease' in step ? Math.floor(step.lease / leaseUnit) * leaseUnit : 30_000,
      retry: 'retry' in step && step.retry,
      value: 'value' in step ? step.value : null,
    })
  }

  ingest(key: number, retry: 'safe' | 'manual', max: number, now: number): boolean {
    if (this.#ids.has(key)) return false
    this.tasks.push({
      state: 'ready',
      holder: null,
      epoch: 0,
      attempts: 0,
      max,
      retry,
      lease: null,
      readyAt: now,
      memos: {},
    })
    this.#ids.set(key, this.tasks.length)
    return true
  }

  expire(now: number): void {
    for (const task of this.tasks) {
      if (task.state !== 'claimed' || task.lease! > now) continue
      task.state = task.retry === 'manual' ? 'interrupted' : task.attempts >= task.max ? 'failed' : 'ready'
      task.holder = null
      task.lease = null
      task.readyAt = task.state === 'ready' ? now : null
    }
  }

  apply(call: Call, now: number): { outcome: Outcome; changed: boolean } {
    this.expire(now)
    const write = writeOf(call)
    const request = `${call.actor}/${call.requestId}`
    const prior = this.#requests.get(request)
    if (prior && prior.write !== write) return { outcome: { code: 'request_id_reused' }, changed: false }
    const task = this.tasks[call.id - 1]
    const memoExists = call.t === 'memo' && task !== undefined && Object.hasOwn(task.memos, MEMO_KEY)
    const outcome = prior?.outcome ?? this.#apply(call, now)
    const result = 'ok' in outcome ? outcome.ok : null
    if (result === null) return { outcome, changed: false }
    if (call.t === 'claim' && 'id' in result) this.#remembered.set(`${call.actor}/${result.id}`, result.epoch)
    if (!prior) this.#requests.set(request, { write, outcome })
    return { outcome, changed: !prior && !memoExists }
  }

  #apply(call: Call, now: number): Outcome {
    if (call.t === 'claim') {
      let index = -1
      this.tasks.forEach((task, i) => {
        if (task.state === 'ready' && (index < 0 || task.readyAt! < this.tasks[index]!.readyAt!)) index = i
      })
      if (index < 0) return { ok: null }
      const task = this.tasks[index]!
      Object.assign(task, {
        state: 'claimed',
        holder: call.actor,
        epoch: task.epoch + 1,
        attempts: task.attempts + 1,
        lease: now + call.lease,
        readyAt: null,
      })
      return this.#ok(index + 1)
    }
    const task = this.tasks[call.id - 1]
    if (!task) return { code: 'not_found' }
    const end = (state: TaskState, attempts = task.attempts) =>
      Object.assign(task, { state, attempts, holder: null, lease: null, readyAt: state === 'ready' ? now : null })
    switch (call.t) {
      case 'unblock':
        if (task.state !== 'blocked') return { code: 'invalid_transition' }
        end('ready')
        return this.#ok(call.id)
      case 'requeue':
        if (task.state !== 'interrupted') return { code: 'invalid_transition' }
        end('ready', task.attempts - 1)
        return this.#ok(call.id)
      case 'cancel':
        if (TERMINAL.has(task.state)) return { code: 'invalid_transition' }
        end('canceled')
        return this.#ok(call.id)
      case 'log':
        return { ok: { text: call.text } }
      case 'memo':
        if (!Object.hasOwn(task.memos, MEMO_KEY)) task.memos[MEMO_KEY] = call.value
        return { ok: { value: task.memos[MEMO_KEY] } }
    }
    if (task.state !== 'claimed' || task.holder !== call.actor || task.epoch !== call.epoch)
      return { code: 'claim_lost' }
    switch (call.t) {
      case 'renew':
        task.lease = now + call.lease
        break
      case 'complete':
        end('completed')
        break
      case 'fail':
        end(call.retry && task.attempts < task.max ? 'ready' : 'failed')
        break
      case 'release':
        end('ready', task.attempts - 1)
        break
      case 'block':
        end('blocked', task.attempts - 1)
        break
    }
    return this.#ok(call.id)
  }

  #ok(id: number): Outcome {
    const task = this.tasks[id - 1]!
    return { ok: { id, state: task.state, epoch: task.epoch } }
  }
}

function replay(events: TaskEvent[]): Map<number, Omit<ModelTask, 'max' | 'retry'>> {
  const tasks = new Map<number, Omit<ModelTask, 'max' | 'retry'>>()
  for (const event of events) {
    const data = event.data as { epoch?: number; leaseExpiresAt?: number | null; key?: string; value?: unknown }
    const task = tasks.get(event.taskId) ?? {
      state: 'ready',
      holder: null,
      epoch: 0,
      attempts: 0,
      lease: null,
      readyAt: event.at,
      memos: {},
    }
    switch (event.type) {
      case 'task.created':
        break
      case 'task.claimed':
        Object.assign(task, {
          state: 'claimed',
          holder: event.actor,
          epoch: data.epoch,
          lease: data.leaseExpiresAt,
          attempts: task.attempts + 1,
          readyAt: null,
        })
        break
      case 'task.renewed':
        task.lease = data.leaseExpiresAt ?? null
        break
      case 'task.logged':
        break
      case 'task.memo':
        assert.equal(Object.hasOwn(task.memos, data.key!), false, 'a memo appends only its first write')
        task.memos[data.key!] = data.value
        break
      default:
        Object.assign(task, {
          state: event.to,
          holder: null,
          lease: null,
          readyAt: event.to === 'ready' ? event.at : null,
        })
        if (event.type === 'task.released' || event.type === 'task.blocked' || event.type === 'task.requeued')
          task.attempts -= 1
    }
    tasks.set(event.taskId, task)
  }
  return tasks
}

export function checkInvariants(model: Model, now: number, tasks: TaskView[], events: TaskEvent[]): void {
  model.expire(now)
  assert.deepEqual(
    events.map((e) => e.seq),
    events.map((_, i) => i + 1),
    'event sequence numbers have no gaps',
  )
  const replayedTasks = replay(events)
  assert.equal(tasks.length, model.tasks.length, 'one task per dedupe key')
  for (const task of tasks) {
    const { max, retry, ...expected } = model.tasks[task.id - 1]!
    const observed = {
      state: task.state,
      holder: task.holder,
      epoch: task.epoch,
      attempts: task.attempts,
      lease: task.leaseExpiresAt,
      readyAt: task.readyAt,
      memos: task.memos,
    }
    assert.deepEqual(observed, expected, `task ${task.id} matches the model`)
    assert.deepEqual([task.maxAttempts, task.retry], [max, retry])
    assert.ok(task.attempts >= 0 && task.attempts <= task.maxAttempts, 'attempts stay within bounds')
    assert.equal(
      task.state === 'claimed',
      task.holder !== null && task.leaseExpiresAt! > now,
      'only a live claim has a holder',
    )

    const own = events.filter((e) => e.taskId === task.id)
    assert.equal(own[0]?.from, null)
    for (let i = 1; i < own.length; i++)
      assert.equal(own[i]!.from, own[i - 1]!.to, `task ${task.id}'s events form a chain`)
    assert.equal(own.at(-1)!.to, task.state, `task ${task.id}'s last event matches its state`)
    const replayed = replayedTasks.get(task.id)
    assert.deepEqual(replayed, observed, `replaying task ${task.id}'s events reproduces its state`)
    const finished = own.findIndex((e) => TERMINAL.has(e.to))
    if (finished >= 0)
      assert.ok(
        own.slice(finished + 1).every((e) => e.from === e.to),
        'a finished task never changes state',
      )
  }
}
