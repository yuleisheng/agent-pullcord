import assert from 'node:assert/strict'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { after, test } from 'node:test'
import fc from 'fast-check'
import { openStore, StoreError, type Store, type Task, type TaskEvent, type TaskState } from '../src/store.ts'

// Random sequences of operations from three agents, run against the store and against a
// model written from the lifecycle table in docs/PRD.md. After every step the two must
// agree, and the invariants must hold. A failure prints a seed and a shrunk sequence:
// keep it as a regression case.

const dir = mkdtempSync(join(tmpdir(), 'pullcord-lifecycle-'))
after(() => rmSync(dir, { recursive: true, force: true }))

const AGENTS = ['agent-a', 'agent-b', 'agent-c']
const KEYS = 4
const TERMINAL = new Set<TaskState>(['completed', 'failed', 'canceled'])

type Op =
  | { t: 'ingest'; key: number; retry: 'safe' | 'manual'; max: number }
  | { t: 'claim'; agent: number; lease: number }
  | { t: 'renew' | 'complete' | 'release' | 'block'; agent: number; key: number; stale: boolean }
  | { t: 'fail'; agent: number; key: number; stale: boolean; retry: boolean }
  | { t: 'unblock' | 'requeue' | 'cancel'; key: number }
  | { t: 'advance'; ms: number }
  | { t: 'repeat' }

const agent = fc.integer({ min: 0, max: AGENTS.length - 1 })
const key = fc.integer({ min: 0, max: KEYS - 1 })
const held = (t: 'renew' | 'complete' | 'release' | 'block') => fc.record({ t: fc.constant(t), agent, key, stale: fc.boolean() })
const op: fc.Arbitrary<Op> = fc.oneof(
  fc.record({ t: fc.constant('ingest' as const), key, retry: fc.constantFrom('safe' as const, 'manual' as const), max: fc.integer({ min: 1, max: 3 }) }),
  { arbitrary: fc.record({ t: fc.constant('claim' as const), agent, lease: fc.integer({ min: 1_000, max: 5_000 }) }), weight: 3 },
  held('renew'),
  held('complete'),
  held('release'),
  held('block'),
  fc.record({ t: fc.constant('fail' as const), agent, key, stale: fc.boolean(), retry: fc.boolean() }),
  fc.record({ t: fc.constantFrom('unblock' as const, 'requeue' as const, 'cancel' as const), key }),
  { arbitrary: fc.record({ t: fc.constant('advance' as const), ms: fc.integer({ min: 0, max: 6_000 }) }), weight: 2 },
  fc.constant({ t: 'repeat' as const }),
)

/** A write as sent: everything needed to send it again. */
interface Call {
  t: Exclude<Op['t'], 'ingest' | 'advance' | 'repeat'>
  actor: string
  id: number
  epoch: number
  lease: number
  retry: boolean
  requestId: string
}

type Outcome = { ok: { id: number; state: TaskState; epoch: number } | null } | { code: string }

interface ModelTask {
  state: TaskState
  holder: string | null
  epoch: number
  attempts: number
  max: number
  retry: 'safe' | 'manual'
  lease: number | null
}

class Model {
  tasks: ModelTask[] = []
  ids = new Map<number, number>()
  requests = new Map<string, Outcome>()

  ingest(key: number, retry: 'safe' | 'manual', max: number): void {
    if (this.ids.has(key)) return
    this.tasks.push({ state: 'ready', holder: null, epoch: 0, attempts: 0, max, retry, lease: null })
    this.ids.set(key, this.tasks.length)
  }

  expire(now: number): void {
    for (const task of this.tasks) {
      if (task.state !== 'claimed' || task.lease! > now) continue
      task.state = task.attempts >= task.max ? 'failed' : task.retry === 'safe' ? 'ready' : 'interrupted'
      task.holder = null
      task.lease = null
    }
  }

  apply(call: Call, now: number): Outcome {
    this.expire(now)
    const prior = this.requests.get(call.requestId)
    if (prior) return prior
    const outcome = this.#apply(call, now)
    if ('ok' in outcome && outcome.ok !== null) this.requests.set(call.requestId, outcome)
    return outcome
  }

  #apply(call: Call, now: number): Outcome {
    if (call.t === 'claim') {
      const index = this.tasks.findIndex((task) => task.state === 'ready')
      if (index < 0) return { ok: null }
      const task = this.tasks[index]!
      Object.assign(task, { state: 'claimed', holder: call.actor, epoch: task.epoch + 1, attempts: task.attempts + 1, lease: now + call.lease })
      return this.#ok(index + 1)
    }
    const task = this.tasks[call.id - 1]
    if (!task) return { code: 'not_found' }
    const end = (state: TaskState, attempts = task.attempts) => Object.assign(task, { state, attempts, holder: null, lease: null })
    switch (call.t) {
      case 'unblock':
      case 'requeue':
        if (task.state !== (call.t === 'unblock' ? 'blocked' : 'interrupted')) return { code: 'invalid_transition' }
        task.state = 'ready'
        return this.#ok(call.id)
      case 'cancel':
        if (TERMINAL.has(task.state)) return { code: 'invalid_transition' }
        end('canceled')
        return this.#ok(call.id)
    }
    if (task.state !== 'claimed' || task.holder !== call.actor || task.epoch !== call.epoch) return { code: 'claim_lost' }
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

function send(store: Store, call: Call): Outcome {
  const who = { actor: call.actor, requestId: call.requestId }
  const holder = { ...who, epoch: call.epoch }
  try {
    const task: Task | null = (() => {
      switch (call.t) {
        case 'claim':
          return store.claim({ queue: 'q', leaseMs: call.lease }, who)
        case 'renew':
          return store.renew(call.id, { ...holder, leaseMs: call.lease })
        case 'complete':
          return store.complete(call.id, { ...holder, result: 'done' })
        case 'fail':
          return store.fail(call.id, { ...holder, reason: 'broke', retry: call.retry })
        case 'release':
          return store.release(call.id, holder)
        case 'block':
          return store.block(call.id, { ...holder, reason: 'approve?' })
        case 'unblock':
          return store.unblock(call.id, who)
        case 'requeue':
          return store.requeue(call.id, who)
        case 'cancel':
          return store.cancel(call.id, { ...who, reason: 'resolved' })
      }
    })()
    return { ok: task && { id: task.id, state: task.state, epoch: task.epoch } }
  } catch (error) {
    if (error instanceof StoreError) return { code: error.code }
    throw error
  }
}

function checkInvariants(store: Store, model: Model, now: number): void {
  model.expire(now) // reads record expired leases too
  const events: TaskEvent[] = []
  for (let page = store.events({ limit: 1000 }); page.length > 0; page = store.events({ after: page.at(-1)!.seq, limit: 1000 })) {
    events.push(...page)
  }
  assert.deepEqual(
    events.map((e) => e.seq),
    events.map((_, i) => i + 1),
    'event sequence numbers have no gaps',
  )
  const { tasks } = store.tasks()
  assert.equal(tasks.length, model.tasks.length, 'one task per dedupe key')
  for (const task of tasks) {
    const expected = model.tasks[task.id - 1]!
    assert.deepEqual(
      { state: task.state, holder: task.holder, epoch: task.epoch, attempts: task.attempts, lease: task.leaseExpiresAt },
      { state: expected.state, holder: expected.holder, epoch: expected.epoch, attempts: expected.attempts, lease: expected.lease },
      `task ${task.id} matches the model`,
    )
    assert.ok(task.attempts >= 0 && task.attempts <= task.maxAttempts, 'attempts stay within bounds')
    assert.equal(task.state === 'claimed', task.holder !== null && task.leaseExpiresAt! > now, 'only a live claim has a holder')

    const own = events.filter((e) => e.taskId === task.id)
    assert.equal(own[0]?.from, null)
    for (let i = 1; i < own.length; i++) assert.equal(own[i]!.from, own[i - 1]!.to, `task ${task.id}'s events form a chain`)
    assert.equal(own.at(-1)!.to, task.state, `task ${task.id}'s last event matches its state`)
    const finished = own.findIndex((e) => TERMINAL.has(e.to))
    if (finished >= 0) assert.ok(own.slice(finished + 1).every((e) => e.from === e.to), 'a finished task never changes state')
  }
}

test('generated sequences keep the store equal to the lifecycle model', () => {
  let run = 0
  fc.assert(
    fc.property(fc.array(op, { maxLength: 60 }), (ops) => {
      let now = 0
      const store = openStore({ path: join(dir, `${run++}.db`), clock: () => now })
      const model = new Model()
      const remembered = AGENTS.map(() => new Map<number, number>())
      let last: { call: Call; outcome: Outcome } | undefined
      try {
        ops.forEach((step, i) => {
          if (step.t === 'advance') {
            now += step.ms
          } else if (step.t === 'ingest') {
            store.ingest({ queue: 'q', kind: 'k', source: 's', dedupeKey: `k${step.key}`, title: 't', retry: step.retry, maxAttempts: step.max }, { actor: 'source:s' })
            model.ingest(step.key, step.retry, step.max)
          } else {
            const call: Call =
              step.t === 'repeat'
                ? last?.call ?? { t: 'claim', actor: AGENTS[0]!, id: 0, epoch: 0, lease: 1_000, retry: false, requestId: `r${i}` }
                : (() => {
                    const actor = AGENTS['agent' in step ? step.agent : 0]!
                    const id = 'key' in step ? (model.ids.get(step.key) ?? 99) : 0
                    const epoch = (remembered[AGENTS.indexOf(actor)]!.get(id) ?? 0) - ('stale' in step && step.stale ? 1 : 0)
                    const lease = 'lease' in step ? step.lease : 3_000
                    return { t: step.t, actor, id, epoch, lease, retry: 'retry' in step && step.retry, requestId: `r${i}` }
                  })()
            const before = store.events({ limit: 1000 }).length
            const outcome = send(store, call)
            assert.deepEqual(outcome, model.apply(call, now), `step ${i}: ${JSON.stringify(call)}`)
            if ('code' in outcome) {
              const added = store.events({ after: before, limit: 1000 })
              assert.ok(added.every((e) => e.type === 'task.lease_expired'), 'a failed write appends no event')
            }
            if ('ok' in outcome && outcome.ok && call.t === 'claim') remembered[AGENTS.indexOf(call.actor)]!.set(outcome.ok.id, outcome.ok.epoch)
            if (step.t !== 'repeat') last = { call, outcome }
          }
          checkInvariants(store, model, now)
        })
      } finally {
        store.close()
      }
    }),
    { numRuns: 200 },
  )
})
