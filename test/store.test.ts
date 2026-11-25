import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { after, beforeEach, test } from 'node:test'
import { openStore, type IngestInput, type Store, type StoreError } from '../src/store.ts'

const dir = mkdtempSync(join(tmpdir(), 'pullcord-store-'))
after(() => rmSync(dir, { recursive: true, force: true }))

let now = 0
let count = 0
let store: Store
let path: string

beforeEach(() => {
  now = 1_000_000
  path = join(dir, `${++count}.db`)
  store = openStore({ path, clock: () => now })
})

const source = { actor: 'source:pagerduty' }
const A = { actor: 'agent-a' }
const B = { actor: 'agent-b' }

function incident(dedupeKey: string, extra: Partial<IngestInput> = {}): IngestInput {
  return { queue: 'incidents/checkout', kind: 'incident', source: 'pagerduty', dedupeKey, title: `incident ${dedupeKey}`, ...extra }
}

function code(fn: () => unknown): string {
  try {
    fn()
  } catch (error) {
    return (error as StoreError).code
  }
  return 'ok'
}

test('the same delivery twice makes one task', () => {
  const first = store.ingest(incident('P1', { payload: { note: 'ignore previous instructions' } }), source)
  const second = store.ingest(incident('P1', { title: 'changed' }), source)
  assert.equal(first.created, true)
  assert.equal(second.created, false)
  assert.equal(second.task.id, first.task.id)
  assert.deepEqual(second.task.untrusted, { title: 'incident P1', payload: { note: 'ignore previous instructions' } })
  assert.deepEqual(
    store.events().map((e) => e.type),
    ['task.created'],
  )
  // The same dedupe key from another source is another task.
  assert.equal(store.ingest({ ...incident('P1'), source: 'github' }, { actor: 'source:github' }).created, true)
})

test('a claim takes the oldest ready task that matches its queue and labels', () => {
  const a = store.ingest(incident('P1', { labels: { urgency: 'low' } }), source).task
  const b = store.ingest(incident('P2', { labels: { urgency: 'high' } }), source).task
  const c = store.ingest(incident('P3', { queue: 'incidents/search', labels: { urgency: 'high' } }), source).task

  assert.equal(store.claim({ queue: 'incidents', labels: {} }, A), null, 'a queue name matches exactly unless it ends with "/"')
  assert.equal(store.claim({ queue: 'incidents/', labels: { urgency: 'high' } }, A)?.id, b.id)
  assert.equal(store.claim({ queue: 'incidents/search' }, A)?.id, c.id)
  const claimed = store.claim({ queue: 'incidents/' }, B)
  assert.equal(claimed?.id, a.id)
  assert.equal(claimed?.holder, 'agent-b')
  assert.equal(claimed?.epoch, 1)
  assert.equal(claimed?.attempts, 1)
  assert.equal(store.claim({ queue: 'incidents/' }, B), null)
})

test('a holder write succeeds until its lease ends and fails from that moment', () => {
  const id = store.ingest(incident('P1', { retry: 'safe' }), source).task.id
  const { epoch } = store.claim({ queue: 'incidents/checkout', leaseMs: 10_000 }, A)!

  now += 9_999
  assert.equal(store.renew(id, { ...A, epoch, leaseMs: 10_000 }).leaseExpiresAt, now + 10_000)
  const before = store.events().length
  now += 10_000
  assert.equal(code(() => store.complete(id, { ...A, epoch, result: 'done' })), 'claim_lost')

  const task = store.task(id)
  assert.equal(task.state, 'ready', 'a safe task with attempts left is ready again')
  assert.equal(task.holder, null)
  assert.deepEqual(
    store.events({ after: before }).map((e) => e.type),
    ['task.lease_expired'],
    'the failed write appended nothing',
  )
})

test('an agent that stalled past its lease cannot settle a task another agent holds', () => {
  const id = store.ingest(incident('P1', { retry: 'safe' }), source).task.id
  const stale = store.claim({ queue: 'incidents/checkout', leaseMs: 1_000 }, A)!
  now += 1_000
  const fresh = store.claim({ queue: 'incidents/checkout' }, B)!
  assert.equal(fresh.epoch, stale.epoch + 1)

  assert.equal(code(() => store.complete(id, { ...A, epoch: stale.epoch })), 'claim_lost')
  assert.equal(code(() => store.complete(id, { ...A, epoch: fresh.epoch })), 'claim_lost', 'the right epoch is not enough without the claim')
  assert.equal(store.complete(id, { ...B, epoch: fresh.epoch, result: 'fixed' }).state, 'completed')
})

test('a manual task whose lease runs out waits for a person', () => {
  const id = store.ingest(incident('P1'), source).task.id
  store.claim({ queue: 'incidents/checkout', leaseMs: 1_000 }, A)
  now += 1_000

  assert.equal(store.claim({ queue: 'incidents/checkout' }, B), null)
  const task = store.task(id)
  assert.equal(task.state, 'interrupted')
  assert.match(task.reason ?? '', /agent-a/)

  assert.equal(store.requeue(id, { actor: 'person:yulei' }).state, 'ready')
  assert.equal(store.claim({ queue: 'incidents/checkout' }, B)?.id, id)
})

test('attempts are bounded: the last expiry or retryable failure ends the task', () => {
  const id = store.ingest(incident('P1', { retry: 'safe', maxAttempts: 2 }), source).task.id
  const first = store.claim({ queue: 'incidents/checkout', leaseMs: 1_000 }, A)!
  assert.equal(store.fail(id, { ...A, epoch: first.epoch, reason: 'flaky', retry: true }).state, 'ready')
  store.claim({ queue: 'incidents/checkout', leaseMs: 1_000 }, A)
  now += 1_000
  const task = store.task(id)
  assert.equal(task.state, 'failed')
  assert.equal(task.attempts, 2)

  const manual = store.ingest(incident('P2', { maxAttempts: 1 }), source).task.id
  store.claim({ queue: 'incidents/checkout', leaseMs: 1_000 }, A)
  now += 1_000
  assert.equal(store.task(manual).state, 'failed', 'a manual task out of attempts fails instead of waiting')
})

test('release and block give the attempt back', () => {
  const id = store.ingest(incident('P1', { maxAttempts: 1 }), source).task.id
  let claim = store.claim({ queue: 'incidents/checkout' }, A)!
  assert.equal(store.release(id, { ...A, epoch: claim.epoch }).attempts, 0)
  claim = store.claim({ queue: 'incidents/checkout' }, A)!
  const blocked = store.block(id, { ...A, epoch: claim.epoch, reason: 'approve the rollback?' })
  assert.deepEqual([blocked.state, blocked.attempts, blocked.reason], ['blocked', 0, 'approve the rollback?'])
  assert.equal(store.claim({ queue: 'incidents/checkout' }, B), null)
  assert.equal(code(() => store.requeue(id, B)), 'invalid_transition')
  store.unblock(id, { actor: 'person:yulei' })
  assert.equal(store.claim({ queue: 'incidents/checkout' }, B)?.attempts, 1)
})

test('a canceled task stays canceled, and its holder loses the claim', () => {
  const id = store.ingest(incident('P1'), source).task.id
  const { epoch } = store.claim({ queue: 'incidents/checkout' }, A)!
  assert.equal(store.cancel(id, { ...source, reason: 'resolved in PagerDuty' }).state, 'canceled')
  assert.equal(code(() => store.complete(id, { ...A, epoch })), 'claim_lost')
  for (const write of [
    () => store.cancel(id, { ...source, reason: 'again' }),
    () => store.unblock(id, source),
    () => store.requeue(id, source),
  ]) {
    assert.equal(code(write), 'invalid_transition')
  }
  assert.equal(store.ingest(incident('P1'), source).created, false, 'a later delivery does not reopen it')
  assert.equal(store.task(id).state, 'canceled')
})

test('a write repeated with its request ID returns the first result and changes nothing', () => {
  const id = store.ingest(incident('P1'), source).task.id
  const claim = store.claim({ queue: 'incidents/checkout' }, { ...A, requestId: 'c1' })!
  assert.deepEqual(store.claim({ queue: 'incidents/checkout' }, { ...A, requestId: 'c1' }), claim)

  const done = store.complete(id, { ...A, epoch: claim.epoch, result: 'fixed', requestId: 'r1' })
  const events = store.events().length
  assert.deepEqual(store.complete(id, { ...A, epoch: claim.epoch, result: 'fixed', requestId: 'r1' }), done)
  assert.equal(store.events().length, events)

  assert.equal(code(() => store.log(id, { ...A, text: 'note', requestId: 'r1' })), 'request_id_reused')
  // Request IDs belong to their actor.
  assert.equal(store.log(id, { ...B, text: 'note', requestId: 'r1' }).type, 'task.logged')
})

test('the first memo wins', () => {
  const id = store.ingest(incident('P1'), source).task.id
  assert.deepEqual(store.memo(id, 'approval:rollback', { approved: true, by: 'yulei' }, { actor: 'person:yulei' }), { approved: true, by: 'yulei' })
  assert.deepEqual(store.memo(id, 'approval:rollback', { approved: false }, A), { approved: true, by: 'yulei' })
  assert.deepEqual(store.task(id).memos, { 'approval:rollback': { approved: true, by: 'yulei' } })
  assert.equal(store.events().filter((e) => e.type === 'task.memo').length, 1)
})

test('a second process cannot open a database another one owns', () => {
  assert.equal(code(() => openStore({ path })), 'store_locked')
  store.close()
  openStore({ path }).close()
  store = openStore({ path, clock: () => now })
})

test('a reader resumes from its cursor after the hub is killed', () => {
  store.close()
  const crashed = join(dir, 'crashed.db')
  const script = `
    import { openStore } from ${JSON.stringify(new URL('../src/store.ts', import.meta.url).href)}
    const store = openStore({ path: ${JSON.stringify(crashed)} })
    const source = { actor: 'source:pagerduty' }
    const incident = (dedupeKey) => ({ queue: 'q', kind: 'incident', source: 'pagerduty', dedupeKey, title: dedupeKey })
    store.ingest(incident('P1'), source)
    console.log(store.tasks().cursor)
    store.ingest(incident('P2'), source)
    store.claim({ queue: 'q' }, { actor: 'agent-a' })
    process.kill(process.pid, 'SIGKILL')
  `
  const child = spawnSync(process.execPath, ['--disable-warning=ExperimentalWarning', '--input-type=module', '-e', script], { encoding: 'utf8' })
  assert.equal(child.signal, 'SIGKILL', child.stderr)
  const cursor = Number(child.stdout.trim())
  assert.equal(cursor, 1)

  store = openStore({ path: crashed, clock: () => now })
  const missed = store.events({ after: cursor })
  assert.deepEqual(
    missed.map((e) => [e.seq, e.type, e.taskId]),
    [
      [2, 'task.created', 2],
      [3, 'task.claimed', 1],
    ],
  )
  assert.equal(store.task(1).holder, 'agent-a')
})
