import assert from 'node:assert/strict'
import { spawn, spawnSync } from 'node:child_process'
import { createHash } from 'node:crypto'
import { once } from 'node:events'
import { existsSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createInterface } from 'node:readline'
import { after, test, type TestContext } from 'node:test'
import { fileURLToPath } from 'node:url'
import { createHttpServer } from '../src/serve.ts'
import { openStore, type Retry } from '../src/store.ts'

const dir = mkdtempSync(join(tmpdir(), 'pullcord-http-'))
const executable = fileURLToPath(new URL('../src/serve.ts', import.meta.url))
const reader = 'a1'.repeat(32)
const blind = 'b2'.repeat(32)
const exact = 'd4'.repeat(32)
const allReader = 'e5'.repeat(32)
const watcher = 'f6'.repeat(32)
const unknownKey = 'c3'.repeat(32)
const now = 1_234_000
const source = { actor: 'source:fixture' }

after(() => rmSync(dir, { recursive: true, force: true }))

function digest(key: string): string {
  return createHash('sha256').update(key).digest('hex')
}

function credential(agent: string, key: string, queues: string[], can: string[]) {
  return { agent, sha256: digest(key), queues, can }
}

function fixture(name: string) {
  const db = join(dir, `${name}.db`)
  const keys = join(dir, `${name}.json`)
  const credentials = [
    credential('reader', reader, ['incidents/'], ['read', 'events']),
    credential('blind', blind, ['*'], ['events']),
    credential('exact', exact, ['incidents/checkout'], ['read']),
    credential('all-reader', allReader, ['*'], ['read']),
    credential('watcher', watcher, ['*'], ['read', 'events']),
  ]
  writeFileSync(keys, JSON.stringify(credentials))
  const store = openStore({ path: db, clock: () => now })
  const payload = { original_key: 'outside data' }
  const ingest = (queue: string, dedupeKey: string, labels: Record<string, string>) =>
    store.ingest({ queue, dedupeKey, labels, kind: 'incident', source: 'fake', title: 'Check checkout', payload }, source).task.id
  const checkout = ingest('incidents/checkout', 'one', { team: 'payments', signal: 'x=y' })
  const search = ingest('incidents/search', 'two', { team: 'payments', signal: 'other' })
  const sibling = ingest('incidents-private/checkout', 'three', { team: 'payments', signal: 'x=y' })
  const nested = ingest('incidents/checkout/backend', 'four', { team: 'payments', signal: 'x=y' })
  store.cancel(nested, { actor: 'source:fixture', reason: 'resolved' })
  store.memo(checkout, '__proto__', { approved: true }, { actor: 'agent:test' })
  store.close()
  return { db, keys, credentials, checkout, search, sibling, nested, payload }
}

function args(db: string, keys: string): string[] {
  return ['--disable-warning=ExperimentalWarning', executable, '--db', db, '--keys', keys, '--listen', '127.0.0.1:0']
}

function client(origin: string) {
  function request(path: string, key: string | undefined, init: RequestInit = {}) {
    const headers = new Headers(init.headers)
    if (key !== undefined) headers.set('Authorization', `Bearer ${key}`)
    return fetch(origin + path, { ...init, headers, signal: init.signal ?? AbortSignal.timeout(5_000) })
  }
  return {
    request,
    get: (path: string, key?: string) => request(path, key),
    post: (path: string, key: string, body: unknown, signal?: AbortSignal) => request(path, key, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body), signal,
    }),
  }
}

async function start(db: string, keys: string, clock?: number) {
  const flags = clock === undefined ? [] : ['--import', `data:text/javascript,Date.now=()=>${clock}`]
  const child = spawn(process.execPath, [...flags, ...args(db, keys)], { stdio: ['ignore', 'pipe', 'pipe'] })
  const done = once(child, 'close')
  let stderr = ''
  child.stderr.setEncoding('utf8').on('data', (chunk) => { stderr += chunk })
  const lines = createInterface({ input: child.stdout })
  for await (const line of lines) {
    if (line.startsWith('Listening on ')) {
      const origin = line.slice('Listening on '.length)
      return {
        ...client(origin),
        stop: async (signal: NodeJS.Signals = 'SIGTERM') => { child.kill(signal); return (await done)[1] },
      }
    }
  }
  await done
  assert.fail(`serve did not start: ${stderr}`)
}

async function error(response: Response, status: number, code: string) {
  assert.equal(response.status, status)
  const body = await response.json()
  assert.equal(body.code, code)
  assert.ok(body.message)
}

async function ok(response: Response) {
  const body = await response.json()
  assert.equal(response.status, 200, JSON.stringify(body))
  return body
}

test('HTTP task reads combine their filters without exposing other queues or trusting source fields', { timeout: 10_000 }, async (t) => {
  const { db, keys, checkout, search, sibling, nested, payload } = fixture('read')
  const server = await start(db, keys)
  t.after(async () => { await server.stop() })

  const visible = await (await server.get('/tasks', reader)).json()
  assert.deepEqual(visible.tasks.map(({ id }: { id: number }) => id), [checkout, search, nested])
  assert.equal(Object.hasOwn(visible, 'cursor'), false)

  const all = await (await server.get('/tasks', allReader)).json()
  assert.deepEqual(all.tasks.map(({ id }: { id: number }) => id), [checkout, search, sibling, nested])
  assert.equal(Object.hasOwn(all, 'cursor'), false)
  assert.deepEqual(await (await server.get('/tasks', watcher)).json(), { ...all, cursor: 6 })

  const response = await server.get('/tasks?queue=incidents%2F&state=ready&label=team%3Dpayments&label=signal%3Dx%3Dy', reader)
  assert.equal(response.status, 200)
  assert.equal(response.headers.get('Cache-Control'), 'no-store')
  const filtered = await response.json()
  assert.equal(Object.hasOwn(filtered, 'cursor'), false)
  const { tasks } = filtered
  assert.deepEqual(tasks.map(({ id }: { id: number }) => id), [checkout])

  const detail = await (await server.get(`/tasks/${checkout}`, reader)).json()
  assert.equal(detail.dedupe_key, 'one')
  assert.equal(detail.ready_at, now)
  assert.deepEqual(detail.untrusted, { title: 'Check checkout', payload })
  assert.deepEqual(detail.memos, { ['__proto__']: { approved: true } })
  assert.equal(Object.hasOwn(detail, 'title'), false)
  assert.equal(Object.hasOwn(detail, 'payload'), false)
  assert.equal(Object.hasOwn(tasks[0], 'memos'), false)
  await error(await server.get('/tasks?state=waiting', reader), 400, 'invalid_argument')
  await error(await server.get('/tasks?label=payments', reader), 400, 'invalid_argument')
})

test('an HTTP read at the lease deadline returns post-expiry tasks and cursor', { timeout: 10_000 }, async (t) => {
  const db = join(dir, 'expiry.db')
  const keys = join(dir, 'expiry.json')
  writeFileSync(keys, JSON.stringify([credential('watcher', watcher, ['*'], ['read', 'events'])]))
  const seed = openStore({ path: db, clock: () => now })
  const cases = [
    { retry: 'safe', maxAttempts: 3, expected: 'ready' },
    { retry: 'safe', maxAttempts: 1, expected: 'failed' },
    { retry: 'manual', maxAttempts: 1, expected: 'interrupted' },
  ] as const
  for (const { retry, maxAttempts, expected } of cases) {
    const queue = `incidents/${expected}`
    seed.ingest({ queue, kind: 'incident', source: 'fake', dedupeKey: expected, title: expected, retry, maxAttempts }, source)
    assert.equal(seed.claim({ queue, leaseMs: 10_000 }, { actor: 'agent:holder' })?.state, 'claimed')
  }
  seed.close()

  const deadline = now + 10_000
  const before = await start(db, keys, deadline - 1)
  t.after(() => before.stop())
  const claimed = await before.get('/tasks', watcher)
  assert.equal(claimed.status, 200)
  const snapshot = await claimed.json()
  assert.equal(snapshot.cursor, 6)
  assert.deepEqual(snapshot.tasks.map(({ state, lease_expires_at }: { state: string; lease_expires_at: number }) => [state, lease_expires_at]),
    [['claimed', deadline], ['claimed', deadline], ['claimed', deadline]])
  await before.stop()

  const boundary = await start(db, keys, deadline)
  t.after(() => boundary.stop())
  const response = await boundary.get('/tasks', watcher)
  assert.equal(response.status, 200)
  const { tasks, cursor } = await response.json()
  assert.equal(cursor, 9)
  assert.deepEqual(tasks.map(({ state, holder, lease_expires_at }: { state: string; holder: unknown; lease_expires_at: unknown }) => [state, holder, lease_expires_at]),
    cases.map(({ expected }) => [expected, null, null]))
  await boundary.stop()

  const audit = openStore({ path: db, clock: () => deadline - 1 })
  try {
    assert.deepEqual(audit.events({ after: snapshot.cursor }).map(({ seq, taskId, type, from, to, at }) => [seq, taskId, type, from, to, at]),
      cases.map(({ expected }, i) => [7 + i, 1 + i, 'task.lease_expired', 'claimed', expected, deadline]))
  } finally {
    audit.close()
  }
})

test('HTTP reads need a key, action, and queue grant; a restart loads revoked keys out', { timeout: 10_000 }, async () => {
  const { db, keys, credentials, checkout, sibling, nested } = fixture('auth')
  const server = await start(db, keys)
  try {
    const challenge = await server.get('/tasks')
    assert.equal(challenge.headers.get('WWW-Authenticate'), 'Bearer')
    await error(challenge, 401, 'unauthorized')
    for (const key of [unknownKey, digest(reader)]) await error(await server.get('/tasks', key), 401, 'unauthorized')
    await error(await server.get('/tasks', blind), 403, 'forbidden')
    await error(await server.get(`/tasks/${checkout}`, blind), 403, 'forbidden')
    await error(await server.get('/tasks?queue=incidents-private%2Fcheckout', reader), 403, 'forbidden')
    await error(await server.get('/tasks?queue=incidents%2F', exact), 403, 'forbidden')
    await error(await server.get(`/tasks/${nested}`, exact), 404, 'not_found')
    await error(await server.get(`/tasks/${sibling}`, reader), 404, 'not_found')
    await error(await server.get('/tasks/999', reader), 404, 'not_found')
    assert.equal((await server.get(`/tasks/${checkout}`, exact)).status, 200)
    writeFileSync(keys, JSON.stringify(credentials.slice(1)))
    assert.equal((await server.get(`/tasks/${checkout}`, reader)).status, 200)
  } finally {
    await server.stop()
  }

  const restarted = await start(db, keys)
  try {
    await error(await restarted.get(`/tasks/${checkout}`, reader), 401, 'unauthorized')
    assert.equal((await restarted.get(`/tasks/${checkout}`, exact)).status, 200)
  } finally {
    await restarted.stop()
  }
})

test('serve rejects malformed listen addresses before opening storage or a listener', () => {
  const keys = join(dir, 'listen.json')
  const db = join(dir, 'listen.db')
  writeFileSync(keys, '[]')
  const guard = `import { Server } from 'node:net'; Server.prototype.listen = () => { throw new Error('Unexpected listen') }`
  for (const address of ['[:0', ']:0', '[]:0', '[::1:0', '[::1]]:0', ':0', ' :0']) {
    const flags = args(db, keys)
    flags[flags.length - 1] = address
    const result = spawnSync(process.execPath, ['--import', `data:text/javascript,${encodeURIComponent(guard)}`, ...flags], { encoding: 'utf8', timeout: 5_000 })
    assert.equal(result.status, 1, address)
    assert.match(result.stderr, /Pass --listen as host:port/, address)
    assert.equal(existsSync(db), false, address)
  }
})

test('serve fails closed on a malformed key file without printing its contents', () => {
  const keys = join(dir, 'invalid.json')
  writeFileSync(keys, reader)
  const result = spawnSync(process.execPath, args(join(dir, 'invalid.db'), keys), { encoding: 'utf8', timeout: 5_000 })
  assert.equal(result.status, 1)
  assert.match(result.stderr, /key file is not valid JSON/)
  assert.equal((result.stdout + result.stderr).includes(reader), false)
})

const claimPath = '/tasks/claim'
const checkoutQueue = 'incidents/checkout'
const agentA = 'ab'.repeat(32)
const agentB = 'bc'.repeat(32)

function ingestTask(store: ReturnType<typeof openStore>, queue: string, dedupeKey: string, retry: Retry = 'manual', labels = {}) {
  return store.ingest({ queue, dedupeKey, retry, labels, title: dedupeKey, source: 'fixture', kind: 'incident' }, source).task.id
}

test('one of 20 HTTP claims wins and its request ID survives an abrupt server restart', { timeout: 15_000 }, async (t) => {
  const db = join(dir, 'race.db')
  const keys = join(dir, 'race.json')
  const competitors = Array.from({ length: 20 }, (_, i) => (i + 1).toString(16).padStart(64, '0'))
  writeFileSync(keys, JSON.stringify([
    credential('watcher', watcher, ['*'], ['read', 'events']),
    ...competitors.map((key, i) => credential(`agent-${i}`, key, [checkoutQueue], ['claim'])),
  ]))
  const seed = openStore({ path: db })
  const id = ingestTask(seed, checkoutQueue, 'race')
  seed.close()

  const server = await start(db, keys)
  t.after(async () => { await server.stop() })
  const request = { queue: checkoutQueue, request_id: 'claim-one' }
  const responses = await Promise.all(competitors.map((key) => server.post(claimPath, key, request)))
  const winners = responses.flatMap((response, i) => response.status === 200 ? [i] : [])
  assert.equal(winners.length, 1)
  const [winner] = winners
  for (const [i, response] of responses.entries()) {
    if (i !== winner) await error(response, 409, 'nothing_ready')
  }
  const claimed = await ok(responses[winner])
  assert.deepEqual([claimed.id, claimed.holder, claimed.epoch, claimed.attempts], [id, `agent:agent-${winner}`, 1, 1])
  assert.equal(await server.stop('SIGKILL'), 'SIGKILL')

  const restarted = await start(db, keys)
  t.after(async () => { await restarted.stop() })
  assert.deepEqual(await ok(await restarted.post(claimPath, competitors[winner], { ...request, wait: 1 })), claimed)
  await error(await restarted.post(claimPath, competitors[winner], { ...request, lease: 600 }), 409, 'request_id_reused')
  const { tasks, cursor } = await ok(await restarted.get('/tasks', watcher))
  assert.deepEqual(tasks, [claimed])
  assert.equal(cursor, 2)

  await restarted.stop()
  const audit = openStore({ path: db })
  try {
    assert.deepEqual(audit.events({ after: 1 }).map(({ type, actor, requestId }) => [type, actor, requestId]), [
      ['task.claimed', `agent:agent-${winner}`, 'claim-one'],
    ])
  } finally {
    audit.close()
  }
})

async function workingHub(t: TestContext, name: string) {
  let clock = now
  let onAccess: (() => void) | undefined
  const store = openStore({ path: join(dir, `${name}.db`), clock: () => {
    onAccess?.()
    onAccess = undefined
    return clock
  } })
  const server = createHttpServer(store, [
    credential('a', agentA, ['incidents/'], ['claim', 'renew', 'complete', 'release', 'fail', 'block']),
    credential('b', agentB, ['incidents/'], ['claim', 'complete']),
    credential('reader', reader, ['incidents/'], ['read']),
    credential('exact', exact, ['incidents/search'], ['claim', 'complete']),
  ])
  t.after(async () => {
    const closed = once(server, 'close')
    server.close()
    server.closeAllConnections()
    await closed
    store.close()
  })
  const listening = once(server, 'listening')
  server.listen(0, '127.0.0.1')
  await listening
  const addr = server.address()
  assert.ok(addr && typeof addr !== 'string')
  return {
    ...client(`http://127.0.0.1:${addr.port}`),
    server,
    store,
    advance: (ms: number) => { clock += ms },
    nextAccess: () => new Promise<void>((resolve) => { onAccess = resolve }),
  }
}

test('HTTP holder writes and replays leave expiry to a read, and a reclaimed task rejects the prior epoch', { timeout: 10_000 }, async (t) => {
  const hub = await workingHub(t, 'fence')
  const id = ingestTask(hub.store, checkoutQueue, 'safe', 'safe')
  const detail = `/tasks/${id}`
  const complete = `/tasks/${id}/complete`
  const renew = `/tasks/${id}/renew`
  const claim = { queue: checkoutQueue, lease: 10, request_id: 'first' }

  await error(await hub.post(claimPath, reader, claim), 403, 'forbidden')
  await error(await hub.post(claimPath, exact, { ...claim, queue: 'incidents/' }), 403, 'forbidden')
  const first = await ok(await hub.post(claimPath, agentA, claim))
  assert.equal(first.lease_expires_at, now + 10_000)
  const finish = { epoch: first.epoch, request_id: 'finish', result: 'resolved ✓' }
  await error(await hub.post(complete, exact, finish), 404, 'not_found')
  await error(await hub.post(`/tasks/${id}/release`, agentB, finish), 403, 'forbidden')
  await error(await hub.post(complete, agentB, { ...finish, actor: 'agent:a' }), 409, 'claim_lost')

  hub.advance(9_999)
  const renewal = { epoch: first.epoch, request_id: 'renew', lease: 11 }
  const renewed = await ok(await hub.post(renew, agentA, renewal))
  assert.equal(renewed.lease_expires_at, now + 20_999)
  const beforeExpiry = 3
  hub.advance(11_000)
  await error(await hub.post(complete, agentA, finish), 409, 'claim_lost')
  assert.deepEqual(await ok(await hub.post(renew, agentA, renewal)), renewed)
  assert.deepEqual(await ok(await hub.post(claimPath, agentA, claim)), first)

  hub.advance(-1)
  assert.deepEqual(hub.store.events({ after: beforeExpiry }), [])
  assert.deepEqual(await ok(await hub.get(detail, reader)), { ...renewed, memos: {} })
  hub.advance(1)
  const expired = await ok(await hub.get(detail, reader))
  assert.deepEqual([expired.state, expired.holder, expired.lease_expires_at], ['ready', null, null])
  hub.advance(-1)
  assert.deepEqual(hub.store.events({ after: beforeExpiry }).map(({ type }) => type), ['task.lease_expired'])
  hub.advance(1)

  const fresh = await ok(await hub.post(claimPath, agentA, { ...claim, request_id: 'second' }))
  assert.equal(fresh.epoch, first.epoch + 1)
  await error(await hub.post(complete, agentB, { ...finish, epoch: fresh.epoch }), 409, 'claim_lost')
  await error(await hub.post(complete, agentA, finish), 409, 'claim_lost')
  assert.deepEqual(hub.store.events({ after: beforeExpiry }).map(({ type }) => type), ['task.lease_expired', 'task.claimed'])
  const finalWrite = { ...finish, epoch: fresh.epoch }
  const done = await ok(await hub.post(complete, agentA, finalWrite))
  assert.deepEqual([done.state, done.holder, done.result], ['completed', null, 'resolved ✓'])
  assert.deepEqual(await ok(await hub.post(complete, agentA, finalWrite)), done)
  await error(await hub.post(complete, agentA, { ...finalWrite, result: 'different' }), 409, 'request_id_reused')
  assert.deepEqual(hub.store.events({ after: beforeExpiry }).map(({ type }) => type), ['task.lease_expired', 'task.claimed', 'task.completed'])
})

test('HTTP claims filter labels, retry misses, and never reclaim expired manual tasks', { timeout: 10_000 }, async (t) => {
  const hub = await workingHub(t, 'manual')
  ingestTask(hub.store, checkoutQueue, 'not-selected', 'manual', { urgency: 'low' })
  const id = ingestTask(hub.store, checkoutQueue, 'manual', 'manual', { urgency: 'high', team: 'checkout' })
  const select = { queue: 'incidents/', labels: { urgency: 'high', team: 'checkout' }, lease: 10 }
  const claim = (request_id: string) => hub.post(claimPath, agentA, { ...select, request_id })
  const first = await ok(await claim('initial'))
  assert.equal(first.id, id)
  await error(await claim('retry-miss'), 409, 'nothing_ready')
  const released = await ok(await hub.post(`/tasks/${id}/release`, agentA, { epoch: first.epoch, request_id: 'release' }))
  assert.deepEqual([released.state, released.attempts], ['ready', 0])
  const second = await ok(await claim('retry-miss'))
  const retried = await ok(await hub.post(`/tasks/${id}/fail`, agentA, { epoch: second.epoch, request_id: 'fail', reason: 'try again', retry: true }))
  assert.deepEqual([retried.state, retried.attempts], ['ready', 1])
  const third = await ok(await claim('third'))
  hub.advance(10_000)
  await error(await hub.post(`/tasks/${id}/block`, agentA, { epoch: third.epoch, request_id: 'late', reason: 'too late' }), 409, 'claim_lost')
  await error(await claim('after-expiry'), 409, 'nothing_ready')
  const interrupted = await ok(await hub.get(`/tasks/${id}`, reader))
  assert.deepEqual([interrupted.state, interrupted.holder, interrupted.attempts], ['interrupted', null, 2])

  const another = await ok(await hub.post(claimPath, agentA, { queue: checkoutQueue, request_id: 'remaining' }))
  const blocked = await ok(await hub.post(`/tasks/${another.id}/block`, agentA, { epoch: another.epoch, request_id: 'block', reason: 'ask a person' }))
  assert.deepEqual([blocked.state, blocked.attempts, blocked.reason], ['blocked', 0, 'ask a person'])
})

test('HTTP claims wait for a match and reject malformed or oversized writes without claiming', { timeout: 10_000 }, async (t) => {
  const hub = await workingHub(t, 'waiting')
  const body = { queue: checkoutQueue, request_id: 'waiting' }
  const checked = hub.nextAccess()
  const waiting = hub.post(claimPath, agentA, { ...body, wait: 2 })
  await checked
  const id = ingestTask(hub.store, checkoutQueue, 'arriving')
  assert.equal((await ok(await waiting)).id, id)

  const ready = ingestTask(hub.store, checkoutQueue, 'untouched')
  const bounded = { ...body, request_id: 'bounds', wait: 30, lease: 3_600 }
  await error(await hub.post(claimPath, agentA, { queue: checkoutQueue }), 400, 'invalid_argument')
  for (const wait of [31, 0.5]) await error(await hub.post(claimPath, agentA, { ...bounded, wait }), 400, 'invalid_argument')
  for (const lease of [9, 10.5, 3_601]) await error(await hub.post(claimPath, agentA, { ...bounded, lease }), 400, 'invalid_argument')
  const rawClaim = (raw: string, contentType: string) => hub.request(claimPath, agentA, { method: 'POST', body: raw, headers: { 'Content-Type': contentType } })
  await error(await rawClaim('{', 'application/json'), 400, 'invalid_argument')
  await error(await rawClaim(JSON.stringify({ ...body, request_id: 'wrong-type' }), 'text/plain'), 415, 'invalid_argument')
  const prefix = JSON.stringify(bounded).slice(0, -1) + ',"padding":"'
  const suffix = '"}'
  const available = 65_536 - Buffer.byteLength(prefix + suffix)
  const padding = 'é'.repeat(Math.floor(available / 2)) + 'x'.repeat(available % 2)
  await error(await rawClaim(prefix + padding + 'x' + suffix, 'application/json'), 413, 'invalid_argument')
  const { tasks } = await ok(await hub.get('/tasks?state=ready', reader))
  assert.deepEqual(tasks.map(({ dedupe_key }: { dedupe_key: string }) => dedupe_key), ['untouched'])
  assert.deepEqual(hub.store.events().map(({ type }) => type), ['task.created', 'task.claimed', 'task.created'])
  const withinLimits = await ok(await rawClaim(prefix + padding + suffix, 'application/json'))
  assert.deepEqual([withinLimits.id, withinLimits.lease_expires_at], [ready, now + 3_600_000])
})

test('an HTTP claim does not take a task when its next poll reaches the deadline', { timeout: 10_000 }, async (t) => {
  const hub = await workingHub(t, 'wait-deadline')
  let elapsed = 0
  t.mock.method(performance, 'now', () => elapsed)
  const checked = hub.nextAccess()
  const waiting = hub.post(claimPath, agentA, { queue: checkoutQueue, request_id: 'deadline', wait: 1 })
  await checked

  elapsed = 1_000
  const id = ingestTask(hub.store, checkoutQueue, 'at-deadline')
  await error(await waiting, 409, 'nothing_ready')
  assert.equal((await ok(await hub.get(`/tasks/${id}`, reader))).state, 'ready')
  assert.deepEqual(hub.store.events().map(({ type }) => type), ['task.created'])
})

test('a disconnected HTTP claim stops looking for a task', { timeout: 10_000 }, async (t) => {
  const hub = await workingHub(t, 'disconnect')
  const checked = hub.nextAccess()
  const disconnected = new Promise<void>((resolve) => hub.server.once('request', (_, res) => res.once('close', resolve)))
  const abort = new AbortController()
  const abandoned = hub.post(claimPath, agentA, { queue: checkoutQueue, request_id: 'abandoned', wait: 1 }, abort.signal)
  const rejected = assert.rejects(abandoned, { name: 'AbortError' })
  await checked
  abort.abort()
  await rejected
  await disconnected

  const id = ingestTask(hub.store, checkoutQueue, 'after-disconnect')
  const started = performance.now()
  await error(await hub.post(claimPath, agentB, { queue: checkoutQueue, labels: { missing: 'yes' }, request_id: 'timeout', wait: 1 }), 409, 'nothing_ready')
  assert.ok(performance.now() - started >= 1_000)
  const { tasks } = await ok(await hub.get('/tasks?state=ready', reader))
  assert.deepEqual(tasks.map((task: { id: number }) => task.id), [id])
  assert.deepEqual(hub.store.events().map(({ type }) => type), ['task.created'])
})
