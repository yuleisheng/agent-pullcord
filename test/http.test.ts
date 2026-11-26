import assert from 'node:assert/strict'
import { spawn, spawnSync } from 'node:child_process'
import { createHash, createHmac } from 'node:crypto'
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
    store.ingest(
      { queue, dedupeKey, labels, kind: 'incident', source: 'fake', title: 'Check checkout', payload },
      source,
    ).task.id
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
    post: (path: string, key: string, body: unknown, signal?: AbortSignal) =>
      request(path, key, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
        signal,
      }),
  }
}

async function start(db: string, keys: string, clock?: number, sources?: string) {
  const flags = clock === undefined ? [] : ['--import', `data:text/javascript,Date.now=()=>${clock}`]
  const sourceFlags = sources === undefined ? [] : ['--sources', sources]
  const child = spawn(process.execPath, [...flags, ...args(db, keys), ...sourceFlags], {
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  const done = once(child, 'close')
  let stderr = ''
  child.stderr.setEncoding('utf8').on('data', (chunk) => {
    stderr += chunk
  })
  const lines = createInterface({ input: child.stdout })
  for await (const line of lines) {
    if (line.startsWith('Listening on ')) {
      const origin = line.slice('Listening on '.length)
      return {
        ...client(origin),
        stop: async (signal: NodeJS.Signals = 'SIGTERM') => {
          child.kill(signal)
          return (await done)[1]
        },
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

test(
  'HTTP task reads combine their filters without exposing other queues or trusting source fields',
  { timeout: 10_000 },
  async (t) => {
    const { db, keys, checkout, search, sibling, nested, payload } = fixture('read')
    const server = await start(db, keys)
    t.after(async () => {
      await server.stop()
    })

    const visible = await (await server.get('/tasks', reader)).json()
    assert.deepEqual(
      visible.tasks.map(({ id }: { id: number }) => id),
      [checkout, search, nested],
    )
    assert.equal(Object.hasOwn(visible, 'cursor'), false)

    const all = await (await server.get('/tasks', allReader)).json()
    assert.deepEqual(
      all.tasks.map(({ id }: { id: number }) => id),
      [checkout, search, sibling, nested],
    )
    assert.equal(Object.hasOwn(all, 'cursor'), false)
    assert.deepEqual(await (await server.get('/tasks', watcher)).json(), { ...all, cursor: 6 })

    const response = await server.get(
      '/tasks?queue=incidents%2F&state=ready&label=team%3Dpayments&label=signal%3Dx%3Dy',
      reader,
    )
    assert.equal(response.status, 200)
    assert.equal(response.headers.get('Cache-Control'), 'no-store')
    const filtered = await response.json()
    assert.equal(Object.hasOwn(filtered, 'cursor'), false)
    const { tasks } = filtered
    assert.deepEqual(
      tasks.map(({ id }: { id: number }) => id),
      [checkout],
    )

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
  },
)

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
    seed.ingest(
      { queue, kind: 'incident', source: 'fake', dedupeKey: expected, title: expected, retry, maxAttempts },
      source,
    )
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
  assert.deepEqual(
    snapshot.tasks.map(({ state, lease_expires_at }: { state: string; lease_expires_at: number }) => [
      state,
      lease_expires_at,
    ]),
    [
      ['claimed', deadline],
      ['claimed', deadline],
      ['claimed', deadline],
    ],
  )
  await before.stop()

  const boundary = await start(db, keys, deadline)
  t.after(() => boundary.stop())
  const response = await boundary.get('/tasks', watcher)
  assert.equal(response.status, 200)
  const { tasks, cursor } = await response.json()
  assert.equal(cursor, 9)
  assert.deepEqual(
    tasks.map(({ state, holder, lease_expires_at }: { state: string; holder: unknown; lease_expires_at: unknown }) => [
      state,
      holder,
      lease_expires_at,
    ]),
    cases.map(({ expected }) => [expected, null, null]),
  )
  await boundary.stop()

  const audit = openStore({ path: db, clock: () => deadline - 1 })
  try {
    assert.deepEqual(
      audit
        .events({ after: snapshot.cursor })
        .map(({ seq, taskId, type, from, to, at }) => [seq, taskId, type, from, to, at]),
      cases.map(({ expected }, i) => [7 + i, 1 + i, 'task.lease_expired', 'claimed', expected, deadline]),
    )
  } finally {
    audit.close()
  }
})

test(
  'HTTP reads need a key, action, and queue grant; a restart loads revoked keys out',
  { timeout: 10_000 },
  async () => {
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
  },
)

test('serve rejects malformed listen addresses before opening storage or a listener', () => {
  const keys = join(dir, 'listen.json')
  const db = join(dir, 'listen.db')
  writeFileSync(keys, '[]')
  const guard = `import { Server } from 'node:net'; Server.prototype.listen = () => { throw new Error('Unexpected listen') }`
  for (const address of ['[:0', ']:0', '[]:0', '[::1:0', '[::1]]:0', ':0', ' :0']) {
    const flags = args(db, keys)
    flags[flags.length - 1] = address
    const result = spawnSync(
      process.execPath,
      ['--import', `data:text/javascript,${encodeURIComponent(guard)}`, ...flags],
      { encoding: 'utf8', timeout: 5_000 },
    )
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

function ingestTask(
  store: ReturnType<typeof openStore>,
  queue: string,
  dedupeKey: string,
  retry: Retry = 'manual',
  labels = {},
) {
  return store.ingest(
    { queue, dedupeKey, retry, labels, title: dedupeKey, source: 'fixture', kind: 'incident' },
    source,
  ).task.id
}

test(
  'one of 20 HTTP claims wins and its receipt and event survive an abrupt server restart',
  { timeout: 15_000 },
  async (t) => {
    const db = join(dir, 'race.db')
    const keys = join(dir, 'race.json')
    const competitors = Array.from({ length: 20 }, (_, i) => (i + 1).toString(16).padStart(64, '0'))
    writeFileSync(
      keys,
      JSON.stringify([
        credential('watcher', watcher, ['*'], ['read', 'events']),
        ...competitors.map((key, i) => credential(`agent-${i}`, key, [checkoutQueue], ['claim'])),
      ]),
    )
    const seed = openStore({ path: db })
    const id = ingestTask(seed, checkoutQueue, 'race')
    seed.close()

    const server = await start(db, keys)
    t.after(async () => {
      await server.stop()
    })
    const before = await ok(await server.get('/tasks', watcher))
    assert.equal(before.cursor, 1)
    const request = { queue: checkoutQueue, request_id: 'claim-one' }
    const responses = await Promise.all(competitors.map((key) => server.post(claimPath, key, request)))
    const winners = responses.flatMap((response, i) => (response.status === 200 ? [i] : []))
    assert.equal(winners.length, 1)
    const [winner] = winners
    for (const [i, response] of responses.entries()) {
      if (i !== winner) await error(response, 409, 'nothing_ready')
    }
    const claimed = await ok(responses[winner])
    assert.deepEqual([claimed.id, claimed.holder, claimed.epoch, claimed.attempts], [id, `agent:agent-${winner}`, 1, 1])
    assert.equal(await server.stop('SIGKILL'), 'SIGKILL')

    const restarted = await start(db, keys)
    t.after(async () => {
      await restarted.stop()
    })
    assert.deepEqual(await ok(await restarted.post(claimPath, competitors[winner], { ...request, wait: 1 })), claimed)
    await error(
      await restarted.post(claimPath, competitors[winner], { ...request, lease: 600 }),
      409,
      'request_id_reused',
    )
    const { tasks, cursor } = await ok(await restarted.get('/tasks', watcher))
    assert.deepEqual(tasks, [claimed])
    assert.equal(cursor, 2)

    const catchup = await ok(await restarted.get(`/events?after=${before.cursor}`, watcher))
    assert.equal(catchup.cursor, before.cursor + 1)
    assert.deepEqual(
      catchup.events.map(
        ({
          seq,
          task_id,
          type,
          actor,
          request_id,
        }: {
          seq: number
          task_id: number
          type: string
          actor: string
          request_id: string
        }) => [seq, task_id, type, actor, request_id],
      ),
      [[before.cursor + 1, id, 'task.claimed', `agent:agent-${winner}`, 'claim-one']],
    )
    assert.deepEqual(await ok(await restarted.get(`/events?after=${catchup.cursor}`, watcher)), {
      events: [],
      cursor: catchup.cursor,
    })
  },
)

async function workingHub(t: TestContext, name: string, sources: unknown = []) {
  let clock = now
  let onAccess: (() => void) | undefined
  const store = openStore({
    path: join(dir, `${name}.db`),
    clock: () => {
      onAccess?.()
      onAccess = undefined
      return clock
    },
  })
  const server = createHttpServer(
    store,
    [
      credential(
        'a',
        agentA,
        ['incidents/'],
        ['claim', 'renew', 'complete', 'release', 'fail', 'block', 'log', 'memo'],
      ),
      credential('b', agentB, ['incidents/'], ['claim', 'complete', 'memo']),
      credential('reader', reader, ['incidents/'], ['read', 'events']),
      credential('exact', exact, ['incidents/search'], ['claim', 'complete']),
      credential('watcher', watcher, ['*'], ['read', 'events']),
      credential('events-only', blind, ['*'], ['events']),
      credential('no-events', allReader, ['*'], ['read']),
      credential('operator', unknownKey, [checkoutQueue], ['admin']),
    ],
    sources,
  )
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
    advance: (ms: number) => {
      clock += ms
    },
    nextAccess: () =>
      new Promise<void>((resolve) => {
        onAccess = resolve
      }),
  }
}

test(
  'HTTP holder writes and replays leave expiry to a read, and a reclaimed task rejects the prior epoch',
  { timeout: 10_000 },
  async (t) => {
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
    assert.deepEqual(
      hub.store.events({ after: beforeExpiry }).map(({ type }) => type),
      ['task.lease_expired'],
    )
    hub.advance(1)

    const fresh = await ok(await hub.post(claimPath, agentA, { ...claim, request_id: 'second' }))
    assert.equal(fresh.epoch, first.epoch + 1)
    await error(await hub.post(complete, agentB, { ...finish, epoch: fresh.epoch }), 409, 'claim_lost')
    await error(await hub.post(complete, agentA, finish), 409, 'claim_lost')
    assert.deepEqual(
      hub.store.events({ after: beforeExpiry }).map(({ type }) => type),
      ['task.lease_expired', 'task.claimed'],
    )
    const finalWrite = { ...finish, epoch: fresh.epoch }
    const done = await ok(await hub.post(complete, agentA, finalWrite))
    assert.deepEqual([done.state, done.holder, done.result], ['completed', null, 'resolved ✓'])
    assert.deepEqual(await ok(await hub.post(complete, agentA, finalWrite)), done)
    await error(await hub.post(complete, agentA, { ...finalWrite, result: 'different' }), 409, 'request_id_reused')
    assert.deepEqual(
      hub.store.events({ after: beforeExpiry }).map(({ type }) => type),
      ['task.lease_expired', 'task.claimed', 'task.completed'],
    )
  },
)

test(
  'HTTP claims filter labels, retry misses, and never reclaim expired manual tasks',
  { timeout: 10_000 },
  async (t) => {
    const hub = await workingHub(t, 'manual')
    ingestTask(hub.store, checkoutQueue, 'not-selected', 'manual', { urgency: 'low' })
    const id = ingestTask(hub.store, checkoutQueue, 'manual', 'manual', { urgency: 'high', team: 'checkout' })
    const select = { queue: 'incidents/', labels: { urgency: 'high', team: 'checkout' }, lease: 10 }
    const claim = (request_id: string) => hub.post(claimPath, agentA, { ...select, request_id })
    const first = await ok(await claim('initial'))
    assert.equal(first.id, id)
    await error(await claim('retry-miss'), 409, 'nothing_ready')
    const released = await ok(
      await hub.post(`/tasks/${id}/release`, agentA, { epoch: first.epoch, request_id: 'release' }),
    )
    assert.deepEqual([released.state, released.attempts], ['ready', 0])
    const second = await ok(await claim('retry-miss'))
    const retried = await ok(
      await hub.post(`/tasks/${id}/fail`, agentA, {
        epoch: second.epoch,
        request_id: 'fail',
        reason: 'try again',
        retry: true,
      }),
    )
    assert.deepEqual([retried.state, retried.attempts], ['ready', 1])
    const third = await ok(await claim('third'))
    hub.advance(10_000)
    await error(
      await hub.post(`/tasks/${id}/block`, agentA, { epoch: third.epoch, request_id: 'late', reason: 'too late' }),
      409,
      'claim_lost',
    )
    await error(await claim('after-expiry'), 409, 'nothing_ready')
    const interrupted = await ok(await hub.get(`/tasks/${id}`, reader))
    assert.deepEqual([interrupted.state, interrupted.holder, interrupted.attempts], ['interrupted', null, 2])

    const another = await ok(await hub.post(claimPath, agentA, { queue: checkoutQueue, request_id: 'remaining' }))
    const blocked = await ok(
      await hub.post(`/tasks/${another.id}/block`, agentA, {
        epoch: another.epoch,
        request_id: 'block',
        reason: 'ask a person',
      }),
    )
    assert.deepEqual([blocked.state, blocked.attempts, blocked.reason], ['blocked', 0, 'ask a person'])
  },
)

test(
  'HTTP claims wait for a match and reject malformed or oversized writes without claiming',
  { timeout: 10_000 },
  async (t) => {
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
    for (const wait of [31, 0.5])
      await error(await hub.post(claimPath, agentA, { ...bounded, wait }), 400, 'invalid_argument')
    for (const lease of [9, 10.5, 3_601])
      await error(await hub.post(claimPath, agentA, { ...bounded, lease }), 400, 'invalid_argument')
    const rawClaim = (raw: string, contentType: string) =>
      hub.request(claimPath, agentA, { method: 'POST', body: raw, headers: { 'Content-Type': contentType } })
    await error(await rawClaim('{', 'application/json'), 400, 'invalid_argument')
    await error(
      await rawClaim(JSON.stringify({ ...body, request_id: 'wrong-type' }), 'text/plain'),
      415,
      'invalid_argument',
    )
    const prefix = JSON.stringify(bounded).slice(0, -1) + ',"padding":"'
    const suffix = '"}'
    const available = 65_536 - Buffer.byteLength(prefix + suffix)
    const padding = 'é'.repeat(Math.floor(available / 2)) + 'x'.repeat(available % 2)
    await error(await rawClaim(prefix + padding + 'x' + suffix, 'application/json'), 413, 'invalid_argument')
    const { tasks } = await ok(await hub.get('/tasks?state=ready', reader))
    assert.deepEqual(
      tasks.map(({ dedupe_key }: { dedupe_key: string }) => dedupe_key),
      ['untouched'],
    )
    assert.deepEqual(
      hub.store.events().map(({ type }) => type),
      ['task.created', 'task.claimed', 'task.created'],
    )
    const withinLimits = await ok(await rawClaim(prefix + padding + suffix, 'application/json'))
    assert.deepEqual([withinLimits.id, withinLimits.lease_expires_at], [ready, now + 3_600_000])
  },
)

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
  assert.deepEqual(
    hub.store.events().map(({ type }) => type),
    ['task.created'],
  )
})

test('a disconnected HTTP claim stops looking for a task', { timeout: 10_000 }, async (t) => {
  const hub = await workingHub(t, 'disconnect')
  const checked = hub.nextAccess()
  const disconnected = new Promise<void>((resolve) =>
    hub.server.once('request', (_, res) => res.once('close', resolve)),
  )
  const abort = new AbortController()
  const abandoned = hub.post(
    claimPath,
    agentA,
    { queue: checkoutQueue, request_id: 'abandoned', wait: 1 },
    abort.signal,
  )
  const rejected = assert.rejects(abandoned, { name: 'AbortError' })
  await checked
  abort.abort()
  await rejected
  await disconnected

  const id = ingestTask(hub.store, checkoutQueue, 'after-disconnect')
  const started = performance.now()
  await error(
    await hub.post(claimPath, agentB, {
      queue: checkoutQueue,
      labels: { missing: 'yes' },
      request_id: 'timeout',
      wait: 1,
    }),
    409,
    'nothing_ready',
  )
  assert.ok(performance.now() - started >= 1_000)
  const { tasks } = await ok(await hub.get('/tasks?state=ready', reader))
  assert.deepEqual(
    tasks.map((task: { id: number }) => task.id),
    [id],
  )
  assert.deepEqual(
    hub.store.events().map(({ type }) => type),
    ['task.created'],
  )
})

test(
  'an exact-ID claim applies action, queue, lease and replay rules without falling back to the queue',
  { timeout: 10_000 },
  async (t) => {
    const hub = await workingHub(t, 'exact-claim')
    const older = ingestTask(hub.store, checkoutQueue, 'older-ready-task')
    const id = ingestTask(hub.store, checkoutQueue, 'selected-by-id')
    const body = { id, lease: 10, request_id: 'exact' }
    await error(await hub.post(claimPath, reader, body), 403, 'forbidden')
    await error(await hub.post(claimPath, exact, body), 404, 'not_found')
    await error(await hub.post(claimPath, agentA, { ...body, id: 999 }), 404, 'not_found')
    await error(await hub.post(claimPath, agentA, { ...body, queue: checkoutQueue }), 400, 'invalid_argument')
    await error(await hub.post(claimPath, agentA, { ...body, labels: {} }), 400, 'invalid_argument')

    const contenders = [agentA, agentB]
    const responses = await Promise.all(contenders.map((key) => hub.post(claimPath, key, body)))
    const winner = responses.findIndex((response) => response.status === 200)
    assert.notEqual(winner, -1)
    await error(responses[1 - winner], 409, 'nothing_ready')
    const task = await ok(responses[winner])
    assert.deepEqual([task.id, task.epoch, task.lease_expires_at], [id, 1, now + 10_000])
    assert.deepEqual(await ok(await hub.post(claimPath, contenders[winner], { ...body, wait: 1 })), task)
    await error(await hub.post(claimPath, contenders[winner], { ...body, id: older }), 409, 'request_id_reused')
    assert.equal((await ok(await hub.get(`/tasks/${older}`, reader))).state, 'ready')
    assert.deepEqual(
      (await ok(await hub.get('/events?after=2', watcher))).events.map((event: { type: string; task_id: number }) => [
        event.type,
        event.task_id,
      ]),
      [['task.claimed', id]],
    )
  },
)

test(
  'the global watch waits for new events and resumes at the exact cursor, without a hidden backlog cap',
  { timeout: 10_000 },
  async (t) => {
    const hub = await workingHub(t, 'watch')
    const id = ingestTask(hub.store, checkoutQueue, 'in-scope')
    await error(await hub.get('/events'), 401, 'unauthorized')
    await error(await hub.get('/events', reader), 403, 'forbidden')
    await error(await hub.get('/events', allReader), 403, 'forbidden')
    for (const query of ['after=-1', 'after=0x1', 'after=9007199254740992', 'wait=31', 'wait=1.5']) {
      await error(await hub.get(`/events?${query}`, watcher), 400, 'invalid_argument')
    }

    const checked = hub.nextAccess()
    const waiting = hub.get('/events?after=1&wait=2', blind)
    await checked
    for (let i = 0; i < 102; i++) hub.store.log(id, { actor: 'agent:fixture', text: `note ${i}` })
    const elsewhere = ingestTask(hub.store, 'outside', 'out-of-scope')
    const { events, cursor } = await ok(await waiting)
    assert.equal(cursor, 104)
    assert.deepEqual(
      events.map(({ seq }: { seq: number }) => seq),
      Array.from({ length: 103 }, (_, i) => i + 2),
    )
    assert.deepEqual(events.at(-1), {
      seq: cursor,
      task_id: elsewhere,
      type: 'task.created',
      actor: source.actor,
      request_id: null,
      from: null,
      to: 'ready',
      data: { queue: 'outside', source: 'fixture', dedupeKey: 'out-of-scope' },
      at: now,
    })
    assert.deepEqual(await ok(await hub.get(`/events?after=${cursor}`, watcher)), { events: [], cursor })
  },
)

test(
  'HTTP notes, memos and admin writes enforce their own scopes and survive repeat requests without extra events',
  { timeout: 10_000 },
  async (t) => {
    const hub = await workingHub(t, 'other-writes')
    const id = ingestTask(hub.store, checkoutQueue, 'mutable')
    const outside = ingestTask(hub.store, 'incidents/search', 'outside-admin')
    const write = (action: string, key: string, request_id: string, fields = {}, task = id) =>
      hub.post(`/tasks/${task}/${action}`, key, { request_id, ...fields })

    const note = { text: 'one note', actor: 'agent:operator' }
    await error(await write('log', agentB, 'note', note), 403, 'forbidden')
    await error(await write('memo', exact, 'denied', { key: 'x', value: 1 }, outside), 403, 'forbidden')
    await error(await write('memo', agentA, 'missing', { key: 'x' }), 400, 'invalid_argument')
    const logged = await ok(await write('log', agentA, 'note', note))
    assert.deepEqual(logged, {
      task_id: id,
      type: 'task.logged',
      actor: 'agent:a',
      request_id: 'note',
      from: 'ready',
      to: 'ready',
      data: { text: 'one note' },
      at: now,
    })
    assert.deepEqual(await ok(await write('log', agentA, 'note', note)), logged)
    await error(await write('memo', agentA, 'note', { key: 'x', value: 1 }), 409, 'request_id_reused')

    const approval = { key: 'approval', value: null }
    for (const value of ['1e400', '{"nested":[-1e400]}']) {
      await error(
        await hub.request(`/tasks/${id}/memo`, agentA, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: `{"request_id":"first","key":"approval","value":${value}}`,
        }),
        400,
        'invalid_argument',
      )
    }
    assert.deepEqual((await ok(await hub.get(`/tasks/${id}`, reader))).memos, {})
    assert.deepEqual(await ok(await hub.get('/events?after=3', watcher)), { events: [], cursor: 3 })
    assert.deepEqual(await ok(await write('memo', agentA, 'first', approval)), { value: null })
    assert.deepEqual(await ok(await write('memo', agentA, 'first', approval)), { value: null })
    await error(await write('memo', agentA, 'first', { ...approval, value: false }), 409, 'request_id_reused')
    assert.deepEqual(await ok(await write('memo', agentB, 'first', { ...approval, value: false })), { value: null })
    assert.deepEqual((await ok(await hub.get(`/tasks/${id}`, reader))).memos, { approval: null })

    const first = await ok(await hub.post(claimPath, agentA, { id, lease: 10, request_id: 'first-claim' }))
    await error(await write('unblock', agentA, 'denied'), 403, 'forbidden')
    await error(await write('unblock', unknownKey, 'unblock'), 409, 'invalid_transition')
    await ok(await write('block', agentA, 'block', { reason: 'review', epoch: first.epoch }))
    const unblocked = await ok(await write('unblock', unknownKey, 'unblock'))
    assert.deepEqual([unblocked.state, unblocked.attempts], ['ready', 0])
    assert.deepEqual(await ok(await write('unblock', unknownKey, 'unblock')), unblocked)

    await ok(await hub.post(claimPath, agentA, { id, lease: 10, request_id: 'second-claim' }))
    hub.advance(10_000)
    const requeued = await ok(await write('requeue', unknownKey, 'requeue'))
    assert.deepEqual([requeued.state, requeued.attempts], ['ready', 0])
    const last = await ok(await hub.post(claimPath, agentA, { id, request_id: 'last-claim' }))
    await error(await write('cancel', unknownKey, 'cancel', { reason: 'stop' }, outside), 404, 'not_found')
    const canceled = await ok(await write('cancel', unknownKey, 'cancel', { reason: 'stop' }))
    assert.deepEqual([canceled.state, canceled.reason, canceled.holder], ['canceled', 'stop', null])
    await error(await write('complete', agentA, 'stale', { epoch: last.epoch }), 409, 'claim_lost')
    await error(await write('requeue', unknownKey, 'terminal'), 409, 'invalid_transition')
    const { events } = await ok(await hub.get('/events?after=2', watcher))
    assert.deepEqual(events[0], { ...logged, seq: 3 })
    assert.deepEqual(
      events.map(({ type }: { type: string }) => type),
      [
        'task.logged',
        'task.memo',
        'task.claimed',
        'task.blocked',
        'task.unblocked',
        'task.claimed',
        'task.lease_expired',
        'task.requeued',
        'task.claimed',
        'task.canceled',
      ],
    )
  },
)

const primarySecret = 'fixture primary secret'
const secondarySecret = 'fixture secondary secret'

const signatureHeaders = {
  pagerduty: ['X-PagerDuty-Signature', 'v1='],
  github: ['X-Hub-Signature-256', 'sha256='],
  generic: ['X-Pullcord-Signature', 'sha256='],
}

function signedDelivery(
  hub: ReturnType<typeof client>,
  source: string,
  provider: keyof typeof signatureHeaders,
  secret: string,
  body: string,
  headers: Record<string, string> = {},
) {
  const digest = createHmac('sha256', secret).update(body).digest('hex')
  const [header, prefix] = signatureHeaders[provider]
  return hub.request(`/ingest/${source}`, undefined, {
    method: 'POST',
    body,
    headers: { ...headers, [header]: prefix + digest },
  })
}

function pagerDutyDelivery(
  hub: ReturnType<typeof client>,
  source: string,
  type: 'triggered' | 'resolved',
  secret: string,
  at = now,
  id = `delivery-${type}`,
) {
  return signedDelivery(
    hub,
    source,
    'pagerduty',
    secret,
    JSON.stringify(
      {
        event: {
          id,
          event_type: `incident.${type}`,
          occurred_at: new Date(at).toISOString(),
          data: { id: 'PGR0VU2', title: 'Source title', queue: 'producer-queue', retry: 'manual' },
        },
      },
      null,
      2,
    ),
  )
}

test(
  'signed webhook retries survive restart and resolution fences only the matching source task',
  { timeout: 15_000 },
  async (t) => {
    const db = join(dir, 'ingest.db')
    const keys = join(dir, 'ingest-keys.json')
    const sources = join(dir, 'ingest-sources.json')
    writeFileSync(
      keys,
      JSON.stringify([
        credential('watcher', watcher, ['*'], ['read', 'events']),
        credential('a', agentA, [checkoutQueue], ['claim', 'complete']),
      ]),
    )
    writeFileSync(
      sources,
      JSON.stringify([
        { name: 'pagerduty', provider: 'pagerduty', queue: checkoutQueue, retry: 'safe', secret: primarySecret },
        { name: 'secondary', provider: 'pagerduty', queue: 'elsewhere', secret: secondarySecret },
      ]),
      { mode: 0o600 },
    )

    const server = await start(db, keys, now, sources)
    t.after(() => server.stop())
    await error(await server.post('/ingest/pagerduty', watcher, {}), 401, 'unauthorized')
    await error(
      await pagerDutyDelivery(server, 'pagerduty', 'triggered', primarySecret, now - 300_001),
      401,
      'unauthorized',
    )
    assert.equal((await pagerDutyDelivery(server, 'secondary', 'resolved', secondarySecret)).status, 202)
    assert.deepEqual(await ok(await server.get('/events', watcher)), { events: [], cursor: 0 })
    const sent = await Promise.all(
      Array.from({ length: 3 }, () => pagerDutyDelivery(server, 'pagerduty', 'triggered', primarySecret)),
    )
    for (const response of sent) {
      assert.equal(response.status, 202)
      assert.deepEqual(await response.json(), {})
    }
    const {
      tasks: [task],
      cursor,
    } = await ok(await server.get('/tasks', watcher))
    assert.equal(cursor, 1)
    assert.deepEqual(
      [task.queue, task.retry, task.state, task.dedupe_key, task.untrusted.title],
      [checkoutQueue, 'safe', 'ready', 'PGR0VU2', 'Source title'],
    )
    assert.equal(await server.stop('SIGKILL'), 'SIGKILL')

    const restarted = await start(db, keys, now, sources)
    t.after(() => restarted.stop())
    assert.equal((await pagerDutyDelivery(restarted, 'pagerduty', 'triggered', primarySecret)).status, 202)
    assert.equal((await pagerDutyDelivery(restarted, 'secondary', 'triggered', secondarySecret)).status, 202)
    const claim = await ok(await restarted.post(claimPath, agentA, { id: task.id, request_id: 'held' }))
    await error(await pagerDutyDelivery(restarted, 'pagerduty', 'resolved', secondarySecret), 401, 'unauthorized')
    assert.equal((await pagerDutyDelivery(restarted, 'secondary', 'resolved', secondarySecret)).status, 202)
    const afterSecondary = await ok(await restarted.get('/tasks', watcher))
    assert.deepEqual(
      afterSecondary.tasks.map(({ state }: { state: string }) => state),
      ['claimed', 'canceled'],
    )
    assert.equal(afterSecondary.tasks[0].holder, claim.holder)
    assert.equal(afterSecondary.tasks[0].epoch, claim.epoch)

    const canceled = await Promise.all(
      Array.from({ length: 3 }, () => pagerDutyDelivery(restarted, 'pagerduty', 'resolved', primarySecret)),
    )
    for (const response of canceled) assert.equal(response.status, 202)
    assert.equal(
      (await pagerDutyDelivery(restarted, 'pagerduty', 'resolved', primarySecret, now, 'another-resolution')).status,
      202,
    )
    await error(
      await restarted.post(`/tasks/${task.id}/complete`, agentA, { request_id: 'stale', epoch: claim.epoch }),
      409,
      'claim_lost',
    )
    const snapshot = await ok(await restarted.get('/tasks', watcher))
    assert.deepEqual(
      snapshot.tasks.map(({ state }: { state: string }) => state),
      ['canceled', 'canceled'],
    )
    const { events } = await ok(await restarted.get('/events?after=1', watcher))
    assert.deepEqual(
      events.map(({ type, actor, request_id }: { type: string; actor: string; request_id: string }) => [
        type,
        actor,
        request_id,
      ]),
      [
        ['task.created', 'source:secondary', null],
        ['task.claimed', 'agent:a', 'held'],
        ['task.canceled', 'source:secondary', 'delivery-resolved'],
        ['task.canceled', 'source:pagerduty', 'delivery-resolved'],
      ],
    )
  },
)

test(
  'signed ignored notifications are acknowledged even outside the actionable freshness window',
  { timeout: 10_000 },
  async (t) => {
    const db = join(dir, 'ingest-ignore.db')
    const keys = join(dir, 'ingest-ignore-keys.json')
    const sources = join(dir, 'ingest-ignore-sources.json')
    writeFileSync(keys, JSON.stringify([credential('watcher', watcher, ['*'], ['read', 'events'])]))
    writeFileSync(
      sources,
      JSON.stringify([
        { name: 'pagerduty', provider: 'pagerduty', queue: checkoutQueue, secret: primarySecret },
        { name: 'github', provider: 'github', queue: checkoutQueue, secret: secondarySecret },
      ]),
      { mode: 0o600 },
    )
    const hub = await start(db, keys, now, sources)
    t.after(() => hub.stop())
    const stale = new Date(now - 300_001).toISOString()
    const ping = JSON.stringify({ event: { event_type: 'pagey.ping', occurred_at: stale } })
    const workflow = (conclusion: string) =>
      JSON.stringify({
        action: 'completed',
        workflow_run: { id: 1, name: 'CI', status: 'completed', conclusion, updated_at: stale },
      })
    const githubHeaders = { 'X-GitHub-Event': 'workflow_run' }
    for (const response of await Promise.all([
      signedDelivery(hub, 'pagerduty', 'pagerduty', primarySecret, ping),
      signedDelivery(hub, 'github', 'github', secondarySecret, workflow('success'), githubHeaders),
    ])) {
      assert.equal(response.status, 202)
      assert.deepEqual(await response.json(), {})
    }
    await error(await signedDelivery(hub, 'pagerduty', 'pagerduty', 'wrong-secret', ping), 401, 'unauthorized')
    await error(
      await pagerDutyDelivery(hub, 'pagerduty', 'resolved', primarySecret, now - 300_001),
      401,
      'unauthorized',
    )
    await error(
      await signedDelivery(hub, 'github', 'github', secondarySecret, workflow('failure'), githubHeaders),
      401,
      'unauthorized',
    )
    assert.deepEqual(await ok(await hub.get('/tasks', watcher)), { tasks: [], cursor: 0 })
  },
)

test(
  'ingest bounds chunked bodies and rate-limits verified deliveries per configured name',
  { timeout: 10_000 },
  async (t) => {
    const hub = await workingHub(t, 'ingest-limit', [
      { name: 'generic', provider: 'generic', queue: checkoutQueue, secret: primarySecret },
      { name: 'second', provider: 'generic', queue: 'elsewhere', secret: secondarySecret },
    ])
    const bytes = new Uint8Array(512 * 1024)
    const chunked: RequestInit & { duplex: 'half' } = {
      method: 'POST',
      duplex: 'half',
      body: new ReadableStream({
        start(controller) {
          controller.enqueue(bytes)
          controller.enqueue(bytes)
          controller.enqueue(Uint8Array.of(32))
          controller.close()
        },
      }),
    }
    await error(await hub.request('/ingest/generic', undefined, chunked), 413, 'invalid_argument')
    await error(await hub.post('/ingest/unknown', watcher, {}), 404, 'not_found')
    await error(await hub.post('/ingest/generic', watcher, {}), 401, 'unauthorized')

    const send = (source: string, secret: string, dedupe_key = 'limit-test', timestamp = Date.now()) =>
      signedDelivery(hub, source, 'generic', secret, JSON.stringify({ timestamp, dedupe_key, title: 'Fixture' }))
    await error(await signedDelivery(hub, 'generic', 'generic', primarySecret, '{'), 400, 'invalid_argument')
    await error(await send('generic', primarySecret, 'stale', Date.now() - 600_000), 401, 'unauthorized')
    for (let i = 0; i < 118; i++) {
      const response = await send('generic', primarySecret)
      assert.equal(response.status, 202, `delivery ${i + 1}`)
      await response.text()
    }
    await error(await signedDelivery(hub, 'generic', 'generic', primarySecret, '{'), 429, 'rate_limited')
    await error(await send('generic', primarySecret, 'would-be-new'), 429, 'rate_limited')
    assert.equal((await send('second', secondarySecret)).status, 202)
    const { tasks, cursor } = await ok(await hub.get('/tasks', watcher))
    assert.equal(cursor, 2)
    assert.deepEqual(
      tasks.map(({ source, dedupe_key }: { source: string; dedupe_key: string }) => [source, dedupe_key]),
      [
        ['generic', 'limit-test'],
        ['second', 'limit-test'],
      ],
    )
  },
)
