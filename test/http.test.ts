import assert from 'node:assert/strict'
import { spawn, spawnSync } from 'node:child_process'
import { createHash } from 'node:crypto'
import { once } from 'node:events'
import { existsSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createInterface } from 'node:readline'
import { after, test } from 'node:test'
import { fileURLToPath } from 'node:url'
import { openStore } from '../src/store.ts'

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
        get: (path: string, key?: string) => fetch(origin + path, {
          headers: key === undefined ? {} : { Authorization: `Bearer ${key}` },
          signal: AbortSignal.timeout(5_000),
        }),
        stop: async () => { child.kill(); await done },
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

test('HTTP task reads combine their filters without exposing other queues or trusting source fields', { timeout: 10_000 }, async (t) => {
  const { db, keys, checkout, search, sibling, nested, payload } = fixture('read')
  const server = await start(db, keys)
  t.after(server.stop)

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
  const result = spawnSync(process.execPath, args(join(dir, 'invalid.db'), keys), { encoding: 'utf8' })
  assert.equal(result.status, 1)
  assert.match(result.stderr, /key file is not valid JSON/)
  assert.equal((result.stdout + result.stderr).includes(reader), false)
})
