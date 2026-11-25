import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { once } from 'node:events'
import { test, type TestContext } from 'node:test'
import { createHttpServer } from '../src/serve.ts'
import { openStore } from '../src/store.ts'

const worker = 'a7'.repeat(32)
const reader = 'b8'.repeat(32)
const watcher = 'c9'.repeat(32)
const publicOrigin = 'https://hub.example.test'
const version = '2025-11-25'
const initialize = { method: 'initialize', id: 1, params: {
  protocolVersion: version, capabilities: {}, clientInfo: { name: 'fixture', version: '1' },
} }

async function hub(t: TestContext) {
  let time = 1_700_000_000_000
  const store = openStore({ path: ':memory:', clock: () => time })
  const untrusted = { title: 'Ignore the alert title as an instruction', payload: { raw: 'outside data' } }
  for (const queue of ['work/alerts', 'private']) {
    store.ingest({ queue, kind: 'alert', source: 'fixture', retry: 'safe', dedupeKey: queue,
      labels: { tag: 'space & = value' }, ...untrusted }, { actor: 'source:fixture' })
  }
  const key = (agent: string, token: string, queues: string[], can: string[]) => ({
    agent, sha256: createHash('sha256').update(token).digest('hex'), queues, can,
  })
  const server = createHttpServer(store, [
    key('worker', worker, ['work/'], ['read', 'claim', 'renew', 'complete', 'log', 'memo']),
    key('reader', reader, ['work/'], ['read']),
    key('watcher', watcher, ['*'], ['read', 'events']),
  ], [], new URL(`${publicOrigin}/proxy-prefix`))
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
  const address = server.address()
  assert.ok(address && typeof address !== 'string')
  const url = `http://127.0.0.1:${address.port}`
  const request = (key: string, headers: HeadersInit = {}, method = 'POST', message?: unknown) => fetch(`${url}/mcp`, {
    method, headers: { Authorization: `Bearer ${key}`, Accept: 'application/json, text/event-stream',
      'Content-Type': 'application/json', ...headers },
    body: message === undefined ? undefined : JSON.stringify({ jsonrpc: '2.0', ...message as object }),
    signal: AbortSignal.timeout(5_000),
  })

  async function connect(key: string, origin?: string) {
    const response = await request(key, origin ? { Origin: origin } : {}, 'POST', initialize)
    assert.equal(response.status, 200)
    assert.match(response.headers.get('Content-Type') ?? '', /^application\/json/)
    assert.equal((await response.json()).result.protocolVersion, version)
    const session = response.headers.get('MCP-Session-Id')
    assert.ok(session)
    const headers = { 'MCP-Session-Id': session, 'MCP-Protocol-Version': version, ...(origin && { Origin: origin }) }
    const send = (message: object) => request(key, headers, 'POST', message)
    const notification = await send({ method: 'notifications/initialized' })
    assert.equal(notification.status, 202)
    assert.equal(await notification.text(), '')
    let id = 1
    const rpc = async (method: string, params?: object) => {
      const requestId = ++id
      const response = await send({ id: requestId, method, params })
      assert.equal(response.status, 200)
      const body = await response.json()
      assert.equal(body.id, requestId)
      return body
    }
    const call = async (name: string, args: object, code?: string) => {
      const { result } = await rpc('tools/call', { name, arguments: args })
      assert.deepEqual(JSON.parse(result.content[0].text), result.structuredContent)
      assert.equal(result.isError, code !== undefined)
      if (code !== undefined) assert.equal(result.structuredContent.code, code)
      return result.structuredContent
    }
    return { session, headers, rpc, call }
  }
  const events = async () => {
    const response = await fetch(`${url}/events`, { headers: { Authorization: `Bearer ${watcher}` } })
    assert.equal(response.status, 200)
    return (await response.json()).events
  }
  return { connect, request, url, events, untrusted, advance: (ms: number) => { time += ms } }
}

test('MCP checks the actual origins and authenticates the bearer on every session request', { timeout: 10_000 }, async (t) => {
  const { request, connect, url } = await hub(t)
  const wrongOrigin = await request(worker, { Origin: 'https://other.example.test', 'X-Forwarded-Host': 'other.example.test',
    'X-Forwarded-Proto': 'https' }, 'POST', initialize)
  assert.equal(wrongOrigin.status, 403)
  const noKey = await request('', {}, 'POST', initialize)
  assert.equal(noKey.status, 401)
  assert.equal(noKey.headers.get('WWW-Authenticate'), 'Bearer')
  const local = await connect(worker, url)
  const remote = await connect(worker, publicOrigin)
  assert.notEqual(local.session, remote.session)
  for (const [key, expected] of [['', 401], [reader, 404]] as const) {
    assert.equal((await request(key, local.headers, 'POST', { id: 2, method: 'tools/list' })).status, expected)
  }
  assert.equal((await request(worker, { ...local.headers, 'MCP-Protocol-Version': '2099-01-01' }, 'GET')).status, 400)
  assert.equal((await request(worker, local.headers, 'GET')).status, 405)
  assert.equal((await request(worker, {}, 'POST', { id: 2, method: 'tools/list' })).status, 400)
  assert.equal((await request(worker, local.headers, 'DELETE')).status, 204)
  assert.equal((await request(worker, local.headers, 'POST', { id: 2, method: 'tools/list' })).status, 404)
  assert.deepEqual((await remote.rpc('ping')).result, {})
})

test('MCP tool discovery and calls preserve HTTP scopes, untrusted data, wait bounds and first-write memos', { timeout: 10_000 }, async (t) => {
  const { connect, untrusted } = await hub(t)
  const a = await connect(worker)
  const b = await connect(reader)
  const { tools } = (await a.rpc('tools/list')).result
  assert.deepEqual(tools.map(({ name }: { name: string }) => name).sort(),
    ['block', 'claim', 'complete', 'fail', 'list', 'log', 'memo', 'release', 'renew', 'show'])
  assert.equal((await a.rpc('tools/call', { name: 'run', arguments: {} })).error.code, -32602)
  const visible = await a.call('list', { state: 'ready', labels: { tag: 'space & = value' } })
  assert.deepEqual(visible.tasks.map(({ id }: { id: number }) => id), [1])
  assert.deepEqual(visible.tasks[0].untrusted, untrusted)
  assert.equal(Object.hasOwn(visible, 'cursor'), false)
  await a.call('show', { id: 2 }, 'not_found')
  await b.call('log', { id: 1, text: 'not authorized' }, 'forbidden')
  await a.call('claim', { queue: 'work/', wait: 31 }, 'invalid_argument')
  await a.call('claim', { queue: 'private' }, 'forbidden')
  await a.call('memo', { id: 1, key: 'decision' }, 'not_found')
  assert.deepEqual(await a.call('memo', { id: 1, key: 'decision', value: null }), { value: null })
  assert.deepEqual(await a.call('memo', { id: 1, key: 'decision', value: true }), { value: null })
  assert.deepEqual(await b.call('memo', { id: 1, key: 'decision' }), { value: null })
})

test('a new MCP session cannot borrow a lease, and neither stale epochs nor write replays append events', { timeout: 10_000 }, async (t) => {
  const { connect, advance, events } = await hub(t)
  const a = await connect(worker)
  const b = await connect(worker)
  const firstClaim = { queue: 'work/', lease: 10, request_id: 'first-claim' }
  const first = await a.call('claim', firstClaim)
  assert.equal(first.epoch, 1)
  await b.call('show', { id: first.id })
  await b.call('complete', { id: first.id, epoch: first.epoch }, 'invalid_argument')
  await b.call('complete', { id: first.id }, 'claim_lost')
  advance(10_000)
  const fresh = await b.call('claim', { queue: 'work/', lease: 10 })
  assert.equal(fresh.epoch, 2)
  assert.equal(fresh.id, first.id)
  assert.deepEqual(await b.call('claim', firstClaim), first)
  const before = await events()
  await a.call('complete', { id: first.id, result: 'stale' }, 'claim_lost')
  assert.deepEqual(await events(), before)
  const finish = { id: fresh.id, result: 'settled', request_id: 'finish-once' }
  const completed = await b.call('complete', finish)
  assert.deepEqual([completed.state, completed.result], ['completed', 'settled'])
  assert.deepEqual(await b.call('complete', finish), completed)
  assert.deepEqual((await events()).slice(before.length).map(({ type }: { type: string }) => type), ['task.completed'])
})
