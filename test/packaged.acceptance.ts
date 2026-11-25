import assert from 'node:assert/strict'
import { execFileSync, spawn } from 'node:child_process'
import { createHash, createHmac } from 'node:crypto'
import { once } from 'node:events'
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createInterface } from 'node:readline'
import { after, before, test } from 'node:test'
import fc from 'fast-check'
import type { TaskEvent, TaskState } from '../src/store.ts'
import { AGENTS, checkInvariants, MEMO_KEY, Model, operation, type Call, type Outcome } from './lifecycle-model.ts'

const root = mkdtempSync(join(tmpdir(), 'pullcord-packaged-'))
const installed = join(root, 'installed')
const bin = join(installed, 'node_modules', '.bin', 'pullcord')
const initialTime = 1_730_000_000_000
const watcher = 'fa'.repeat(32)
const keys = Array.from({ length: 20 }, (_, i) => (i + 1).toString(16).padStart(64, '0'))
const providers = [
  { name: 'pagerduty', provider: 'pagerduty', queue: 'incidents/pagerduty', secret: 'fixture pagerduty secret', retry: 'manual' },
  { name: 'safe', provider: 'generic', queue: 'incidents/safe', secret: 'fixture safe secret', retry: 'safe' },
  { name: 'manual', provider: 'generic', queue: 'incidents/manual', secret: 'fixture manual secret', retry: 'manual' },
] as const

interface WireTask {
  id: number
  dedupe_key: string
  state: TaskState
  holder: string | null
  epoch: number
  attempts: number
  max_attempts: number
  lease_expires_at: number | null
  ready_at: number | null
  retry: 'safe' | 'manual'
}

interface WireEvent {
  seq: number
  task_id: number
  type: string
  actor: string
  request_id: string | null
  from: TaskState | null
  to: TaskState
  data: unknown
  at: number
}

before(() => {
  const pack = JSON.parse(execFileSync('npm', ['pack', '--json', '--pack-destination', root], { encoding: 'utf8', timeout: 60_000 }))
  assert.equal(pack.length, 1)
  const files = pack[0].files.map(({ path }: { path: string }) => path) as string[]
  assert.ok(files.includes('dist/cli/index.js'))
  assert.ok(files.every((path) => /^dist\/.+\.js$/.test(path) || ['package.json', 'README.md', 'LICENSE'].includes(path)))
  execFileSync('npm', ['install', '--offline', '--ignore-scripts', '--no-audit', '--no-fund', '--prefix', installed, join(root, pack[0].filename)], { timeout: 60_000 })
})
after(() => rmSync(root, { recursive: true, force: true }))

function temporaryHub(name: string) {
  const cwd = mkdtempSync(join(root, `${name}-`))
  const clock = join(cwd, 'clock')
  let now = initialTime
  const setClock = (value: number) => {
    now = value
    writeFileSync(clock, String(value))
  }
  setClock(now)
  return { cwd, clock, setClock, now: () => now }
}

function workspace(name: string, sources: readonly object[] = providers) {
  const ws = temporaryHub(name)
  const entry = (agent: string, token: string, can: string[]) => ({
    agent, sha256: createHash('sha256').update(token).digest('hex'), queues: ['*'], can,
  })
  writeFileSync(join(ws.cwd, 'pullcord-keys.json'), JSON.stringify([
    entry('watcher', watcher, ['read', 'events']),
    ...keys.map((key, i) => entry(`worker-${i}`, key, ['claim', 'renew', 'complete', 'fail', 'release', 'block', 'log', 'memo', 'admin'])),
  ]), { mode: 0o600 })
  writeFileSync(join(ws.cwd, 'pullcord-sources.json'), JSON.stringify(sources), { mode: 0o600 })
  return ws
}

async function listen(ws: ReturnType<typeof temporaryHub>, options: string[] = []) {
  const preload = `import {readFileSync} from 'node:fs'; Date.now = () => Number(readFileSync(${JSON.stringify(ws.clock)}, 'utf8'));`
  const child = spawn(bin, ['serve', '--listen', '127.0.0.1:0', '--json', ...options], {
    cwd: ws.cwd, stdio: ['ignore', 'pipe', 'pipe'],
    env: { ...process.env, NODE_OPTIONS: `--import=data:text/javascript,${encodeURIComponent(preload)}`, NODE_NO_WARNINGS: '1' },
  })
  const exited = once(child, 'close')
  let stderr = ''
  child.stderr.setEncoding('utf8').on('data', (chunk) => { stderr += chunk })
  const lines = createInterface({ input: child.stdout })
  const startup = setTimeout(() => child.kill('SIGKILL'), 10_000)
  let url: string | undefined
  try {
    for await (const line of lines) {
      url = JSON.parse(line).url
      if (url) break
    }
    if (!url) assert.fail(`Packaged serve did not start: ${stderr}`)
  } catch (error) {
    child.kill('SIGKILL')
    await exited
    throw error
  } finally {
    clearTimeout(startup)
  }

  const stop = async (signal: NodeJS.Signals = 'SIGTERM') => {
    child.kill(signal)
    return (await exited)[1]
  }
  const request = (path: string, init: RequestInit = {}) => fetch(url + path, { ...init, signal: AbortSignal.timeout(8_000) })
  const get = (path: string, key = watcher) => request(path, { headers: { authorization: `Bearer ${key}` } })
  const post = (path: string, body: object, key = keys[0]) => request(path, {
    method: 'POST', body: JSON.stringify(body), headers: { authorization: `Bearer ${key}`, 'content-type': 'application/json' },
  })
  const signed = (name: string, header: string, prefix: string, secret: string, fixture: object, headers = {}) => {
    const body = JSON.stringify(fixture, null, 2)
    const digest = createHmac('sha256', secret).update(body).digest('hex')
    return request(`/ingest/${name}`, { method: 'POST', body, headers: { ...headers, [header]: `${prefix}${digest}`, 'content-type': 'application/json' } })
  }
  const pagerduty = (action: 'triggered' | 'resolved', secret: string = providers[0].secret) =>
    signed('pagerduty', 'X-PagerDuty-Signature', 'v1=', secret, { event: {
      id: `delivery-${action}`, event_type: `incident.${action}`, resource_type: 'incident', occurred_at: new Date(ws.now()).toISOString(),
      data: { id: 'PGR0VU2', title: 'Checkout latency', html_url: 'https://example.test/incidents/PGR0VU2' },
    } })
  const generic = (name: string, secret: string, dedupeKey: string) =>
    signed(name, 'X-Pullcord-Signature', 'sha256=', secret, { timestamp: ws.now(), dedupe_key: dedupeKey, title: 'Fixture signal' })
  const snapshot = async () => ok<{ tasks: WireTask[]; cursor: number }>(await get('/tasks'))
  const events = async (after = 0) => ok<{ events: WireEvent[]; cursor: number }>(await get(`/events?after=${after}`))
  return { url, request, get, post, pagerduty, generic, signed, snapshot, events, stop }
}

async function ok<T>(response: Response, status = 200): Promise<T> {
  const body = await response.json()
  assert.equal(response.status, status, JSON.stringify(body))
  return body as T
}

async function rejected(response: Response, status: number, code: string): Promise<void> {
  assert.equal((await ok<{ code: string }>(response, status)).code, code)
}

test('packaged serve makes one task from concurrent signed PagerDuty retries and rejects a bad signature', { timeout: 20_000 }, async (t) => {
  const hub = await listen(workspace('pagerduty'))
  t.after(() => hub.stop())
  await rejected(await hub.pagerduty('triggered', 'not the provider secret'), 401, 'unauthorized')
  assert.deepEqual(await hub.snapshot(), { tasks: [], cursor: 0 })
  await ok(await hub.pagerduty('triggered'), 202)
  await Promise.all([hub.pagerduty('triggered'), hub.pagerduty('triggered')].map(async (delivery) => ok(await delivery, 202)))
  const { tasks, cursor } = await hub.snapshot()
  assert.deepEqual(tasks.map(({ dedupe_key, state }) => [dedupe_key, state]), [['PGR0VU2', 'ready']])
  assert.equal(cursor, 1)
  assert.deepEqual((await hub.events()).events.map(({ type }) => type), ['task.created'])
})

test('exactly one of twenty agent keys gets the packaged hub’s sole ready task', { timeout: 20_000 }, async (t) => {
  const hub = await listen(workspace('race'))
  t.after(() => hub.stop())
  await ok(await hub.pagerduty('triggered'), 202)
  const responses = await Promise.all(keys.map((key, i) => hub.post('/tasks/claim', { queue: 'incidents/', request_id: `race-${i}` }, key)))
  const wins = responses.flatMap((response, i) => response.status === 200 ? [i] : [])
  assert.equal(wins.length, 1)
  const [winner] = wins
  const claimed = await ok<WireTask>(responses[winner])
  assert.deepEqual([claimed.id, claimed.epoch, claimed.attempts, claimed.holder], [1, 1, 1, `agent:worker-${winner}`])
  for (const [i, response] of responses.entries()) if (i !== winner) await rejected(response, 409, 'nothing_ready')
  assert.deepEqual((await hub.events(1)).events.map(({ type, actor, request_id }) => [type, actor, request_id]),
    [['task.claimed', claimed.holder, `race-${winner}`]])
})

test('packaged serve fences a stalled agent and does not automatically requeue a manual task', { timeout: 20_000 }, async (t) => {
  const ws = workspace('expiry')
  const hub = await listen(ws)
  t.after(() => hub.stop())
  await ok(await hub.generic('safe', providers[1].secret, 'safe'), 202)
  await ok(await hub.generic('manual', providers[2].secret, 'manual'), 202)
  const claim = async (queue: string, key: string, request_id: string) =>
    ok<WireTask>(await hub.post('/tasks/claim', { queue, request_id, lease: 10 }, key))
  const stale = await claim('incidents/safe', keys[0], 'safe-a')
  const manual = await claim('incidents/manual', keys[0], 'manual-a')
  ws.setClock(initialTime + 9_999)
  assert.deepEqual((await hub.snapshot()).tasks.map(({ state }) => state), ['claimed', 'claimed'])
  ws.setClock(initialTime + 10_000)
  const fresh = await claim('incidents/safe', keys[1], 'safe-b')
  assert.deepEqual([fresh.id, fresh.epoch, fresh.holder], [stale.id, stale.epoch + 1, 'agent:worker-1'])
  const before = await hub.snapshot()
  await rejected(await hub.post(`/tasks/${stale.id}/complete`, { request_id: 'late', epoch: stale.epoch }, keys[0]), 409, 'claim_lost')
  assert.deepEqual(await hub.snapshot(), before)
  assert.equal((await ok<WireTask>(await hub.post(`/tasks/${fresh.id}/complete`, { request_id: 'finish', epoch: fresh.epoch }, keys[1]))).state, 'completed')
  await rejected(await hub.post('/tasks/claim', { id: manual.id, request_id: 'no-auto-retry' }, keys[1]), 409, 'nothing_ready')
  assert.equal((await ok<WireTask>(await hub.get(`/tasks/${manual.id}`))).state, 'interrupted')
})

test('a signed source cancellation clears a held claim on packaged serve', { timeout: 20_000 }, async (t) => {
  const hub = await listen(workspace('cancellation'))
  t.after(() => hub.stop())
  await ok(await hub.pagerduty('triggered'), 202)
  const task = await ok<WireTask>(await hub.post('/tasks/claim', { queue: 'incidents/', request_id: 'claim' }))
  await ok(await hub.pagerduty('resolved'), 202)
  await ok(await hub.pagerduty('resolved'), 202)
  const actual = await ok<WireTask>(await hub.get(`/tasks/${task.id}`))
  assert.deepEqual([actual.state, actual.holder, actual.lease_expires_at], ['canceled', null, null])
  await rejected(await hub.post(`/tasks/${task.id}/renew`, { request_id: 'stale-renew', epoch: task.epoch }), 409, 'claim_lost')
  const { events } = await hub.events(2)
  assert.deepEqual(events.map(({ type, actor, request_id, from, to }) => [type, actor, request_id, from, to]),
    [['task.canceled', 'source:pagerduty', 'delivery-resolved', 'claimed', 'canceled']])
})

test('the packaged watcher resumes from its durable cursor after SIGKILL', { timeout: 20_000 }, async (t) => {
  const ws = workspace('watch')
  const first = await listen(ws)
  t.after(() => first.stop())
  await ok(await first.pagerduty('triggered'), 202)
  const { cursor } = await first.snapshot()
  const task = await ok<WireTask>(await first.post('/tasks/claim', { queue: 'incidents/', request_id: 'claim' }))
  await ok(await first.post(`/tasks/${task.id}/memo`, { request_id: 'decision', key: 'approved', value: null }))
  await ok(await first.post(`/tasks/${task.id}/complete`, { request_id: 'finish', epoch: task.epoch }))
  assert.equal(await first.stop('SIGKILL'), 'SIGKILL')

  const restarted = await listen(ws)
  t.after(() => restarted.stop())
  await ok(await restarted.pagerduty('triggered'), 202)
  assert.deepEqual(await ok(await restarted.post(`/tasks/${task.id}/memo`, { request_id: 'decision', key: 'approved', value: null })), { value: null })
  const catchup = await restarted.events(cursor)
  assert.deepEqual(catchup.events.map(({ seq, type, request_id }) => [seq, type, request_id]), [
    [2, 'task.claimed', 'claim'], [3, 'task.memo', 'decision'], [4, 'task.completed', 'finish'],
  ])
  assert.equal(catchup.cursor, 4)
  assert.deepEqual(await restarted.events(catchup.cursor), { events: [], cursor: 4 })
})

const modelQueue = 'incidents/model'
const modelSources = [providers[1], providers[2]].map((source) => ({ ...source, queue: modelQueue }))

async function send(hub: Awaited<ReturnType<typeof listen>>, call: Call): Promise<Outcome> {
  const { t, id, epoch, retry, text, value } = call
  const lease = call.lease / 1000
  const body = {
    claim: { queue: modelQueue, lease }, renew: { epoch, lease }, complete: { epoch, result: 'done' },
    fail: { epoch, reason: 'broke', retry }, release: { epoch }, block: { epoch, reason: 'approve?' },
    unblock: {}, requeue: {}, cancel: { reason: 'resolved' }, log: { text }, memo: { key: MEMO_KEY, value },
  }[t]
  const path = t === 'claim' ? '/tasks/claim' : `/tasks/${id}/${t}`
  const response = await hub.post(path, { ...body, request_id: call.requestId }, keys[AGENTS.indexOf(call.actor)])
  const data = await response.json()
  if (!response.ok) {
    assert.equal(response.status, data.code === 'not_found' ? 404 : 409, JSON.stringify(data))
    return data.code === 'nothing_ready' && t === 'claim' ? { ok: null } : { code: data.code }
  }
  assert.equal(response.status, 200)
  if (t === 'memo') return { ok: { value: data.value } }
  if (t === 'log') {
    assert.equal(data.type, 'task.logged')
    return { ok: data.data }
  }
  return { ok: { id: data.id, state: data.state, epoch: data.epoch } }
}

test('generated sequences match the lifecycle model over the packaged HTTP API after every step', { timeout: 120_000 }, async () => {
  await fc.assert(fc.asyncProperty(fc.array(operation, { minLength: 12, maxLength: 44, size: 'large' }), async (operations) => {
    const ws = workspace('model', modelSources)
    const hub = await listen(ws)
    const model = new Model()
    const log: TaskEvent[] = []
    let cursor = 0
    const seed = [{ t: 'ingest', key: 0 }, { t: 'ingest', key: 1 }] as const
    try {
      for (const [i, step] of [...seed, ...operations].entries()) {
        let changed = false
        if (step.t === 'advance') {
          ws.setClock(ws.now() + step.ms)
        } else if (step.t === 'ingest') {
          const source = modelSources[step.key % 2]
          await ok(await hub.generic(source.name, source.secret, `k${step.key}`), 202)
          changed = model.ingest(step.key, source.retry, 3, ws.now())
        } else {
          const call = model.call(step, i, 1000)
          const expected = model.apply(call, ws.now())
          assert.deepEqual(await send(hub, call), expected.outcome, `step ${i}: ${JSON.stringify(call)}`)
          changed = expected.changed
        }

        const snapshot = await hub.snapshot()
        const page = await hub.events(cursor)
        assert.equal(page.cursor, snapshot.cursor)
        assert.equal(page.events.filter(({ type }) => type !== 'task.lease_expired').length, changed ? 1 : 0, `step ${i}: ${step.t}`)
        log.push(...page.events.map(({ task_id, request_id, ...event }) => ({ ...event, taskId: task_id, requestId: request_id })))
        cursor = page.cursor
        const tasks = await Promise.all(snapshot.tasks.map(async ({ id, max_attempts, lease_expires_at, ready_at, ...task }) => {
          const { memos } = await ok<{ memos: Record<string, unknown> }>(await hub.get(`/tasks/${id}`))
          return { ...task, id, maxAttempts: max_attempts, leaseExpiresAt: lease_expires_at, readyAt: ready_at, memos }
        }))
        checkInvariants(model, ws.now(), tasks, log)
      }
    } finally {
      await hub.stop()
      rmSync(ws.cwd, { recursive: true, force: true })
    }
  }), { numRuns: 32 })
})

test('the installed CLI takes a new hub from init to its first claimed task and configures an MCP agent', { timeout: 20_000 }, async (t) => {
  const ws = temporaryHub('onboarding')
  const origin = 'https://public.example.test'
  const publicUrl = `${origin}/pullcord`
  const cli = <T>(args: string[], url = publicUrl, key = '') => JSON.parse(execFileSync(bin, ['--json', ...args], {
    cwd: ws.cwd, encoding: 'utf8', timeout: 5_000,
    env: { ...process.env, NODE_NO_WARNINGS: '1', PULLCORD_URL: url, PULLCORD_KEY: key, XDG_STATE_HOME: join(ws.cwd, 'state') },
  })) as T
  const { admin_key } = cli<{ admin_key: string }>(['init'])
  const agent = cli<{ key: string; mcp: { mcpServers: { pullcord: { url: string; headers: { Authorization: string } } } } }>(
    ['keys', 'create', 'demo-agent', '--queues', 'demo/'])
  const source = (provider: string) => cli<{ secret: string; webhook_url: string }>(
    ['sources', 'add', provider, '--queue', `demo/${provider}`, '--public-url', publicUrl])
  const generic = source('generic')
  const github = source('github')
  assert.equal(generic.webhook_url, `${publicUrl}/ingest/generic`)
  assert.equal(github.webhook_url, `${publicUrl}/ingest/github`)
  const mcp = agent.mcp.mcpServers.pullcord
  assert.equal(mcp.url, `${publicUrl}/mcp`)

  const hub = await listen(ws, ['--public-url', publicUrl])
  t.after(() => hub.stop())
  assert.match(hub.url, /^http:\/\/127\.0\.0\.1:[1-9]\d*$/)
  await ok(await hub.generic('generic', generic.secret, 'first-task'), 202)
  const claimed = cli<WireTask>(['claim', 'demo/generic'], hub.url, agent.key)
  assert.deepEqual([claimed.state, claimed.dedupe_key, claimed.holder], ['claimed', 'first-task', 'agent:demo-agent'])
  assert.equal(cli<WireTask>(['complete', String(claimed.id), 'verified'], hub.url, agent.key).state, 'completed')

  await ok(await hub.signed('github', 'X-Hub-Signature-256', 'sha256=', github.secret, {
    action: 'opened', issue: { id: 109, title: 'Checkout broken', created_at: new Date(ws.now()).toISOString(), html_url: 'https://example.test/issues/109' },
  }, { 'X-GitHub-Event': 'issues' }), 202)
  const headers = { ...mcp.headers, Origin: origin, 'Content-Type': 'application/json', Accept: 'application/json, text/event-stream' }
  const rpc = (message: object) => hub.request('/mcp', { method: 'POST', headers, body: JSON.stringify({ jsonrpc: '2.0', ...message }) })
  const init = await rpc({ id: 1, method: 'initialize', params: { protocolVersion: '2025-11-25', capabilities: {}, clientInfo: { name: 'fixture', version: '1' } } })
  const session = init.headers.get('MCP-Session-Id')
  assert.ok(session)
  const { result } = await ok<{ result: { protocolVersion: string } }>(init)
  Object.assign(headers, { 'MCP-Session-Id': session, 'MCP-Protocol-Version': result.protocolVersion })
  assert.equal((await rpc({ method: 'notifications/initialized' })).status, 202)
  const call = async (id: number, name: string, args: object) => {
    const response = await rpc({ id, method: 'tools/call', params: { name, arguments: args } })
    const { result } = await ok<{ result: { isError: boolean; structuredContent: WireTask } }>(response)
    assert.equal(result.isError, false, JSON.stringify(result.structuredContent))
    return result.structuredContent
  }
  const issue = await call(2, 'claim', { queue: 'demo/github' })
  assert.deepEqual([issue.state, issue.dedupe_key], ['claimed', 'issue:109'])
  assert.equal((await call(3, 'complete', { id: issue.id })).state, 'completed')
  assert.deepEqual((await ok<{ tasks: WireTask[] }>(await hub.get('/tasks', admin_key))).tasks.map(({ state }) => state), ['completed', 'completed'])
})
