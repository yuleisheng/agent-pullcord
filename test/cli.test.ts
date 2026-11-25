import assert from 'node:assert/strict'
import { spawn, type ChildProcess } from 'node:child_process'
import { createHash } from 'node:crypto'
import { once } from 'node:events'
import { mkdtempSync, readFileSync, readdirSync, rmSync, statSync } from 'node:fs'
import { createServer, type Server } from 'node:http'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createInterface } from 'node:readline'
import { test, type TestContext } from 'node:test'
import { fileURLToPath } from 'node:url'
import { createHttpServer } from '../src/serve.ts'
import { openStore } from '../src/store.ts'

const bin = fileURLToPath(new URL('../src/cli/index.ts', import.meta.url))
const first = 'a4'.repeat(32)
const second = 'b1'.repeat(32)
const clockStart = 2_009_000

function directory(t: TestContext) {
  const dir = mkdtempSync(join(tmpdir(), 'pullcord-cli-'))
  t.after(() => rmSync(dir, { recursive: true, force: true }))
  return dir
}

type CliResult = { code: number | null; signal: NodeJS.Signals | null; stdout: string; stderr: string }

function cli(t: TestContext, cwd: string, url?: string) {
  function start(args: string[], key = first, preload?: string) {
    const flags = preload ? ['--import', `data:text/javascript,${encodeURIComponent(preload)}`] : []
    const child = spawn(process.execPath, ['--disable-warning=ExperimentalWarning', ...flags, bin, '--json', ...args], {
      cwd, env: { ...process.env, XDG_STATE_HOME: join(cwd, 'state'), PULLCORD_URL: url, PULLCORD_KEY: key },
      stdio: preload ? ['ignore', 'pipe', 'pipe', 'ipc'] : ['ignore', 'pipe', 'pipe'],
    })
    let stdout = '', stderr = ''
    child.stdout!.setEncoding('utf8').on('data', (chunk) => { stdout += chunk })
    child.stderr!.setEncoding('utf8').on('data', (chunk) => { stderr += chunk })
    const done = new Promise<CliResult>((resolve, reject) => {
      child.once('error', reject)
      child.once('close', (code, signal) => resolve({ code, signal, stdout, stderr }))
    })
    t.after(async () => {
      if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL')
      await done
    })
    return { child, done }
  }
  return { start, call: (args: string[], key?: string) => start(args, key).done }
}

function success(result: CliResult, code = 0) {
  assert.equal(result.code, code, result.stderr)
  assert.equal(result.signal, null)
  return JSON.parse(result.stdout)
}

function failure(result: CliResult, code: string) {
  assert.equal(result.code, 1)
  assert.equal(result.stdout, '')
  const error = JSON.parse(result.stderr)
  assert.equal(error.code, code)
  assert.ok(error.message)
  return error
}

async function listen(t: TestContext, server: Server) {
  const listening = once(server, 'listening')
  server.listen(0, '127.0.0.1')
  await listening
  t.after(() => new Promise<void>((resolve) => {
    server.close(() => resolve())
    server.closeAllConnections()
  }))
  const address = server.address()
  assert.ok(address && typeof address !== 'string')
  return `http://127.0.0.1:${address.port}`
}

async function hub(t: TestContext) {
  const dir = directory(t)
  let now = clockStart
  const store = openStore({ path: join(dir, 'hub.db'), clock: () => now })
  const can = ['read', 'claim', 'renew', 'complete', 'fail', 'release', 'block']
  const server = createHttpServer(store, [first, second].map((key, index) => ({
    agent: `worker-${index}`, sha256: createHash('sha256').update(key).digest('hex'), queues: ['incidents/'], can,
  })))
  server.once('close', () => store.close())
  const url = await listen(t, server)
  return {
    ...cli(t, dir, url), dir, setTime: (time: number) => { now = time },
    seed: (queue: string, dedupe: string, retry: 'safe' | 'manual' = 'manual', labels?: Record<string, string>) =>
      store.ingest({ queue, kind: 'incident', source: 'fake', dedupeKey: dedupe, retry, labels, title: 'Outside text' }, { actor: 'source:fake' }).task.id,
  }
}

test('a CLI claim races under the server fence and saves a private epoch for another invocation', { timeout: 10_000 }, async (t) => {
  const server = await hub(t)
  const queue = 'incidents/checkout + billing'
  server.seed(queue, 'older', 'safe', { team: 'payments', signal: 'other' })
  const task = server.seed(queue, 'match', 'safe', { team: 'payments', signal: 'x=y' })
  const id = String(task)
  const claimArgs = ['claim', 'incidents/', '--label', 'team=payments', '--label', 'signal=x=y', '--wait', '0s', '--lease', '10s']
  const contenders = [first, second]
  const outcomes = await Promise.all(contenders.map((key) => server.call(claimArgs, key)))
  const winner = outcomes.findIndex((result) => result.code === 0)
  assert.notEqual(winner, -1)
  const loser = 1 - winner
  const claimed = success(outcomes[winner])
  assert.equal(claimed.id, task)
  assert.equal(claimed.lease_expires_at, clockStart + 10_000)
  failure(outcomes[loser], 'nothing_ready')
  failure(await server.call(['complete', id], contenders[loser]), 'invalid_argument')
  failure(await server.call(['tasks'], 'cc'.repeat(32)), 'unauthorized')

  const cache = join(server.dir, 'state', 'pullcord')
  const names = readdirSync(cache)
  assert.equal(names.length, 1)
  assert.equal(statSync(cache).mode & 0o777, 0o700)
  assert.equal(statSync(join(cache, names[0])).mode & 0o777, 0o600)
  assert.equal(readFileSync(join(cache, names[0]), 'utf8').trim(), '1')
  assert.ok(!names[0].includes(contenders[winner]))
  const listed = success(await server.call(['tasks', '--queue', queue, '--state', 'claimed']))
  assert.deepEqual(listed.tasks.map(({ id }: { id: number }) => id), [task])
  const detail = success(await server.call(['show', id]))
  assert.deepEqual(detail.untrusted, { title: 'Outside text', payload: null })

  server.setTime(clockStart + 10_000)
  assert.equal(success(await server.call(claimArgs, contenders[loser])).epoch, 2)
  failure(await server.call(['complete', id, 'stale result'], contenders[winner]), 'claim_lost')
  const renewed = success(await server.call(['renew', id, '--lease', '2m'], contenders[loser]))
  assert.equal(renewed.lease_expires_at, clockStart + 130_000)
  const completed = success(await server.call(['complete', id, 'fresh result'], contenders[loser]))
  assert.deepEqual([completed.state, completed.result, completed.epoch], ['completed', 'fresh result', 2])
})

test('release, retry and block pass their distinct holder operations to the server', { timeout: 10_000 }, async (t) => {
  const server = await hub(t)
  const queue = 'incidents/work'
  const id = String(server.seed(queue, 'task'))
  const claim = () => server.call(['claim', queue])
  success(await claim())
  const released = success(await server.call(['release', id]))
  assert.deepEqual([released.state, released.attempts], ['ready', 0])
  success(await claim())
  const failed = success(await server.call(['fail', id, 'retriable', '--retry']))
  assert.deepEqual([failed.state, failed.attempts, failed.reason], ['ready', 1, 'retriable'])
  success(await claim())
  const blocked = success(await server.call(['block', id, 'Proceed without a backup?']))
  assert.deepEqual([blocked.state, blocked.attempts, blocked.reason], ['blocked', 1, 'Proceed without a backup?'])
})

const worker = `console.log('worker ' + process.pid); setInterval(() => {}, 10_000)`

async function workerPid(child: ChildProcess) {
  const lines = createInterface({ input: child.stderr! })
  try {
    for await (const line of lines) {
      const match = /^worker (\d+)$/.exec(line)
      if (match) return Number(match[1])
    }
  } finally {
    lines.close()
  }
  assert.fail('The command never started.')
}

test('run executes literal command arguments, settles from exit status, and releases only after Ctrl-C stops the child', { timeout: 10_000 }, async (t) => {
  const server = await hub(t)
  const queue = 'incidents/run'
  const id = server.seed(queue, 'success')
  const succeeded = await server.call(['run', queue, '--', process.execPath, '-e',
    'console.log(JSON.stringify([JSON.parse(process.env.PULLCORD_TASK).id, process.argv.slice(1)]))', '--', '--json', '$(not a command)'])
  assert.equal(success(succeeded).state, 'completed')
  assert.deepEqual(JSON.parse(succeeded.stderr), [id, ['--json', '$(not a command)']])

  server.seed(queue, 'failure')
  const failed = success(await server.call(['run', queue, '--', process.execPath, '-e', 'process.exit(23)']), 23)
  assert.deepEqual([failed.state, failed.reason], ['failed', 'Command exited with status 23.'])

  const interruptId = server.seed(queue, 'interrupt')
  const running = server.start(['run', queue, '--', process.execPath, '-e', worker])
  const pid = await workerPid(running.child)
  assert.equal(running.child.kill('SIGINT'), true)
  const released = success(await running.done, 130)
  assert.deepEqual([released.id, released.state, released.attempts], [interruptId, 'ready', 0])
  assert.throws(() => process.kill(pid, 0), { code: 'ESRCH' })
})

test('run stops its child when its renewal is fenced and never attempts a settlement', { timeout: 10_000 }, async (t) => {
  const server = await hub(t)
  const queue = 'incidents/renewal'
  const id = String(server.seed(queue, 'lose-lease'))
  const preload = `
    import { once } from 'node:events';
    import { createRequire, syncBuiltinESMExports } from 'node:module';
    const timers = createRequire('/preload.js')('node:timers/promises');
    const delay = timers.setTimeout;
    timers.setTimeout = (ms, value, options) => ms === 300_000
      ? once(process, 'message', { signal: options.signal }).then(() => value)
      : delay(ms, value, options);
    syncBuiltinESMExports();`
  const running = server.start(['run', queue, '--', process.execPath, '-e', worker], first, preload)
  const pid = await workerPid(running.child)
  assert.equal(success(await server.call(['show', id])).lease_expires_at, clockStart + 900_000)
  server.setTime(clockStart + 900_000)
  running.child.send('renew')
  const result = await running.done
  assert.equal(result.code, 1)
  assert.equal(result.stdout, '')
  assert.deepEqual(result.stderr.trim().split('\n').map((line) => line.startsWith('worker ') ? line : JSON.parse(line).code),
    [`worker ${pid}`, 'claim_lost'])
  assert.throws(() => process.kill(pid, 0), { code: 'ESRCH' })
  assert.equal(success(await server.call(['show', id])).state, 'interrupted')
})

test('run by ID claims that exact task without reading other tasks or falling back to the queue', { timeout: 10_000 }, async (t) => {
  const writes: Record<string, unknown>[] = []
  const url = await listen(t, createServer(async (req, res) => {
    let text = ''
    for await (const chunk of req.setEncoding('utf8')) text += chunk
    const body = text ? JSON.parse(text) : {}
    writes.push(body)
    res.setHeader('Content-Type', 'application/json')
    if (req.method === 'POST' && req.url === '/tasks/claim' && body.id === 8 && body.queue === undefined) {
      res.end(JSON.stringify({ id: 8, epoch: 4, queue: 'incidents/exact', state: 'claimed' }))
    } else if (req.method === 'POST' && req.url === '/tasks/8/complete' && body.epoch === 4) {
      res.end(JSON.stringify({ id: 8, epoch: 4, queue: 'incidents/exact', state: 'completed' }))
    } else {
      res.writeHead(404).end(JSON.stringify({ code: 'not_found', message: 'No visible task.' }))
    }
  }))
  const command = cli(t, directory(t), url)
  const result = success(await command.call(['run', '8', '--', process.execPath, '-e', '']), 0)
  assert.deepEqual([result.id, result.state], [8, 'completed'])
  failure(await command.call(['run', '99', '--', process.execPath, '-e', 'console.log("must not run")']), 'not_found')
  const ids = writes.map(({ request_id }) => request_id)
  assert.equal(writes.length, 3)
  assert.equal(new Set(ids).size, 3)
  assert.ok(ids.every((id) => typeof id === 'string' && id.length > 0))
})

test('events --follow emits in order, retries the last emitted cursor, and aborts the long poll on Ctrl-C', { timeout: 10_000 }, async (t) => {
  let reached: () => void = () => {}
  let disconnected: () => void = () => {}
  const waiting = new Promise<void>((resolve) => { reached = resolve })
  const closed = new Promise<void>((resolve) => { disconnected = resolve })
  const seen: (string | null)[][] = []
  const url = await listen(t, createServer((req, res) => {
    const query = new URL(req.url!, 'http://test').searchParams
    const after = query.get('after')
    const wait = query.get('wait')
    seen.push([after, wait])
    res.setHeader('Content-Type', 'application/json')
    if (after === '4') res.end(JSON.stringify({ events: [{ seq: 5 }, { seq: 6 }], cursor: 6 }))
    else if (after === '6' && seen.length === 3) {
      res.writeHead(503).end(JSON.stringify({ code: 'internal_error', message: 'Temporarily restarting.' }))
    } else if (after === '6') res.end(JSON.stringify({ events: [{ seq: 7 }], cursor: 7 }))
    else if (after === '7') {
      res.once('close', disconnected)
      reached()
    } else res.writeHead(400).end(JSON.stringify({ code: 'invalid_argument', message: 'Unexpected cursor.' }))
  }))
  const command = cli(t, directory(t), url)
  assert.deepEqual(success(await command.call(['events', '--after', '4'])), { events: [{ seq: 5 }, { seq: 6 }], cursor: 6 })
  const following = command.start(['events', '--after', '4', '--follow'])
  await Promise.race([waiting, following.done.then((result) => assert.fail(result.stderr))])
  following.child.kill('SIGINT')
  const stopped = await following.done
  assert.equal(stopped.code, 130)
  assert.equal(stopped.stderr, '')
  assert.deepEqual(stopped.stdout.trim().split('\n').map((line) => JSON.parse(line)), [{ seq: 5 }, { seq: 6 }, { seq: 7 }])
  assert.deepEqual(seen, [['4', null], ['4', '30'], ['6', '30'], ['6', '30'], ['7', '30']])
  await closed
})
