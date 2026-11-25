import assert from 'node:assert/strict'
import { spawn, type ChildProcess } from 'node:child_process'
import { createHash } from 'node:crypto'
import { once } from 'node:events'
import { existsSync, mkdtempSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from 'node:fs'
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
const runner = 'db'.repeat(32)
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
    const group = process.platform !== 'win32'
    const child = spawn(process.execPath, ['--disable-warning=ExperimentalWarning', ...flags, bin, '--json', ...args], {
      cwd, detached: group, env: { ...process.env, XDG_STATE_HOME: join(cwd, 'state'), PULLCORD_URL: url, PULLCORD_KEY: key },
      stdio: preload ? ['ignore', 'pipe', 'pipe', 'ipc'] : ['ignore', 'pipe', 'pipe'],
    })
    let stdout = '', stderr = ''
    let finished = false
    child.stdout!.setEncoding('utf8').on('data', (chunk) => { stdout += chunk })
    child.stderr!.setEncoding('utf8').on('data', (chunk) => { stderr += chunk })
    const done = new Promise<CliResult>((resolve, reject) => {
      child.once('error', reject)
      child.once('close', (code, signal) => {
        finished = true
        resolve({ code, signal, stdout, stderr })
      })
    })
    t.after(async () => {
      if (!finished && child.pid) {
        try {
          if (group) process.kill(-child.pid, 'SIGKILL')
          else child.kill('SIGKILL')
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error
        }
      }
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
  const holder = ['claim', 'renew', 'complete', 'fail', 'release', 'block']
  const agent = [...holder, 'read', 'log', 'memo']
  const credentials = [
    { key: first, can: agent }, { key: second, can: [...agent, 'admin'] }, { key: runner, can: holder },
  ]
  const server = createHttpServer(store, credentials.map(({ key, can }, index) => ({
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
const wrappedWorker = process.platform === 'win32' ? worker : `
  const { spawn } = require('node:child_process');
  const child = spawn(process.execPath, ['-e', ${JSON.stringify(worker)}], { stdio: ['ignore', 'pipe', 'ignore'] });
  child.stdout.pipe(process.stdout);
  process.on('SIGINT', () => {});
  process.on('SIGTERM', () => {});
`

async function matchingLine(stream: NonNullable<ChildProcess['stdout']>, match: (line: string) => boolean) {
  const lines = createInterface({ input: stream })
  try {
    for await (const line of lines) if (match(line)) return line
  } finally {
    lines.close()
  }
  assert.fail('The command exited without the expected output.')
}

async function workerPid(t: TestContext, child: ChildProcess) {
  const pid = Number((await matchingLine(child.stderr!, (line) => /^worker \d+$/.test(line))).slice('worker '.length))
  t.after(() => {
    try {
      process.kill(pid, 'SIGKILL')
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error
    }
  })
  return pid
}

test('run executes literal command arguments, settles from exit status, and stops the wrapped worker before release', { timeout: 10_000 }, async (t) => {
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
  const running = server.start(['run', queue, '--', process.execPath, '-e', wrappedWorker])
  const pid = await workerPid(t, running.child)
  assert.equal(running.child.kill('SIGINT'), true)
  const released = success(await running.done, 130)
  assert.deepEqual([released.id, released.state, released.attempts], [interruptId, 'ready', 0])
  assert.throws(() => process.kill(pid, 0), { code: 'ESRCH' })
})

test('run renews at a third of the lease and stops its wrapped worker when renewal loses the claim', { timeout: 10_000 }, async (t) => {
  const server = await hub(t)
  const queue = 'incidents/renewal'
  const id = String(server.seed(queue, 'lose-lease'))
  const preload = `
    import { once } from 'node:events';
    import { createRequire, syncBuiltinESMExports } from 'node:module';
    const timers = createRequire('/preload.js')('node:timers/promises');
    const delay = timers.setTimeout;
    timers.setTimeout = (ms, value, options) => {
      if (ms !== 300_000) return delay(ms, value, options);
      process.send('scheduled');
      return once(process, 'message', { signal: options.signal }).then(() => value);
    };
    syncBuiltinESMExports();`
  const running = server.start(['run', queue, '--', process.execPath, '-e', wrappedWorker], first, preload)
  const scheduled = once(running.child, 'message')
  const pid = await workerPid(t, running.child)
  assert.equal(success(await server.call(['show', id])).lease_expires_at, clockStart + 900_000)
  assert.equal((await scheduled)[0], 'scheduled')
  server.setTime(clockStart + 300_000)
  const next = once(running.child, 'message')
  running.child.send('renew')
  assert.equal((await next)[0], 'scheduled')
  assert.equal(success(await server.call(['show', id])).lease_expires_at, clockStart + 1_200_000)
  server.setTime(clockStart + 1_200_000)
  running.child.send('renew')
  const result = await running.done
  assert.equal(result.code, 1)
  assert.equal(result.stdout, '')
  assert.deepEqual(result.stderr.trim().split('\n').map((line) => line.startsWith('worker ') ? line : JSON.parse(line).code),
    [`worker ${pid}`, 'claim_lost'])
  assert.throws(() => process.kill(pid, 0), { code: 'ESRCH' })
  assert.equal(success(await server.call(['show', id])).state, 'interrupted')
})

test('run by ID can claim a younger task without read access and never falls back to its queue', { timeout: 10_000 }, async (t) => {
  const server = await hub(t)
  const queue = 'incidents/exact'
  const older = server.seed(queue, 'older')
  const requested = server.seed(queue, 'younger')
  const outOfScope = server.seed('private/queue', 'out-of-scope')
  const execute = (id: number) => server.call(['run', String(id), '--', process.execPath, '-e', ''], runner)
  failure(await server.call(['show', String(requested)], runner), 'forbidden')
  const settled = success(await execute(requested))
  assert.deepEqual([settled.id, settled.state], [requested, 'completed'])
  failure(await execute(outOfScope), 'not_found')
  failure(await execute(4_001), 'not_found')
  assert.equal(success(await server.call(['show', String(older)])).state, 'ready')
})

test('log and memo need no epoch, but admin writes still enforce scope and cancel fences the holder', { timeout: 15_000 }, async (t) => {
  const server = await hub(t)
  const queue = 'incidents/oversight'
  const id = String(server.seed(queue, 'admin'))
  const outside = String(server.seed('private/queue', 'outside'))
  const logged = success(await server.call(['log', id, 'Investigating']))
  assert.equal(logged.type, 'task.logged')
  assert.deepEqual(logged.data, { text: 'Investigating' })
  assert.deepEqual(success(await server.call(['memo', id, 'approved', 'false'])), { value: false })
  assert.deepEqual(success(await server.call(['memo', id, 'approved', 'true'])), { value: false })
  assert.deepEqual(success(await server.call(['memo', id, 'approved'])), { value: false })
  failure(await server.call(['memo', id, 'number', '{"nested":[1e400]}']), 'invalid_argument')
  failure(await server.call(['memo', id, 'number']), 'not_found')
  assert.deepEqual(success(await server.call(['memo', id, 'number', '42'])), { value: 42 })
  assert.deepEqual(success(await server.call(['memo', id, 'note', 'free text'])), { value: 'free text' })
  failure(await server.call(['memo', id, 'constructor']), 'not_found')

  success(await server.call(['claim', queue]))
  success(await server.call(['block', id, 'Needs approval']))
  failure(await server.call(['unblock', id]), 'forbidden')
  assert.equal(success(await server.call(['unblock', id], second)).state, 'ready')
  success(await server.call(['claim', queue, '--lease', '10s']))
  server.setTime(clockStart + 10_000)
  assert.equal(success(await server.call(['requeue', id], second)).state, 'ready')
  success(await server.call(['claim', queue]))
  failure(await server.call(['cancel', outside, 'out of scope'], second), 'not_found')
  assert.equal(success(await server.call(['cancel', id, 'No longer needed', '--json'], second)).reason, 'No longer needed')
  failure(await server.call(['complete', id]), 'claim_lost')
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
    } else if (after === '6' && seen.length === 4) {
      res.writeHead(200, { 'Content-Length': '100' })
      res.write('{"events":[{"seq":7}', () => res.destroy())
    } else if (after === '6') res.end(JSON.stringify({ events: [{ seq: 7 }], cursor: 7 }))
    else if (after === '7') {
      res.once('close', disconnected)
      reached()
    } else if (after === '9') res.end('{"events":')
    else res.writeHead(400).end(JSON.stringify({ code: 'invalid_argument', message: 'Unexpected cursor.' }))
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
  assert.deepEqual(seen, [['4', null], ['4', '30'], ['6', '30'], ['6', '30'], ['6', '30'], ['7', '30']])
  await closed
  failure(await command.call(['events', '--after', '9', '--follow']), 'invalid_response')
  assert.deepEqual(seen.at(-1), ['9', '30'])
})

test('a setup crash leaves a lock that can be recovered without overwriting configuration', { timeout: 10_000 }, async (t) => {
  const dir = directory(t)
  const command = cli(t, dir)
  const keys = 'pullcord-keys.json'
  const lock = `${keys}.pullcord-lock`
  success(await command.call(['init']))
  const before = readFileSync(join(dir, keys), 'utf8')
  const preload = `
    import { createRequire, syncBuiltinESMExports } from 'node:module';
    const fs = createRequire('/preload.js')('node:fs');
    const open = fs.openSync;
    fs.openSync = (path, ...args) => {
      const fd = open(path, ...args);
      if (path === ${JSON.stringify(lock)}) process.kill(process.pid, 'SIGKILL');
      return fd;
    };
    syncBuiltinESMExports();`
  const args = ['keys', 'revoke', 'admin']
  const crashed = await command.start(args, first, preload).done
  assert.equal(crashed.signal, 'SIGKILL')
  const blocked = failure(await command.call(args), 'store_locked')
  assert.ok(blocked.message.includes(lock))
  assert.match(blocked.message, /stop all setup commands/i)
  assert.equal(readFileSync(join(dir, keys), 'utf8'), before)
  rmSync(join(dir, lock))
  assert.equal(success(await command.call(args)).revoked, true)
  assert.deepEqual(success(await command.call(['keys', 'list'])), { keys: [] })
})

test('init and keys issue secrets once and serve reloads revocation only on restart', { timeout: 15_000 }, async (t) => {
  const dir = directory(t)
  const db = 'team.db'
  const keys = 'team-keys.json'
  const setup = cli(t, dir, 'https://hub.example.test/pullcord')
  const initialized = success(await setup.call(['init', '--db', db, '--keys', keys]))
  assert.deepEqual([initialized.db, initialized.keys], [db, keys])
  assert.match(initialized.admin_key, /^[0-9a-f]{64}$/)
  assert.ok(existsSync(join(dir, db)))
  const before = readFileSync(join(dir, keys), 'utf8')
  const admin = JSON.parse(before)[0]
  assert.deepEqual([admin.agent, admin.queues, admin.sha256], ['admin', ['*'], createHash('sha256').update(initialized.admin_key).digest('hex')])
  failure(await setup.call(['init', '--db', 'new.db', '--keys', keys]), 'invalid_argument')
  assert.ok(!existsSync(join(dir, 'new.db')))
  assert.equal(readFileSync(join(dir, keys), 'utf8'), before)

  const created = success(await setup.call(['keys', 'create', 'watcher', '--keys', keys, '--queues', '*', '--can', 'read,events']))
  const agent = success(await setup.call(['keys', 'create', 'worker', '--keys', keys, '--queues', 'incidents/,issues']))
  assert.deepEqual(agent.queues, ['incidents/', 'issues'])
  assert.ok(agent.can.includes('claim') && !agent.can.includes('events') && !agent.can.includes('admin'))
  assert.deepEqual(created.mcp, { mcpServers: { pullcord: {
    url: 'https://hub.example.test/pullcord/mcp', headers: { Authorization: `Bearer ${created.key}` },
  } } })
  failure(await setup.call(['keys', 'create', 'watcher', '--keys', keys, '--queues', '*']), 'invalid_argument')
  const list = success(await setup.call(['keys', 'list', '--keys', keys]))
  assert.deepEqual(list.keys, [
    { agent: 'admin', queues: ['*'], can: admin.can },
    { agent: 'watcher', queues: ['*'], can: ['read', 'events'] },
    { agent: 'worker', queues: agent.queues, can: agent.can },
  ])
  assert.equal(statSync(join(dir, keys)).mode & 0o777, 0o600)
  const stored = readFileSync(join(dir, keys), 'utf8')
  for (const key of [initialized.admin_key, created.key, agent.key]) assert.ok(!stored.includes(key))

  failure(await setup.call(['serve', '--public-url', 'https://hub.example.test/?hidden=value', '--keys', keys]), 'invalid_argument')
  async function serve() {
    const server = setup.start(['serve', '--listen', '127.0.0.1:0', '--public-url', 'https://hub.example.test/pullcord', '--db', db, '--keys', keys])
    const { url } = JSON.parse(await matchingLine(server.child.stdout!, (line) => line.startsWith('{')))
    assert.match(url, /^http:\/\/127\.0\.0\.1:[1-9]\d*$/)
    return { ...cli(t, dir, url), stop: () => { server.child.kill('SIGTERM'); return server.done } }
  }
  const server = await serve()
  assert.deepEqual(success(await server.call(['tasks'], created.key)), { tasks: [], cursor: 0 })
  assert.deepEqual(success(await server.call(['tasks'], agent.key)), { tasks: [] })
  assert.equal(success(await setup.call(['keys', 'revoke', 'watcher', '--keys', keys])).restart_required, true)
  assert.deepEqual(success(await server.call(['tasks'], created.key)), { tasks: [], cursor: 0 })
  await server.stop()

  const restarted = await serve()
  failure(await restarted.call(['tasks'], created.key), 'unauthorized')
  assert.deepEqual(success(await restarted.call(['tasks'], initialized.admin_key)), { tasks: [], cursor: 0 })
  await restarted.stop()
})

test('sources add issues generated secrets but imports the PagerDuty secret without echoing it', { timeout: 10_000 }, async (t) => {
  const dir = directory(t)
  const command = cli(t, dir, 'https://hub.example.test')
  const file = join(dir, 'pullcord-sources.json')
  const github = success(await command.call(['sources', 'add', 'github', '--name', 'ci', '--queue', 'ci/failures', '--retry', 'safe']))
  assert.deepEqual([github.webhook_url, github.retry, github.restart_required], ['https://hub.example.test/ingest/ci', 'safe', true])
  assert.match(github.secret, /^[0-9a-f]{64}$/)
  const before = readFileSync(file, 'utf8')
  const pagerduty = ['sources', 'add', 'pagerduty', '--queue', 'incidents/production', '--public-url', 'https://alerts.example.test/base']
  const missing = failure(await command.call(pagerduty), 'invalid_argument')
  assert.match(missing.message, /https:\/\/alerts\.example\.test\/base\/ingest\/pagerduty/)
  assert.equal(readFileSync(file, 'utf8'), before)
  const supplied = 'pagerduty-provided-secret-+é'
  writeFileSync(join(dir, 'secret.txt'), `${supplied}\r\n`, { mode: 0o600 })
  const imported = await command.call([...pagerduty, '--secret-file', 'secret.txt'])
  const { webhook_url, secret } = success(imported)
  assert.equal(webhook_url, 'https://alerts.example.test/base/ingest/pagerduty')
  assert.equal(secret, undefined)
  assert.ok(!`${imported.stdout}${imported.stderr}`.includes(supplied))
  assert.deepEqual(JSON.parse(readFileSync(file, 'utf8')), [
    { name: 'ci', provider: 'github', queue: 'ci/failures', retry: 'safe', secret: github.secret },
    { name: 'pagerduty', provider: 'pagerduty', queue: 'incidents/production', retry: 'manual', secret: supplied },
  ])
  assert.equal(statSync(file).mode & 0o777, 0o600)
})
