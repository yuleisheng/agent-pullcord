#!/usr/bin/env node
import { constants } from 'node:os'
import { setTimeout as delay } from 'node:timers/promises'
import { parseArgs, type ParseArgsOptionsConfig } from 'node:util'
import { StoreError } from '../store-contract.ts'
import { CliError, Client, invalid } from './client.ts'
import { run } from './run.ts'

const usage = `Usage: pullcord [--json] <command>

Setup:
  init [--db <path>]
  serve [--listen <addr>] [--public-url <url>]
  sources add <pagerduty|github|generic> --queue <queue> [--retry safe|manual]
  keys create <agent> --queues <scope[,scope...]> [--can <action[,action...]>]
  keys list | keys revoke <agent>

Work:
  claim <queue> [--label k=v] [--wait 30s] [--lease 15m]
  renew <id> [--lease 15m]
  complete <id> [result] | fail <id> <reason> [--retry]
  release <id> | block <id> <question>
  run <queue|id> -- <command> [args...]
  log <id> <note> | memo <id> <key> [value]

Oversight:
  tasks [--queue <name>] [--state <state>] | show <id>
  unblock <id> | requeue <id> | cancel <id> <reason>
  events [--after <cursor>] [--follow]

Remote commands use PULLCORD_URL and PULLCORD_KEY.`

function options<T extends ParseArgsOptionsConfig>(args: string[], schema: T, min: number, max = min) {
  let parsed
  try {
    parsed = parseArgs({ args, options: schema, allowPositionals: true })
  } catch (error) {
    invalid(error instanceof Error ? error.message : 'Check the command options.')
  }
  if (parsed.positionals.length < min || parsed.positionals.length > max) {
    invalid('Wrong number of arguments. Run pullcord --help for usage.')
  }
  return parsed
}

function integer(value: string, min: number, name: string): number {
  const number = Number(value)
  if (!/^(0|[1-9]\d*)$/.test(value) || !Number.isSafeInteger(number) || number < min) {
    invalid(`Use ${min === 0 ? 'a non-negative' : 'a positive'} integer ${name}.`)
  }
  return number
}

function seconds(value: string | undefined, name: string): number | undefined {
  if (value === undefined) return undefined
  const match = /^(\d+)([smh])?$/.exec(value)
  const unit = match?.[2]
  const number = Number(match?.[1]) * (unit === 'h' ? 3_600 : unit === 'm' ? 60 : 1)
  if (!Number.isSafeInteger(number)) invalid(`Use a whole-second duration for ${name}, such as 30s or 15m.`)
  return number
}

function labels(entries: string[] | undefined): Record<string, string> | undefined {
  if (!entries?.length) return undefined
  const labels = Object.fromEntries(entries.map((label) => {
    const split = label.indexOf('=')
    if (split < 1) invalid('Write each --label as key=value.')
    return [label.slice(0, split), label.slice(split + 1)]
  }))
  if (Object.keys(labels).length !== entries.length) invalid('Specify each --label key once.')
  return labels
}

function output(value: unknown, json: boolean): Promise<void> {
  return new Promise((resolve, reject) => {
    process.stdout.write(`${JSON.stringify(value, undefined, json ? undefined : 2)}\n`, (error) => error ? reject(error) : resolve())
  })
}

type Events = { events: { seq: number }[]; cursor: number }

async function events(client: Client, after: number, follow: boolean, json: boolean): Promise<Events | undefined> {
  if (!follow) return client.request(`events?after=${after}`)
  const abort = new AbortController()
  const interrupt = (signal: NodeJS.Signals) => {
    process.exitCode = 128 + constants.signals[signal]
    abort.abort()
  }
  process.on('SIGINT', interrupt)
  process.on('SIGTERM', interrupt)
  try {
    while (!abort.signal.aborted) {
      let page: Events
      try {
        page = await client.request<Events>(`events?after=${after}&wait=30`, undefined, abort.signal)
      } catch (error) {
        if (abort.signal.aborted) break
        if (!(error instanceof CliError) || (error.code !== 'network_error' && (error.status ?? 0) < 500)) throw error
        await delay(1_000, undefined, { signal: abort.signal }).catch(() => {})
        continue
      }
      for (const event of page.events) {
        await output(event, json)
        after = event.seq
      }
    }
  } finally {
    process.off('SIGINT', interrupt)
    process.off('SIGTERM', interrupt)
  }
}

async function dispatch(command: string, args: string[], json: boolean): Promise<unknown> {
  const client = new Client()
  switch (command) {
    case 'tasks': {
      const { values } = options(args, { queue: { type: 'string' }, state: { type: 'string' } }, 0)
      const query = new URLSearchParams()
      if (values.queue !== undefined) query.set('queue', values.queue)
      if (values.state !== undefined) query.set('state', values.state)
      return client.request(`tasks?${query}`)
    }
    case 'show': {
      const { positionals: [id] } = options(args, {}, 1)
      return client.request(`tasks/${integer(id, 1, 'task ID')}`)
    }
    case 'claim': {
      const { positionals: [queue], values } = options(args, {
        label: { type: 'string', multiple: true }, wait: { type: 'string' }, lease: { type: 'string' },
      }, 1)
      return client.claim({ queue, labels: labels(values.label) }, {
        wait: seconds(values.wait, '--wait'), lease: seconds(values.lease, '--lease'),
      })
    }
    case 'renew': {
      const { positionals: [id], values } = options(args, { lease: { type: 'string' } }, 1)
      const lease = seconds(values.lease, '--lease')
      return client.holder(command, client.savedClaim(integer(id, 1, 'task ID')), { lease })
    }
    case 'complete':
    case 'fail':
    case 'release':
    case 'block': {
      const count = command === 'fail' || command === 'block' ? 2 : 1
      const { positionals: [id, text], values } = options(args, {
        ...(command === 'fail' && { retry: { type: 'boolean' } }),
      }, count, command === 'complete' ? 2 : count)
      const claim = client.savedClaim(integer(id, 1, 'task ID'))
      const body = command === 'complete' ? { result: text }
        : command === 'fail' ? { reason: text, retry: values.retry }
        : command === 'block' ? { reason: text }
        : {}
      return client.holder(command, claim, body)
    }
    case 'run': {
      const separator = args.indexOf('--')
      if (separator < 0 || separator === args.length - 1) invalid('Use pullcord run <queue|id> -- <command> [args...].')
      const { positionals: [target] } = options(args.slice(0, separator), {}, 1)
      const selector = /^\d+$/.test(target) ? { id: integer(target, 1, 'task ID') } : { queue: target }
      return run(client, selector, args.slice(separator + 1), json)
    }
    case 'log':
    case 'unblock':
    case 'requeue':
    case 'cancel': {
      const { positionals: [id, text] } = options(args, {}, command === 'log' || command === 'cancel' ? 2 : 1)
      const body = command === 'log' ? { text } : command === 'cancel' ? { reason: text } : {}
      return client.write(`tasks/${integer(id, 1, 'task ID')}/${command}`, body)
    }
    case 'memo': {
      const { positionals: [id, key, text] } = options(args, {}, 2, 3)
      const task = `tasks/${integer(id, 1, 'task ID')}`
      if (text !== undefined) {
        let value: unknown
        try { value = JSON.parse(text) } catch { value = text }
        return client.write(`${task}/memo`, { key, value })
      }
      const { memos } = await client.request<{ memos: Record<string, unknown> }>(task)
      if (!Object.hasOwn(memos, key)) throw new CliError('not_found', 'No memo with that key is set on this task.')
      return { value: memos[key] }
    }
    case 'events': {
      const { values } = options(args, { after: { type: 'string' }, follow: { type: 'boolean' } }, 0)
      return events(client, values.after === undefined ? 0 : integer(values.after, 0, 'cursor'), values.follow === true, json)
    }
    default:
      invalid('Unknown command. Run pullcord --help for usage.')
  }
}

const argv = process.argv.slice(2)
const delimiter = argv.indexOf('--')
const front = delimiter < 0 ? argv : argv.slice(0, delimiter)
const json = front.includes('--json')
const [command, ...args] = front.filter((arg) => arg !== '--json')

try {
  if (!command || front.includes('--help') || front.includes('-h')) {
    process.stdout.write(`${usage}\n`)
  } else {
    const result = await dispatch(command, delimiter < 0 ? args : [...args, ...argv.slice(delimiter)], json)
    if (result !== undefined) await output(result, json)
  }
} catch (error) {
  const code = error instanceof CliError || error instanceof StoreError ? error.code : 'internal_error'
  const message = error instanceof Error ? error.message : 'The command could not finish.'
  process.stderr.write(`${json ? JSON.stringify({ code, message }) : `${code}: ${message}`}\n`)
  process.exitCode = 1
}
