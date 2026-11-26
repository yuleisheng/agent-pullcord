import { createHash, randomBytes, randomUUID } from 'node:crypto'
import { closeSync, linkSync, lstatSync, openSync, readFileSync, renameSync, rmSync, writeFileSync } from 'node:fs'
import { ACTIONS, DEFAULT_DB, DEFAULT_KEYS, DEFAULT_SOURCES, parseKeyConfig, parseSourceConfig } from '../config.ts'
import { CliError, hubUrl, invalid } from './client.ts'

const AGENT_ACTIONS = ['read', 'claim', 'renew', 'complete', 'fail', 'release', 'block', 'log', 'memo']

function validate<T>(parse: (value: unknown) => T, value: unknown): T {
  try {
    return parse(value)
  } catch (error) {
    invalid(error instanceof Error ? error.message : 'The hub configuration is invalid.')
  }
}

function readConfig<T>(path: string, parse: (value: unknown) => T, missing?: T): T {
  let raw: string
  try {
    raw = readFileSync(path, 'utf8')
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') {
      if (missing !== undefined) return missing
      invalid(`No key file at ${path}. Run pullcord init or pass --keys <path>.`)
    }
    throw error
  }
  let value: unknown
  try {
    value = JSON.parse(raw)
  } catch {
    invalid(`The configuration file at ${path} is not valid JSON.`)
  }
  return validate(parse, value)
}

function editConfig<T>(path: string, edit: () => T): T {
  const lock = `${path}.pullcord-lock`
  let fd: number
  try {
    fd = openSync(lock, 'wx', 0o600)
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'EEXIST') {
      throw new CliError(
        'store_locked',
        `Configuration ${path} is locked. If a setup command crashed, stop all setup commands using this file before removing ${lock} and retrying.`,
      )
    }
    throw error
  }
  try {
    return edit()
  } finally {
    closeSync(fd)
    rmSync(lock, { force: true })
  }
}

function writeConfig(path: string, entries: unknown[], replace = true) {
  const temporary = `${path}.${randomUUID()}`
  try {
    writeFileSync(temporary, `${JSON.stringify(entries, undefined, 2)}\n`, { flag: 'wx', mode: 0o600 })
    if (replace) renameSync(temporary, path)
    else linkSync(temporary, path)
  } finally {
    rmSync(temporary, { force: true })
  }
}

function issueKey() {
  const key = randomBytes(32).toString('hex')
  const sha256 = createHash('sha256').update(key).digest('hex')
  return { key, sha256 }
}

export async function init({ db = DEFAULT_DB, keys = DEFAULT_KEYS }: { db?: string; keys?: string }) {
  const { initializeDatabase } = await import('../serve.ts')
  return editConfig(keys, () => {
    if (lstatSync(keys, { throwIfNoEntry: false }))
      invalid('A key file already exists. Use pullcord keys to manage it.')
    const { key, sha256 } = issueKey()
    initializeDatabase(db)
    writeConfig(keys, [{ agent: 'admin', queues: ['*'], can: ACTIONS, sha256 }], false)
    return { db, keys, admin_key: key }
  })
}

function csv(value: string): string[] {
  return value.split(',').map((part) => part.trim())
}

export function createKey(
  agent: string,
  { keys = DEFAULT_KEYS, queues, can }: { keys?: string; queues?: string; can?: string },
) {
  if (queues === undefined) invalid('Pass --queues <scope[,scope...]>.')
  const url = new URL('mcp', hubUrl()).href
  return editConfig(keys, () => {
    const { key, sha256 } = issueKey()
    const scopes = csv(queues)
    const actions = can === undefined ? AGENT_ACTIONS : csv(can)
    const entries = validate(parseKeyConfig, [
      ...readConfig(keys, parseKeyConfig),
      { agent, queues: scopes, can: actions, sha256 },
    ])
    writeConfig(keys, entries)
    const mcp = { mcpServers: { pullcord: { url, headers: { Authorization: `Bearer ${key}` } } } }
    return { agent, queues: scopes, can: actions, key, mcp, restart_required: true }
  })
}

export function listKeys(keys = DEFAULT_KEYS) {
  return { keys: readConfig(keys, parseKeyConfig).map(({ agent, queues, can }) => ({ agent, queues, can })) }
}

export function revokeKey(agent: string, keys = DEFAULT_KEYS) {
  return editConfig(keys, () => {
    const entries = readConfig(keys, parseKeyConfig)
    const remaining = entries.filter((entry) => entry.agent !== agent)
    if (remaining.length === entries.length) throw new CliError('not_found', 'No key belongs to that agent.')
    writeConfig(keys, remaining)
    return { agent, revoked: true, restart_required: true }
  })
}

export function addSource(
  provider: string,
  options: {
    sources?: string
    name?: string
    queue?: string
    retry?: string
    'public-url'?: string
    'secret-file'?: string
  },
) {
  const { sources = DEFAULT_SOURCES, name = provider, queue, retry = 'manual', 'secret-file': secretFile } = options
  if (queue === undefined) invalid('Pass --queue <queue>.')
  const generated = secretFile === undefined && provider !== 'pagerduty'
  let secret = generated ? randomBytes(32).toString('hex') : undefined
  if (secretFile !== undefined) {
    const bytes = readFileSync(secretFile)
    try {
      secret = new TextDecoder('utf-8', { fatal: true }).decode(bytes).replace(/\r?\n$/, '')
    } catch {
      invalid('The signing secret file is not valid UTF-8.')
    }
  }
  const [source] = validate(parseSourceConfig, [{ name, provider, queue, retry, secret: secret ?? 'pending' }])
  const webhook_url = new URL(`ingest/${encodeURIComponent(source.name)}`, hubUrl(options['public-url'])).href
  if (secret === undefined) {
    invalid(
      `PagerDuty issues the signing secret. Set up ${webhook_url} there, then pass its secret with --secret-file <path>.`,
    )
  }
  return editConfig(sources, () => {
    const entries = validate(parseSourceConfig, [...readConfig(sources, parseSourceConfig, []), source])
    writeConfig(sources, entries)
    return { name, provider, queue, retry, webhook_url, ...(generated && { secret }), restart_required: true }
  })
}
