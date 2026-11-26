import { SOURCE_KINDS, type SourceKind } from './sources/index.ts'
import type { Retry } from './store.ts'

export const DEFAULT_DB = 'pullcord.db'
export const DEFAULT_KEYS = 'pullcord-keys.json'
export const DEFAULT_SOURCES = 'pullcord-sources.json'

export const HOLDER_ACTIONS = ['renew', 'complete', 'fail', 'release', 'block'] as const
export const ACTIONS = ['read', 'events', 'claim', ...HOLDER_ACTIONS, 'log', 'memo', 'admin'] as const
export type Action = (typeof ACTIONS)[number]

export interface KeyConfig {
  agent: string
  sha256: string
  queues: string[]
  can: Action[]
}

export interface SourceConfig {
  name: string
  provider: SourceKind
  queue: string
  secret: string
  retry?: Retry
}

const CONFIG_NAME = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$/

export function parsePublicUrl(value: string): URL {
  const url = URL.parse(value)
  if (
    !url ||
    (url.protocol !== 'http:' && url.protocol !== 'https:') ||
    url.username ||
    url.password ||
    url.search ||
    url.hash
  ) {
    throw new Error('Use an absolute HTTP(S) URL without credentials, a query, or a fragment.')
  }
  return url
}

export function parseKeyConfig(config: unknown): KeyConfig[] {
  if (!Array.isArray(config)) throw new Error('The key file must contain a JSON array.')
  const agents = new Set<string>()
  const digests = new Set<string>()
  return (config as unknown[]).map((entry) => {
    if (typeof entry !== 'object' || entry === null || Array.isArray(entry))
      throw new Error('Each key entry must be a JSON object.')
    const { agent, sha256, queues, can } = entry as Record<string, unknown>
    if (typeof agent !== 'string' || !CONFIG_NAME.test(agent) || agents.has(agent)) {
      throw new Error(
        'Each key needs a unique agent name (1–64 letters, digits, dots, underscores, or hyphens, starting with a letter or digit).',
      )
    }
    if (typeof sha256 !== 'string' || !/^[0-9a-f]{64}$/.test(sha256) || digests.has(sha256)) {
      throw new Error('Each key needs a unique lowercase hex SHA-256 digest, not the bearer key.')
    }
    if (!Array.isArray(queues) || !queues.every((queue) => typeof queue === 'string' && queue.trim().length > 0)) {
      throw new Error('Each key needs a queues array of exact names, slash-terminated prefixes, or "*" for all queues.')
    }
    if (!Array.isArray(can) || !can.every((action) => ACTIONS.some((known) => known === action))) {
      throw new Error(`Each key needs a can array of supported actions: ${ACTIONS.join(', ')}.`)
    }
    agents.add(agent)
    digests.add(sha256)
    return { agent, sha256, queues, can }
  })
}

export function parseSourceConfig(config: unknown): SourceConfig[] {
  if (!Array.isArray(config)) throw new Error('The source file must contain a JSON array.')
  const names = new Set<string>()
  return (config as unknown[]).map((entry) => {
    if (typeof entry !== 'object' || entry === null || Array.isArray(entry))
      throw new Error('Each source entry must be a JSON object.')
    const { name, provider: kind, queue, secret, retry } = entry as Record<string, unknown>
    const provider = SOURCE_KINDS.find((candidate) => candidate === kind)
    if (typeof name !== 'string' || !CONFIG_NAME.test(name) || names.has(name)) {
      throw new Error(
        'Each source needs a unique name (1–64 letters, digits, dots, underscores, or hyphens, starting with a letter or digit).',
      )
    }
    if (!provider) throw new Error(`Choose one of these source providers: ${SOURCE_KINDS.join(', ')}.`)
    if (typeof queue !== 'string' || !queue.trim() || queue === '*' || queue.endsWith('/')) {
      throw new Error('Each source needs an exact queue name, not a wildcard or prefix.')
    }
    if (typeof secret !== 'string' || !secret.trim()) throw new Error('Each source needs a non-empty signing secret.')
    if (retry !== undefined && retry !== 'manual' && retry !== 'safe')
      throw new Error('Choose manual or safe for the source retry policy.')
    names.add(name)
    return { name, provider, queue, secret, retry }
  })
}
