import { StoreError } from './store.ts'
import type { ErrorCode, Labels, Store, Task, TaskState } from './store.ts'

const ACTIONS = ['read'] as const
type Action = (typeof ACTIONS)[number]

const STATES = ['ready', 'claimed', 'blocked', 'interrupted', 'completed', 'failed', 'canceled'] as const satisfies readonly TaskState[]
const STORE_STATUS: Record<ErrorCode, number> = {
  not_found: 404,
  claim_lost: 409,
  invalid_transition: 409,
  invalid_argument: 400,
  request_id_reused: 409,
  store_locked: 503,
}

interface Agent {
  actor: string
  queues: string[]
  can: Action[]
}

class ApiError extends Error {
  readonly status: number
  readonly code: string

  constructor(status: number, code: string, message: string) {
    super(message)
    this.status = status
    this.code = code
  }
}

function keyring(config: unknown): Map<string, Agent> {
  if (!Array.isArray(config)) throw new Error('The key file must contain a JSON array.')
  const keys = new Map<string, Agent>()
  const agents = new Set<string>()
  for (const entry of config as unknown[]) {
    if (!isObject(entry)) throw new Error('Each key entry must be a JSON object.')
    const { agent, sha256, queues, can } = entry
    if (typeof agent !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$/.test(agent) || agents.has(agent)) {
      throw new Error('Each key needs a unique agent name (1–64 letters, digits, dots, underscores, or hyphens, starting with a letter or digit).')
    }
    if (typeof sha256 !== 'string' || !/^[0-9a-f]{64}$/.test(sha256) || keys.has(sha256)) {
      throw new Error('Each key needs a unique lowercase hex SHA-256 digest, not the bearer key.')
    }
    if (!Array.isArray(queues) || !queues.every((queue) => typeof queue === 'string' && queue.trim().length > 0)) {
      throw new Error('Each key needs a queues array of exact names, slash-terminated prefixes, or "*" for all queues.')
    }
    if (!Array.isArray(can) || !can.every((action) => ACTIONS.some((known) => known === action))) {
      throw new Error(`Each key needs a can array of supported actions: ${ACTIONS.join(', ')}.`)
    }
    keys.set(sha256, { actor: `agent:${agent}`, queues, can })
    agents.add(agent)
  }
  return keys
}

async function authenticate(request: Request, keys: Map<string, Agent>): Promise<Agent> {
  const token = /^Bearer ([0-9a-f]{64})$/i.exec(request.headers.get('authorization') ?? '')?.[1]
  if (token !== undefined) {
    const hash = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(token))
    const digest = Array.from(new Uint8Array(hash), (byte) => byte.toString(16).padStart(2, '0')).join('')
    const agent = keys.get(digest)
    if (agent) return agent
  }
  throw new ApiError(401, 'unauthorized', 'Send a valid agent key in the Authorization: Bearer header.')
}

function requireAction(agent: Agent, action: Action): void {
  if (!agent.can.includes(action)) throw new ApiError(403, 'forbidden', `Use a key with the ${action} action.`)
}

function covers(agent: Agent, queue: string): boolean {
  return agent.queues.some((scope) => scope === '*' || (scope.endsWith('/') ? queue.startsWith(scope) : queue === scope))
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function invalid(message: string): never {
  throw new ApiError(400, 'invalid_argument', message)
}

function taskId(part: string): number {
  const id = Number(part)
  if (!/^[1-9]\d*$/.test(part) || !Number.isSafeInteger(id)) invalid('Use a positive integer task ID.')
  return id
}

function listFilter(query: URLSearchParams): { queue?: string; state?: TaskState; labels?: Labels } {
  const queue = query.get('queue') ?? undefined
  if (queue !== undefined && !queue.trim()) invalid('Use a non-empty queue name or omit the queue filter.')
  const stateName = query.get('state')
  const state = STATES.find((known) => known === stateName)
  if (stateName !== null && state === undefined) invalid(`Use one of these states: ${STATES.join(', ')}.`)
  const entries = query.getAll('label')
  const labels = Object.fromEntries(entries.map((label) => {
    const split = label.indexOf('=')
    if (split < 1) invalid('Write each label filter as label=key=value.')
    return [label.slice(0, split), label.slice(split + 1)]
  }))
  if (Object.keys(labels).length !== entries.length) invalid('Specify each label filter once.')
  return { queue, state, labels }
}

function taskJson(task: Task) {
  return {
    id: task.id,
    queue: task.queue,
    kind: task.kind,
    source: task.source,
    dedupe_key: task.dedupeKey,
    correlation_key: task.correlationKey,
    severity: task.severity,
    labels: task.labels,
    source_url: task.sourceUrl,
    adapter: task.adapter,
    retry: task.retry,
    max_attempts: task.maxAttempts,
    attempts: task.attempts,
    state: task.state,
    holder: task.holder,
    epoch: task.epoch,
    lease_expires_at: task.leaseExpiresAt,
    ready_at: task.readyAt,
    result: task.result,
    reason: task.reason,
    created_at: task.createdAt,
    updated_at: task.updatedAt,
    untrusted: task.untrusted,
  }
}

function scopedTask(store: Store, agent: Agent, id: number) {
  const task = store.task(id)
  if (!covers(agent, task.queue)) throw new StoreError('not_found', `No task ${id}.`)
  return task
}

function json(body: unknown, status = 200): Response {
  const headers = new Headers({ 'Cache-Control': 'no-store' })
  if (status === 401) headers.set('WWW-Authenticate', 'Bearer')
  return Response.json(body, { status, headers })
}

export function createApi(store: Store, config: unknown): (request: Request) => Promise<Response> {
  const keys = keyring(config)
  return async (request) => {
    try {
      const agent = await authenticate(request, keys)
      const { pathname, searchParams } = new URL(request.url)
      const task = /^\/tasks\/([^/]+)$/.exec(pathname)
      if (request.method === 'GET' && pathname === '/tasks') {
        requireAction(agent, 'read')
        const filter = listFilter(searchParams)
        if (filter.queue !== undefined && !covers(agent, filter.queue)) {
          throw new ApiError(403, 'forbidden', 'Choose a queue or prefix covered by this key.')
        }
        const { tasks, cursor } = store.tasks(filter)
        return json({ tasks: tasks.filter(({ queue }) => covers(agent, queue)).map(taskJson), cursor })
      }
      if (request.method === 'GET' && task) {
        requireAction(agent, 'read')
        const detail = scopedTask(store, agent, taskId(task[1]))
        return json({ ...taskJson(detail), memos: detail.memos })
      }
      throw new ApiError(404, 'not_found', 'Use a supported API endpoint.')
    } catch (error) {
      if (error instanceof ApiError || error instanceof StoreError) {
        const status = error instanceof ApiError ? error.status : STORE_STATUS[error.code]
        return json({ code: error.code, message: error.message }, status)
      }
      return json({ code: 'internal_error', message: 'The hub could not handle the request. Try again or contact its operator.' }, 500)
    }
  }
}
