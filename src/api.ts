import { MAX_LEASE_MS, MIN_LEASE_MS, StoreError, type ErrorCode } from './store-contract.ts'
import type { Labels, Store, Task, TaskState, Writer } from './store.ts'

type TaskStore = Pick<Store, 'tasks' | 'task' | 'taskQueue' | 'claim' | 'renew' | 'complete' | 'fail' | 'release' | 'block'>

const HOLDER_ACTIONS = ['renew', 'complete', 'fail', 'release', 'block'] as const
type HolderAction = (typeof HOLDER_ACTIONS)[number]
const ACTIONS = ['read', 'events', 'claim', ...HOLDER_ACTIONS] as const
type Action = (typeof ACTIONS)[number]

const MAX_BODY_BYTES = 64 * 1024
const MAX_WAIT_SECONDS = 30
const CLAIM_POLL_MS = 250

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

function requireQueue(agent: Agent, selector: string): void {
  if (!covers(agent, selector)) throw new ApiError(403, 'forbidden', 'Choose a queue or prefix covered by this key.')
}

function requireTaskQueue(store: TaskStore, agent: Agent, id: number): void {
  if (!covers(agent, store.taskQueue(id))) throw new StoreError('not_found', `No task ${id}.`)
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

function nonempty(value: unknown, field: string): string {
  if (typeof value !== 'string' || !value.trim()) invalid(`${field} must be a non-empty string.`)
  return value
}

function integer(value: unknown, field: string): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value)) invalid(`${field} must be an integer.`)
  return value
}

function leaseMs(value: unknown): number | undefined {
  if (value === undefined) return undefined
  const ms = integer(value, 'lease') * 1000
  if (ms < MIN_LEASE_MS || ms > MAX_LEASE_MS) invalid(`lease must be ${MIN_LEASE_MS / 1000} to ${MAX_LEASE_MS / 1000} seconds.`)
  return ms
}

async function jsonBody(request: Request): Promise<Record<string, unknown>> {
  if (request.headers.get('content-type')?.split(';', 1)[0].trim().toLowerCase() !== 'application/json') {
    throw new ApiError(415, 'invalid_argument', 'Send Content-Type: application/json.')
  }
  const reader = request.body?.getReader()
  if (!reader) invalid('Send a JSON object in the request body.')
  const decoder = new TextDecoder('utf-8', { fatal: true })
  let bytes = 0
  let text = ''
  try {
    for (;;) {
      const { done, value } = await reader.read()
      if (done) break
      bytes += value.byteLength
      if (bytes > MAX_BODY_BYTES) throw new ApiError(413, 'invalid_argument', 'Keep the JSON request body within 64 KiB.')
      text += decoder.decode(value, { stream: true })
    }
    const body: unknown = JSON.parse(text + decoder.decode())
    if (isObject(body)) return body
  } catch (error) {
    if (error instanceof ApiError) throw error
  } finally {
    reader.releaseLock()
  }
  invalid('Send a valid UTF-8 JSON object in the request body.')
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

function pause(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const done = () => {
      clearTimeout(timer)
      signal.removeEventListener('abort', done)
      resolve()
    }
    const timer = setTimeout(done, ms)
    signal.addEventListener('abort', done, { once: true })
    if (signal.aborted) done()
  })
}

async function claimTask(store: TaskStore, agent: Agent, request: Request): Promise<Task> {
  requireAction(agent, 'claim')
  const body = await jsonBody(request)
  const queue = nonempty(body.queue, 'queue')
  requireQueue(agent, queue)
  let labels: Labels | undefined
  if (body.labels !== undefined) {
    if (!isObject(body.labels) || !Object.values(body.labels).every((value) => typeof value === 'string')) {
      invalid('labels must map strings to strings.')
    }
    labels = body.labels as Labels
  }
  const wait = body.wait === undefined ? 0 : integer(body.wait, 'wait')
  if (wait < 0 || wait > MAX_WAIT_SECONDS) invalid(`wait must be 0 to ${MAX_WAIT_SECONDS} seconds.`)
  const filter = { queue, labels, leaseMs: leaseMs(body.lease) }
  const who: Writer = { actor: agent.actor, requestId: nonempty(body.request_id, 'request_id') }
  const deadline = performance.now() + wait * 1000
  do {
    request.signal.throwIfAborted()
    const claimed = store.claim(filter, who)
    if (claimed) return claimed
    const remaining = deadline - performance.now()
    if (remaining <= 0) break
    await pause(Math.min(remaining, CLAIM_POLL_MS), request.signal)
  } while (performance.now() < deadline)
  throw new ApiError(409, 'nothing_ready', 'No matching task is ready. Try claiming again later.')
}

async function holderWrite(store: TaskStore, agent: Agent, action: HolderAction, part: string, request: Request): Promise<Task> {
  requireAction(agent, action)
  const id = taskId(part)
  const body = await jsonBody(request)
  const epoch = integer(body.epoch, 'epoch')
  if (epoch < 1) invalid('epoch must be the positive integer returned by claim.')
  const who = { actor: agent.actor, epoch, requestId: nonempty(body.request_id, 'request_id') }
  request.signal.throwIfAborted()
  requireTaskQueue(store, agent, id)
  switch (action) {
    case 'renew':
      return store.renew(id, { ...who, leaseMs: leaseMs(body.lease) })
    case 'complete': {
      const { result } = body
      if (result !== undefined && typeof result !== 'string') invalid('result must be a string.')
      return store.complete(id, { ...who, result })
    }
    case 'fail': {
      const { retry } = body
      if (retry !== undefined && typeof retry !== 'boolean') invalid('retry must be a boolean.')
      return store.fail(id, { ...who, reason: nonempty(body.reason, 'reason'), retry })
    }
    case 'release':
      return store.release(id, who)
    case 'block':
      return store.block(id, { ...who, reason: nonempty(body.reason, 'reason') })
  }
}

function json(body: unknown, status = 200): Response {
  const headers = new Headers({ 'Cache-Control': 'no-store' })
  if (status === 401) headers.set('WWW-Authenticate', 'Bearer')
  return Response.json(body, { status, headers })
}

export function createApi(store: TaskStore, config: unknown): (request: Request) => Promise<Response> {
  const keys = keyring(config)
  return async (request) => {
    try {
      const agent = await authenticate(request, keys)
      const { pathname, searchParams } = new URL(request.url)
      const task = /^\/tasks\/([^/]+)$/.exec(pathname)
      if (request.method === 'GET' && pathname === '/tasks') {
        requireAction(agent, 'read')
        const filter = listFilter(searchParams)
        if (filter.queue !== undefined) requireQueue(agent, filter.queue)
        const { tasks, cursor } = store.tasks(filter)
        const visible = tasks.filter(({ queue }) => covers(agent, queue)).map(taskJson)
        const canWatch = agent.queues.includes('*') && agent.can.includes('events')
        return json(canWatch ? { tasks: visible, cursor } : { tasks: visible })
      }
      if (request.method === 'GET' && task) {
        requireAction(agent, 'read')
        const id = taskId(task[1])
        requireTaskQueue(store, agent, id)
        const detail = store.task(id)
        return json({ ...taskJson(detail), memos: detail.memos })
      }
      if (request.method === 'POST') {
        if (pathname === '/tasks/claim') return json(taskJson(await claimTask(store, agent, request)))
        const write = /^\/tasks\/([^/]+)\/([^/]+)$/.exec(pathname)
        const action = HOLDER_ACTIONS.find((known) => known === write?.[2])
        if (write && action) return json(taskJson(await holderWrite(store, agent, action, write[1], request)))
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
