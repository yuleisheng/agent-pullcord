import { createHash, randomUUID } from 'node:crypto'
import { lstatSync, mkdirSync, readFileSync, renameSync, rmSync, writeFileSync } from 'node:fs'
import { homedir } from 'node:os'
import { isAbsolute, join } from 'node:path'
import { parsePublicUrl } from '../config.ts'

export const DEFAULT_URL = 'http://127.0.0.1:7337'

export class CliError extends Error {
  readonly code: string
  readonly status?: number

  constructor(code: string, message: string, status?: number) {
    super(message)
    this.code = code
    this.status = status
  }
}

export function invalid(message: string): never {
  throw new CliError('invalid_argument', message)
}

export function hubUrl(value = process.env.PULLCORD_URL ?? DEFAULT_URL): URL {
  try {
    const url = parsePublicUrl(value)
    url.pathname = url.pathname.replace(/\/*$/, '/')
    return url
  } catch (error) {
    invalid(error instanceof Error ? error.message : 'Use a valid hub URL.')
  }
}

export type Claim = { id: number; epoch: number }
export type ClaimSelector = { queue: string; labels?: Record<string, string> } | { id: number }

export class Client {
  readonly #url = hubUrl()
  readonly #key = process.env.PULLCORD_KEY

  async request<T>(path: string, body?: Record<string, unknown>, signal?: AbortSignal): Promise<T> {
    if (!this.#key || !/^[0-9a-f]{64}$/i.test(this.#key)) {
      throw new CliError('unauthorized', 'Set PULLCORD_KEY to a valid agent key.')
    }
    const headers = new Headers({ Authorization: `Bearer ${this.#key}` })
    if (body) headers.set('Content-Type', 'application/json')
    const timeout = AbortSignal.timeout(40_000)
    let response: Response
    try {
      response = await fetch(new URL(path, this.#url), {
        method: body ? 'POST' : 'GET', headers, body: body && JSON.stringify(body), redirect: 'error',
        signal: signal ? AbortSignal.any([signal, timeout]) : timeout,
      })
    } catch {
      signal?.throwIfAborted()
      throw new CliError('network_error', 'Could not reach the hub. Check PULLCORD_URL and try again.')
    }
    let data: unknown
    try {
      data = await response.json()
    } catch {
      signal?.throwIfAborted()
      throw new CliError('invalid_response', 'The hub did not return JSON.', response.status)
    }
    if (!response.ok) {
      if (data && typeof data === 'object' && 'code' in data && 'message' in data &&
          typeof data.code === 'string' && typeof data.message === 'string') {
        throw new CliError(data.code, data.message, response.status)
      }
      throw new CliError('invalid_response', `The hub returned HTTP ${response.status}.`, response.status)
    }
    return data as T
  }

  write<T>(path: string, body: Record<string, unknown>, signal?: AbortSignal): Promise<T> {
    return this.request<T>(path, { ...body, request_id: randomUUID() }, signal)
  }

  async claim(selector: ClaimSelector, options: { wait?: number; lease?: number } = {}): Promise<Claim> {
    const task = await this.write<Claim>('tasks/claim', { ...selector, ...options })
    if (!Number.isSafeInteger(task?.id) || task.id < 1 || !Number.isSafeInteger(task.epoch) || task.epoch < 1) {
      throw new CliError('invalid_response', 'The hub returned no valid task ID and epoch for the claim.')
    }
    try {
      const file = this.#claimFile(task.id)
      const temporary = `${file}.${randomUUID()}`
      try {
        writeFileSync(temporary, `${task.epoch}\n`, { mode: 0o600, flag: 'wx' })
        renameSync(temporary, file)
      } finally {
        rmSync(temporary, { force: true })
      }
    } catch (error) {
      await this.holder('release', task).catch(() => {})
      throw error
    }
    return task
  }

  savedClaim(id: number): Claim {
    if (!this.#key) throw new CliError('unauthorized', 'Set PULLCORD_KEY to a valid agent key.')
    try {
      const file = this.#claimFile(id)
      const stat = lstatSync(file)
      if (!stat.isFile() || stat.mode & 0o077 || (process.getuid && stat.uid !== process.getuid())) {
        invalid('The saved claim file must be a private file owned by you.')
      }
      const epoch = Number(readFileSync(file, 'utf8'))
      if (!Number.isSafeInteger(epoch) || epoch < 1) invalid(`The saved epoch for task ${id} is invalid.`)
      return { id, epoch }
    } catch (error) {
      if (error instanceof Error && 'code' in error && error.code === 'ENOENT') {
        invalid(`No epoch is saved for task ${id} with this hub and key. Claim it with pullcord first.`)
      }
      throw error
    }
  }

  holder<T>(action: string, claim: Claim, body: Record<string, unknown> = {}, signal?: AbortSignal): Promise<T> {
    return this.write<T>(`tasks/${claim.id}/${action}`, { ...body, epoch: claim.epoch }, signal)
  }

  #claimFile(id: number): string {
    const xdg = process.env.XDG_STATE_HOME
    const dir = join(xdg && isAbsolute(xdg) ? xdg : join(homedir(), '.local', 'state'), 'pullcord')
    mkdirSync(dir, { recursive: true, mode: 0o700 })
    const stat = lstatSync(dir)
    if (!stat.isDirectory() || stat.mode & 0o077 || (process.getuid && stat.uid !== process.getuid())) {
      invalid('The pullcord state directory must be private and owned by you.')
    }
    const scope = createHash('sha256').update(this.#url.href).update('\0').update(this.#key ?? '').digest('hex')
    return join(dir, `${scope}-${id}.epoch`)
  }
}
