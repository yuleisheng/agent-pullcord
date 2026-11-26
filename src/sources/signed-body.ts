import type { IngestInput } from '../store.ts'

export type SourceEvent =
  | { action: 'ignore' }
  | {
      action: 'create'
      task: Pick<IngestInput, 'dedupeKey' | 'kind' | 'title' | 'sourceUrl'> & {
        payload: Record<string, unknown>
        adapter: string
      }
    }
  | { action: 'cancel'; dedupeKey: string; reason: string; requestId: string }

export class SourceError extends Error {
  readonly code: 'invalid_argument' | 'unauthorized'
  readonly status: 400 | 401

  constructor(code: SourceError['code'], message: string) {
    super(message)
    this.name = 'SourceError'
    this.code = code
    this.status = code === 'unauthorized' ? 401 : 400
  }
}

const MAX_SKEW_MS = 5 * 60_000
const ISO_TIME = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/
const encoder = new TextEncoder()
const decoder = new TextDecoder('utf-8', { fatal: true })

export async function verifySignature(
  secret: string,
  signatures: string[],
  prefix: string,
  bytes: Uint8Array<ArrayBuffer>,
): Promise<void> {
  const digests = signatures
    .map((value) => value.trim())
    .filter((value) => value.startsWith(prefix))
    .map((value) => value.slice(prefix.length))
    .filter((hex) => /^[a-f\d]{64}$/i.test(hex))
  if (digests.length) {
    const key = await crypto.subtle.importKey('raw', encoder.encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, [
      'verify',
    ])
    for (const hex of digests) {
      const signature = Uint8Array.from({ length: 32 }, (_, index) =>
        Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16),
      )
      if (await crypto.subtle.verify('HMAC', key, signature, bytes)) return
    }
  }
  throw new SourceError('unauthorized', 'Send a valid signature for this source and request body.')
}

export function decodeSourceBody(bytes: Uint8Array<ArrayBuffer>): Record<string, unknown> {
  let payload: unknown
  try {
    payload = JSON.parse(decoder.decode(bytes))
  } catch {
    throw new SourceError('invalid_argument', 'Send a valid UTF-8 JSON object as the webhook body.')
  }
  return requiredObject(payload, 'The webhook body')
}

export function requiredObject(value: unknown, field: string): Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new SourceError('invalid_argument', `${field} must be an object.`)
  }
  return value as Record<string, unknown>
}

export function requiredString(value: unknown, field: string): string {
  if (typeof value !== 'string' || !value.trim()) {
    throw new SourceError('invalid_argument', `${field} must be a non-empty string.`)
  }
  return value
}

export function isoTime(value: unknown): number {
  return typeof value === 'string' && ISO_TIME.test(value) ? Date.parse(value) : NaN
}

export function requireFresh(value: unknown, now: number): void {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || Math.abs(value - now) > MAX_SKEW_MS) {
    throw new SourceError('unauthorized', 'Send a signed webhook timestamp within five minutes of the hub clock.')
  }
}

export function sourceUrl(value: unknown): string | undefined {
  if (typeof value !== 'string') return undefined
  try {
    const url = new URL(value)
    if ((url.protocol === 'https:' || url.protocol === 'http:') && !url.username && !url.password) return url.href
  } catch {
    return undefined
  }
}
