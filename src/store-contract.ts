export type ErrorCode =
  'not_found' | 'claim_lost' | 'invalid_transition' | 'invalid_argument' | 'request_id_reused' | 'store_locked'

export class StoreError extends Error {
  readonly code: ErrorCode
  constructor(code: ErrorCode, message: string) {
    super(message)
    this.name = 'StoreError'
    this.code = code
  }
}

export const DEFAULT_LEASE_MS = 15 * 60_000
export const MIN_LEASE_MS = 10_000
export const MAX_LEASE_MS = 60 * 60_000
