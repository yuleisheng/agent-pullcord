import { pagerDuty } from './pagerduty.ts'
import { signedBody, type SourceEvent } from './signed-body.ts'

export { SourceError, type SourceEvent } from './signed-body.ts'

export const SOURCE_KINDS = ['pagerduty'] as const
export type SourceKind = (typeof SOURCE_KINDS)[number]

export async function readSourceEvent(
  provider: SourceKind,
  secret: string,
  headers: Headers,
  body: Uint8Array<ArrayBuffer>,
  now: number,
): Promise<SourceEvent> {
  switch (provider) {
    case 'pagerduty': {
      const signatures = headers.get('x-pagerduty-signature')?.split(',') ?? []
      return pagerDuty(await signedBody(secret, signatures, 'v1=', body), now)
    }
  }
}
