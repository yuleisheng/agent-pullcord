import { generic } from './generic.ts'
import { gitHub } from './github.ts'
import { pagerDuty } from './pagerduty.ts'
import { signedBody, type SourceEvent } from './signed-body.ts'

export { SourceError, type SourceEvent } from './signed-body.ts'

export const SOURCE_KINDS = ['pagerduty', 'github', 'generic'] as const
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
    case 'github': {
      const signatures = [headers.get('x-hub-signature-256') ?? '']
      return gitHub(await signedBody(secret, signatures, 'sha256=', body), headers.get('x-github-event'), now)
    }
    case 'generic': {
      const signatures = [headers.get('x-pullcord-signature') ?? '']
      return generic(await signedBody(secret, signatures, 'sha256=', body), now)
    }
  }
}
