import { generic } from './generic.ts'
import { gitHub } from './github.ts'
import { pagerDuty } from './pagerduty.ts'
import { decodeSourceBody, verifySignature, type SourceEvent } from './signed-body.ts'

export { SourceError, type SourceEvent } from './signed-body.ts'

export const SOURCE_KINDS = ['pagerduty', 'github', 'generic'] as const
export type SourceKind = (typeof SOURCE_KINDS)[number]

const SIGNATURE_HEADERS = {
  pagerduty: ['x-pagerduty-signature', 'v1='],
  github: ['x-hub-signature-256', 'sha256='],
  generic: ['x-pullcord-signature', 'sha256='],
} as const

export async function verifySourceDelivery(
  provider: SourceKind,
  secret: string,
  headers: Headers,
  body: Uint8Array<ArrayBuffer>,
): Promise<(now: number) => SourceEvent> {
  const [header, prefix] = SIGNATURE_HEADERS[provider]
  const signature = headers.get(header) ?? ''
  const signatures = provider === 'pagerduty' ? signature.split(',') : [signature]
  await verifySignature(secret, signatures, prefix, body)

  return (now) => {
    const payload = decodeSourceBody(body)
    switch (provider) {
      case 'pagerduty':
        return pagerDuty(payload, now)
      case 'github':
        return gitHub(payload, headers.get('x-github-event'), now)
      case 'generic':
        return generic(payload, now)
    }
  }
}
