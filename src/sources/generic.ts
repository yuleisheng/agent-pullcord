import { requiredString, requireFresh, type SourceEvent } from './signed-body.ts'

export function generic(payload: Record<string, unknown>, now: number): SourceEvent {
  requireFresh(payload.timestamp, now)
  return {
    action: 'create',
    task: {
      kind: 'event',
      dedupeKey: requiredString(payload.dedupe_key, 'dedupe_key'),
      title: requiredString(payload.title, 'title'),
      payload,
      adapter: 'generic:v1',
    },
  }
}
