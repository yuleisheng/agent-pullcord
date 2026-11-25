import { isoTime, requiredObject, requiredString, requireFresh, sourceUrl, type SourceEvent } from './signed-body.ts'

export function pagerDuty(payload: Record<string, unknown>, now: number): SourceEvent {
  const event = requiredObject(payload.event, 'event')
  const eventType = requiredString(event.event_type, 'event.event_type')
  if (eventType !== 'incident.triggered' && eventType !== 'incident.resolved') return { action: 'ignore' }
  requireFresh(isoTime(event.occurred_at), now)

  const incident = requiredObject(event.data, 'event.data')
  const dedupeKey = requiredString(incident.id, 'event.data.id')
  if (eventType === 'incident.resolved') {
    return {
      action: 'cancel',
      dedupeKey,
      reason: 'PagerDuty resolved the incident.',
      requestId: requiredString(event.id, 'event.id'),
    }
  }

  return {
    action: 'create',
    task: {
      kind: 'incident',
      dedupeKey,
      title: requiredString(incident.title, 'event.data.title'),
      sourceUrl: sourceUrl(incident.html_url),
      payload,
      adapter: 'pagerduty:v3',
    },
  }
}
