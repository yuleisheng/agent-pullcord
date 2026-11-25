import assert from 'node:assert/strict'
import { createHmac } from 'node:crypto'
import { test } from 'node:test'
import { readSourceEvent, SourceError } from '../src/sources/index.ts'

const now = Date.parse('2026-11-25T13:06:43.318Z')
const pagerDutySecret = 'fake-pagerduty-webhook-secret'
const encoder = new TextEncoder()

function hmac(secret: string, body: string | Uint8Array): string {
  return createHmac('sha256', secret).update(body).digest('hex')
}

function pagerDutyEvent(eventType: string, at = now) {
  return {
    event: {
      id: eventType === 'incident.triggered' ? '01HVP0TRIGGER1234' : '01HVP0RESOLVE9876',
      event_type: eventType,
      resource_type: 'incident',
      occurred_at: new Date(at).toISOString(),
      agent: null,
      data: {
        id: 'PGR0VU2',
        type: 'incident',
        title: 'Checkout failed – 上海',
        status: eventType === 'incident.resolved' ? 'resolved' : 'triggered',
        html_url: 'https://pd.example.test/incidents/PGR0VU2',
      },
    },
  }
}

function signedPagerDuty(payload: object) {
  const body = encoder.encode(JSON.stringify(payload, null, 2) + '\n')
  const headers = new Headers({ 'X-PagerDuty-Signature': `v1=${hmac(pagerDutySecret, body)}` })
  return { body, headers }
}

function pagerDuty(payload: object, clock = now) {
  const { headers, body } = signedPagerDuty(payload)
  return readSourceEvent('pagerduty', pagerDutySecret, headers, body, clock)
}

function sourceError(status: number, code: string) {
  return (error: unknown) => error instanceof SourceError && error.status === status && error.code === code
}

test('PagerDuty triggers and resolves the same incident across distinct delivery IDs', async () => {
  const payload = pagerDutyEvent('incident.triggered')
  const { body, headers } = signedPagerDuty(payload)
  headers.append('X-PagerDuty-Signature', `v1=${hmac('fake-prior-secret', body)}`)
  const resolve = pagerDutyEvent('incident.resolved')
  const [first, retry, laterTrigger, cancel] = await Promise.all([
    readSourceEvent('pagerduty', 'fake-prior-secret', headers, body, now),
    readSourceEvent('pagerduty', pagerDutySecret, headers, body, now),
    pagerDuty({ event: { ...payload.event, id: 'different-trigger-identifier' } }),
    pagerDuty(resolve),
  ])
  assert.deepEqual(first, {
    action: 'create',
    task: {
      kind: 'incident',
      dedupeKey: 'PGR0VU2',
      title: 'Checkout failed – 上海',
      sourceUrl: 'https://pd.example.test/incidents/PGR0VU2',
      payload,
      adapter: 'pagerduty:v3',
    },
  })
  assert.deepEqual(retry, first)
  assert.equal(laterTrigger.action === 'create' && laterTrigger.task.dedupeKey, 'PGR0VU2')
  assert.deepEqual(cancel, {
    action: 'cancel', dedupeKey: 'PGR0VU2', reason: 'PagerDuty resolved the incident.', requestId: '01HVP0RESOLVE9876',
  })
})

test('PagerDuty authenticates exact bytes and does not apply unverified resolutions', async () => {
  const payload = pagerDutyEvent('incident.resolved')
  const { body, headers } = signedPagerDuty(payload)
  const unverified = sourceError(401, 'unauthorized')
  await assert.rejects(readSourceEvent('pagerduty', pagerDutySecret, headers, encoder.encode(JSON.stringify(payload)), now), unverified)
  await assert.rejects(readSourceEvent('pagerduty', 'wrong-secret', headers, body, now), unverified)
  const wrongVersion = new Headers({ 'X-PagerDuty-Signature': `v2=${hmac(pagerDutySecret, body)}` })
  await assert.rejects(readSourceEvent('pagerduty', pagerDutySecret, wrongVersion, body, now), unverified)

  const malformedBytes = Uint8Array.of(0x7b, 0xff, 0x7d)
  const signed = new Headers({ 'X-PagerDuty-Signature': `v1=${hmac(pagerDutySecret, malformedBytes)}` })
  await assert.rejects(readSourceEvent('pagerduty', pagerDutySecret, new Headers(), malformedBytes, now), unverified)
  await assert.rejects(readSourceEvent('pagerduty', pagerDutySecret, signed, malformedBytes, now), sourceError(400, 'invalid_argument'))
})

test('PagerDuty bounds the signed occurrence time before create, cancel, or ignore', async () => {
  assert.equal((await pagerDuty(pagerDutyEvent('incident.triggered', now - 300_000))).action, 'create')
  assert.equal((await pagerDuty(pagerDutyEvent('incident.resolved', now + 300_000))).action, 'cancel')
  await assert.rejects(pagerDuty(pagerDutyEvent('incident.triggered', now - 300_001)), sourceError(401, 'unauthorized'))
  await assert.rejects(pagerDuty(pagerDutyEvent('incident.resolved', now + 300_001)), sourceError(401, 'unauthorized'))

  const ping = {
    event: {
      id: '01CH754SM17TWPE2V2H4VPBRO7', event_type: 'pagey.ping', resource_type: 'pagey',
      occurred_at: new Date(now).toISOString(), data: { message: 'Hello from Pagey!', type: 'ping' },
    },
  }
  assert.deepEqual(await pagerDuty(ping), { action: 'ignore' })
  await assert.rejects(pagerDuty(ping, now + 300_001), sourceError(401, 'unauthorized'))

  const payload = pagerDutyEvent('incident.resolved')
  await assert.rejects(pagerDuty({ event: { ...payload.event, occurred_at: '2026-11-25T13:06:43.318' } }), sourceError(401, 'unauthorized'))
  await assert.rejects(pagerDuty({ event: { ...payload.event, data: { id: ['PGR0VU2'] } } }), sourceError(400, 'invalid_argument'))
})
