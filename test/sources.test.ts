import assert from 'node:assert/strict'
import { createHmac } from 'node:crypto'
import { test } from 'node:test'
import { SourceError, verifySourceDelivery, type SourceKind } from '../src/sources/index.ts'

const now = Date.parse('2026-11-25T13:06:43.318Z')
const pagerDutySecret = 'fake-pagerduty-webhook-secret'
const githubSecret = 'fake-github-webhook-secret'
const genericSecret = 'fake-generic-webhook-secret'
const encoder = new TextEncoder()

async function readSourceEvent(
  provider: SourceKind,
  secret: string,
  headers: Headers,
  body: Uint8Array<ArrayBuffer>,
  clock: number,
) {
  const readEvent = await verifySourceDelivery(provider, secret, headers, body)
  return readEvent(clock)
}

function hmac(secret: string, body: Uint8Array): string {
  return createHmac('sha256', secret).update(body).digest('hex')
}

function signedDelivery(payload: object, secret: string, header: string, prefix = 'sha256=') {
  const body = encoder.encode(JSON.stringify(payload, null, 2) + '\n')
  const headers = new Headers({ [header]: `${prefix}${hmac(secret, body)}` })
  return { body, headers }
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
  return signedDelivery(payload, pagerDutySecret, 'X-PagerDuty-Signature', 'v1=')
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
    action: 'cancel',
    dedupeKey: 'PGR0VU2',
    reason: 'PagerDuty resolved the incident.',
    requestId: '01HVP0RESOLVE9876',
  })
})

test('PagerDuty authenticates exact bytes and does not apply unverified resolutions', async () => {
  const payload = pagerDutyEvent('incident.resolved')
  const { body, headers } = signedPagerDuty(payload)
  const unverified = sourceError(401, 'unauthorized')
  await assert.rejects(
    readSourceEvent('pagerduty', pagerDutySecret, headers, encoder.encode(JSON.stringify(payload)), now),
    unverified,
  )
  await assert.rejects(readSourceEvent('pagerduty', 'wrong-secret', headers, body, now), unverified)
  const wrongVersion = new Headers({ 'X-PagerDuty-Signature': `v2=${hmac(pagerDutySecret, body)}` })
  await assert.rejects(readSourceEvent('pagerduty', pagerDutySecret, wrongVersion, body, now), unverified)

  const malformedBytes = Uint8Array.of(0x7b, 0xff, 0x7d)
  const signed = new Headers({ 'X-PagerDuty-Signature': `v1=${hmac(pagerDutySecret, malformedBytes)}` })
  await assert.rejects(readSourceEvent('pagerduty', pagerDutySecret, new Headers(), malformedBytes, now), unverified)
  await assert.rejects(
    readSourceEvent('pagerduty', pagerDutySecret, signed, malformedBytes, now),
    sourceError(400, 'invalid_argument'),
  )
})

test('PagerDuty bounds the signed occurrence time before creating or canceling a task', async () => {
  assert.equal((await pagerDuty(pagerDutyEvent('incident.triggered', now - 300_000))).action, 'create')
  assert.equal((await pagerDuty(pagerDutyEvent('incident.resolved', now + 300_000))).action, 'cancel')
  await assert.rejects(pagerDuty(pagerDutyEvent('incident.triggered', now - 300_001)), sourceError(401, 'unauthorized'))
  await assert.rejects(pagerDuty(pagerDutyEvent('incident.resolved', now + 300_001)), sourceError(401, 'unauthorized'))

  const ping = {
    event: {
      id: '01CH754SM17TWPE2V2H4VPBRO7',
      event_type: 'pagey.ping',
      resource_type: 'pagey',
      occurred_at: new Date(now).toISOString(),
      data: { message: 'Hello from Pagey!', type: 'ping' },
    },
  }
  assert.deepEqual(await pagerDuty(ping), { action: 'ignore' })
  assert.deepEqual(await pagerDuty(ping, now + 300_001), { action: 'ignore' })

  const payload = pagerDutyEvent('incident.resolved')
  await assert.rejects(
    pagerDuty({ event: { ...payload.event, occurred_at: '2026-11-25T13:06:43.318' } }),
    sourceError(401, 'unauthorized'),
  )
  await assert.rejects(
    pagerDuty({ event: { ...payload.event, data: { id: ['PGR0VU2'] } } }),
    sourceError(400, 'invalid_argument'),
  )
})

const githubIssue = {
  action: 'opened',
  issue: {
    id: 91073,
    number: 42,
    title: 'Fix flaky integration test',
    created_at: new Date(now).toISOString(),
    html_url: 'https://github.example.test/acme/app/issues/42',
  },
}
const githubRun = {
  action: 'completed',
  workflow_run: {
    id: 91073,
    run_attempt: 1,
    name: 'CI',
    display_title: 'Build the release candidate',
    status: 'completed',
    conclusion: 'failure',
    created_at: new Date(now - 75 * 60_000).toISOString(),
    updated_at: new Date(now).toISOString(),
    html_url: 'https://github.example.test/acme/app/actions/runs/91073',
  },
}

function signedGitHub(event: string, payload: object) {
  const { body, headers } = signedDelivery(payload, githubSecret, 'X-Hub-Signature-256')
  headers.set('X-GitHub-Event', event)
  return { body, headers }
}

function github(event: string, payload: object) {
  const { body, headers } = signedGitHub(event, payload)
  return readSourceEvent('github', githubSecret, headers, body, now)
}

test('GitHub namespaces issue and workflow IDs and keeps one task identity across run attempts', async () => {
  assert.deepEqual(await github('issues', githubIssue), {
    action: 'create',
    task: {
      kind: 'issue',
      dedupeKey: 'issue:91073',
      title: 'Fix flaky integration test',
      sourceUrl: 'https://github.example.test/acme/app/issues/42',
      payload: githubIssue,
      adapter: 'github:v1',
    },
  })
  assert.deepEqual(await github('workflow_run', githubRun), {
    action: 'create',
    task: {
      kind: 'workflow_run',
      dedupeKey: 'workflow_run:91073',
      title: 'Build the release candidate',
      sourceUrl: 'https://github.example.test/acme/app/actions/runs/91073',
      payload: githubRun,
      adapter: 'github:v1',
    },
  })
  const retry = await github('workflow_run', {
    ...githubRun,
    workflow_run: { ...githubRun.workflow_run, run_attempt: 2, display_title: null, html_url: 'javascript:alert(1)' },
  })
  assert.equal(retry.action === 'create' && retry.task.dedupeKey, 'workflow_run:91073')
  assert.equal(retry.action === 'create' && retry.task.title, 'CI')
  assert.equal(retry.action === 'create' && retry.task.sourceUrl, undefined)
})

test('GitHub ignores other actions and authenticates even an ignored event', async () => {
  assert.deepEqual(await github('issues', { ...githubIssue, action: 'closed' }), { action: 'ignore' })
  assert.deepEqual(await github('workflow_run', { ...githubRun, action: 'in_progress' }), { action: 'ignore' })
  assert.deepEqual(
    await github('workflow_run', {
      ...githubRun,
      workflow_run: { ...githubRun.workflow_run, conclusion: 'success' },
    }),
    { action: 'ignore' },
  )
  const ping = { zen: 'Keep it logically awesome.', hook_id: 19 }
  assert.deepEqual(await github('ping', ping), { action: 'ignore' })
  const { body, headers } = signedGitHub('ping', ping)
  headers.delete('X-Hub-Signature-256')
  headers.set('X-Hub-Signature', `sha1=${createHmac('sha1', githubSecret).update(body).digest('hex')}`)
  await assert.rejects(readSourceEvent('github', githubSecret, headers, body, now), sourceError(401, 'unauthorized'))
})

test('GitHub rejects stale events despite fresh unsigned headers or unrelated timestamps', async () => {
  const payload = {
    ...githubIssue,
    issue: {
      ...githubIssue.issue,
      created_at: new Date(now - 300_001).toISOString(),
      updated_at: new Date(now).toISOString(),
    },
  }
  const { body, headers } = signedGitHub('issues', payload)
  headers.set('Date', new Date(now).toUTCString())
  headers.set('X-GitHub-Delivery', 'b3a542bf-b9f7-454d-85bc-ff84284d6c04')
  const unauthorized = sourceError(401, 'unauthorized')
  await assert.rejects(readSourceEvent('github', githubSecret, headers, body, now), unauthorized)
  const freshBody = signedGitHub('issues', githubIssue).body
  await assert.rejects(readSourceEvent('github', githubSecret, headers, freshBody, now), unauthorized)
  await assert.rejects(
    github('workflow_run', {
      ...githubRun,
      workflow_run: { ...githubRun.workflow_run, updated_at: new Date(now + 300_001).toISOString() },
    }),
    unauthorized,
  )
  await assert.rejects(
    github('issues', { ...githubIssue, issue: { ...githubIssue.issue, id: 9_007_199_254_740_992 } }),
    sourceError(400, 'invalid_argument'),
  )
})

function generic(payload: object) {
  const { body, headers } = signedDelivery(payload, genericSecret, 'X-Pullcord-Signature')
  return readSourceEvent('generic', genericSecret, headers, body, now)
}

test('Generic HMAC uses the signed dedupe key without letting the body set hub fields', async () => {
  const payload = {
    timestamp: now,
    dedupe_key: 'build#994',
    title: 'Failed deployment – π',
    data: { service: 'checkout' },
    action: 'cancel',
    queue: 'admin',
    source: 'another',
    retry: 'safe',
    kind: 'arbitrary',
    state: 'completed',
  }
  assert.deepEqual(await generic(payload), {
    action: 'create',
    task: { kind: 'event', dedupeKey: 'build#994', title: 'Failed deployment – π', payload, adapter: 'generic:v1' },
  })
  const retry = await generic({ ...payload, timestamp: now + 450, title: 'Changed retry message' })
  assert.equal(retry.action === 'create' && retry.task.dedupeKey, 'build#994')
})

test('Generic HMAC binds the timestamp to the body and rejects stale or malformed input', async () => {
  const payload = { timestamp: now - 300_001, dedupe_key: 'event-7', title: 'Hello' }
  const { headers } = signedDelivery(payload, genericSecret, 'X-Pullcord-Signature')
  const unauthorized = sourceError(401, 'unauthorized')
  const freshBody = signedDelivery({ ...payload, timestamp: now }, genericSecret, 'X-Pullcord-Signature').body
  await assert.rejects(readSourceEvent('generic', genericSecret, headers, freshBody, now), unauthorized)
  await assert.rejects(generic(payload), unauthorized)
  await assert.rejects(generic({ ...payload, timestamp: String(now) }), unauthorized)
  await assert.rejects(generic({ ...payload, timestamp: now, dedupe_key: '' }), sourceError(400, 'invalid_argument'))
})
