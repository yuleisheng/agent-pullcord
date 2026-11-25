import { isoTime, requiredObject, requiredString, requireFresh, SourceError, sourceUrl, type SourceEvent } from './signed-body.ts'

function createTask(
  kind: 'issue' | 'workflow_run',
  resource: Record<string, unknown>,
  title: string,
  payload: Record<string, unknown>,
): SourceEvent {
  const { id } = resource
  if (typeof id !== 'number' || !Number.isSafeInteger(id) || id < 1) {
    throw new SourceError('invalid_argument', `${kind}.id must be a positive safe integer.`)
  }
  return {
    action: 'create',
    task: {
      kind,
      dedupeKey: `${kind}:${id}`,
      title,
      sourceUrl: sourceUrl(resource.html_url),
      payload,
      adapter: 'github:v1',
    },
  }
}

export function gitHub(payload: Record<string, unknown>, eventHeader: string | null, now: number): SourceEvent {
  // GitHub provides no signed delivery-time header: https://docs.github.com/en/webhooks/webhook-events-and-payloads#delivery-headers
  const event = requiredString(eventHeader, 'X-GitHub-Event')
  if (event === 'issues' && requiredString(payload.action, 'action') === 'opened') {
    const issue = requiredObject(payload.issue, 'issue')
    requireFresh(isoTime(issue.created_at), now)
    return createTask('issue', issue, requiredString(issue.title, 'issue.title'), payload)
  }
  if (event === 'workflow_run' && requiredString(payload.action, 'action') === 'completed') {
    const run = requiredObject(payload.workflow_run, 'workflow_run')
    if (run.status !== 'completed') throw new SourceError('invalid_argument', 'workflow_run.status must be completed for a completed event.')
    if (requiredString(run.conclusion, 'workflow_run.conclusion') !== 'failure') return { action: 'ignore' }
    requireFresh(isoTime(run.updated_at), now)
    const title = typeof run.display_title === 'string' && run.display_title.trim() ? run.display_title : run.name
    return createTask('workflow_run', run, requiredString(title, 'workflow_run.display_title or workflow_run.name'), payload)
  }
  return { action: 'ignore' }
}
