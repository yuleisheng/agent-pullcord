import assert from 'node:assert/strict'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { after, test } from 'node:test'
import fc from 'fast-check'
import { openStore, StoreError, type Store, type Task } from '../src/store.ts'
import { checkInvariants, MEMO_KEY, Model, operation, type Call, type Outcome } from './lifecycle-model.ts'

const dir = mkdtempSync(join(tmpdir(), 'pullcord-lifecycle-'))
after(() => rmSync(dir, { recursive: true, force: true }))

function send(store: Store, call: Call): Outcome {
  const who = { actor: call.actor, requestId: call.requestId }
  const holder = { ...who, epoch: call.epoch }
  try {
    if (call.t === 'memo') return { ok: { value: store.memo(call.id, MEMO_KEY, call.value, who) } }
    if (call.t === 'log') {
      const event = store.log(call.id, { ...who, text: call.text })
      assert.equal(event.type, 'task.logged')
      return { ok: event.data as { text: string } }
    }
    const task: Task | null = (() => {
      switch (call.t) {
        case 'claim':
          return store.claim({ queue: 'q', leaseMs: call.lease }, who)
        case 'renew':
          return store.renew(call.id, { ...holder, leaseMs: call.lease })
        case 'complete':
          return store.complete(call.id, { ...holder, result: 'done' })
        case 'fail':
          return store.fail(call.id, { ...holder, reason: 'broke', retry: call.retry })
        case 'release':
          return store.release(call.id, holder)
        case 'block':
          return store.block(call.id, { ...holder, reason: 'approve?' })
        case 'unblock':
          return store.unblock(call.id, who)
        case 'requeue':
          return store.requeue(call.id, who)
        case 'cancel':
          return store.cancel(call.id, { ...who, reason: 'resolved' })
      }
    })()
    return { ok: task && { id: task.id, state: task.state, epoch: task.epoch } }
  } catch (error) {
    if (error instanceof StoreError) return { code: error.code }
    throw error
  }
}

test('generated sequences keep the store equal to the lifecycle model', () => {
  let run = 0
  fc.assert(fc.property(fc.array(operation, { maxLength: 80, size: 'max' }), (steps) => {
    let now = 0
    const store = openStore({ path: join(dir, `${run++}.db`), clock: () => now })
    const model = new Model()
    let cursor = 0
    try {
      for (const [i, step] of steps.entries()) {
        let changed = false
        if (step.t === 'advance') {
          now += step.ms
        } else if (step.t === 'ingest') {
          const { created } = store.ingest({ queue: 'q', kind: 'k', source: 's', dedupeKey: `k${step.key}`, title: 't',
            retry: step.retry, maxAttempts: step.max }, { actor: 'source:s' })
          changed = model.ingest(step.key, step.retry, step.max, now)
          assert.equal(created, changed)
        } else {
          const call = model.call(step, i)
          const expected = model.apply(call, now)
          assert.deepEqual(send(store, call), expected.outcome, `step ${i}: ${JSON.stringify(call)}`)
          changed = expected.changed
        }
        const events = store.events()
        assert.equal(events.slice(cursor).filter((e) => e.type !== 'task.lease_expired').length,
          changed ? 1 : 0, `step ${i}: a write appends one event only when it changes something`)
        cursor = events.length
        checkInvariants(model, now, store.tasks().tasks.map(({ id }) => store.task(id)), events)
      }
    } finally {
      store.close()
    }
  }), { numRuns: 200 })
})
