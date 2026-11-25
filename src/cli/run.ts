import { spawn, type ChildProcess } from 'node:child_process'
import { constants } from 'node:os'
import { setTimeout as delay } from 'node:timers/promises'
import { DEFAULT_LEASE_MS } from '../store-contract.ts'
import { CliError, type ClaimSelector, type Client } from './client.ts'

type Exit = { code: number; reason?: string }

const useProcessGroup = process.platform !== 'win32'
const STOP_GRACE_MS = 2_000
const STOP_POLL_MS = 25

function watchExit(child: ChildProcess): Promise<Exit> {
  return new Promise((resolve) => {
    child.once('error', (error: NodeJS.ErrnoException) => {
      resolve({ code: error.code === 'ENOENT' ? 127 : 1, reason: `Could not start the command${error.code ? ` (${error.code})` : ''}.` })
    })
    child.once('close', (code, signal) => {
      if (signal) resolve({ code: 128 + constants.signals[signal], reason: `Command was terminated by ${signal}.` })
      else if (code === 0) resolve({ code })
      else resolve({ code: code ?? 1, reason: `Command exited with status ${code ?? 1}.` })
    })
  })
}

function signalCommand(child: ChildProcess, signal: NodeJS.Signals | 0): boolean {
  if (!child.pid) return false
  if (!useProcessGroup) {
    return child.exitCode === null && child.signalCode === null && child.kill(signal)
  }
  try {
    return process.kill(-child.pid, signal)
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error
    return false
  }
}

async function waitForCommandToStop(child: ChildProcess): Promise<boolean> {
  const deadline = performance.now() + STOP_GRACE_MS
  while (signalCommand(child, 0)) {
    if (performance.now() >= deadline) return false
    await delay(STOP_POLL_MS)
  }
  return true
}

async function stopCommand(child: ChildProcess, signal: NodeJS.Signals): Promise<void> {
  if (!signalCommand(child, signal) || await waitForCommandToStop(child)) return
  if (!signalCommand(child, 'SIGKILL') || await waitForCommandToStop(child)) return
  throw new CliError('command_not_stopped', 'Could not confirm the command stopped. The claim will expire.')
}

export async function run(client: Client, selector: ClaimSelector, command: string[], json: boolean): Promise<unknown> {
  const renewals = new AbortController()
  const interval = DEFAULT_LEASE_MS / 3
  let child: ChildProcess | undefined
  let shutdown: Promise<void> | undefined
  let shutdownError: unknown
  let interrupted: NodeJS.Signals | undefined

  function stopChild(signal: NodeJS.Signals) {
    renewals.abort()
    if (child && !shutdown) shutdown = stopCommand(child, signal).catch((error: unknown) => { shutdownError = error })
  }

  function interrupt(signal: NodeJS.Signals) {
    interrupted ??= signal
    stopChild(signal)
  }

  process.on('SIGINT', interrupt)
  process.on('SIGTERM', interrupt)
  try {
    const task = await client.claim(selector, { lease: DEFAULT_LEASE_MS / 1_000 })
    let exit: Exit | undefined
    if (!interrupted) {
      try {
        child = spawn(command[0], command.slice(1), {
          detached: useProcessGroup, env: { ...process.env, PULLCORD_TASK: JSON.stringify(task) }, stdio: [0, json ? 2 : 1, 2],
        })
      } catch {
        exit = { code: 1, reason: 'Could not start the command.' }
      }
      if (child) {
        let renewalFailure: unknown
        const renewing = (async () => {
          while (await delay(interval, true, { signal: renewals.signal }).catch(() => false) && !renewals.signal.aborted) {
            await client.holder('renew', task, { lease: DEFAULT_LEASE_MS / 1_000 }, AbortSignal.timeout(interval))
          }
        })().catch((error: unknown) => {
          renewalFailure = error
          stopChild('SIGTERM')
        })
        exit = await watchExit(child)
        renewals.abort()
        await renewing
        await shutdown
        if (renewalFailure) throw renewalFailure
        if (shutdownError) throw shutdownError
      }
    }

    if (interrupted) {
      process.exitCode = 128 + constants.signals[interrupted]
      return await client.holder('release', task)
    }
    if (exit?.reason) {
      process.exitCode = exit.code
      return await client.holder('fail', task, { reason: exit.reason })
    }
    return await client.holder('complete', task)
  } finally {
    process.off('SIGINT', interrupt)
    process.off('SIGTERM', interrupt)
  }
}
