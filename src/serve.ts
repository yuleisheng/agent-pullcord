import { readFileSync } from 'node:fs'
import { createServer } from 'node:http'
import { Readable } from 'node:stream'
import { parseArgs } from 'node:util'
import { createApi } from './api.ts'
import { DEFAULT_DB, DEFAULT_KEYS, DEFAULT_SOURCES } from './config.ts'
import { openStore, type Store } from './store.ts'

export function createHttpServer(store: Store, keys: unknown, sources: unknown = []) {
  const api = createApi(store, keys, sources)
  return createServer({ requestTimeout: 30_000 }, async (req, res) => {
    try {
      const target = req.url?.startsWith('/') ? req.url : '/'
      const abort = new AbortController()
      res.once('close', () => abort.abort())
      const init: RequestInit & { duplex?: 'half' } = {
        method: req.method,
        headers: Object.fromEntries(Object.entries(req.headersDistinct).map(([name, values]) => [name, values?.join(', ') ?? ''])),
        signal: abort.signal,
      }
      if (req.method === 'POST') {
        init.body = Readable.toWeb(req, { strategy: { highWaterMark: 1 } }) as ReadableStream<Uint8Array>
        init.duplex = 'half'
      }
      const response = await api(new Request(`http://pullcord${target}`, init))
      const body = await response.text()
      if (req.method === 'POST' && !req.readableEnded) res.setHeader('Connection', 'close')
      if (!res.destroyed) res.writeHead(response.status, Object.fromEntries(response.headers)).end(body)
    } catch {
      res.destroy()
    }
  })
}

function readConfig(path: string, kind: 'key' | 'source', optional = false): unknown {
  let raw: string
  try {
    raw = readFileSync(path, 'utf8')
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT' && optional) return []
    throw new Error(`The ${kind} file could not be read. Check its path and permissions.`)
  }
  try {
    return JSON.parse(raw)
  } catch {
    throw new Error(`The ${kind} file is not valid JSON.`)
  }
}

function openHubStore(path: string): Store {
  const umask = process.umask(0o077)
  try {
    return openStore({ path })
  } finally {
    process.umask(umask)
  }
}

export function initializeDatabase(path: string): void {
  openHubStore(path).close()
}

export function serve(args: string[]): void {
  const { values } = parseArgs({
    args,
    options: {
      db: { type: 'string', default: DEFAULT_DB },
      keys: { type: 'string', default: DEFAULT_KEYS },
      sources: { type: 'string' },
      listen: { type: 'string', default: '127.0.0.1:7337' },
      json: { type: 'boolean' },
    },
  })
  const addr = /^(?:\[([^\s[\]]+)\]|([^:\s[\]]+)):(\d+)$/.exec(values.listen)
  if (!addr || Number(addr[3]) > 65535) throw new Error('Pass --listen as host:port (use [address]:port for IPv6).')
  const host = addr[1] ?? addr[2]
  const port = Number(addr[3])
  const keys = readConfig(values.keys, 'key')
  const sources = readConfig(values.sources ?? DEFAULT_SOURCES, 'source', values.sources === undefined)
  const store = openHubStore(values.db)
  let server: ReturnType<typeof createHttpServer>
  try {
    server = createHttpServer(store, keys, sources)
  } catch (error) {
    store.close()
    throw error
  }
  server.on('error', (error) => {
    console.error(values.json ? JSON.stringify({ code: 'network_error', message: error.message }) : error.message)
    process.exitCode = 1
    server.close()
  })
  server.once('close', () => store.close())
  server.listen(port, host, () => {
    const bound = server.address()
    if (bound && typeof bound !== 'string') {
      const address = bound.family === 'IPv6' ? `[${bound.address}]` : bound.address
      const url = `http://${address}:${bound.port}`
      console.log(values.json ? JSON.stringify({ url }) : `Listening on ${url}`)
    }
  })
}

if (import.meta.main) {
  try {
    serve(process.argv.slice(2))
  } catch (error) {
    console.error(error instanceof Error ? error.message : 'The hub could not start.')
    process.exitCode = 1
  }
}
