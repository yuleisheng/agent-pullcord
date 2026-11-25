import { readFileSync } from 'node:fs'
import { createServer } from 'node:http'
import { Readable } from 'node:stream'
import { parseArgs } from 'node:util'
import { createApi } from './api.ts'
import { openStore, type Store } from './store.ts'

export function createHttpServer(store: Store, keys: unknown) {
  const api = createApi(store, keys)
  return createServer({ requestTimeout: 30_000 }, async (req, res) => {
    try {
      const target = req.url?.startsWith('/') ? req.url : '/'
      const abort = new AbortController()
      res.once('close', () => abort.abort())
      const init: RequestInit & { duplex?: 'half' } = {
        method: req.method,
        headers: {
          authorization: req.headersDistinct.authorization?.join(', ') ?? '',
          'content-type': req.headersDistinct['content-type']?.join(', ') ?? '',
        },
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

function serve(): void {
  const { values } = parseArgs({
    options: {
      db: { type: 'string', default: 'pullcord.db' },
      keys: { type: 'string' },
      listen: { type: 'string', default: '127.0.0.1:7337' },
    },
  })
  if (!values.keys) throw new Error('Pass --keys <path> to a JSON file of scoped key digests.')
  const addr = /^(?:\[([^\s[\]]+)\]|([^:\s[\]]+)):(\d+)$/.exec(values.listen)
  if (!addr || Number(addr[3]) > 65535) throw new Error('Pass --listen as host:port (use [address]:port for IPv6).')
  const host = addr[1] ?? addr[2]
  const port = Number(addr[3])
  const rawKeys = readFileSync(values.keys, 'utf8')
  let keys: unknown
  try {
    keys = JSON.parse(rawKeys)
  } catch {
    throw new Error('The key file is not valid JSON.')
  }

  const store = openStore({ path: values.db })
  const server = createHttpServer(store, keys)
  server.on('error', (error) => {
    console.error(error.message)
    process.exitCode = 1
  })
  server.once('close', () => store.close())
  server.listen(port, host, () => {
    const bound = server.address()
    if (bound && typeof bound !== 'string') {
      const address = bound.family === 'IPv6' ? `[${bound.address}]` : bound.address
      console.log(`Listening on http://${address}:${bound.port}`)
    }
  })
}

if (import.meta.main) {
  try {
    serve()
  } catch (error) {
    console.error(error instanceof Error ? error.message : 'The hub could not start.')
    process.exitCode = 1
  }
}
