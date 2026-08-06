# celld-demo — a shared canvas that explains itself

A collaborative drawing canvas where the UI doubles as a live diagram of the
technology it runs on: [celld](https://celld.dev), self-hosted, distributed
Durable Objects by the Deno team.

Every room is **one Durable Object** — a small single-threaded server with a
name and a private SQLite database. Next to the canvas, the page shows that
object introspecting itself in real time:

- **object id** — `idFromName(room)`: same room name, same object, wherever it lives
- **cold activations** — climbs every time the object is evicted (hibernation,
  node restart) and wakes back up with its state intact
- **open sockets / peers** — every browser in the room holds a hibernatable
  WebSocket into the same object
- **strokes in sqlite / last durable seq** — each stroke is a row; the ack
  arrives only after the write is durable in your S3 bucket (RPO = 0)
- **wire** — the actual JSON frames going over the socket

Open the same URL in two windows and draw. Kill the node, start it again —
the room comes back from the bucket, `cold activations` ticks up.

## Why Durable Objects make this trivial

| Concern | Plain Node.js | Durable Object |
|---|---|---|
| Ordering concurrent edits | locks / Redis + Lua | object is single-threaded: message order = stroke order |
| All room clients on one node | sticky sessions + pub/sub | `idFromName(room)` routes everyone to the same object |
| State survives restarts | separate DB + cache + migrations | the object's SQLite *is* the DB, next to the code |
| 10 000 idle rooms | 10 000 open sockets hold a process | hibernation: ~0 cost, wakes in ms |
| Scale to N machines | all of the above × N | start another node with the same `--bucket` |

The whole server is [index.js](index.js): a Worker that routes `/ws/:room` to
the room's object, and the `Canvas` class (~150 lines) that accepts sockets,
persists strokes, and broadcasts. The UI is inlined in the same file — the
deploy is two files.

## Run it

Requires an S3-compatible bucket — that is celld's entire coordination layer
(no control plane, no consensus). Locally, MinIO works:

```bash
docker run -d --name celld-minio -p 127.0.0.1:9000:9000 minio/minio server /data
docker run --rm --network host --entrypoint sh minio/mc -c \
  "mc alias set local http://127.0.0.1:9000 minioadmin minioadmin && mc mb local/cells"
```

Install celld and esbuild, deploy, run a node:

```bash
curl -fsSL https://celld.dev/install.sh | sh
npm install   # esbuild, used by `celld deploy` for bundling

export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_REGION=us-east-1
export PATH="$PWD/node_modules/.bin:$PATH"

celld deploy . --bucket s3://cells --endpoint http://127.0.0.1:9000
celld --bucket s3://cells --endpoint http://127.0.0.1:9000 --listen 127.0.0.1:8080
```

Open <http://127.0.0.1:8080/> in two windows. Any path is a room:
`/lobby`, `/friends`, `/whatever` — each one is its own object with its own
SQLite database.

The code is a standard Wrangler project (`wrangler.jsonc` is the config format
celld reads), so it also runs unchanged on `wrangler dev` or Cloudflare.

## Optional: an agent as a peer

[agent/](agent/) contains a Claude-powered drawing partner. It is deliberately
*not* special: just another WebSocket client of the same object, speaking the
same protocol with `?role=agent`. It watches strokes and chat, and draws back.

```bash
cd agent && npm install
ANTHROPIC_API_KEY=... CANVAS_URL=ws://127.0.0.1:8080 node agent.mjs lobby
```

Not used on the public deployment.

## Security notes

- The server never executes anything a client sends: peers exchange small JSON
  data frames; every field is validated, clamped, and length-capped server-side.
- Per-socket rate limiting, per-room socket cap (32), history bounded by rows
  (2000) and bytes (4 MB, oldest trimmed), strict CSP on the page.
- Only a real WebSocket upgrade instantiates a room object; plain requests to
  `/ws/*` are rejected in the Worker before touching a Durable Object.
- The node binds to localhost; expose it through a reverse proxy (Caddy/nginx)
  that terminates TLS and proxies WebSockets. Room creation is open by design
  (any URL path is a room) — add proxy-level rate limiting if that matters
  to you.
