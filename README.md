# celld-demo — place: a forward-only pixel board that explains itself

An r/place-style pixel board on [celld](https://celld.dev) — self-hosted,
distributed Durable Objects by the Deno team. Live at
[canvas.krasnoperov.me](https://canvas.krasnoperov.me/).

One pixel per minute per identity. **Forward-only: nothing is ever erased** —
every pixel ever placed is a row in a tile's SQLite, forever. The panel next to
the board is the app introspecting the technology it runs on, in real time.

## Architecture

Write and read paths scale on different axes, so they are separate:

```
WRITE (bounded by cooldown, batched)
  POST /place/px -> Worker -> User DO (atomic cooldown) -> Tile DO
  The tile buffers placements and flushes them as ONE multi-row insert every
  ~50ms; callers are acked only after the flush is durable (RPO = 0).

READ (frames on the CDN, zero sockets)
  Tile DOs cut frames on durable alarms: a diff frame ~1/s when dirty, a full
  frame every 30 diffs. Frames are immutable blobs behind long-lived cache
  headers; a 1-second manifest points at them. Viewers poll the Cloudflare
  cache — the origin renders frames at a fixed rate and never sees them.
```

- The 512×512 board is **4 Tile objects** of 256×256 — sharding by
  construction: a pixel war in one corner cannot slow the rest, and a bigger
  board is just more tiles.
- The **cooldown is a User object per identity** — a single-threaded
  check-and-set that cannot be raced. It doesn't care whether its owner is a
  human or an agent; OAuth slots into the same addressing later.
- Identity is a random 128-bit cookie for now.
- `/place/history/:tx/:ty` streams the append-only log (the timelapse feed);
  inspect mode on the page shows who owns any pixel and since when.

There are deliberately **no formal scalability defects** left in the serving
path: viewer cost at the origin is O(1) per tile (manifest + frame renders),
write throughput is one durable flush per tile per 50ms regardless of batch
size, and per-user state is sharded across User objects. The remaining levers
(more tiles, more nodes on the same bucket) are configuration, not redesign.

Frame/manifest URLs end in `.bin` deliberately: BIN is on Cloudflare's
default-cacheable extension list, so the CDN caches them with zero zone
configuration. A Cache Rule can make the URLs prettier later.

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

Open <http://127.0.0.1:8080/>. The code is a standard Wrangler project
(`wrangler.jsonc` is the config format celld reads), so it also runs unchanged
on `wrangler dev` or Cloudflare.

## Security notes

- The server never executes anything a client sends: every field of every
  request is validated, clamped, and length-capped server-side.
- Cooldown per identity, a flood floor per tile, strict CSP on the page.
- The node binds to localhost; expose it through a reverse proxy (Caddy/nginx)
  that terminates TLS.
