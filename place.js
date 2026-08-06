// place — an r/place-style pixel board on Durable Objects. Forward-only:
// pixels are never wiped, every placement is a row in a tile's SQLite forever.
//
// Architecture (the "correct" tier from the design doc, on by default):
//
//   WRITE PATH — bounded by cooldown, batched:
//     POST /place/px -> Worker -> User shard (atomic cooldown) -> Tile.
//     Both objects buffer their writes and flush each ~50ms window as ONE
//     durable unit; callers are acked only after the flush is durable.
//     This removes the per-write flush ceiling without weakening RPO=0.
//
//   READ PATH — frames on the CDN, zero sockets:
//     Each tile cuts frames on a durable alarm: a diff frame (~1s cadence,
//     only when dirty) and a full frame every FULL_EVERY diffs. Frames are
//     immutable blobs served with long-lived cache headers; a tiny manifest
//     (edge TTL 1s) points at them. A million viewers poll the Cloudflare
//     cache; the origin renders frames at a fixed rate and never sees them.
//
// The cooldown lives in a fixed set of shard objects (see the User section);
// it doesn't care whether its owner is a human, an agent, or an OAuth login.
//
// uid doubles as the bearer token, so it must never leave the server intact:
// every public surface emits at most an 8-hex prefix of it.

const TILE = 256;                 // pixels per tile side
const TILES = 2;                  // board is TILES x TILES tiles
const BOARD = TILE * TILES;
const COOLDOWN_MS = 60_000;
const PALETTE = [
  "#ffffff", "#e4e4e4", "#888888", "#222222",
  "#ffa7d1", "#e50000", "#e59500", "#a06a42",
  "#e5d900", "#94e044", "#02be01", "#00d3dd",
  "#0083c7", "#0000ea", "#cf6ee4", "#820080",
];
const TILE_RATE = 30;             // per-tile placement ceiling, per second
const FLUSH_MS = 50;              // write batching window
const FRAME_MS = 1000;            // min interval between diff frames
const FULL_EVERY = 30;            // cut a full frame every N diffs

// Frame wire format (little-endian):
//   full frame: 32768 bytes, one nibble per pixel, row-major
//   diff frame: u32 per pixel: (idx & 0xffff) | (color << 16)

// --- Tile: one object per 256x256 board section -----------------------------

export class Tile {
  constructor(state, env) {
    this.state = state;
    this.activations = null;
    this.rate = { ts: 0, n: 0 };
    this.pending = [];            // buffered placements awaiting the next flush
    this.state.storage.sql.exec(
      `CREATE TABLE IF NOT EXISTS pixels (
         seq INTEGER PRIMARY KEY AUTOINCREMENT,
         x INTEGER NOT NULL, y INTEGER NOT NULL, c INTEGER NOT NULL,
         uid TEXT NOT NULL, ts INTEGER NOT NULL
       )`,
    );
  }

  // Bitmap = current section state, one nibble per pixel (32 KB). Saved with
  // every flush; on activation any tail beyond the save is replayed from SQL.
  init() {
    return (this.initing ??= (async () => {
      this.activations = ((await this.state.storage.get("activations")) ?? 0) + 1;
      await this.state.storage.put("activations", this.activations);
      const saved = await this.state.storage.get("bitmap");
      this.bitmap = saved?.bytes
        ? new Uint8Array(saved.bytes) : new Uint8Array((TILE * TILE) / 2);
      this.seq = saved?.seq ?? 0;
      const tail = this.state.storage.sql
        .exec("SELECT seq, x, y, c FROM pixels WHERE seq > ? ORDER BY seq", this.seq)
        .toArray();
      for (const p of tail) { this.setNibble(p.x, p.y, p.c); this.seq = p.seq; }
      // frames: { list: [{seq, from}], fullSeq, prevFullSeq }
      this.frames = (await this.state.storage.get("frames"))
        ?? { list: [], fullSeq: -1, prevFullSeq: null };
      if (this.frames.fullSeq === -1) await this.cutFullFrame();  // genesis frame
    })());
  }

  setNibble(x, y, c) {
    const i = y * TILE + x;
    const b = i >> 1;
    this.bitmap[b] = (i & 1)
      ? (this.bitmap[b] & 0x0f) | (c << 4)
      : (this.bitmap[b] & 0xf0) | c;
  }

  // --- write path ----------------------------------------------------------

  // Batching under celld's deadlock guard: a handler may only await work it
  // owns, so every request sleeps out the batching window on its own timer;
  // whichever wakes first flushes the whole buffer, the rest find their row
  // already assigned. One durable unit per window either way.
  async place(x, y, c, uid) {
    const entry = { x, y, c, uid, ts: Date.now(), seq: null, err: null };
    this.pending.push(entry);
    await new Promise((r) => setTimeout(r, FLUSH_MS));
    if (entry.seq === null && !entry.err) {
      await this.flush().catch(() => {});   // failure lands on entry.err below
    }
    if (entry.err || entry.seq === null) throw entry.err ?? new Error("flush incomplete");
    return entry.seq;
  }

  async flush() {
    const batch = this.pending;
    this.pending = [];
    if (batch.length === 0) return;
    try {
      // One durable unit for the whole batch: a single multi-row INSERT.
      const values = batch.map(() => "(?, ?, ?, ?, ?)").join(", ");
      const args = batch.flatMap((p) => [p.x, p.y, p.c, p.uid, p.ts]);
      const rows = this.state.storage.sql.exec(
        `INSERT INTO pixels (x, y, c, uid, ts) VALUES ${values} RETURNING seq`,
        ...args,
      ).toArray();
      batch.forEach((p, i) => {
        this.setNibble(p.x, p.y, p.c);
        p.seq = rows[i].seq;
      });
      this.seq = rows[rows.length - 1].seq;
      await this.state.storage.put("bitmap", { seq: this.seq, bytes: this.bitmap.buffer.slice(0) });

      // Arm the frame alarm unless one is already pending. The alarm is
      // durable: it survives hibernation, so the last pixels always frame.
      if ((await this.state.storage.getAlarm()) === null) {
        await this.state.storage.setAlarm(Date.now() + FRAME_MS);
      }
    } catch (err) {
      // The whole window fails together — no caller may ack a write that
      // didn't reach durability.
      for (const p of batch) p.err = err;
      throw err;
    }
  }

  // --- read path: frame cutting on a durable alarm -------------------------

  async alarm() {
    await this.init();
    if (this.seq > this.lastFrameSeq()) await this.cutDiffFrame();
    if (this.pending.length > 0 || this.seq > this.lastFrameSeq()) {
      await this.state.storage.setAlarm(Date.now() + FRAME_MS);
    }
  }

  lastFrameSeq() {
    const l = this.frames.list;
    return l.length ? l[l.length - 1].seq : this.frames.fullSeq;
  }

  async cutDiffFrame() {
    const from = this.lastFrameSeq();
    const rows = this.state.storage.sql.exec(
      "SELECT x, y, c FROM pixels WHERE seq > ? ORDER BY seq", from).toArray();
    if (rows.length === 0) return;
    const buf = new Uint32Array(rows.length);
    rows.forEach((p, i) => { buf[i] = (p.y * TILE + p.x) | (p.c << 16); });
    await this.state.storage.put(`f:${this.seq}`, buf.buffer);
    this.frames.list.push({ seq: this.seq, from });

    if (this.frames.list.length >= FULL_EVERY) await this.cutFullFrame();
    await this.state.storage.put("frames", this.frames);
  }

  // A new full frame resets the serving window: the diffs it covers are
  // pruned, and only one previous full is kept for viewers mid-chain (the
  // CDN keeps serving older immutable frames from its cache regardless).
  async cutFullFrame() {
    await this.state.storage.put(`f:${this.seq}:full`, this.bitmap.buffer.slice(0));
    for (const f of this.frames.list) await this.state.storage.delete(`f:${f.seq}`);
    if (this.frames.prevFullSeq != null) {
      await this.state.storage.delete(`f:${this.frames.prevFullSeq}:full`);
    }
    this.frames = { list: [], fullSeq: this.seq, prevFullSeq: this.frames.fullSeq };
    await this.state.storage.put("frames", this.frames);
  }

  meta() {
    return {
      activations: this.activations,
      placements: this.seq,   // total placements ever (overwrites included)
    };
  }

  // --- plumbing ------------------------------------------------------------

  async fetch(request) {
    await this.init();
    const url = new URL(request.url);

    if (url.pathname === "/px" && request.method === "POST") {
      const now = Date.now();
      if (now - this.rate.ts >= 1000) this.rate = { ts: now, n: 0 };
      if (++this.rate.n > TILE_RATE) return json({ ok: false, error: "tile busy" }, 429);
      const { x, y, c, uid } = await request.json();
      const seq = await this.place(x, y, c, uid);
      return json({ ok: true, seq });
    }

    if (url.pathname === "/manifest") {
      // Everything a cold or warm viewer needs: which full frame to load and
      // which diffs to chain on top. Meta rides along for the UI panel.
      return json({
        seq: this.lastFrameSeq(),
        full: this.frames.fullSeq,
        diffs: this.frames.list
          .filter((f) => f.seq > this.frames.fullSeq)
          .map((f) => ({ seq: f.seq, from: f.from })),
        meta: this.meta(),
      });
    }

    let m = url.pathname.match(/^\/frame\/(\d+)(:full)?$/);
    if (m) {
      const key = `f:${m[1]}${m[2] ?? ""}`;
      const bytes = await this.state.storage.get(key);
      if (!bytes) return new Response("frame expired", { status: 404 });
      return new Response(bytes, {
        headers: { "content-type": "application/octet-stream" },
      });
    }

    // uid is the bearer token — public reads get an 8-hex prefix, never more.
    if (url.pathname === "/who") {
      const x = +url.searchParams.get("x"), y = +url.searchParams.get("y");
      const rows = this.state.storage.sql.exec(
        "SELECT substr(uid, 1, 8) AS uid, ts FROM pixels WHERE x = ? AND y = ? ORDER BY seq DESC LIMIT 1",
        x, y,
      ).toArray();
      return json(rows[0] ?? null);
    }

    if (url.pathname === "/history") {
      const since = +(url.searchParams.get("since") ?? 0);
      const rows = this.state.storage.sql.exec(
        "SELECT seq, x, y, c, substr(uid, 1, 8) AS uid, ts FROM pixels WHERE seq > ? ORDER BY seq LIMIT 10000",
        since,
      ).toArray();
      return json(rows);
    }

    return new Response("not found", { status: 404 });
  }
}

// --- User: cooldown SHARDS, not an object per person -------------------------
//
// A dedicated object per user is the wrong granularity for 16 bytes of state:
// every placement would durably write an unbatchable one-row database (1 Class
// A op each on R2), and a viral peak makes "unique users of the last 5 min"
// resident in RAM. Instead the cooldown lives in USER_SHARDS objects addressed
// by hash(uid): atomicity is unchanged (a shard is single-threaded), writes
// batch exactly like the tile's, rejects don't write at all, and the object
// population is a constant. Per-user objects come back later, opt-in, for the
// features that earn them (standing orders, agents) — not for a timestamp.
//
// The class keeps the name `User` so celld migrations stay untouched.

const USER_SHARDS = 64;

function shardFor(uid) {
  let h = 0x811c9dc5;                       // FNV-1a over the uid hex
  for (let i = 0; i < uid.length; i++) {
    h ^= uid.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return `s:${(h >>> 0) % USER_SHARDS}`;
}

export class User {
  constructor(state, env) {
    this.state = state;
    this.pending = [];                       // accepted placements awaiting flush
    this.pendingNext = new Map();            // uid -> nextAt not yet durable
    this.state.storage.sql.exec(
      `CREATE TABLE IF NOT EXISTS users (
         uid TEXT PRIMARY KEY,
         nextAt INTEGER NOT NULL,
         placed INTEGER NOT NULL
       )`,
    );
  }

  row(uid) {
    return this.state.storage.sql
      .exec("SELECT nextAt, placed FROM users WHERE uid = ?", uid)
      .toArray()[0];
  }

  // An awaited kv write: makes the whole window's SQL durable before any ack.
  barrier() {
    return this.state.storage.put("v", Date.now());
  }

  // Accept/reject is decided synchronously against SQL + the in-flight
  // overlay, so two requests for the same uid in one batching window cannot
  // both pass. Accepted entries flush as ONE upsert per window (same
  // own-timer pattern as the tile — see its comment on the deadlock guard).
  async tryPlace(uid) {
    const now = Date.now();
    const eff = this.pendingNext.get(uid) ?? this.row(uid)?.nextAt ?? 0;
    if (now < eff) return { ok: false, cooldown: true, next: eff };  // reject: zero writes
    const next = now + COOLDOWN_MS;
    this.pendingNext.set(uid, next);
    const entry = { uid, next, done: false, err: null };
    this.pending.push(entry);
    await new Promise((r) => setTimeout(r, FLUSH_MS));
    if (!entry.done && !entry.err) await this.flush().catch(() => {});
    if (entry.err || !entry.done) {
      return { ok: false, error: "cooldown store unavailable" };
    }
    const placed = this.row(uid)?.placed ?? 1;
    return { ok: true, next, placed };
  }

  async flush() {
    const batch = this.pending;
    this.pending = [];
    if (batch.length === 0) return;
    try {
      const values = batch.map(() => "(?, ?, 1)").join(", ");
      const args = batch.flatMap((p) => [p.uid, p.next]);
      this.state.storage.sql.exec(
        `INSERT INTO users (uid, nextAt, placed) VALUES ${values}
         ON CONFLICT(uid) DO UPDATE SET
           nextAt = excluded.nextAt, placed = users.placed + 1`,
        ...args,
      );
      await this.barrier();
      for (const p of batch) {
        p.done = true;
        if (this.pendingNext.get(p.uid) === p.next) this.pendingNext.delete(p.uid);
      }
    } catch (err) {
      // Fail the whole window together and roll the overlay back so the
      // affected users aren't stuck behind a cooldown that never persisted.
      for (const p of batch) {
        p.err = err;
        if (this.pendingNext.get(p.uid) === p.next) this.pendingNext.delete(p.uid);
      }
      throw err;
    }
  }

  async fetch(request) {
    const url = new URL(request.url);
    const uid = url.searchParams.get("uid") ?? "";
    if (!/^[0-9a-f]{32}$/.test(uid)) return json({ ok: false }, 400);

    if (url.pathname === "/try" && request.method === "POST") {
      return json(await this.tryPlace(uid));
    }
    // Refund: the cooldown was consumed but the pixel never landed (e.g. the
    // tile's owner was mid-failover). Give the minute back. Conditional on the
    // exact nextAt that was armed, so a stale refund can't clear a newer
    // cooldown. Rare -> unbatched.
    if (url.pathname === "/refund" && request.method === "POST") {
      const next = +(url.searchParams.get("next") ?? 0);
      if (this.pendingNext.get(uid) === next) this.pendingNext.delete(uid);
      this.state.storage.sql.exec(
        "UPDATE users SET nextAt = 0, placed = MAX(0, placed - 1) WHERE uid = ? AND nextAt = ?",
        uid, next);
      await this.barrier();
      return json({ ok: true });
    }
    if (url.pathname === "/me") {
      const r = this.row(uid);
      return json({
        next: this.pendingNext.get(uid) ?? r?.nextAt ?? 0,
        placed: r?.placed ?? 0,
      });
    }
    return new Response("not found", { status: 404 });
  }
}

// --- Worker-side routing for /place* ----------------------------------------

// Frame and manifest URLs use the .bin extension deliberately: BIN is on
// Cloudflare's default-cacheable extension list, so the CDN caches them with
// zero zone configuration (a Cache Rule can make the URLs prettier later).

export async function handlePlace(request, env) {
  const url = new URL(request.url);
  const path = url.pathname;

  if (path === "/" || path === "/place" || path === "/place/") {
    const { setCookie } = identity(request);
    const headers = {
      "content-type": "text/html; charset=utf-8",
      "x-content-type-options": "nosniff",
      "referrer-policy": "no-referrer",
      "content-security-policy":
        "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; " +
        "connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'none'",
    };
    if (setCookie) headers["set-cookie"] = setCookie;
    return new Response(PLACE_HTML, { headers });
  }

  // POST /place/px {x, y, c} — the write path
  if (path === "/place/px" && request.method === "POST") {
    const { uid, setCookie } = identity(request);
    if (+(request.headers.get("content-length") ?? 0) > 1024) {
      return json({ ok: false }, 413);
    }
    let body;
    try { body = await request.json(); } catch { return json({ ok: false }, 400); }
    const { x, y, c } = body ?? {};
    if (!Number.isInteger(x) || !Number.isInteger(y) || !Number.isInteger(c) ||
        x < 0 || x >= BOARD || y < 0 || y >= BOARD || c < 0 || c >= PALETTE.length) {
      return json({ ok: false, error: "out of range" }, 400);
    }
    const user = env.USER.get(env.USER.idFromName(shardFor(uid)));
    const verdict = await (await user.fetch(`https://do/try?uid=${uid}`, { method: "POST" })).json();
    if (!verdict.ok) {
      return withCookie(json(verdict, verdict.cooldown ? 429 : 503), setCookie);
    }
    const tx = Math.floor(x / TILE), ty = Math.floor(y / TILE);
    const tile = env.TILE.get(env.TILE.idFromName(`t:${tx},${ty}`));
    let tileResult = null;
    try {
      const resp = await tile.fetch("https://do/px", {
        method: "POST",
        body: JSON.stringify({ x: x % TILE, y: y % TILE, c, uid }),
      });
      tileResult = await resp.json();
    } catch {}
    if (!tileResult?.ok) {
      // The pixel never landed — the tile is mid-failover, shed the write
      // under flood, or failed its flush. The cooldown was already consumed:
      // hand the minute back (conditional on the exact nextAt we armed).
      await user.fetch(`https://do/refund?uid=${uid}&next=${verdict.next}`, { method: "POST" })
        .catch(() => {});
      return withCookie(
        json({ ok: false, error: tileResult?.error ?? "board is rebalancing — try again in a few seconds" },
          tileResult ? 429 : 503),
        setCookie);
    }
    return withCookie(
      json({ ...tileResult, next: verdict.next, placed: verdict.placed }), setCookie);
  }

  // GET /place/me — cooldown state for the current identity
  if (path === "/place/me") {
    const { uid, setCookie } = identity(request);
    const user = env.USER.get(env.USER.idFromName(shardFor(uid)));
    const me = await (await user.fetch(`https://do/me?uid=${uid}`)).json();
    return withCookie(json({ ...me, uid: uid.slice(0, 8) }), setCookie);
  }

  // GET /place/manifest/:tx/:ty.bin — tiny pointer, edge-cached for 1s
  let m = path.match(/^\/place\/manifest\/(\d)\/(\d)\.bin$/);
  if (m && +m[1] < TILES && +m[2] < TILES) {
    const tile = env.TILE.get(env.TILE.idFromName(`t:${m[1]},${m[2]}`));
    try {
      const resp = await tile.fetch("https://do/manifest");
      if (!resp.ok) throw new Error("tile error");    // never cache-wrap an error as 200
      return new Response(await resp.text(), {
        headers: {
          "content-type": "application/json",
          "cache-control": "public, max-age=0, s-maxage=1, stale-while-revalidate=5",
        },
      });
    } catch {
      return json({ error: "rebalancing" }, 503);   // don't leak runtime errors
    }
  }

  // GET /place/frame/:tx/:ty/:seq[.full].bin — immutable, cached ~forever
  m = path.match(/^\/place\/frame\/(\d)\/(\d)\/(\d+)(\.full)?\.bin$/);
  if (m && +m[1] < TILES && +m[2] < TILES) {
    const tile = env.TILE.get(env.TILE.idFromName(`t:${m[1]},${m[2]}`));
    try {
      const resp = await tile.fetch(`https://do/frame/${m[3]}${m[4] ? ":full" : ""}`);
      if (!resp.ok) return new Response("frame expired", { status: 404 });
      return new Response(resp.body, {
        headers: {
          "content-type": "application/octet-stream",
          "cache-control": "public, max-age=31536000, immutable",
        },
      });
    } catch {
      return new Response("rebalancing", { status: 503 });
    }
  }

  // GET /place/who?x=&y= — provenance of one pixel
  if (path === "/place/who") {
    const x = +url.searchParams.get("x"), y = +url.searchParams.get("y");
    if (!Number.isInteger(x) || !Number.isInteger(y) ||
        x < 0 || x >= BOARD || y < 0 || y >= BOARD) return json(null, 400);
    const tile = env.TILE.get(env.TILE.idFromName(
      `t:${Math.floor(x / TILE)},${Math.floor(y / TILE)}`));
    const who = await (await tile.fetch(
      `https://do/who?x=${x % TILE}&y=${y % TILE}`)).json();
    return json(who && { uid: who.uid, ts: who.ts });
  }

  // GET /place/history/:tx/:ty?since= — the forward-only log (timelapse feed)
  m = path.match(/^\/place\/history\/(\d)\/(\d)$/);
  if (m && +m[1] < TILES && +m[2] < TILES) {
    const tile = env.TILE.get(env.TILE.idFromName(`t:${m[1]},${m[2]}`));
    return tile.fetch(`https://do/history?since=${+(url.searchParams.get("since") ?? 0)}`);
  }

  return new Response("not found", { status: 404 });
}

// Identity: a random 128-bit token in a cookie. Guessing someone else's id is
// infeasible; minting a fresh one is allowed (anonymous board). OAuth slots in
// here later without touching the objects.
function identity(request) {
  const cookie = request.headers.get("Cookie") ?? "";
  const found = cookie.match(/(?:^|;\s*)pid=([0-9a-f]{32})/);
  if (found) return { uid: found[1], setCookie: null };
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  const uid = [...bytes].map((b) => b.toString(16).padStart(2, "0")).join("");
  return {
    uid,
    setCookie: `pid=${uid}; Path=/; Max-Age=31536000; HttpOnly; SameSite=Lax; Secure`,
  };
}

function withCookie(resp, setCookie) {
  if (!setCookie) return resp;
  const h = new Headers(resp.headers);
  h.append("set-cookie", setCookie);
  return new Response(resp.body, { status: resp.status, headers: h });
}

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status, headers: { "content-type": "application/json" },
  });
}

// ---------------------------------------------------------------------------
// The /place page.
// ---------------------------------------------------------------------------

const PLACE_HTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>place — forward-only pixel board on durable objects</title>
<style>
  :root {
    color-scheme: dark;
    --bg: #0d0d10; --panel: #131318; --line: #202027;
    --fg: #d9d9e0; --mut: #70707c; --dim: #4c4c56; --hot: #e8b45a;
  }
  * { box-sizing: border-box; margin: 0; }
  body {
    background: var(--bg); color: var(--fg);
    font: 13px/1.5 ui-monospace, "SF Mono", "Cascadia Mono", Menlo, Consolas, monospace;
    min-height: 100vh; padding: clamp(14px, 3vw, 28px);
    display: flex; flex-direction: column; gap: 14px;
    max-width: 1440px; margin: 0 auto;
  }
  header { display: flex; gap: 14px; align-items: baseline; flex-wrap: wrap; }
  header h1 { font-size: 15px; font-weight: 600; color: #fff; }
  header .who { color: var(--mut); }
  header .hint { color: var(--dim); margin-left: auto; }
  main { display: flex; gap: 14px; align-items: flex-start; flex-wrap: wrap; }
  .left { flex: 1 1 560px; min-width: 320px; display: flex; flex-direction: column; gap: 10px; }
  #viewport {
    width: min(100%, 78vh); aspect-ratio: 1; margin: 0 auto;
    overflow: hidden; position: relative;
    background: #121217; border: 1px solid var(--line); border-radius: 10px;
    touch-action: none; cursor: crosshair;
  }
  #viewport.grabbing { cursor: grabbing; }
  #board {
    position: absolute; left: 0; top: 0; width: 512px; height: 512px;
    image-rendering: pixelated; transform-origin: 0 0;
  }
  #cursorbox {
    position: absolute; pointer-events: none; display: none;
    border: 1px solid #fff; box-shadow: 0 0 0 1px #000, inset 0 0 0 1px #00000055;
    border-radius: 1px;
  }
  #cursorbox.cool { border-style: dashed; background: transparent !important; }
  #cursorbox.pop { animation: pop .25s ease-out; }
  @keyframes pop { 0% { transform: scale(1.8); } 100% { transform: scale(1); } }
  @media (prefers-reduced-motion: reduce) { #cursorbox.pop { animation: none; } }
  .bar { display: flex; gap: 6px; align-items: center; flex-wrap: wrap; }
  .pal {
    width: 22px; height: 22px; border: 1px solid #00000088; cursor: pointer; padding: 0;
    border-radius: 3px;
  }
  .pal.on { outline: 2px solid #fff; outline-offset: 1px; }
  button.txt {
    background: transparent; color: var(--mut); border: 1px solid var(--line);
    border-radius: 7px; padding: 4px 10px; cursor: pointer; font: inherit;
  }
  button.txt:hover { color: var(--fg); border-color: #3a3a45; }
  button.txt.on { color: var(--hot); border-color: var(--hot); }
  #cool {
    flex: 1; min-width: 140px; height: 26px; border: 1px solid var(--line); border-radius: 7px;
    position: relative; overflow: hidden; display: flex; align-items: center; justify-content: center;
    color: var(--mut);
  }
  #coolbar { position: absolute; left: 0; top: 0; bottom: 0; background: #e8b45a22; width: 0; }
  #cool span { position: relative; }
  #cool.ready { color: var(--hot); border-color: var(--hot); }
  #toast { color: var(--mut); min-height: 1.5em; }
  #toast b { color: var(--hot); font-weight: 600; }
  aside { flex: 0 1 340px; min-width: 300px; display: flex; flex-direction: column; gap: 10px; }
  .card { background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 11px 14px; }
  .card h2 {
    font-size: 10px; font-weight: 600; letter-spacing: .14em; text-transform: uppercase;
    color: var(--dim); margin-bottom: 9px;
  }
  .kv { display: grid; grid-template-columns: auto 1fr; gap: 3px 12px; }
  .kv dt { color: var(--mut); }
  .kv dd { color: var(--fg); text-align: right; }
  table { border-collapse: collapse; width: 100%; font-size: 12px; }
  th { text-align: left; color: var(--dim); font-weight: 600; padding: 2px 8px 4px 0; }
  td { color: var(--mut); padding: 2px 8px 2px 0; }
  td:first-child { color: var(--fg); }
  #pipe { height: 150px; overflow-y: auto; display: flex; flex-direction: column; gap: 2px; font-size: 12px; }
  #pipe div { flex: none; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; color: #797985; }
  #pipe b { color: #bcbcc8; font-weight: 600; }
  #pipe .hit { color: #7fd07f; }
  #pipe .miss { color: #e8b45a; }
  .note { color: var(--dim); font-size: 12px; }
  .note p { margin-bottom: 7px; }
  .note p:last-child { margin-bottom: 0; }
  .note em { color: var(--mut); font-style: normal; }
  footer { color: var(--dim); font-size: 12px; }
  footer a { color: var(--mut); text-decoration: none; border-bottom: 1px solid var(--line); }
  footer a:hover { color: var(--fg); }
</style>
</head>
<body>
<header>
  <h1>place</h1>
  <span class="who" id="who">&mdash;</span>
  <span class="hint">one pixel per minute &middot; forward-only, nothing is ever erased</span>
</header>
<main>
  <div class="left">
    <div id="viewport">
      <canvas id="board" width="512" height="512"></canvas>
      <div id="cursorbox"></div>
    </div>
    <div class="bar" id="palette"></div>
    <div class="bar">
      <button class="txt" id="zout" title="zoom out (-)">&minus;</button>
      <button class="txt" id="zfit" title="fit board (0)">fit</button>
      <button class="txt" id="zin" title="zoom in (+)">+</button>
      <button class="txt" id="inspect" title="who placed this pixel? (i)">inspect</button>
      <span class="note" id="coords">&mdash;</span>
      <div id="cool"><div id="coolbar"></div><span id="cooltext">&mdash;</span></div>
    </div>
    <div id="toast">pick a color, click a pixel &middot; wheel or pinch to zoom, drag to pan</div>
  </div>
  <aside>
    <div class="card">
      <h2>you</h2>
      <dl class="kv">
        <dt>identity</dt><dd id="m-uid">&mdash;</dd>
        <dt>pixels placed</dt><dd id="m-placed">&mdash;</dd>
        <dt>next pixel</dt><dd id="m-next">&mdash;</dd>
      </dl>
    </div>
    <div class="card">
      <h2>board &mdash; 4 tile objects</h2>
      <table id="tiles">
        <tr><th>tile</th><th>px</th><th>frame</th><th>wakes</th><th>cdn</th></tr>
      </table>
    </div>
    <div class="card">
      <h2>frame pipeline</h2>
      <div id="pipe"></div>
    </div>
    <div class="card note">
      <p><em>no sockets.</em> this page polls a 1-second manifest and fetches immutable
      frame blobs &mdash; both cached by the CDN, so viewers cost the origin ~nothing,
      however many there are. the cdn column shows where each response came from.</p>
      <p><em>frames are cut by durable alarms.</em> each 256&times;256 tile is its own
      object with its own SQLite; it batches writes into single durable flushes and
      renders a diff frame ~1/s, a full frame every 30 diffs.</p>
      <p><em>forward-only:</em> every pixel ever placed is a row, forever. inspect mode
      shows who owns any pixel and since when.</p>
    </div>
  </aside>
</main>
<footer>
  a meta-demo: the panel on the right is the app introspecting the technology
  it runs on &mdash; <a href="https://celld.dev" rel="noopener">celld</a>,
  self-hosted durable objects &middot;
  <a href="https://github.com/krasnoperov/celld-demo" rel="noopener">source</a>
</footer>
<script>
(function () {
  var TILE = ${TILE}, TILES = ${TILES}, BOARD = ${BOARD};
  var PALETTE = ${JSON.stringify(PALETTE)};
  var cv = document.getElementById("board");
  var cx = cv.getContext("2d");
  var vp = document.getElementById("viewport");
  var toast = document.getElementById("toast");
  var color = 5, scale = 1, ox = 0, oy = 0, inspectMode = false;
  var nextAt = 0;

  function el(id) { return document.getElementById(id); }
  function esc(s) { return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;"); }
  function note(html) { toast.innerHTML = html; }

  var pipe = el("pipe");
  function logPipe(label, detail, cls) {
    var d = document.createElement("div");
    d.innerHTML = "<b>" + esc(label) + "</b> " + esc(detail || "") +
      (cls ? ' <span class="' + cls + '">' + cls.toUpperCase() + "</span>" : "");
    pipe.appendChild(d);
    while (pipe.children.length > 50) pipe.removeChild(pipe.firstChild);
    pipe.scrollTop = pipe.scrollHeight;
  }

  // --- palette -------------------------------------------------------------
  var pal = el("palette");
  PALETTE.forEach(function (hex, i) {
    var b = document.createElement("button");
    b.className = "pal" + (i === color ? " on" : "");
    b.style.background = hex;
    b.onclick = function () {
      color = i;
      pal.querySelectorAll(".pal").forEach(function (x) { x.classList.remove("on"); });
      b.classList.add("on");
      drawCursor();
    };
    pal.appendChild(b);
  });

  // --- board rendering -----------------------------------------------------
  function px(x, y, c) { cx.fillStyle = PALETTE[c]; cx.fillRect(x, y, 1, 1); }
  function drawFull(tx, ty, bytes) {
    var u8 = new Uint8Array(bytes);
    for (var i = 0; i < u8.length; i++) {
      var p = i * 2;
      var x0 = tx * TILE + (p % TILE), y0 = ty * TILE + Math.floor(p / TILE);
      px(x0, y0, u8[i] & 15);
      px(x0 + 1, y0, u8[i] >> 4);
    }
  }
  function drawDiff(tx, ty, bytes) {
    var u32 = new Uint32Array(bytes);
    for (var i = 0; i < u32.length; i++) {
      var idx = u32[i] & 0xffff, c = (u32[i] >> 16) & 15;
      px(tx * TILE + (idx % TILE), ty * TILE + Math.floor(idx / TILE), c);
    }
    return u32.length;
  }

  // --- zoom & pan ----------------------------------------------------------
  // Crisp rendering: when zoomed in, the effective scale is snapped so every
  // board pixel maps to a whole number of DEVICE pixels (no shimmering, no
  // uneven pixel widths on any DPI). At fit level the whole board is shown.
  var dpr = window.devicePixelRatio || 1;
  var cbox = el("cursorbox");
  var hover = null;               // [x, y] under the cursor, board coords
  function fitScale() { return Math.min(vp.clientWidth, vp.clientHeight) / BOARD; }
  function eff() {
    var raw = fitScale() * scale;
    if (scale === 1) return raw;                       // overview: exact fit
    return Math.max(1 / dpr, Math.round(raw * dpr) / dpr);
  }
  function apply() {
    dpr = window.devicePixelRatio || 1;   // tracks browser zoom changes
    var f = eff(), bw = BOARD * f;
    var W = vp.clientWidth, H = vp.clientHeight;
    ox = bw <= W ? (W - bw) / 2 : Math.min(0, Math.max(W - bw, ox));
    oy = bw <= H ? (H - bw) / 2 : Math.min(0, Math.max(H - bw, oy));
    ox = Math.round(ox * dpr) / dpr;                   // snap: no blurry seams
    oy = Math.round(oy * dpr) / dpr;
    cv.style.transform = "translate(" + ox + "px," + oy + "px) scale(" + f + ")";
    drawCursor();
  }
  function zoomAt(cxp, cyp, factor) {
    var f0 = eff();
    scale = Math.min(32, Math.max(1, scale * factor));
    var f1 = eff();
    ox = cxp - (cxp - ox) * (f1 / f0);
    oy = cyp - (cyp - oy) * (f1 / f0);
    apply();
  }
  function center() { scale = 1; apply(); }
  function zoomButton(factor) {
    return function () { zoomAt(vp.clientWidth / 2, vp.clientHeight / 2, factor); };
  }
  el("zin").onclick = zoomButton(2);
  el("zout").onclick = zoomButton(0.5);
  el("zfit").onclick = center;
  vp.addEventListener("wheel", function (ev) {
    ev.preventDefault();
    var r = vp.getBoundingClientRect();
    // Gentle, magnitude-proportional zoom: ~1.13x per wheel notch, smooth on
    // trackpads (many small deltas), capped per event.
    var factor = Math.exp(Math.max(-240, Math.min(240, -ev.deltaY)) * 0.0012);
    zoomAt(ev.clientX - r.left, ev.clientY - r.top, factor);
  }, { passive: false });
  vp.addEventListener("dblclick", function (ev) {
    var r = vp.getBoundingClientRect();
    zoomAt(ev.clientX - r.left, ev.clientY - r.top, 2);
  });

  // The cursor box previews exactly which pixel a click will paint, in the
  // selected color; dashed while the cooldown is running.
  function drawCursor() {
    var f = eff();
    if (!hover || f < 4) { cbox.style.display = "none"; return; }
    cbox.style.display = "block";
    cbox.style.width = cbox.style.height = f + "px";
    cbox.style.left = (ox + hover[0] * f) + "px";
    cbox.style.top = (oy + hover[1] * f) + "px";
    cbox.style.background = inspectMode ? "transparent" : PALETTE[color] + "b0";
    cbox.classList.toggle("cool", !inspectMode && Date.now() < nextAt);
  }

  function boardPos(x, y) {
    var r = vp.getBoundingClientRect();
    var f = eff();
    return [Math.floor((x - r.left - ox) / f), Math.floor((y - r.top - oy) / f)];
  }
  function inBoard(p) { return p[0] >= 0 && p[0] < BOARD && p[1] >= 0 && p[1] < BOARD; }

  // Pointers: one finger/button drags (or clicks), two fingers pinch-zoom.
  var pointers = {}, drag = null, pinch = null;
  function pointerCount() { return Object.keys(pointers).length; }
  function pinchInfo() {
    var ids = Object.keys(pointers);
    var a = pointers[ids[0]], b = pointers[ids[1]];
    var r = vp.getBoundingClientRect();
    return {
      d: Math.hypot(a.x - b.x, a.y - b.y) || 1,
      mx: (a.x + b.x) / 2 - r.left,
      my: (a.y + b.y) / 2 - r.top,
    };
  }
  vp.addEventListener("pointerdown", function (ev) {
    vp.setPointerCapture(ev.pointerId);
    pointers[ev.pointerId] = { x: ev.clientX, y: ev.clientY };
    if (pointerCount() === 2) {
      var pi = pinchInfo();
      pinch = { d: pi.d, scale: scale };
      drag = null;
    } else {
      drag = { x: ev.clientX, y: ev.clientY, ox: ox, oy: oy, moved: false };
    }
  });
  vp.addEventListener("pointermove", function (ev) {
    if (pointers[ev.pointerId]) {
      pointers[ev.pointerId] = { x: ev.clientX, y: ev.clientY };
    }
    if (pinch && pointerCount() === 2) {
      var pi = pinchInfo();
      var f0 = eff();
      scale = Math.min(32, Math.max(1, pinch.scale * (pi.d / pinch.d)));
      var f1 = eff();
      ox = pi.mx - (pi.mx - ox) * (f1 / f0);
      oy = pi.my - (pi.my - oy) * (f1 / f0);
      apply();
      return;
    }
    var p = boardPos(ev.clientX, ev.clientY);
    hover = inBoard(p) ? p : null;
    el("coords").textContent = hover
      ? "(" + p[0] + ", " + p[1] + ") \\u00b7 " + (Math.round(eff() * 10) / 10) + "\\u00d7" : "";
    if (drag) {
      var dx = ev.clientX - drag.x, dy = ev.clientY - drag.y;
      if (Math.abs(dx) + Math.abs(dy) > 5) drag.moved = true;
      if (drag.moved) {
        vp.classList.add("grabbing");
        ox = drag.ox + dx; oy = drag.oy + dy;
        apply();
        return;
      }
    }
    drawCursor();
  });
  function pointerEnd(ev) {
    delete pointers[ev.pointerId];
    if (pointerCount() < 2) pinch = null;
    vp.classList.remove("grabbing");
    var wasClick = drag && !drag.moved && ev.type === "pointerup";
    drag = null;
    if (!wasClick) return;
    var p = boardPos(ev.clientX, ev.clientY);
    if (!inBoard(p)) return;
    if (inspectMode) { who(p[0], p[1]); return; }
    place(p[0], p[1]);
  }
  vp.addEventListener("pointerup", pointerEnd);
  vp.addEventListener("pointercancel", pointerEnd);
  vp.addEventListener("pointerleave", function () { hover = null; drawCursor(); });

  function setInspect(on) {
    inspectMode = on;
    el("inspect").classList.toggle("on", inspectMode);
    note(inspectMode ? "inspect: click any pixel to see who placed it" : "pick a color, click a pixel");
    drawCursor();
  }
  el("inspect").onclick = function () { setInspect(!inspectMode); };

  // Keyboard: +/- zoom, 0 fit, i inspect, arrows pan.
  window.addEventListener("keydown", function (ev) {
    if (ev.ctrlKey || ev.metaKey || ev.altKey) return;
    var pan = 64;
    if (ev.key === "+" || ev.key === "=") zoomButton(2)();
    else if (ev.key === "-") zoomButton(0.5)();
    else if (ev.key === "0") center();
    else if (ev.key === "i") setInspect(!inspectMode);
    else if (ev.key === "ArrowLeft") { ox += pan; apply(); }
    else if (ev.key === "ArrowRight") { ox -= pan; apply(); }
    else if (ev.key === "ArrowUp") { oy += pan; apply(); }
    else if (ev.key === "ArrowDown") { oy -= pan; apply(); }
    else return;
    ev.preventDefault();
  });

  // --- placing -------------------------------------------------------------
  function place(x, y) {
    if (Date.now() < nextAt) {
      note("cooldown \\u2014 next pixel in " + Math.ceil((nextAt - Date.now()) / 1000) + "s");
      return;
    }
    var col = color;   // capture: palette may change while the request flies
    fetch("/place/px", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ x: x, y: y, c: col }),
    }).then(function (r) { return r.json(); }).then(function (r) {
      if (r.ok) {
        px(x, y, col);
        nextAt = r.next;
        el("m-placed").textContent = r.placed;
        cbox.classList.remove("pop");
        void cbox.offsetWidth;               // restart the animation
        cbox.classList.add("pop");
        note("placed <b>(" + x + ", " + y + ")</b> \\u2014 row " + r.seq + " in tile t:" +
          Math.floor(x / TILE) + "," + Math.floor(y / TILE));
      } else if (r.cooldown) {
        nextAt = r.next;
        note("cooldown \\u2014 next pixel in " + Math.ceil((nextAt - Date.now()) / 1000) + "s");
      } else {
        note(esc(r.error || "rejected"));
      }
    }).catch(function () { note("network error"); });
  }

  function who(x, y) {
    fetch("/place/who?x=" + x + "&y=" + y)
      .then(function (r) { return r.json(); })
      .then(function (w) {
        if (!w) { note("(" + x + ", " + y + ") \\u2014 untouched since genesis"); return; }
        var age = Math.round((Date.now() - w.ts) / 1000);
        var when = age < 90 ? age + "s ago" : age < 5400 ? Math.round(age / 60) + "m ago"
          : Math.round(age / 3600) + "h ago";
        note("(" + x + ", " + y + ") \\u2014 placed by <b>anon-" + esc(w.uid) + "</b> " + when);
      });
  }

  // --- cooldown UI ---------------------------------------------------------
  setInterval(function () {
    var left = nextAt - Date.now();
    var coolEl = el("cool");
    if (left <= 0) {
      coolEl.classList.add("ready");
      el("coolbar").style.width = "0";
      el("cooltext").textContent = "pixel ready";
      el("m-next").textContent = "now";
    } else {
      coolEl.classList.remove("ready");
      el("coolbar").style.width = (100 * left / 60000) + "%";
      el("cooltext").textContent = Math.ceil(left / 1000) + "s";
      el("m-next").textContent = Math.ceil(left / 1000) + "s";
    }
    drawCursor();
  }, 250);

  // --- me ------------------------------------------------------------------
  fetch("/place/me").then(function (r) { return r.json(); }).then(function (me) {
    el("m-uid").textContent = "anon-" + me.uid;
    el("who").textContent = "you are anon-" + me.uid;
    el("m-placed").textContent = me.placed;
    nextAt = me.next;
  });

  // --- CDN frame loop ------------------------------------------------------
  var tileRows = {};
  var tbl = el("tiles");
  function tileRow(tx, ty) {
    var key = tx + "," + ty;
    if (!tileRows[key]) {
      var tr = document.createElement("tr");
      tr.innerHTML = "<td>t:" + key + "</td><td>\\u2014</td><td>\\u2014</td><td>\\u2014</td><td>\\u2014</td>";
      tbl.appendChild(tr);
      tileRows[key] = tr.children;
    }
    return tileRows[key];
  }
  function cdnStatus(resp) {
    var s = resp.headers.get("cf-cache-status");
    return s ? s.toLowerCase() : null;
  }
  function frameUrl(tx, ty, seq, full) {
    return "/place/frame/" + tx + "/" + ty + "/" + seq + (full ? ".full" : "") + ".bin";
  }

  function fetchOk(url) {
    return fetch(url).then(function (r) {
      if (!r.ok) throw new Error("http " + r.status);
      return r;
    });
  }
  function tileLoop(tx, ty) {
    var row = tileRow(tx, ty);
    var mySeq = -1;
    function tick() {
      // The whole update chain is returned, so a poll never overlaps a
      // half-applied previous one and every failure lands in the catch.
      fetchOk("/place/manifest/" + tx + "/" + ty + ".bin")
        .then(function (r) {
          var st = cdnStatus(r);
          return r.json().then(function (man) { return { man: man, st: st }; });
        })
        .then(function (res) {
          var man = res.man;
          row[1].textContent = man.meta.placements;
          row[2].textContent = man.seq;
          row[3].textContent = man.meta.activations;
          if (res.st) row[4].textContent = res.st;
          if (man.seq === mySeq) return;

          // Cold, or fell out of the diff window -> load the full frame first.
          var chain = man.diffs.filter(function (d) { return d.from >= mySeq && d.seq > mySeq; });
          var contiguous = chain.length && chain[0].from === mySeq;
          var start = (mySeq >= 0 && contiguous)
            ? Promise.resolve(mySeq)
            : fetchOk(frameUrl(tx, ty, man.full, true))
                .then(function (r) {
                  logPipe("t:" + tx + "," + ty, "full frame seq " + man.full + " (32KB)", cdnStatus(r));
                  return r.arrayBuffer();
                })
                .then(function (b) {
                  if (b.byteLength !== TILE * TILE / 2) throw new Error("bad full frame");
                  drawFull(tx, ty, b);
                  return man.full;
                });

          return start.then(function (at) {
            var todo = man.diffs.filter(function (d) { return d.from >= at; });
            return todo.reduce(function (p, d) {
              return p.then(function () {
                return fetchOk(frameUrl(tx, ty, d.seq, false)).then(function (r) {
                  var st = cdnStatus(r);
                  return r.arrayBuffer().then(function (b) {
                    var n = drawDiff(tx, ty, b);
                    logPipe("t:" + tx + "," + ty, "diff " + d.from + "\\u2192" + d.seq + " (" + n + "px)", st);
                  });
                });
              });
            }, Promise.resolve()).then(function () { mySeq = man.seq; });
          });
        })
        .catch(function () { row[4].textContent = "err"; })
        .then(function () { setTimeout(tick, 1000); });
    }
    tick();
  }
  for (var ty = 0; ty < TILES; ty++) for (var tx = 0; tx < TILES; tx++) tileLoop(tx, ty);
  apply();
  window.addEventListener("resize", apply);
})();
</script>
</body>
</html>`;
