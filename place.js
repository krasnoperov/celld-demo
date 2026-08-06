// place — an r/place-style pixel board on Durable Objects. Forward-only:
// pixels are never wiped, every placement is a row in a tile's SQLite forever.
//
// Architecture (matches the design doc):
// - the 512x512 board is 4 Tile objects of 256x256 — real sharding from day one
// - a User object per identity enforces the cooldown atomically (tryPlace)
// - placements go over HTTP POST (the write path); live updates over WS per
//   tile (ladder tier 1; CDN frames replace this transport at scale)
// - identity is a random 128-bit cookie for now; the User object doesn't care
//   whether its owner is a human, an agent, or a future OAuth login.

export const TILE = 256;          // pixels per tile side
export const TILES = 2;           // board is TILES x TILES tiles
export const BOARD = TILE * TILES;
const COOLDOWN_MS = 60_000;
const PALETTE = [
  "#ffffff", "#e4e4e4", "#888888", "#222222",
  "#ffa7d1", "#e50000", "#e59500", "#a06a42",
  "#e5d900", "#94e044", "#02be01", "#00d3dd",
  "#0083c7", "#0000ea", "#cf6ee4", "#820080",
];
const TILE_RATE = 30;             // placements/s per tile (flood floor)
const BITMAP_SAVE_EVERY = 8;      // persist bitmap every N placements

// --- Tile: one object per 256x256 board section -----------------------------

export class Tile {
  constructor(state, env) {
    this.state = state;
    this.bornAt = Date.now();
    this.activations = null;
    this.rate = { ts: 0, n: 0 };
    this.state.storage.sql.exec(
      `CREATE TABLE IF NOT EXISTS pixels (
         seq INTEGER PRIMARY KEY AUTOINCREMENT,
         x INTEGER NOT NULL, y INTEGER NOT NULL, c INTEGER NOT NULL,
         uid TEXT NOT NULL, ts INTEGER NOT NULL
       )`,
    );
  }

  // Bitmap = current board section, one nibble per pixel (32 KB). Persisted
  // every N placements; on activation the tail is replayed from SQLite.
  init() {
    return (this.initing ??= (async () => {
      this.activations = ((await this.state.storage.get("activations")) ?? 0) + 1;
      await this.state.storage.put("activations", this.activations);
      const saved = await this.state.storage.get("bitmap");
      this.bitmap = saved?.bytes
        ? new Uint8Array(saved.bytes) : new Uint8Array((TILE * TILE) / 2);
      this.savedSeq = saved?.seq ?? 0;
      const tail = this.state.storage.sql
        .exec("SELECT x, y, c FROM pixels WHERE seq > ? ORDER BY seq", this.savedSeq)
        .toArray();
      for (const p of tail) this.setNibble(p.x, p.y, p.c);
      this.unsaved = tail.length;
    })());
  }

  setNibble(x, y, c) {
    const i = y * TILE + x;
    const b = i >> 1;
    this.bitmap[b] = (i & 1)
      ? (this.bitmap[b] & 0x0f) | (c << 4)
      : (this.bitmap[b] & 0xf0) | c;
  }

  async saveBitmap(seq) {
    await this.state.storage.put("bitmap", { seq, bytes: this.bitmap.buffer.slice(0) });
    this.savedSeq = seq;
    this.unsaved = 0;
  }

  async meta() {
    await this.init();
    const row = this.state.storage.sql
      .exec("SELECT COUNT(*) AS n, COALESCE(MAX(seq), 0) AS maxseq FROM pixels").one();
    return {
      bornAt: this.bornAt, now: Date.now(), activations: this.activations,
      sockets: this.state.getWebSockets().length, pixels: row.n, maxseq: row.maxseq,
    };
  }

  async fetch(request) {
    await this.init();
    const url = new URL(request.url);

    if (url.pathname === "/px" && request.method === "POST") {
      const now = Date.now();
      if (now - this.rate.ts >= 1000) this.rate = { ts: now, n: 0 };
      if (++this.rate.n > TILE_RATE) return json({ ok: false, error: "tile busy" }, 429);
      const { x, y, c, uid } = await request.json();
      const { seq } = this.state.storage.sql.exec(
        "INSERT INTO pixels (x, y, c, uid, ts) VALUES (?, ?, ?, ?, ?) RETURNING seq",
        x, y, c, uid, now,
      ).one();
      this.setNibble(x, y, c);
      if (++this.unsaved >= BITMAP_SAVE_EVERY) await this.saveBitmap(seq);
      this.broadcast({ type: "px", x, y, c, seq });
      return json({ ok: true, seq });
    }

    if (url.pathname === "/who") {
      const x = +url.searchParams.get("x"), y = +url.searchParams.get("y");
      const rows = this.state.storage.sql.exec(
        "SELECT uid, ts, c FROM pixels WHERE x = ? AND y = ? ORDER BY seq DESC LIMIT 1",
        x, y,
      ).toArray();
      return json(rows[0] ?? null);
    }

    if (url.pathname === "/history") {
      const since = +(url.searchParams.get("since") ?? 0);
      const rows = this.state.storage.sql.exec(
        "SELECT seq, x, y, c, uid, ts FROM pixels WHERE seq > ? ORDER BY seq LIMIT 10000",
        since,
      ).toArray();
      return json(rows);
    }

    if (request.headers.get("Upgrade")?.toLowerCase() === "websocket") {
      if (this.state.getWebSockets().length >= 512) {
        return new Response("tile full", { status: 429 });
      }
      const pair = new WebSocketPair();
      this.state.acceptWebSocket(pair[0]);
      pair[0].send(JSON.stringify({
        type: "snapshot",
        seq: this.savedSeq + this.unsaved,
        bitmap: b64(this.bitmap),
        meta: await this.meta(),
      }));
      return new Response(null, { status: 101, webSocket: pair[1] });
    }

    return new Response("not found", { status: 404 });
  }

  async webSocketMessage(ws, raw) {
    if (typeof raw !== "string" || raw.length > 256) return;
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    if (msg.type === "stats") {
      ws.send(JSON.stringify({ type: "stats", meta: await this.meta() }));
    }
  }

  broadcast(msg) {
    const s = JSON.stringify(msg);
    for (const sock of this.state.getWebSockets()) {
      try { sock.send(s); } catch {}
    }
  }
}

// --- User: one object per identity; the cooldown lives here -----------------

export class User {
  constructor(state, env) {
    this.state = state;
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/try" && request.method === "POST") {
      const now = Date.now();
      const nextAt = (await this.state.storage.get("nextAt")) ?? 0;
      if (now < nextAt) return json({ ok: false, next: nextAt });
      const placed = ((await this.state.storage.get("placed")) ?? 0) + 1;
      // Single-threaded object: this read-check-write cannot race.
      await this.state.storage.put("nextAt", now + COOLDOWN_MS);
      await this.state.storage.put("placed", placed);
      return json({ ok: true, next: now + COOLDOWN_MS, placed });
    }
    if (url.pathname === "/me") {
      return json({
        next: (await this.state.storage.get("nextAt")) ?? 0,
        placed: (await this.state.storage.get("placed")) ?? 0,
      });
    }
    return new Response("not found", { status: 404 });
  }
}

// --- Worker-side routing for /place* ----------------------------------------

export async function handlePlace(request, env) {
  const url = new URL(request.url);
  const path = url.pathname;

  if (path === "/place" || path === "/place/") {
    const { uid, setCookie } = identity(request);
    const headers = {
      "content-type": "text/html; charset=utf-8",
      "x-content-type-options": "nosniff",
      "referrer-policy": "no-referrer",
      "content-security-policy":
        "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; " +
        "connect-src 'self' ws: wss:; img-src 'self' data:; base-uri 'none'; form-action 'none'",
    };
    if (setCookie) headers["set-cookie"] = setCookie;
    return new Response(PLACE_HTML, { headers });
  }

  // POST /place/px {x, y, c} — the write path
  if (path === "/place/px" && request.method === "POST") {
    const { uid, setCookie } = identity(request);
    let body;
    try { body = await request.json(); } catch { return json({ ok: false }, 400); }
    const x = body.x | 0, y = body.y | 0, c = body.c | 0;
    if (x < 0 || x >= BOARD || y < 0 || y >= BOARD || c < 0 || c >= PALETTE.length) {
      return json({ ok: false, error: "out of range" }, 400);
    }
    const user = env.USER.get(env.USER.idFromName(uid));
    const verdict = await (await user.fetch("https://do/try", { method: "POST" })).json();
    if (!verdict.ok) {
      return withCookie(json({ ok: false, cooldown: true, next: verdict.next }, 429), setCookie);
    }
    const tx = Math.floor(x / TILE), ty = Math.floor(y / TILE);
    const tile = env.TILE.get(env.TILE.idFromName(`t:${tx},${ty}`));
    const placed = await (await tile.fetch("https://do/px", {
      method: "POST",
      body: JSON.stringify({ x: x % TILE, y: y % TILE, c, uid }),
    })).json();
    return withCookie(
      json({ ...placed, next: verdict.next, placed: verdict.placed }), setCookie);
  }

  // GET /place/me — cooldown state for the current identity
  if (path === "/place/me") {
    const { uid, setCookie } = identity(request);
    const user = env.USER.get(env.USER.idFromName(uid));
    const me = await (await user.fetch("https://do/me")).json();
    return withCookie(json({ ...me, uid: uid.slice(0, 8) }), setCookie);
  }

  // GET /place/ws/:tx/:ty — live pixel stream for one tile
  let m = path.match(/^\/place\/ws\/(\d)\/(\d)$/);
  if (m && +m[1] < TILES && +m[2] < TILES) {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("websocket upgrade required", { status: 426 });
    }
    return env.TILE.get(env.TILE.idFromName(`t:${m[1]},${m[2]}`)).fetch(request);
  }

  // GET /place/who?x=&y= — provenance of one pixel
  if (path === "/place/who") {
    const x = +url.searchParams.get("x"), y = +url.searchParams.get("y");
    if (!(x >= 0 && x < BOARD && y >= 0 && y < BOARD)) return json(null, 400);
    const tile = env.TILE.get(env.TILE.idFromName(
      `t:${Math.floor(x / TILE)},${Math.floor(y / TILE)}`));
    const who = await (await tile.fetch(
      `https://do/who?x=${x % TILE}&y=${y % TILE}`)).json();
    return json(who && { uid: who.uid.slice(0, 8), ts: who.ts, c: who.c });
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
    setCookie: `pid=${uid}; Path=/place; Max-Age=31536000; HttpOnly; SameSite=Lax; Secure`,
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

function b64(bytes) {
  let s = "";
  for (let i = 0; i < bytes.length; i += 0x8000) {
    s += String.fromCharCode.apply(null, bytes.subarray(i, i + 0x8000));
  }
  return btoa(s);
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
    width: 100%; aspect-ratio: 1; max-height: 78vh; overflow: hidden; position: relative;
    background: #121217; border: 1px solid var(--line); border-radius: 10px;
    touch-action: none; cursor: crosshair;
  }
  #board {
    position: absolute; left: 0; top: 0; width: 512px; height: 512px;
    image-rendering: pixelated; transform-origin: 0 0;
  }
  .bar { display: flex; gap: 6px; align-items: center; flex-wrap: wrap; }
  .pal {
    width: 22px; height: 22px; border: 1px solid #00000088; cursor: pointer; padding: 0;
    border-radius: 3px;
  }
  .pal.on { outline: 2px solid #fff; outline-offset: 1px; }
  .bar .sep { width: 10px; }
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
  .kv dd.hot { color: var(--hot); }
  table { border-collapse: collapse; width: 100%; font-size: 12px; }
  th { text-align: left; color: var(--dim); font-weight: 600; padding: 2px 8px 4px 0; }
  td { color: var(--mut); padding: 2px 8px 2px 0; }
  td:first-child { color: var(--fg); }
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
    <div id="viewport"><canvas id="board" width="512" height="512"></canvas></div>
    <div class="bar" id="palette"></div>
    <div class="bar">
      <button class="txt" id="zin">zoom +</button>
      <button class="txt" id="zout">zoom &minus;</button>
      <button class="txt" id="inspect">inspect</button>
      <span class="note" id="coords">&mdash;</span>
      <div id="cool"><div id="coolbar"></div><span id="cooltext">&mdash;</span></div>
    </div>
    <div id="toast">pick a color, click a pixel</div>
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
        <tr><th>tile</th><th>px</th><th>socks</th><th>wakes</th><th>live</th></tr>
      </table>
    </div>
    <div class="card note">
      <p><em>each 256&times;256 tile is its own durable object</em> with its own SQLite:
      the board shards by construction, a pixel war in one corner can't slow the rest.</p>
      <p><em>your cooldown is its own object too</em> &mdash; a single-threaded
      check-and-set, so it can't be raced. it doesn't care if you're a human or an agent.</p>
      <p><em>forward-only:</em> every pixel ever placed is a row, forever. inspect mode
      shows who owns any pixel and since when.</p>
    </div>
  </aside>
</main>
<footer>
  same durable objects, different game: the freehand
  <a href="/">shared canvas</a> lives next door &middot;
  <a href="https://github.com/krasnoperov/celld-demo" rel="noopener">source</a>
</footer>
<script>
(function () {
  var TILE = 256, TILES = 2, BOARD = 512;
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
    };
    pal.appendChild(b);
  });

  // --- board rendering -----------------------------------------------------
  function px(x, y, c) { cx.fillStyle = PALETTE[c]; cx.fillRect(x, y, 1, 1); }
  function drawBitmap(tx, ty, b64s) {
    var bin = atob(b64s);
    for (var i = 0; i < bin.length; i++) {
      var byte = bin.charCodeAt(i);
      var p = i * 2;
      var x0 = tx * TILE + (p % TILE), y0 = ty * TILE + Math.floor(p / TILE);
      px(x0, y0, byte & 15);
      px(x0 + 1, y0, byte >> 4);
    }
  }

  // --- zoom & pan ----------------------------------------------------------
  function fitScale() {
    return vp.clientWidth / BOARD;
  }
  function apply() {
    var f = fitScale() * scale;
    var maxo = 0, mino = vp.clientWidth - BOARD * f;
    ox = Math.min(maxo, Math.max(mino, ox));
    oy = Math.min(maxo, Math.max(Math.min(0, vp.clientHeight - BOARD * f), oy));
    cv.style.transform = "translate(" + ox + "px," + oy + "px) scale(" + f + ")";
  }
  function zoomAt(cxp, cyp, factor) {
    var f0 = fitScale() * scale;
    scale = Math.min(16, Math.max(1, scale * factor));
    var f1 = fitScale() * scale;
    ox = cxp - (cxp - ox) * (f1 / f0);
    oy = cyp - (cyp - oy) * (f1 / f0);
    apply();
  }
  el("zin").onclick = function () { zoomAt(vp.clientWidth / 2, vp.clientHeight / 2, 2); };
  el("zout").onclick = function () { zoomAt(vp.clientWidth / 2, vp.clientHeight / 2, 0.5); };
  vp.addEventListener("wheel", function (ev) {
    ev.preventDefault();
    var r = vp.getBoundingClientRect();
    zoomAt(ev.clientX - r.left, ev.clientY - r.top, ev.deltaY < 0 ? 1.25 : 0.8);
  }, { passive: false });

  function boardPos(ev) {
    var r = vp.getBoundingClientRect();
    var f = fitScale() * scale;
    return [
      Math.floor((ev.clientX - r.left - ox) / f),
      Math.floor((ev.clientY - r.top - oy) / f),
    ];
  }

  var drag = null;
  vp.addEventListener("pointerdown", function (ev) {
    vp.setPointerCapture(ev.pointerId);
    drag = { x: ev.clientX, y: ev.clientY, ox: ox, oy: oy, moved: false };
  });
  vp.addEventListener("pointermove", function (ev) {
    var p = boardPos(ev);
    el("coords").textContent =
      (p[0] >= 0 && p[0] < BOARD && p[1] >= 0 && p[1] < BOARD)
        ? "(" + p[0] + ", " + p[1] + ") " + scale + "x" : "";
    if (!drag) return;
    var dx = ev.clientX - drag.x, dy = ev.clientY - drag.y;
    if (Math.abs(dx) + Math.abs(dy) > 5) drag.moved = true;
    if (drag.moved) { ox = drag.ox + dx; oy = drag.oy + dy; apply(); }
  });
  vp.addEventListener("pointerup", function (ev) {
    var wasClick = drag && !drag.moved;
    drag = null;
    if (!wasClick) return;
    var p = boardPos(ev);
    if (p[0] < 0 || p[0] >= BOARD || p[1] < 0 || p[1] >= BOARD) return;
    if (inspectMode) { who(p[0], p[1]); return; }
    place(p[0], p[1]);
  });

  el("inspect").onclick = function () {
    inspectMode = !inspectMode;
    el("inspect").classList.toggle("on", inspectMode);
    note(inspectMode ? "inspect: click any pixel to see who placed it" : "pick a color, click a pixel");
  };

  // --- placing -------------------------------------------------------------
  function place(x, y) {
    if (Date.now() < nextAt) {
      note("cooldown \\u2014 next pixel in " + Math.ceil((nextAt - Date.now()) / 1000) + "s");
      return;
    }
    fetch("/place/px", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ x: x, y: y, c: color }),
    }).then(function (r) { return r.json(); }).then(function (r) {
      if (r.ok) {
        px(x, y, color);
        nextAt = r.next;
        el("m-placed").textContent = r.placed;
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
  }, 250);

  // --- me ------------------------------------------------------------------
  fetch("/place/me").then(function (r) { return r.json(); }).then(function (me) {
    el("m-uid").textContent = "anon-" + me.uid;
    el("who").textContent = "you are anon-" + me.uid;
    el("m-placed").textContent = me.placed;
    nextAt = me.next;
  });

  // --- live tiles ----------------------------------------------------------
  var proto = location.protocol === "https:" ? "wss://" : "ws://";
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
  function connectTile(tx, ty) {
    var row = tileRow(tx, ty);
    var retry = 0;
    function go() {
      var ws = new WebSocket(proto + location.host + "/place/ws/" + tx + "/" + ty);
      ws.onopen = function () { retry = 0; row[4].textContent = "live"; };
      ws.onclose = function () {
        row[4].textContent = "re\\u2026";
        setTimeout(go, Math.min(1000 * ++retry, 5000));
      };
      ws.onmessage = function (ev) {
        var m = JSON.parse(ev.data);
        if (m.type === "snapshot") {
          drawBitmap(tx, ty, m.bitmap);
          applyMeta(m.meta);
        } else if (m.type === "px") {
          px(tx * TILE + m.x, ty * TILE + m.y, m.c);
          row[1].textContent = +row[1].textContent + 1 || m.seq;
        } else if (m.type === "stats") {
          applyMeta(m.meta);
        }
      };
      setInterval(function () {
        if (ws.readyState === 1) ws.send(JSON.stringify({ type: "stats" }));
      }, 20000);
      function applyMeta(meta) {
        row[1].textContent = meta.pixels;
        row[2].textContent = meta.sockets;
        row[3].textContent = meta.activations;
      }
    }
    go();
  }
  for (var ty = 0; ty < TILES; ty++) for (var tx = 0; tx < TILES; tx++) connectTile(tx, ty);
  apply();
  window.addEventListener("resize", apply);
})();
</script>
</body>
</html>`;
