// Shared canvas on Durable Objects — a meta-demo.
//
// The app demonstrates the technology it runs on: next to the shared canvas
// the UI shows the room's Durable Object introspecting itself — its id
// (idFromName), cold activations, connected sockets, rows in its private
// SQLite, and the live wire protocol between peers.
//
// One DO instance per room = the room's single-threaded authority:
// - every WebSocket in the room attaches to the same object
// - the stroke log lives in the object's private SQLite database
// - hibernatable sockets mean an idle room costs ~nothing
//
// Security posture: this worker never executes anything a client sends.
// Clients exchange small JSON data frames; every field is validated and
// clamped server-side; the only mutable state is this object's SQLite.

import { Tile, User, handlePlace } from "./place.js";
export { Tile, User };

const ROOM_W = 1000;
const ROOM_H = 700;
const MAX_FRAME = 32 * 1024;        // raw inbound message bytes
const MAX_POINTS = 1500;            // points per stroke
const MAX_ROWS = 2000;              // strokes kept per room (oldest trimmed)
const MAX_BYTES = 4 * 1024 * 1024;  // serialized stroke history kept per room
const MAX_TEXT = 300;               // chat message length
const MAX_SOCKETS = 32;             // concurrent sockets per room
const RATE = 25;                    // frames per socket per second (best effort)

export class Canvas {
  constructor(state, env) {
    this.state = state;
    this.bornAt = Date.now();   // in-memory: resets on every activation
    this.activations = null;    // persisted: counted across activations
    this.rate = new WeakMap();  // per-socket token bucket (best effort, in-memory)
    this.state.storage.sql.exec(
      `CREATE TABLE IF NOT EXISTS strokes (
         seq INTEGER PRIMARY KEY AUTOINCREMENT,
         actor TEXT NOT NULL,
         data TEXT NOT NULL
       )`,
    );
  }

  // Called from every handler: counts cold activations across the object's
  // lifetime. A rising number = the object was evicted/hibernated and woke up.
  init() {
    return (this.initing ??= (async () => {
      this.activations = ((await this.state.storage.get("activations")) ?? 0) + 1;
      await this.state.storage.put("activations", this.activations);
    })());
  }

  async meta(exclude) {
    await this.init();
    const peers = this.state.getWebSockets()
      .filter((ws) => ws !== exclude)   // a closing socket still lists until close returns
      .map((ws) => ws.deserializeAttachment())
      .filter(Boolean);
    const row = this.state.storage.sql
      .exec("SELECT COUNT(*) AS n, COALESCE(MAX(seq), 0) AS maxseq FROM strokes")
      .one();
    return {
      doid: this.doid ?? null,
      bornAt: this.bornAt,
      now: Date.now(),
      activations: this.activations,
      sockets: peers.length,
      strokes: row.n,
      maxseq: row.maxseq,
      peers,
    };
  }

  async fetch(request) {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("websocket upgrade required", { status: 426 });
    }
    if (this.state.getWebSockets().length >= MAX_SOCKETS) {
      return new Response("room full", { status: 429 });
    }
    const url = new URL(request.url);
    const actor = (url.searchParams.get("actor") || "anon").slice(0, 24);
    const role = url.searchParams.get("role") === "agent" ? "agent" : "user";
    this.doid = url.searchParams.get("do") ?? this.doid;

    const pair = new WebSocketPair();
    const server = pair[0];
    // Hibernation API: the host holds the socket; the DO can be evicted from
    // memory between messages and is revived with attachments intact.
    this.state.acceptWebSocket(server);
    server.serializeAttachment({ actor, role });

    server.send(JSON.stringify({
      type: "snapshot",
      w: ROOM_W,
      h: ROOM_H,
      strokes: this.allStrokes(),
      meta: await this.meta(),
    }));
    this.broadcast({ type: "join", actor, role, meta: await this.meta() }, server);
    return new Response(null, { status: 101, webSocket: pair[1] });
  }

  allStrokes() {
    return this.state.storage.sql
      .exec("SELECT data FROM strokes ORDER BY seq")
      .toArray()
      .map((r) => JSON.parse(r.data));
  }

  allowed(ws) {
    const now = Date.now();
    let b = this.rate.get(ws);
    if (!b || now - b.ts >= 1000) { b = { ts: now, n: 0 }; this.rate.set(ws, b); }
    return ++b.n <= RATE;
  }

  async webSocketMessage(ws, raw) {
    if (typeof raw !== "string" || raw.length > MAX_FRAME) return;
    if (!this.allowed(ws)) return;
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    const { actor, role } = ws.deserializeAttachment() ?? {};
    await this.init();

    switch (msg.type) {
      case "stroke": {
        const stroke = sanitizeStroke(msg, actor, role);
        if (!stroke) return;
        const data = JSON.stringify(stroke);
        const { seq } = this.state.storage.sql.exec(
          "INSERT INTO strokes (actor, data) VALUES (?, ?) RETURNING seq",
          actor, data,
        ).one();
        await this.trim(data.length, seq);
        ws.send(JSON.stringify({ type: "ack", seq }));      // sender: "row N is durable"
        this.broadcast({ type: "stroke", stroke, seq }, ws);
        break;
      }
      case "clear":
        this.state.storage.sql.exec("DELETE FROM strokes");
        await this.state.storage.put("bytes", 0);
        this.broadcast({ type: "clear", actor }, ws);
        break;
      case "stats":                                          // object, introspect thyself
        ws.send(JSON.stringify({ type: "stats", meta: await this.meta() }));
        break;
      case "say":     // chat — relayed live, not persisted
      case "status":  // ephemeral presence
        this.broadcast(
          { type: msg.type, text: String(msg.text ?? "").slice(0, MAX_TEXT), actor, role },
          ws,
        );
        break;
    }
  }

  // Bound retained history by rows and by serialized bytes, trimming oldest
  // first, so a room's snapshot can never grow past MAX_BYTES.
  async trim(addedBytes, maxseq) {
    this.state.storage.sql.exec("DELETE FROM strokes WHERE seq <= ?", maxseq - MAX_ROWS);
    let bytes = ((await this.state.storage.get("bytes")) ?? 0) + addedBytes;
    while (bytes > MAX_BYTES) {
      const old = this.state.storage.sql
        .exec("SELECT seq, LENGTH(data) AS len FROM strokes ORDER BY seq LIMIT 64")
        .toArray();
      if (old.length === 0) { bytes = addedBytes; break; }
      for (const r of old) {
        if (bytes <= MAX_BYTES) break;
        this.state.storage.sql.exec("DELETE FROM strokes WHERE seq = ?", r.seq);
        bytes -= r.len;
      }
    }
    await this.state.storage.put("bytes", bytes);
  }

  async webSocketClose(ws) {
    const { actor, role } = ws.deserializeAttachment() ?? {};
    this.broadcast({ type: "leave", actor, role, meta: await this.meta(ws) }, ws);
  }

  broadcast(msg, except) {
    const s = JSON.stringify(msg);
    for (const sock of this.state.getWebSockets()) {
      if (sock === except) continue;
      try { sock.send(s); } catch {}
    }
  }
}

function sanitizeStroke(msg, actor, role) {
  if (!Array.isArray(msg.points) || msg.points.length === 0) return null;
  const points = [];
  for (const p of msg.points.slice(0, MAX_POINTS)) {
    if (!Array.isArray(p)) return null;
    const x = Number(p[0]), y = Number(p[1]);
    if (!Number.isFinite(x) || !Number.isFinite(y)) return null;
    points.push([
      Math.round(Math.min(Math.max(x, 0), ROOM_W)),
      Math.round(Math.min(Math.max(y, 0), ROOM_H)),
    ]);
  }
  const color = /^#[0-9a-fA-F]{3,8}$/.test(msg.color) ? msg.color : "#e8e8f0";
  const size = Math.min(Math.max(Math.round(Number(msg.size) || 4), 1), 32);
  return { actor, role, color, size, points };
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/place" || url.pathname.startsWith("/place/")) {
      return handlePlace(request, env);
    }
    const ws = url.pathname.match(/^\/ws\/([\w-]{1,64})$/);
    if (ws) {
      // Only a real upgrade reaches (and thereby instantiates) the object —
      // a plain GET to /ws/x must not create rooms.
      if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
        return new Response("websocket upgrade required", { status: 426 });
      }
      // Content-addressing: same room name -> same object id -> same object,
      // whichever node it currently lives on. We forward the id so the object
      // can show it in the UI.
      const id = env.CANVAS.idFromName(ws[1]);
      url.searchParams.set("do", id.toString());
      return env.CANVAS.get(id).fetch(new Request(url, request));
    }
    if (url.pathname === "/" || /^\/[\w-]{1,64}$/.test(url.pathname)) {
      return new Response(HTML, {
        headers: {
          "content-type": "text/html; charset=utf-8",
          "x-content-type-options": "nosniff",
          "referrer-policy": "no-referrer",
          "content-security-policy":
            "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; " +
            "connect-src 'self' ws: wss:; img-src 'self'; base-uri 'none'; form-action 'none'",
        },
      });
    }
    return new Response("not found", { status: 404 });
  },
};

// ---------------------------------------------------------------------------
// The whole UI, inlined so the demo stays a two-file deploy.
// Page path = room name ("/" -> "lobby"). Open the same path twice to draw
// together; append any word to the URL to get a fresh room (= a fresh object).
// ---------------------------------------------------------------------------

const HTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>shared canvas — one durable object per room</title>
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
  header h1 { font-size: 15px; font-weight: 600; color: #fff; letter-spacing: .01em; }
  header .room { color: var(--hot); }
  header .who { color: var(--mut); }
  header .hint { color: var(--dim); margin-left: auto; }
  main { display: flex; gap: 14px; align-items: flex-start; flex-wrap: wrap; }
  .left { flex: 1 1 560px; min-width: 320px; display: flex; flex-direction: column; gap: 10px; }
  #board {
    width: 100%; aspect-ratio: 1000 / 700; display: block;
    background-color: #121217;
    background-image: radial-gradient(#1d1d24 1px, transparent 1px);
    background-size: 25px 25px; background-position: 12px 12px;
    border: 1px solid var(--line); border-radius: 10px;
    touch-action: none; cursor: crosshair;
  }
  .bar { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }
  .sw {
    width: 20px; height: 20px; border-radius: 50%;
    border: 2px solid transparent; outline: 1px solid #00000055;
    cursor: pointer; padding: 0; transition: transform .1s;
  }
  .sw:hover { transform: scale(1.15); }
  .sw.on { border-color: var(--bg); outline: 2px solid #fff; }
  #size { width: 84px; accent-color: #6d6d7a; }
  button.txt {
    background: transparent; color: var(--mut); border: 1px solid var(--line);
    border-radius: 7px; padding: 4px 12px; cursor: pointer; font: inherit;
  }
  button.txt:hover { color: var(--fg); border-color: #3a3a45; }
  #chat {
    flex: 1; min-width: 160px; background: var(--panel); color: var(--fg);
    border: 1px solid var(--line); border-radius: 7px; padding: 5px 11px;
    font: inherit; outline: none;
  }
  #chat::placeholder { color: var(--dim); }
  #chat:focus { border-color: #3a3a45; }
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
  .kv dd { color: var(--fg); text-align: right; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .kv dd.hot { color: var(--hot); }
  #peerlist { display: flex; flex-direction: column; gap: 5px; }
  #peerlist .p { display: flex; gap: 8px; align-items: center; }
  #peerlist .dot { width: 8px; height: 8px; border-radius: 50%; flex: none; }
  #peerlist .r { color: var(--dim); margin-left: auto; }
  #peerlist .none { color: var(--dim); }
  #wire { height: 190px; overflow-y: auto; display: flex; flex-direction: column; gap: 2px; font-size: 12px; }
  #wire div { flex: none; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; color: #797985; }
  #wire .in::before  { content: "\\2190 "; color: #6bb2f0; }
  #wire .out::before { content: "\\2192 "; color: #7fd07f; }
  #wire b { color: #bcbcc8; font-weight: 600; }
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
  <h1>shared canvas</h1>
  <span class="room" id="room"></span>
  <span class="who" id="who"></span>
  <span class="hint">open this URL in a second window to draw together</span>
</header>
<main>
  <div class="left">
    <canvas id="board" width="1000" height="700"></canvas>
    <div class="bar">
      <button class="sw on" style="background:#e8e8f0" data-c="#e8e8f0" aria-label="white"></button>
      <button class="sw" style="background:#e8b45a" data-c="#e8b45a" aria-label="amber"></button>
      <button class="sw" style="background:#6bb2f0" data-c="#6bb2f0" aria-label="blue"></button>
      <button class="sw" style="background:#7fd07f" data-c="#7fd07f" aria-label="green"></button>
      <button class="sw" style="background:#ef7676" data-c="#ef7676" aria-label="red"></button>
      <input id="size" type="range" min="2" max="18" value="4" aria-label="brush size">
      <button class="txt" id="clear">clear</button>
      <input id="chat" placeholder="chat with the room — enter to send" maxlength="300">
    </div>
    <div id="toast"></div>
  </div>
  <aside>
    <div class="card">
      <h2>this room's durable object</h2>
      <dl class="kv">
        <dt>object id</dt><dd id="m-doid" title="">&mdash;</dd>
        <dt>status</dt><dd id="m-conn">connecting&hellip;</dd>
        <dt>awake for</dt><dd id="m-born">&mdash;</dd>
        <dt>cold activations</dt><dd id="m-act">&mdash;</dd>
        <dt>open sockets</dt><dd id="m-socks">&mdash;</dd>
        <dt>strokes in sqlite</dt><dd id="m-strokes">&mdash;</dd>
        <dt>last durable seq</dt><dd id="m-seq">&mdash;</dd>
      </dl>
    </div>
    <div class="card">
      <h2>peers on this object</h2>
      <div id="peerlist"></div>
    </div>
    <div class="card">
      <h2>wire</h2>
      <div id="wire"></div>
    </div>
    <div class="card note">
      <p><em>one object per room.</em> idFromName(room) routes every socket in this room to the same single-threaded object &mdash; message order is stroke order; no locks, no pub/sub, no races.</p>
      <p><em>state lives in the object.</em> each stroke is a row in the object's private SQLite; "last durable seq" is acked only after the write is durable in the bucket (RPO&nbsp;=&nbsp;0).</p>
      <p><em>idle rooms cost ~nothing.</em> sockets hibernate on the host and the object is evicted from memory; it wakes in milliseconds &mdash; watch "cold activations" climb after idling or a node restart. state survives: it lives in S3.</p>
    </div>
  </aside>
</main>
<footer>
  a meta-demo: the panel on the right is the app introspecting the technology it runs on
  &mdash; <a href="https://celld.dev" rel="noopener">celld</a>, self-hosted durable objects
  &middot; <a href="https://github.com/krasnoperov/celld-demo" rel="noopener">source</a>
</footer>
<script>
(function () {
  var room = location.pathname.replace(/^\\//, "") || "lobby";
  document.getElementById("room").textContent = "#" + room;

  var cv = document.getElementById("board");
  var cx = cv.getContext("2d");
  cx.lineCap = cx.lineJoin = "round";

  var color = "#e8e8f0";
  // sessionStorage: per-tab identity, so a second window is a second peer
  var me = (sessionStorage.canvasName ||
    (sessionStorage.canvasName = "guest-" + Math.random().toString(36).slice(2, 6)));
  document.getElementById("who").textContent = "you are " + me;
  var toast = document.getElementById("toast");

  function el(id) { return document.getElementById(id); }
  function esc(s) { return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;"); }
  function note(who, text) { toast.innerHTML = "<b>" + esc(who) + "</b> " + esc(text); }

  // --- under-the-hood panel ------------------------------------------------
  var wire = el("wire");
  function logWire(dir, type, detail) {
    var d = document.createElement("div");
    d.className = dir;
    d.innerHTML = "<b>" + esc(type) + "</b> " + esc(detail || "");
    wire.appendChild(d);
    while (wire.children.length > 60) wire.removeChild(wire.firstChild);
    wire.scrollTop = wire.scrollHeight;
  }
  var bornOffset = null; // server bornAt mapped onto the local clock
  function applyMeta(meta) {
    if (!meta) return;
    if (meta.doid) {
      el("m-doid").textContent = meta.doid.slice(0, 8) + "\\u2026" + meta.doid.slice(-4);
      el("m-doid").title = meta.doid;
    }
    el("m-act").textContent = meta.activations;
    el("m-socks").textContent = meta.sockets;
    el("m-strokes").textContent = meta.strokes;
    el("m-seq").textContent = meta.maxseq;
    bornOffset = Date.now() - (meta.now - meta.bornAt);
    renderPeers(meta.peers || []);
  }
  setInterval(function () {
    if (bornOffset === null) return;
    var s = Math.max(0, Math.round((Date.now() - bornOffset) / 1000));
    el("m-born").textContent = s < 120 ? s + "s" : Math.round(s / 60) + "m";
  }, 1000);

  function hue(name) {
    var h = 0;
    for (var i = 0; i < name.length; i++) h = (h * 31 + name.charCodeAt(i)) % 360;
    return "hsl(" + h + ", 55%, 62%)";
  }
  function renderPeers(peers) {
    var box = el("peerlist");
    box.innerHTML = "";
    if (!peers.length) { box.innerHTML = '<div class="none">nobody here yet</div>'; return; }
    peers.forEach(function (p) {
      var d = document.createElement("div");
      d.className = "p";
      d.innerHTML = '<span class="dot" style="background:' + hue(p.actor) + '"></span>' +
        esc(p.actor) + (p.actor === me ? " (you)" : "") +
        '<span class="r">' + esc(p.role) + "</span>";
      box.appendChild(d);
    });
  }

  // --- drawing primitives --------------------------------------------------
  function drawSegment(a, b, c, w) {
    cx.strokeStyle = c; cx.lineWidth = w;
    cx.beginPath(); cx.moveTo(a[0], a[1]); cx.lineTo(b[0], b[1]); cx.stroke();
  }
  function drawStroke(s) {
    var p = s.points;
    if (p.length === 1) { drawSegment(p[0], p[0], s.color, s.size); return; }
    for (var i = 1; i < p.length; i++) drawSegment(p[i - 1], p[i], s.color, s.size);
  }
  // Remote strokes animate in, segment by segment.
  var queue = [], animating = false;
  function animate() {
    if (!queue.length) { animating = false; return; }
    animating = true;
    var job = queue[0], p = job.stroke.points;
    for (var n = 0; n < 3 && job.i < p.length; n++, job.i++) {
      drawSegment(p[Math.max(0, job.i - 1)], p[job.i], job.stroke.color, job.stroke.size);
    }
    if (job.i >= p.length) queue.shift();
    requestAnimationFrame(animate);
  }
  function enqueue(s) { queue.push({ stroke: s, i: 0 }); if (!animating) animate(); }

  // --- websocket -----------------------------------------------------------
  var proto = location.protocol === "https:" ? "wss://" : "ws://";
  var sock, retry = 0;
  function send(o) {
    if (!sock || sock.readyState !== 1) return;
    sock.send(JSON.stringify(o));
    logWire("out", o.type,
      o.type === "stroke" ? o.points.length + " pts" :
      o.type === "say" ? o.text : "");
  }
  function connect() {
    sock = new WebSocket(proto + location.host + "/ws/" + room +
      "?actor=" + encodeURIComponent(me) + "&role=user");
    sock.onopen = function () { retry = 0; el("m-conn").textContent = "live"; el("m-conn").className = "hot"; };
    sock.onclose = function () {
      el("m-conn").textContent = "reconnecting\\u2026"; el("m-conn").className = "";
      setTimeout(connect, Math.min(1000 * ++retry, 5000));
    };
    sock.onmessage = function (ev) {
      var m = JSON.parse(ev.data);
      if (m.type === "snapshot") {
        logWire("in", "snapshot", m.strokes.length + " strokes replayed from sqlite");
        cx.clearRect(0, 0, cv.width, cv.height);
        m.strokes.forEach(drawStroke);
        applyMeta(m.meta);
      } else if (m.type === "stroke") {
        logWire("in", "stroke", "seq " + m.seq + " by " + m.stroke.actor);
        el("m-seq").textContent = m.seq;
        el("m-strokes").textContent = +el("m-strokes").textContent + 1 || 1;
        enqueue(m.stroke);
      } else if (m.type === "ack") {
        logWire("in", "ack", "your stroke is row " + m.seq + " (durable)");
        el("m-seq").textContent = m.seq;
        el("m-strokes").textContent = +el("m-strokes").textContent + 1 || 1;
      } else if (m.type === "clear") {
        logWire("in", "clear", "by " + m.actor);
        queue = []; cx.clearRect(0, 0, cv.width, cv.height);
        el("m-strokes").textContent = 0;
        note(m.actor, "cleared the canvas");
      } else if (m.type === "say" || m.type === "status") {
        logWire("in", m.type, m.actor + ": " + m.text);
        note(m.actor, m.text);
      } else if (m.type === "join") {
        logWire("in", "join", m.actor + " (" + m.role + ")");
        note(m.actor, "joined");
        applyMeta(m.meta);
      } else if (m.type === "leave") {
        logWire("in", "leave", m.actor);
        note(m.actor, "left");
        applyMeta(m.meta);
      } else if (m.type === "stats") {
        logWire("in", "stats", "activations " + m.meta.activations +
          ", sockets " + m.meta.sockets + ", strokes " + m.meta.strokes);
        applyMeta(m.meta);
      }
    };
  }
  connect();
  setInterval(function () { send({ type: "stats" }); }, 15000);

  // --- drawing input -------------------------------------------------------
  function pos(ev) {
    var r = cv.getBoundingClientRect();
    return [
      Math.round((ev.clientX - r.left) * (cv.width / r.width)),
      Math.round((ev.clientY - r.top) * (cv.height / r.height)),
    ];
  }
  var stroke = null;
  cv.addEventListener("pointerdown", function (ev) {
    cv.setPointerCapture(ev.pointerId);
    stroke = { color: color, size: +el("size").value, points: [pos(ev)] };
  });
  cv.addEventListener("pointermove", function (ev) {
    if (!stroke) return;
    var p = pos(ev), last = stroke.points[stroke.points.length - 1];
    if (Math.hypot(p[0] - last[0], p[1] - last[1]) < 3) return;
    stroke.points.push(p);
    drawSegment(last, p, stroke.color, stroke.size);
  });
  function up() {
    if (!stroke) return;
    if (stroke.points.length === 1) drawStroke(stroke);
    send({ type: "stroke", color: stroke.color, size: stroke.size, points: stroke.points });
    stroke = null;
  }
  cv.addEventListener("pointerup", up);
  cv.addEventListener("pointercancel", up);

  // --- toolbar -------------------------------------------------------------
  document.querySelectorAll(".sw").forEach(function (b) {
    b.onclick = function () {
      document.querySelectorAll(".sw").forEach(function (x) { x.classList.remove("on"); });
      b.classList.add("on");
      color = b.dataset.c;
    };
  });
  el("clear").onclick = function () {
    queue = []; cx.clearRect(0, 0, cv.width, cv.height);
    el("m-strokes").textContent = 0;
    send({ type: "clear" });
  };
  var chat = el("chat");
  chat.addEventListener("keydown", function (ev) {
    if (ev.key !== "Enter" || !chat.value.trim()) return;
    note(me, chat.value.trim());
    send({ type: "say", text: chat.value.trim() });
    chat.value = "";
  });
})();
</script>
</body>
</html>`;
