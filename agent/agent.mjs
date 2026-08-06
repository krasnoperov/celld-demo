// The agent side of the canvas. Deliberately symmetrical with the browser:
// it is just another WebSocket client of the same Durable Object, speaking
// the same JSON protocol. The DO doesn't know or care that this peer is a model.
//
//   node agent.mjs [room]           (default room: lobby)
//   CANVAS_URL=ws://host:port       (default: ws://127.0.0.1:8787 — wrangler dev)
//
// Credentials: set ANTHROPIC_API_KEY in the environment.

import Anthropic from "@anthropic-ai/sdk";

const ROOM = process.argv[2] ?? "lobby";
const BASE = (process.env.CANVAS_URL ?? "ws://127.0.0.1:8787").replace(/\/$/, "");
const NAME = "claude";

const client = new Anthropic();

// --- room state, mirrored from the DO --------------------------------------

let W = 1000, H = 700;
let strokes = [];        // full stroke log (snapshot + live)
let requests = [];       // recent "say" messages from humans
let busy = false;
let timer = null;

const ws = new WebSocket(`${BASE}/ws/${ROOM}?actor=${NAME}&role=agent`);

ws.addEventListener("open", () => console.log(`[agent] joined ${BASE}/ws/${ROOM}`));
ws.addEventListener("close", () => { console.log("[agent] disconnected"); process.exit(0); });
ws.addEventListener("error", (e) => { console.error("[agent] socket error:", e.message ?? e); });

ws.addEventListener("message", (ev) => {
  const m = JSON.parse(ev.data);
  switch (m.type) {
    case "snapshot":
      W = m.w; H = m.h; strokes = m.strokes;
      break;
    case "stroke":
      strokes.push(m.stroke);
      if (m.stroke.actor !== NAME) schedule(2000);   // let the human finish drawing
      break;
    case "say":
      requests.push(`${m.actor}: ${m.text}`);
      if (requests.length > 6) requests.shift();
      schedule(300);
      break;
    case "clear":
      strokes = [];
      break;
  }
});

function schedule(ms) {
  clearTimeout(timer);
  timer = setTimeout(think, ms);
}

function send(o) { ws.send(JSON.stringify(o)); }

// --- the Claude call --------------------------------------------------------

const SCHEMA = {
  type: "object",
  properties: {
    say: {
      type: "string",
      description: "One short, playful sentence to the human about what you're drawing.",
    },
    strokes: {
      type: "array",
      description: "Strokes to draw, in order. Each is a polyline on the canvas.",
      items: {
        type: "object",
        properties: {
          color: { type: "string", description: "CSS color, e.g. #e8b45a" },
          size: { type: "integer", description: "Line width in px, 2-18" },
          points: {
            type: "array",
            description: "Polyline [x,y] points. Dense points (every 5-15px) make smooth curves.",
            items: { type: "array", items: { type: "number" } },
          },
        },
        required: ["color", "size", "points"],
        additionalProperties: false,
      },
    },
  },
  required: ["say", "strokes"],
  additionalProperties: false,
};

const SYSTEM = `You are Claude, drawing live on a shared canvas with a human.
The canvas is 1000x700 pixels, origin at the top-left. You see the full
stroke log (theirs and yours) and their recent chat messages.

Respond to what is happening: complete or riff on shapes the human started,
answer drawing requests from chat, or add one tasteful complementary element.
Draw something recognizable using several strokes with dense points for smooth
curves. Stay inside the canvas. Don't erase or scribble over the human's work.
Keep "say" to one short sentence.`;

function compact(list) {
  // Cap what we ship to the model: at most 120 strokes, ~40 points each.
  return list.slice(-120).map((s) => ({
    by: s.actor === NAME ? "you" : s.actor,
    color: s.color,
    size: s.size,
    points: s.points.filter(
      (_, i) => i % Math.ceil(s.points.length / 40) === 0 || i === s.points.length - 1,
    ).map(([x, y]) => [Math.round(x), Math.round(y)]),
  }));
}

async function think() {
  if (busy) { schedule(1500); return; }
  busy = true;
  send({ type: "status", text: "is thinking…" });
  try {
    const response = await client.messages.create({
      model: "claude-opus-5",
      max_tokens: 16000,
      system: SYSTEM,
      output_config: { format: { type: "json_schema", schema: SCHEMA } },
      messages: [{
        role: "user",
        content: JSON.stringify({
          canvas: { w: W, h: H },
          strokes: compact(strokes),
          recent_chat: requests,
        }),
      }],
    });

    if (response.stop_reason === "refusal") {
      send({ type: "say", text: "I'd rather not draw that one — try something else?" });
      return;
    }
    const text = response.content.find((b) => b.type === "text")?.text ?? "{}";
    const { say, strokes: out } = JSON.parse(text);

    if (say) send({ type: "say", text: say });
    for (const s of out ?? []) {
      const stroke = { actor: NAME, role: "agent", ...s };
      strokes.push(stroke);
      send({ type: "stroke", color: s.color, size: s.size, points: s.points });
      await new Promise((r) => setTimeout(r, 150));   // pace the reveal
    }
    console.log(`[agent] drew ${out?.length ?? 0} strokes — "${say}"`);
  } catch (err) {
    console.error("[agent] think failed:", err.message ?? err);
    send({ type: "status", text: "hit an error, standing by" });
  } finally {
    busy = false;
  }
}
