// celld-demo: "place" — a forward-only pixel board on self-hosted Durable
// Objects (celld). The whole server is this Wrangler project; see place.js
// for the architecture (batched write path, CDN-frame read path).

import { handlePlace } from "./place.js";
export { Tile, User } from "./place.js";

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/" || url.pathname === "/place" || url.pathname.startsWith("/place/")) {
      return handlePlace(request, env);
    }
    return new Response("not found", { status: 404 });
  },
};
