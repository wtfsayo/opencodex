// Which GET /v1/models answers the Cloudflare Worker may replay, and under what key. ocx publishes an
// answer (model-list-replay.ts) and the Worker looks it up (deploy/cloudflare), both through this
// file, so the two cannot disagree about what makes two requests the same.
import { CURSOR_USER_AGENT } from "../integrations/cursor-seen";

/**
 * Every request input the /v1/models route in serve-options.ts reads, as a hex digest, or undefined
 * when its answer also depends on something a replay would skip:
 * - an `Origin` header: CORS headers and ocx's origin check;
 * - Cursor's user agent: ocx records when Cursor last asked (cursor-seen.ts);
 * - the Claude Desktop list shapes (`format=desktop-config`, hashed ids): ocx rebuilds its Desktop
 *   alias registry while answering, and later Desktop turns resolve through it.
 */
export async function modelListReplayKey(url: URL, headers: Headers): Promise<string | undefined> {
  if (headers.has("origin")) return undefined;
  const userAgent = headers.get("user-agent") ?? "";
  if (CURSOR_USER_AGENT.test(userAgent.trim())) return undefined;
  const params = url.searchParams;
  if (params.has("format")) return undefined;
  const anthropicVersion = headers.get("anthropic-version") !== null;
  const anthropicList = anthropicVersion || params.get("flavor") === "anthropic";
  if (anthropicList && !params.has("client_version")) {
    const ids = params.get("ids");
    const readable = ids === "cli" || (ids !== "desktop" && /^claude-code\//i.test(userAgent));
    if (!readable) return undefined;
  }
  const query = [...params].sort(([a, x], [b, y]) => a < b ? -1 : a > b ? 1 : x < y ? -1 : x > y ? 1 : 0);
  const key = JSON.stringify(["v1", query, anthropicVersion, userAgent]);
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(key));
  return [...new Uint8Array(digest)].map(byte => byte.toString(16).padStart(2, "0")).join("");
}
