// Which GET /v1/models answers the Cloudflare Worker may replay, and under what key. ocx publishes an
// answer (model-list-replay.ts) and the Worker looks it up (deploy/cloudflare), both through this
// file, so the two cannot disagree about what makes two requests the same.
import { CURSOR_USER_AGENT } from "../integrations/cursor-seen";

/**
 * Every request input the /v1/models route in serve-options.ts reads, canonically, or undefined
 * when its answer also depends on something a replay would skip:
 * - an `Origin` header: CORS headers and ocx's origin check;
 * - Cursor's user agent: ocx records when Cursor last asked (cursor-seen.ts);
 * - the Claude Desktop list shapes (`format=desktop-config`, hashed ids), whose aliases later
 *   Desktop turns resolve through the registry ocx rebuilds while answering. Claude Code's readable
 *   ids decode without it, and ocx builds it at startup as well.
 * - a repeated parameter: the route reads the first value, so the order would matter.
 */
export function modelListReplayInputs(url: URL, headers: Headers): string | undefined {
  if (headers.has("origin")) return undefined;
  const userAgent = headers.get("user-agent") ?? "";
  if (CURSOR_USER_AGENT.test(userAgent.trim())) return undefined;
  const params = url.searchParams;
  const names = [...params.keys()];
  if (new Set(names).size !== names.length) return undefined;
  if (params.has("format")) return undefined;
  const anthropicVersion = headers.get("anthropic-version") !== null;
  // The one thing the route reads from the user agent besides Cursor's.
  const claudeCode = /^claude-code\//i.test(userAgent);
  if ((anthropicVersion || params.get("flavor") === "anthropic") && !params.has("client_version")) {
    const ids = params.get("ids");
    if (!(ids === "cli" || (ids !== "desktop" && claudeCode))) return undefined;
  }
  const query = [...params].sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0);
  return JSON.stringify(["v2", query, anthropicVersion, claudeCode]);
}

/** The digest of `modelListReplayInputs`, or undefined when the answer cannot be replayed. */
export async function modelListReplayKey(url: URL, headers: Headers): Promise<string | undefined> {
  const inputs = modelListReplayInputs(url, headers);
  if (inputs === undefined) return undefined;
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(inputs));
  return [...new Uint8Array(digest)].map(byte => byte.toString(16).padStart(2, "0")).join("");
}
