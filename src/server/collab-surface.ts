// Import-free (types only), so the Cloudflare Worker can recognise the turns ocx treats as
// collaboration without loading effort policy or collaboration.ts.
import type { OcxParsedRequest } from "../types";

/**
 * True when the request carries codex-rs's spawned-child markers, matched EXACTLY.
 * Source of truth (openai/codex @ 6138909d): every collab-spawned child turn sends
 * `x-openai-subagent: collab_spawn` (core/src/responses_metadata.rs) and embeds
 * `"subagent_kind":"thread_spawn"` in the JSON `x-codex-turn-metadata` compatibility
 * header. Both are checked: the WS bridge rebuilds internal requests from the
 * FORWARD_HEADERS allowlist, so either header alone is sufficient evidence.
 *
 * Exact matching matters: upstream emits `x-openai-subagent` for OTHER internal
 * turn categories too (review, compact, memory_consolidation, arbitrary "other"
 * sources — responses_metadata.rs subagent_source). Those are maintenance turns,
 * not spawned children, and must never trip subagentEffortCap.
 */
export function isThreadSpawnRequest(headers: Headers): boolean {
  if (headers.get("x-openai-subagent") === "collab_spawn") return true;
  const turnMeta = headers.get("x-codex-turn-metadata");
  if (!turnMeta) return false;
  try {
    const parsed = JSON.parse(turnMeta) as { subagent_kind?: unknown };
    return parsed.subagent_kind === "thread_spawn";
  } catch {
    return false;
  }
}

/**
 * Detect collaboration surface for a native chat request body.
 * Mirrors Responses collabSurface behavior across function and custom tool representations.
 */
export function chatCollabSurface(chatBody: Record<string, unknown>): "v1" | "v2" | null {
  if (!Array.isArray(chatBody.tools)) return null;
  let namespacedSpawn = false;
  let flatSpawn = false;
  let v1Only = false;
  let v2Only = false;
  for (const raw of chatBody.tools) {
    if (!raw || typeof raw !== "object") continue;
    const tool = raw as Record<string, unknown>;
    let name = "";
    let namespace: string | undefined = undefined;
    if (tool.type === "function" && tool.function && typeof tool.function === "object") {
      const fn = tool.function as Record<string, unknown>;
      name = typeof fn.name === "string" ? fn.name : "";
    } else if (tool.type === "custom" && tool.custom && typeof tool.custom === "object") {
      const cust = tool.custom as Record<string, unknown>;
      name = typeof cust.name === "string" ? cust.name : "";
    } else if (typeof tool.name === "string") {
      name = tool.name;
    }
    if (typeof tool.namespace === "string") namespace = tool.namespace;
    if (name === "spawn_agent") {
      if (namespace) namespacedSpawn = true;
      else flatSpawn = true;
    } else if (name === "send_input" || name === "resume_agent" || name === "close_agent") {
      v1Only = true;
    } else if (name === "send_message" || name === "followup_task" || name === "interrupt_agent" || name === "list_agents") {
      v2Only = true;
    }
  }
  if (!namespacedSpawn && !flatSpawn) return null;
  if (namespacedSpawn && flatSpawn) return null;
  if (v1Only && v2Only) return null;
  if (v1Only) return "v1";
  if (v2Only) return "v2";
  return namespacedSpawn ? "v1" : "v2";
}

export function collabSurface(parsed: OcxParsedRequest): "v1" | "v2" | null {
  let namespacedSpawn = false;
  let flatSpawn = false;
  let v1Only = false;
  let v2Only = false;
  for (const t of parsed.context.tools ?? []) {
    if (t.name === "spawn_agent") {
      if (t.namespace) namespacedSpawn = true;
      else flatSpawn = true;
    } else if (t.name === "send_input" || t.name === "resume_agent" || t.name === "close_agent") {
      v1Only = true;
    } else if (t.name === "send_message" || t.name === "followup_task" || t.name === "interrupt_agent" || t.name === "list_agents") {
      v2Only = true;
    }
  }
  if (!namespacedSpawn && !flatSpawn) return null; // no spawn_agent -> no collab surface
  if (namespacedSpawn && flatSpawn) return null;   // contradictory spawn shapes
  if (v1Only && v2Only) return null;               // contradictory companions
  if (v1Only) return "v1";
  if (v2Only) return "v2";
  return namespacedSpawn ? "v1" : "v2"; // companionless fallbacks (legacy defaults)
}
