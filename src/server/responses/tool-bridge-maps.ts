// The per-request tool maps the SSE bridge restores client tool names from. Kept apart from
// collaboration.ts, whose imports reach routing and state, so the Cloudflare Worker can build them.
import { dottedToolName, namespacedToolName, NAMESPACED_BARE_ALIAS_EXCLUDED_NAMES, toolChoiceToolPredicate } from "../../types";
import type { OcxParsedRequest } from "../../types";
import type { TranslatorBudget } from "../../lib/translator-budget";

export function buildToolBridgeMaps(parsed: OcxParsedRequest, budget?: TranslatorBudget): {
  toolNsMap: Map<string, { namespace: string; name: string; freeform?: true }>;
  declaredToolNames: Set<string>;
  /** Declared parameter schema per request-visible tool name (#1611 integer repair). */
  toolParameterSchemas: Map<string, Record<string, unknown>>;
  freeformToolNames: Set<string>;
  bareCustomToolNames: Set<string>;
  bareFunctionToolNames: Set<string>;
  toolSearchToolNames: Set<string>;
} {
  const toolNsMap = new Map<string, { namespace: string; name: string; freeform?: true }>();
  const declaredToolNames = new Set<string>();
  const toolParameterSchemas = new Map<string, Record<string, unknown>>();
  const freeformToolNames = new Set<string>();
  const bareCustomToolNames = new Set<string>();
  const bareFunctionToolNames = new Set<string>();
  const toolSearchToolNames = new Set<string>();
  const requestedTools = parsed.context.tools ?? [];
  const toolAllowed = toolChoiceToolPredicate(parsed.options.toolChoice, requestedTools);
  const authorizedTools = requestedTools.filter(toolAllowed);
  // A dotted alias is only safe while it names ONE tool. Namespaces and names both come from
  // the caller's tool catalog and may contain dots, so `{ns: "a", name: "b.c"}` and
  // `{ns: "a.b", name: "c"}` flatten to the same "a.b.c". Registering both would let a dotted
  // provider echo restore against whichever was inserted last, which is a dispatch decision made
  // by declaration order. Resolve ownership across the whole catalog FIRST so the outcome does
  // not depend on that order, then register only the aliases that stayed unambiguous.
  const dottedAliasOwners = new Map<string, string | null>();
  for (const t of authorizedTools) {
    if (!t.namespace) continue;
    const alias = dottedToolName(t.namespace, t.name);
    const identity = JSON.stringify([t.namespace, t.name]);
    const owner = dottedAliasOwners.get(alias);
    if (owner === undefined) dottedAliasOwners.set(alias, identity);
    else if (owner !== identity) dottedAliasOwners.set(alias, null);
  }
  // A dotted alias that shadows a canonical `ns__name` or a bare declaration is the same
  // confusion wearing a different spelling, so those lose the alias too.
  for (const t of authorizedTools) {
    const canonical = namespacedToolName(t.namespace, t.name);
    const owner = dottedAliasOwners.get(canonical);
    if (owner !== undefined && owner !== JSON.stringify([t.namespace, t.name])) {
      dottedAliasOwners.set(canonical, null);
    }
    const bare = dottedAliasOwners.get(t.name);
    if (bare !== undefined && bare !== JSON.stringify([t.namespace, t.name])) {
      dottedAliasOwners.set(t.name, null);
    }
  }
  // Bare echo alias (`name` with no namespace spelling, #4679): some providers — observed
  // on the muse family via Command Code — echo a namespaced tool by its bare name. The
  // bare spelling is only a safe alias while it names ONE tool and cannot be read as
  // another identity's canonical or dotted spelling.
  // Code-mode helper spellings never gain a bare alias (#4679 review), whatever namespace
  // declares them and whatever put the alias there. The list is owned by `src/types/tools.ts`,
  // beside the names it protects, because the copy that used to live here drifted to a single
  // namespace and had to be widened twice.
  const bareAliasOwners = new Map<string, string | null>();
  for (const t of authorizedTools) {
    // Bare (no-namespace) declarations participate as owners too: a namespaced tool whose
    // bare name equals a bare-declared function must not gain the bare alias, mirroring how
    // the tool_choice bare path refuses ambiguous owners across the whole request catalog.
    const identity = JSON.stringify([t.namespace ?? null, t.name]);
    const owner = bareAliasOwners.get(t.name);
    if (owner === undefined) bareAliasOwners.set(t.name, identity);
    else if (owner !== identity) bareAliasOwners.set(t.name, null);
  }
  for (const t of authorizedTools) {
    const canonical = namespacedToolName(t.namespace, t.name);
    const owner = bareAliasOwners.get(canonical);
    if (owner !== undefined && owner !== JSON.stringify([t.namespace, t.name])) {
      bareAliasOwners.set(canonical, null);
    }
    const dotted = dottedToolName(t.namespace, t.name);
    const dottedOwner = bareAliasOwners.get(dotted);
    if (dottedOwner !== undefined && dottedOwner !== JSON.stringify([t.namespace, t.name])) {
      bareAliasOwners.set(dotted, null);
    }
  }
  for (const t of authorizedTools) {
    // Upstream output is untrusted: only restore calls for tools the caller authorized.
    const wireName = namespacedToolName(t.namespace, t.name);
    budget?.chargeRetained(new TextEncoder().encode(wireName).byteLength, { kind: "retained_collectors" });
    declaredToolNames.add(wireName);
    // Retained by reference (the schema is already resident in parsed.context.tools),
    // so this adds a map entry rather than a copy of every tool's parameters.
    if (t.parameters && typeof t.parameters === "object") toolParameterSchemas.set(wireName, t.parameters);
    if (t.namespace) {
      budget?.chargeRetained(new TextEncoder().encode(JSON.stringify([wireName, t.namespace, t.name])).byteLength, { kind: "retained_collectors" });
      toolNsMap.set(wireName, { namespace: t.namespace, name: t.name, ...(t.freeform ? { freeform: true } : {}) });
      // Dotted echo alias (`ns.name`, #3402): same tool identity as the flattened wire name,
      // so a provider that echoes the dotted spelling still restores against this entry.
      const dottedName = dottedToolName(t.namespace, t.name);
      // Ambiguous aliases were resolved to null above; skipping them falls back to the
      // unambiguous `ns__name` form, which every provider can still echo.
      if (dottedAliasOwners.get(dottedName) !== null && dottedName !== wireName) {
        budget?.chargeRetained(new TextEncoder().encode(dottedName).byteLength, { kind: "retained_collectors" });
        declaredToolNames.add(dottedName);
        budget?.chargeRetained(new TextEncoder().encode(JSON.stringify([dottedName, t.namespace, t.name])).byteLength, { kind: "retained_collectors" });
        toolNsMap.set(dottedName, { namespace: t.namespace, name: t.name, ...(t.freeform ? { freeform: true } : {}) });
        if (t.parameters && typeof t.parameters === "object") toolParameterSchemas.set(dottedName, t.parameters);
      }
      // Bare echo alias (`name` with no namespace spelling, #4679): same tool identity as
      // the flattened wire name, so a provider that drops the namespace prefix still
      // restores against this entry. Ambiguous bare names were resolved to null above;
      // skipping them falls back to the spellings every provider can still echo.
      // Code-mode helper spellings never gain a bare alias, from ANY namespace (#4792 review).
      // Scoping this to `collaboration` was too narrow to be a boundary: a declaration such as
      // `mcp__remote.exec` donated bare `exec` to the declared set, and normalizeDeclaredToolName
      // then rewrote an undeclared `apply_patch`, `exec_command` or `write_stdin` onto it
      // (src/types/tools.ts). No namespace may turn helper normalization on for a catalog that
      // never declared the code-mode shell.
      //
      // The namespaced tool stays usable AS ITSELF: `ns__name` is added unconditionally above
      // and `ns.name` whenever it is unambiguous, so only the namespace-dropping echo fallback
      // is withdrawn, and only for these six spellings.
      if (
        bareAliasOwners.get(t.name) === JSON.stringify([t.namespace, t.name])
        && !NAMESPACED_BARE_ALIAS_EXCLUDED_NAMES.has(t.name)
      ) {
        budget?.chargeRetained(new TextEncoder().encode(t.name).byteLength, { kind: "retained_collectors" });
        declaredToolNames.add(t.name);
        budget?.chargeRetained(new TextEncoder().encode(JSON.stringify([t.name, t.namespace, t.name])).byteLength, { kind: "retained_collectors" });
        toolNsMap.set(t.name, { namespace: t.namespace, name: t.name, ...(t.freeform ? { freeform: true } : {}) });
        if (t.parameters && typeof t.parameters === "object") toolParameterSchemas.set(t.name, t.parameters);
      }
    }
    if (t.freeform) {
      budget?.chargeRetained(new TextEncoder().encode(t.name).byteLength, { kind: "retained_collectors" });
      freeformToolNames.add(t.name);
      if (!t.namespace || t.namespace === "functions") {
        budget?.chargeRetained(new TextEncoder().encode(t.name).byteLength, { kind: "retained_collectors" });
        bareCustomToolNames.add(t.name);
      }
    } else if (
      !t.toolSearch
      && !t.webSearch
      && !t.imageGeneration
      && !t.videoGeneration
      && (!t.namespace || t.namespace === "functions")
    ) {
      budget?.chargeRetained(new TextEncoder().encode(t.name).byteLength, { kind: "retained_collectors" });
      bareFunctionToolNames.add(t.name);
    }
    if (t.toolSearch) {
      budget?.chargeRetained(new TextEncoder().encode(t.name).byteLength, { kind: "retained_collectors" });
      toolSearchToolNames.add(t.name);
    }
  }
  // Some routed providers echo a bare tool_choice selector instead of the flattened catalog
  // name. Accept only selectors the client actually sent and only when the full request catalog
  // contains one tool with that logical name.
  //
  // A helper spelling selected this way is split rather than refused (#4819). The two things a
  // bare alias does are separable, and passthrough already relies on that: identity RESTORATION
  // runs before authorization there, rewriting the echoed bare name to the namespaced identity
  // the caller declared, and the guard then authorizes `ns__name`. DECLARATION is the part that
  // is unsafe, because a declared-name set carrying bare `exec` is what makes
  // `normalizeDeclaredToolName` rewrite an undeclared `apply_patch`, `exec_command` or
  // `write_stdin` onto the selected tool (src/types/tools.ts).
  //
  // So a helper spelling gets the `toolNsMap` entry and not the `declaredToolNames` entry. The
  // caller nominated exactly one tool by name, `bareNameCounts` proves nothing else answers to
  // it, and restoring it authorizes nothing the request did not already declare. The echo path
  // above withholds both, because a bare echo is a guess rather than a nomination and #4679
  // pinned that shape (`tests/responses/bare-echo-alias.test.ts`).
  const choice = parsed.options.toolChoice;
  const bareChoiceNames = new Set(
    choice && typeof choice === "object"
      ? ("allowedTools" in choice ? choice.allowedTools : [choice.name])
      : [],
  );
  const bareNameCounts = new Map<string, number>();
  for (const t of requestedTools) {
    bareNameCounts.set(t.name, (bareNameCounts.get(t.name) ?? 0) + 1);
  }
  for (const t of authorizedTools) {
    if (!t.namespace || !bareChoiceNames.has(t.name) || bareNameCounts.get(t.name) !== 1 || declaredToolNames.has(t.name)) continue;
    // Restore the identity; declare the name only when it is not a helper spelling.
    if (!NAMESPACED_BARE_ALIAS_EXCLUDED_NAMES.has(t.name)) {
      budget?.chargeRetained(new TextEncoder().encode(t.name).byteLength, { kind: "retained_collectors" });
      declaredToolNames.add(t.name);
    }
    budget?.chargeRetained(new TextEncoder().encode(JSON.stringify([t.name, t.namespace, t.name])).byteLength, { kind: "retained_collectors" });
    toolNsMap.set(t.name, { namespace: t.namespace, name: t.name, ...(t.freeform ? { freeform: true } : {}) });
    if (t.parameters && typeof t.parameters === "object") {
      toolParameterSchemas.set(t.name, t.parameters);
    }
  }
  return {
    toolNsMap,
    declaredToolNames,
    toolParameterSchemas,
    freeformToolNames,
    bareCustomToolNames,
    bareFunctionToolNames,
    toolSearchToolNames,
  };
}
