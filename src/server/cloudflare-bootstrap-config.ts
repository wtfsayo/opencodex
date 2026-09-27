// The rules OCX_BOOTSTRAP_CONFIG_JSON must meet on a Cloudflare deployment, shared by the
// container's supervisor (which refuses to boot on a violation) and the Worker (which must then
// refuse to serve from it). Import-free apart from the retention floor, which is import-free too.
import { MIN_USAGE_LEDGER_MAX_BYTES } from "../usage/retention-contract";

// The Worker reaches ocx only here; see defaultPort in deploy/cloudflare/src/index.ts.
export const CLOUDFLARE_OCX_PORT = 10100;
// Seeded when the operator's bootstrap config sets none. Every snapshot uploads the whole ledger,
// and without a cap it grows for as long as the hub serves requests.
export const DEFAULT_USAGE_LEDGER_MAX_BYTES = 32 * 1024 * 1024;

/** The parsed bootstrap config, or why the container would refuse it. */
export function parseBootstrapConfig(raw: string, port = CLOUDFLARE_OCX_PORT): { config: Record<string, unknown> } | { error: string } {
  let parsed: unknown;
  try { parsed = JSON.parse(raw); } catch { return { error: "OCX_BOOTSTRAP_CONFIG_JSON is not valid JSON" }; }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return { error: "OCX_BOOTSTRAP_CONFIG_JSON must be a JSON object" };
  const config = parsed as Record<string, unknown>;
  if (config.hostname !== undefined && config.hostname !== "0.0.0.0") {
    return { error: `OCX_BOOTSTRAP_CONFIG_JSON must use "hostname": "0.0.0.0" (or omit it); the Worker cannot reach ${String(config.hostname)}` };
  }
  if (config.port !== undefined && config.port !== port) return { error: `OCX_BOOTSTRAP_CONFIG_JSON must use "port": ${port} (or omit it)` };
  const cap = config.usageLedgerMaxBytes;
  if (cap !== undefined && (typeof cap !== "number" || !Number.isSafeInteger(cap) || cap < MIN_USAGE_LEDGER_MAX_BYTES)) {
    // ocx would silently treat it as unset, which removes the cap the seed exists to add.
    return { error: `OCX_BOOTSTRAP_CONFIG_JSON "usageLedgerMaxBytes" must be a whole number of at least ${MIN_USAGE_LEDGER_MAX_BYTES} (or omit it)` };
  }
  return { config };
}

/** What the supervisor writes as config.json on a first boot. */
export function seededBootstrapConfig(config: Record<string, unknown>, port = CLOUDFLARE_OCX_PORT): Record<string, unknown> {
  return { usageLedgerMaxBytes: DEFAULT_USAGE_LEDGER_MAX_BYTES, ...config, hostname: "0.0.0.0", port };
}
