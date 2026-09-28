// The inbound admission limit, in its own module so the Cloudflare Worker applies the same one.
/**
 * Cap decompressed request bodies (a compressed bomb must not inflate unbounded). Codex compresses
 * EVERY responses request with zstd (no size threshold), and image-heavy histories inflate fast:
 * ~12 full-res screenshots as base64 already cross 64MB decompressed. The proxy is fed by the user's
 * own local Codex over loopback, so the bomb threat is weak; this cap is really an OOM guard. Keep it
 * generous enough that ordinary multi-image sessions decode, while still bounding a runaway body.
 */
export const MAX_DECOMPRESSED_BODY_BYTES = 256 * 1024 * 1024;

/**
 * Hard ceiling on the opt-in `maxInboundBodyBytes` (#3573).
 *
 * The opt-in exists because a 922k-token session serializes past the 256 MiB default, and the
 * request that crosses it is the compaction request itself — so the session can no longer
 * shrink and is stuck. An UNBOUNDED inbound cap is not an acceptable answer: this admission
 * limit is the only thing standing between one request and the process heap, and
 * `readBoundedJsonRequestBody` materializes the body several times over (retained wire bytes,
 * decoded bytes, the decoded string, the serialized measurement string, and the parsed object
 * graph), so peak RSS is a MULTIPLE of whatever is admitted here. 512 MiB is the largest value
 * that keeps that multiple survivable on an ordinary machine, and it is what #3573 asked for.
 */
export const MAX_CONFIGURABLE_INBOUND_BODY_BYTES = 512 * 1024 * 1024;

/** Floor for the opt-in. Below this an ordinary multi-image turn cannot be admitted at all. */
export const MIN_CONFIGURABLE_INBOUND_BODY_BYTES = 1024 * 1024;

/**
 * Resolve the configured inbound admission limit, clamped to the supported range.
 *
 * Pure and total on purpose: the schema in `src/config.ts` degrades an invalid hand edit to
 * `undefined` rather than failing the parse, so the schema cannot be the place the ceiling is
 * enforced. Every caller resolves through here, which makes this the single auditable bound
 * regardless of how the config object was produced.
 *
 * Omitted, zero, or non-finite = the 256 MiB default, so an unconfigured proxy admits exactly
 * what it admits today.
 */
export function resolveInboundBodyLimitBytes(configured: number | undefined): number {
  if (configured === undefined || !Number.isFinite(configured) || configured <= 0) {
    return MAX_DECOMPRESSED_BODY_BYTES;
  }
  return Math.min(
    Math.max(Math.floor(configured), MIN_CONFIGURABLE_INBOUND_BODY_BYTES),
    MAX_CONFIGURABLE_INBOUND_BODY_BYTES,
  );
}
