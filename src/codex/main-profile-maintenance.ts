// The refusal text for a request that arrives while native-main profile maintenance runs; its own
// module so a client-facing error formatter can recognise it without auth-context.ts.
export const CODEX_MAIN_PROFILE_MAINTENANCE_MESSAGE =
  "OpenCodex local native-main profile maintenance is active; retry this request";
