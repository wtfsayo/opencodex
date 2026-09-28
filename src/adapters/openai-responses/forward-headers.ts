import { CODEX_RESPONSES_LITE_HEADER } from "../../codex/forward-transport-headers";

// Headers relayed verbatim from the caller in OAuth-passthrough ("forward") mode.
// Its own module so the Cloudflare Worker can read it too. The web-search sidecar reuses the
// exact same forwarded-auth set for its ChatGPT call.
export const FORWARD_HEADERS = [
  "authorization",
  "chatgpt-account-id",
  "openai-beta",
  "originator",
  "session_id",
  "session-id",
  "thread-id",
  "x-client-request-id",
  "x-codex-beta-features",
  "x-codex-installation-id",
  "x-codex-parent-thread-id",
  "x-codex-turn-metadata",
  "x-codex-turn-state",
  "x-codex-window-id",
  "x-oai-attestation",
  "x-openai-subagent",
  "x-responsesapi-include-timing-metrics",
  CODEX_RESPONSES_LITE_HEADER,
];
