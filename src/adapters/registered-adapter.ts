// What every adapter the registry builds carries beyond its own factory, in a module of its own so
// the Cloudflare Worker, which builds the adapters it serves without the whole registry, applies the
// same layers in the same order.
import type { ProviderAdapter } from "./base";
import type { AdapterWire } from "./registry";
import { withClinePassDeepSeekV4ToolReplayCompatibility } from "./cline-pass-deepseek-v4-tool-replay";
import { withUniqueToolCallIds } from "./unique-tool-call-ids";
import { createAdapterTierMetadata } from "../providers/fastwire";
import { withInputMediaGuard } from "./input-media-guard";

/** The openai-chat definition's wrappers around the adapter itself. */
export function wrapOpenAIChatAdapter(adapter: ProviderAdapter): ProviderAdapter {
  return withUniqueToolCallIds(withClinePassDeepSeekV4ToolReplayCompatibility(adapter));
}

/** The media guard and tier metadata every registered adapter gets, for its effective wire. */
export function finishRegisteredAdapter(adapter: ProviderAdapter, wire: AdapterWire): ProviderAdapter {
  if (wire !== "openai-responses") {
    withInputMediaGuard(adapter, wire);
  }
  const buildRequest = adapter.buildRequest.bind(adapter);
  adapter.buildRequest = (parsed, incoming) => {
    const attachTierMetadata = (request: Awaited<ReturnType<ProviderAdapter["buildRequest"]>>) => {
      // OpenAI-family adapters report the exact emitted field themselves. Other adapters
      // still report an exact absence at this serialization boundary, which makes a routed
      // Fast downgrade observable without asking core to infer an outbound body shape.
      request.tierLog ??= createAdapterTierMetadata(
        parsed.options.tierObservation,
        parsed.options.tierDecision,
        null,
        null,
      );
      return request;
    };
    const request = buildRequest(parsed, incoming);
    return request instanceof Promise
      ? request.then(attachTierMetadata)
      : attachTierMetadata(request);
  };
  if (adapter.runTurn && !adapter.tierLogForRunTurn) {
    adapter.tierLogForRunTurn = parsed => createAdapterTierMetadata(
      parsed.options.tierObservation,
      parsed.options.tierDecision,
      null,
      null,
    );
  }
  return adapter;
}
