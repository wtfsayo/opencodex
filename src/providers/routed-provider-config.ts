// The provider config a request is sent with (router.ts routedProviderConfig): a registry
// provider's transport merged over the saved row. Its own module, with key resolution and the
// destination check passed in, so the Cloudflare Worker resolves a built-in provider exactly as ocx
// does without ocx's keychain or DNS checks.
import type { OcxProviderConfig } from "../types";
import { captureProviderApiKeySelection } from "./api-key-selection-capture";
import { PROVIDER_REGISTRY, registryEntryForProviderDestination } from "./registry";
import { providerMatchesRegistryTransportWithStaticGuards, providerSupportsLiveModelDiscovery } from "./static-model-discovery";
import { providerUsesKeyAuthOverride } from "./key-auth-override";
import { resolveModelPolicy } from "./resolved-model-policy";
import { cloneFastWire } from "./fastwire";
import { fastSwitchOff } from "./fast-opt-in";
import { applyDirectReasoningEffortContracts, hasLegacyClinePassReasoningEfforts } from "./derive";

export interface RoutedProviderConfigDeps {
  /** providers/api-key-resolve.ts's resolveProviderApiKey, or the Worker's secrets lookup. */
  resolveApiKey(apiKey: string | undefined): string | undefined;
  assertDestinationAllowed(providerName: string, provider: Pick<OcxProviderConfig, "baseUrl" | "allowPrivateNetwork">): void;
  warnBaseUrlDiscarded(providerName: string, userBaseUrl: string, effectiveBaseUrl: string): void;
}

export function routedProviderConfigWith(providerName: string, provider: OcxProviderConfig, deps: RoutedProviderConfigDeps): OcxProviderConfig {
  const usableResolvedApiKey = (apiKey: string | undefined): string | undefined => {
    const resolved = deps.resolveApiKey(apiKey);
    return typeof resolved === "string" && resolved.trim().length > 0 ? resolved : undefined;
  };
  provider = { ...provider, _apiKeyAttempt: provider._apiKeyAttempt ?? captureProviderApiKeySelection(provider) };
  const registryEntry = PROVIDER_REGISTRY.find(entry => entry.id === providerName);
  if (!registryEntry || !providerMatchesRegistryTransportWithStaticGuards(providerName, provider)) {
    deps.assertDestinationAllowed(providerName, provider);
    // A row whose adapter no longer matches its registry entry still reaches the Responses
    // adapter when one model opts in through `modelAdapters` — a Volcengine Coding Plan config
    // saved on Chat, for instance. The replay-drop flag belongs to the DESTINATION rather than
    // to the provider-wide wire, so it is filled here too; without it that continuation forwards
    // the reasoning item the upstream answers 400 to. Destination matching refuses templated and
    // overridable base URLs, so this cannot follow a retargeted row, and an explicit value wins.
    const destination = registryEntryForProviderDestination(provider);
    return {
      ...provider,
      apiKey: usableResolvedApiKey(provider.apiKey),
      ...(provider.dropResponsesReasoningItems === undefined && destination?.dropResponsesReasoningItems !== undefined
        ? { dropResponsesReasoningItems: destination.dropResponsesReasoningItems }
        : {}),
    };
  }
  const resolvedApiKey = usableResolvedApiKey(provider.apiKey);
  const staticModelCatalog = !providerSupportsLiveModelDiscovery(providerName, provider);
  const repairLegacyMimoFreeAuth = providerName === "mimo-free"
    && staticModelCatalog
    && (provider.authMode === undefined || provider.authMode === "local");
  const explicitKeyOverride = providerUsesKeyAuthOverride(registryEntry, provider, resolvedApiKey);
  const canonicalAuthMode = explicitKeyOverride
    ? "key"
    : repairLegacyMimoFreeAuth
      ? "key"
      : registryEntry.authKind === "forward" || registryEntry.authKind === "oauth"
        ? registryEntry.authKind
        : provider.authMode === "forward" ? undefined : provider.authMode;
  const staticPolicy = resolveModelPolicy({
    providerName,
    modelId: "__provider_static__",
    provider,
    registryEntry,
    transportMatchedRegistry: true,
    ...(canonicalAuthMode ? { effectiveAuth: { authMode: canonicalAuthMode } } : {}),
  }).provider;
  const reasoningEffortMap = staticPolicy.reasoningEffortMap;
  const modelReasoningEffortMap = staticPolicy.modelReasoningEffortMap;
  const modelReasoningEfforts = staticPolicy.modelReasoningEfforts;
  const modelDefaultReasoningEfforts = staticPolicy.modelDefaultReasoningEfforts;
  const modelContextWindows = staticPolicy.modelContextWindows;
  const modelInputModalities = staticPolicy.modelInputModalities;
  // Registry static headers are documented as applying to every upstream request, so they are
  // filled at resolve time rather than only at seed time: a config written before a header
  // existed, or one carrying any header of its own, would otherwise never receive it. User
  // headers win, matched case-insensitively so an override replaces rather than duplicates.
  const headers = staticPolicy.headers;
  const modelMaxInputTokens = staticPolicy.modelMaxInputTokens;
  const modelMaxOutputTokens = staticPolicy.modelMaxOutputTokens;
  const modelSupportsServiceTier = staticPolicy.modelSupportsServiceTier;
  const modelSupportsVerbosity = staticPolicy.modelSupportsVerbosity;
  const noVisionModels = staticPolicy.noVisionModels;
  const noReasoningModels = staticPolicy.noReasoningModels;
  const noTemperatureModels = staticPolicy.noTemperatureModels;
  const noTopPModels = staticPolicy.noTopPModels;
  const noStopModels = staticPolicy.noStopModels;
  const noPenaltyModels = staticPolicy.noPenaltyModels;
  const noJsonSchemaModels = staticPolicy.noJsonSchemaModels;
  const autoToolChoiceOnlyModels = staticPolicy.autoToolChoiceOnlyModels;
  const preserveReasoningContentModels = staticPolicy.preserveReasoningContentModels;
  const requiresReasoningPlaceholderModels = staticPolicy.requiresReasoningPlaceholderModels;
  const reasoningSplitModels = staticPolicy.reasoningSplitModels;
  const inlineThinkTagModels = staticPolicy.inlineThinkTagModels;
  const reasoningDetailsModels = staticPolicy.reasoningDetailsModels;
  const thinkingToggleModels = staticPolicy.thinkingToggleModels;
  const thinkingBudgetModels = staticPolicy.thinkingBudgetModels;
  const registryBaseUrlIsTemplate = /\{[^}]*\}/.test(registryEntry.baseUrl);
  const userBaseUrl = typeof provider.baseUrl === "string" ? provider.baseUrl.trim() : "";
  const userBaseUrlIsResolved = userBaseUrl.length > 0 && !/\{[^}]*\}/.test(userBaseUrl);
  if (registryEntry.allowBaseUrlOverride && !userBaseUrlIsResolved) {
    throw new Error(`Invalid baseUrl for provider "${providerName}": expected a nonblank URL without unresolved placeholders`);
  }
  // Registry template URLs are presets; local/self-hosted entries opt in explicitly.
  const baseUrl = (registryBaseUrlIsTemplate || registryEntry.allowBaseUrlOverride) && userBaseUrlIsResolved
    ? userBaseUrl
    : registryEntry.baseUrl;
  if (userBaseUrlIsResolved) deps.warnBaseUrlDiscarded(providerName, userBaseUrl, baseUrl);
  deps.assertDestinationAllowed(providerName, { baseUrl, allowPrivateNetwork: provider.allowPrivateNetwork });

  const resolved: OcxProviderConfig = {
    ...provider,
    adapter: registryEntry.adapter,
    baseUrl,
    ...(provider.responsesPath === undefined && registryEntry.responsesPath !== undefined
      ? { responsesPath: registryEntry.responsesPath }
      : {}),
    ...(provider.chatCompletionsPath === undefined && registryEntry.chatCompletionsPath !== undefined
      ? { chatCompletionsPath: registryEntry.chatCompletionsPath }
      : {}),
    ...(provider.requiresAdjacentResponsesToolResults === undefined
      && registryEntry.requiresAdjacentResponsesToolResults !== undefined
      ? { requiresAdjacentResponsesToolResults: registryEntry.requiresAdjacentResponsesToolResults }
      : {}),
    ...(provider.requiresPairedResponsesToolResults === undefined
      && registryEntry.requiresPairedResponsesToolResults !== undefined
      ? { requiresPairedResponsesToolResults: registryEntry.requiresPairedResponsesToolResults }
      : {}),
    ...(provider.annotateEmptyToolOutputs === undefined
      && registryEntry.annotateEmptyToolOutputs !== undefined
      ? { annotateEmptyToolOutputs: registryEntry.annotateEmptyToolOutputs }
      : {}),
    ...(provider.fastWire === undefined && registryEntry.fastWire !== undefined
      ? {
        fastWire: cloneFastWire(registryEntry.fastWire),
      }
      : {}),
    ...(provider.supportsServiceTier === undefined && registryEntry.supportsServiceTier !== undefined
      ? { supportsServiceTier: registryEntry.supportsServiceTier }
      : {}),
    // An off Fast switch is a provider-wide denial on the runtime provider, so a Fast policy
    // resolved without the provider name still refuses (providerFastSwitchOff).
    ...(fastSwitchOff(provider, registryEntry) ? { supportsServiceTier: false } : {}),
    // Registry-only web-search capability: without this backfill a saved provider row reaches
    // the Responses adapter with the flag `undefined`, so the capability gate added in #2262
    // reads "unclassified" and forwards Codex's OpenAI-only `web_search` config fields. xAI
    // rejects the whole request before inference ("Argument not supported:
    // external_web_access"), which killed every routed Grok turn on the Responses lane.
    // enrichProviderFromRegistry() already fills this, but the request path resolves through
    // routedProviderConfig() and never called it.
    ...(provider.supportsOpenAiWebSearchToolFields === undefined
      && registryEntry.supportsOpenAiWebSearchToolFields !== undefined
      ? { supportsOpenAiWebSearchToolFields: registryEntry.supportsOpenAiWebSearchToolFields }
      : {}),
    ...(provider.supportsResponsesCustomTools === undefined && registryEntry.supportsResponsesCustomTools !== undefined
      ? { supportsResponsesCustomTools: registryEntry.supportsResponsesCustomTools }
      : {}),
    ...(provider.preserveResponsesReasoningContent === undefined && registryEntry.preserveResponsesReasoningContent !== undefined
      ? { preserveResponsesReasoningContent: registryEntry.preserveResponsesReasoningContent }
      : {}),
    ...(provider.dropResponsesReasoningItems === undefined && registryEntry.dropResponsesReasoningItems !== undefined
      ? { dropResponsesReasoningItems: registryEntry.dropResponsesReasoningItems }
      : {}),
    // The request path resolves through routedProviderConfig() and never calls
    // enrichProviderFromRegistry(), so a saved provider row written before the
    // registry learned this flag must be backfilled here or route.provider never
    // carries it and the showThinkingSummary opt-in stays dead.
    ...(provider.showThinkingSummary === undefined && registryEntry.showThinkingSummary !== undefined
      ? { showThinkingSummary: registryEntry.showThinkingSummary }
      : {}),
    // Registry-only client-facing repair policy (#938): fill only when the
    // saved provider has no explicit policy; clone so runtime never aliases
    // the registry constant.
    ...(provider.responsesItemIdRepair === undefined && registryEntry.responsesItemIdRepair
      ? {
        responsesItemIdRepair: {
          ...(registryEntry.responsesItemIdRepair.message ? { message: [...registryEntry.responsesItemIdRepair.message] } : {}),
          ...(registryEntry.responsesItemIdRepair.reasoning ? { reasoning: [...registryEntry.responsesItemIdRepair.reasoning] } : {}),
          ...(registryEntry.responsesItemIdRepair.repairMissingTerminalIds !== undefined
            ? { repairMissingTerminalIds: registryEntry.responsesItemIdRepair.repairMissingTerminalIds }
            : {}),
          ...(registryEntry.responsesItemIdRepair.repairInvalidIds !== undefined
            ? { repairInvalidIds: registryEntry.responsesItemIdRepair.repairInvalidIds }
            : {}),
        },
      }
      : {}),
    authMode: canonicalAuthMode,
    apiKey: resolvedApiKey,
    ...(staticModelCatalog ? { liveModels: false } : {}),
    ...(headers ? { headers } : {}),
    // Backfill the Google wire mode + Vertex project/location from the registry when the user
    // config omits them, so a minimal `google-vertex`/`google-antigravity` entry still routes
    // through the correct branch (CCA/Vertex) instead of falling back to AI Studio.
    ...(provider.googleMode === undefined && registryEntry.googleMode !== undefined ? { googleMode: registryEntry.googleMode } : {}),
    ...(provider.project === undefined && registryEntry.project !== undefined ? { project: registryEntry.project } : {}),
    ...(provider.location === undefined && registryEntry.location !== undefined ? { location: registryEntry.location } : {}),
    ...(provider.contextWindow === undefined && registryEntry.contextWindow !== undefined ? { contextWindow: registryEntry.contextWindow } : {}),
    ...((provider.reasoningEfforts === undefined || hasLegacyClinePassReasoningEfforts(providerName, provider))
      && registryEntry.reasoningEfforts !== undefined
      ? { reasoningEfforts: [...registryEntry.reasoningEfforts] }
      : {}),
    ...(provider.escapeBuiltinToolNames === undefined && registryEntry.escapeBuiltinToolNames !== undefined ? { escapeBuiltinToolNames: registryEntry.escapeBuiltinToolNames } : {}),
    ...(provider.keyOptional === undefined && registryEntry.keyOptional !== undefined ? { keyOptional: registryEntry.keyOptional } : {}),
    ...(provider.modelSuffixBracketStrip === undefined && registryEntry.modelSuffixBracketStrip !== undefined ? { modelSuffixBracketStrip: registryEntry.modelSuffixBracketStrip } : {}),
    // Scalar backfill: a persisted config created before the flag shipped inherits the registry
    // opt-in, while an explicit user `false` keeps overriding registry `true`.
    ...(provider.parallelToolCalls === undefined && registryEntry.parallelToolCalls !== undefined ? { parallelToolCalls: registryEntry.parallelToolCalls } : {}),
    ...(provider.promptCacheKey === undefined && registryEntry.promptCacheKey !== undefined ? { promptCacheKey: registryEntry.promptCacheKey } : {}),
    ...(provider.chatServiceTier === undefined && registryEntry.chatServiceTier !== undefined ? { chatServiceTier: registryEntry.chatServiceTier } : {}),
    ...(provider.openaiChatEofTolerance === undefined && registryEntry.openaiChatEofTolerance !== undefined
      ? { openaiChatEofTolerance: registryEntry.openaiChatEofTolerance }
      : {}),
    ...(provider.reasoningWireFormat === undefined && registryEntry.reasoningWireFormat !== undefined
      ? { reasoningWireFormat: registryEntry.reasoningWireFormat }
      : {}),
    ...(provider.defaultMaxOutputTokens === undefined && registryEntry.defaultMaxOutputTokens !== undefined
      ? { defaultMaxOutputTokens: registryEntry.defaultMaxOutputTokens }
      : {}),
    ...(modelContextWindows ? { modelContextWindows } : {}),
    ...(modelInputModalities ? { modelInputModalities } : {}),
    ...(modelMaxInputTokens ? { modelMaxInputTokens } : {}),
    ...(modelMaxOutputTokens ? { modelMaxOutputTokens } : {}),
    ...(modelSupportsServiceTier ? { modelSupportsServiceTier } : {}),
    ...(modelSupportsVerbosity ? { modelSupportsVerbosity } : {}),
    ...(modelReasoningEfforts ? { modelReasoningEfforts } : {}),
    ...(modelDefaultReasoningEfforts ? { modelDefaultReasoningEfforts } : {}),
    ...(reasoningEffortMap ? { reasoningEffortMap } : {}),
    ...(modelReasoningEffortMap ? { modelReasoningEffortMap } : {}),
    ...(noVisionModels ? { noVisionModels } : {}),
    ...(noReasoningModels ? { noReasoningModels } : {}),
    ...(noTemperatureModels ? { noTemperatureModels } : {}),
    ...(noTopPModels ? { noTopPModels } : {}),
    ...(noStopModels ? { noStopModels } : {}),
    ...(noPenaltyModels ? { noPenaltyModels } : {}),
    ...(noJsonSchemaModels ? { noJsonSchemaModels } : {}),
    ...(autoToolChoiceOnlyModels ? { autoToolChoiceOnlyModels } : {}),
    ...(preserveReasoningContentModels ? { preserveReasoningContentModels } : {}),
    ...(requiresReasoningPlaceholderModels ? { requiresReasoningPlaceholderModels } : {}),
    ...(reasoningSplitModels ? { reasoningSplitModels } : {}),
    ...(inlineThinkTagModels ? { inlineThinkTagModels } : {}),
    ...(reasoningDetailsModels ? { reasoningDetailsModels } : {}),
    ...(thinkingToggleModels ? { thinkingToggleModels } : {}),
    ...(thinkingBudgetModels ? { thinkingBudgetModels } : {}),
  };
  applyDirectReasoningEffortContracts(registryEntry, resolved, provider);
  return resolved;
}
