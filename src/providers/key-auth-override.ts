// Whether a saved key overrides a registry OAuth provider's login; its own module so request-time
// provider resolution can read it without the key store's keychain access.
import type { OcxProviderConfig } from "../types";
import type { ProviderRegistryEntry } from "./registry";

/** Shared with routing: a key-mode override is effective only while its key resolves. */
export function providerUsesKeyAuthOverride(
  entry: Pick<ProviderRegistryEntry, "authKind" | "allowKeyAuthOverride">,
  provider: Pick<OcxProviderConfig, "authMode">,
  resolvedKey: string | undefined,
): boolean {
  return entry.authKind === "oauth" && entry.allowKeyAuthOverride === true
    && provider.authMode === "key" && typeof resolvedKey === "string" && resolvedKey.trim().length > 0;
}
