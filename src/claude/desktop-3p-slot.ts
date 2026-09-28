// The Claude Desktop 3P alias lookups the inbound model resolver needs. Desktop's registry is kept
// on disk (desktop-3p.ts), so the resolver reads it through this slot, which desktop-3p.ts fills
// when it loads; ocx's Messages handler imports it directly, so the proxy always has it. Where it
// is never loaded (the Cloudflare Worker), no Desktop alias resolves and such turns are declined.
export interface Desktop3pLookup {
  resolve(alias: string): string | null;
  isUnresolved(id: string): boolean;
  /** A date-shaped managed id, whose absence may mean discovery has not finished. */
  isDateShaped(base: string): boolean;
}

let lookup: Desktop3pLookup = {
  resolve: () => null,
  isUnresolved: () => false,
  isDateShaped: () => false,
};

export function registerDesktop3pLookup(next: Desktop3pLookup): void {
  lookup = next;
}

export function desktop3pLookup(): Desktop3pLookup {
  return lookup;
}
