// The Worker runs with nodejs_compat (wrangler.jsonc), which serves the subset of node:crypto the
// OAuth refresh coordinator needs. workers-types declares no node: modules, so the two imports this
// bundle actually makes are declared here — deliberately narrower than @types/node, which would
// also typecheck calls workerd does not implement.
declare module "node:crypto" {
  interface Hash {
    update(data: string): Hash;
    digest(encoding: "hex"): string;
  }
  export function createHash(algorithm: string): Hash;
}
