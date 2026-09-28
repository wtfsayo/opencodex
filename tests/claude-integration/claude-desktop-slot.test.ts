import { expect, test } from "bun:test";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { repoRoot } from "../helpers/repo-root";
import { removeTreeWithRetry } from "../helpers/remove-tree";

// Desktop aliases resolve only once desktop-3p.ts registers into the slot, and nothing but an
// import chain from the server entry guarantees that. A fresh process, so no other test's imports
// can register it first.
test("the proxy's server entry registers the Claude Desktop alias lookup", () => {
  const script = [
    "await import('./src/server/index.ts');",
    "const { desktop3pLookupRegistered } = await import('./src/claude/desktop-3p-slot.ts');",
    "process.exit(desktop3pLookupRegistered() ? 0 : 3);",
  ].join(" ");
  const home = mkdtempSync(join(tmpdir(), "ocx-desktop-slot-"));
  const run = Bun.spawnSync([process.execPath, "-e", script], { cwd: repoRoot(), env: { ...process.env, OPENCODEX_HOME: home }, stdout: "ignore", stderr: "pipe" });
  removeTreeWithRetry(home);
  expect([run.exitCode, run.exitCode === 0 ? "" : run.stderr.toString().slice(0, 400)]).toEqual([0, ""]);
});
