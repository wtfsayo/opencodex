// One Worker-served turn's reservations against the Durable Object's spend ledger
// (deploy/cloudflare/src/spend-ledger.ts). This is request-spend.ts's contract, async: the
// container's tracker calls a local journal synchronously; this one reaches the hub over RPC,
// and the ledger types both speak come from spend-reservation-core.ts, which is all this
// module may import — the file-journal module is outside the Worker's import boundary.
//
// The lifecycle is identical to createRequestSpendTracker: one reservation per physical send
// (charge runs inside the retry ladder's fetch, so a replacement send reserves too), an
// earlier send is marked dispatched once a later one exists, the last send settles with real
// usage or becomes unresolved spend, and a send that never left is abandoned. A refused
// reservation is answered with the workflow-refusal wire shape, found back by its header.
import {
  spendCeilingsConfigured,
  spendPolicyFromConfig,
  workflowSpendDenialSummary,
  WORKFLOW_LOCAL_REFUSAL_HEADER,
  type SpendDenial,
  type SpendReservationDecision,
  type SpendUsage,
} from "../lib/spend-reservation-core";
import type { OcxSpendConfig } from "../types/config";
import type { NativeChatDeps } from "./cloudflare-native-chat-api";

/** The async RPC family the Worker needs; NativeChatDeps carries it flattened. */
export interface WorkerSpendOps {
  spendReserve(request: WorkerSpendReserveRequest): Promise<SpendReservationDecision>;
  /** Fire-and-forget calls: the return promise is the Worker's own waitUntil handle. */
  spendAbandon(sendId: string): void;
  spendMarkDispatched(sendId: string): void;
  spendSettle(sendId: string, usage: SpendUsage): void;
  spendMarkLost(sendId: string): void;
}

export interface WorkerSpendReserveRequest {
  sendId: string;
  scopes: { rootId?: string; identityId?: string; poolId?: string };
  inputTokens: number;
  outputCeilingTokens: number;
}

/** What a charge against the hub ledger answers. */
export type WorkerSpendCharge =
  | { ok: true }
  | { ok: false; refusal: Response };

/**
 * The tracker for one turn. `charge` before each physical send with the send's reserve
 * figures; `resolve` once with the terminal usage (or undefined when none ever arrived).
 * Sentinel answers are Responses so a refusal inside a retry ladder travels back through it
 * unchanged.
 */
export interface WorkerSpendTurn {
  charge(inputTokens: number, outputCeilingTokens: number): Promise<WorkerSpendCharge>;
  /** Settle the newest send with real usage; every earlier one becomes unresolved spend. */
  resolve(usage: SpendUsage | undefined): void;
  readonly refusals: number;
}

const opsFor = (deps: NativeChatDeps): WorkerSpendOps | undefined =>
  deps.spendReserve && deps.spendAbandon && deps.spendMarkDispatched && deps.spendSettle && deps.spendMarkLost
    ? {
        spendReserve: deps.spendReserve,
        spendAbandon: deps.spendAbandon,
        spendMarkDispatched: deps.spendMarkDispatched,
        spendSettle: deps.spendSettle,
        spendMarkLost: deps.spendMarkLost,
      }
    : undefined;

/**
 * The 429 a local spend refusal answers with, byte-for-byte what workflowRefusalResponse
 * produces for a spend denial: classifyError rewrites the body to rate_limit_error /
 * rate_limit_exceeded, the message names the ceiling, and the header carries the real code.
 */
export function workerSpendRefusalResponse(denial: SpendDenial | undefined): Response {
  const reason = denial?.reason === "spend-limit-exceeded" ? "workflow-spend-exhausted" : "workflow-spend-undurable";
  const summary = workflowSpendDenialSummary(reason,
    denial?.reason === "spend-limit-exceeded"
      ? { scope: denial.scope, limit: denial.limit, projected: denial.projected }
      : undefined);
  const refusal = new Response(
    JSON.stringify({ error: { type: "rate_limit_error", code: "rate_limit_exceeded", message: summary.message } }),
    { status: 429, headers: { "content-type": "application/json" } },
  );
  refusal.headers.set(WORKFLOW_LOCAL_REFUSAL_HEADER, summary.code);
  refusal.headers.set("Access-Control-Expose-Headers", WORKFLOW_LOCAL_REFUSAL_HEADER);
  return refusal;
}

/** Whether a response is a local refusal this module minted (sentinel through retry ladders). */
export function isWorkerSpendRefusal(response: Response): boolean {
  return response.headers.get(WORKFLOW_LOCAL_REFUSAL_HEADER) !== null;
}

/**
 * One turn's ledger accounting, or null when this turn needs none.
 *
 * `"declined"` means the config carries a spend ceiling but the deps cannot reserve (an old
 * Worker/DO pairing): the turn falls back to the container, which holds the same authority
 * through the replica journal. Failing open is exactly the bug the durable ledger exists to
 * close, so missing deps under a configured ceiling is a decline, never a skip.
 */
export function workerSpendTurn(
  config: unknown,
  deps: NativeChatDeps,
  scopes: { rootId?: string; identityId?: string; poolId?: string },
): WorkerSpendTurn | "declined" | null {
  const policy = spendPolicyFromConfig(
    config && typeof config === "object" && "spend" in config
      ? (config as { spend?: OcxSpendConfig }).spend
      : undefined);
  if (!spendCeilingsConfigured(policy)) return null;
  const ops = opsFor(deps);
  if (!ops) return "declined";
  // Every send this turn still owes the ledger an answer for, oldest first — the same shape
  // createRequestSpendTracker keeps.
  const live: string[] = [];
  let resolved = false;
  let refusals = 0;
  /** A later reservation proves the earlier sends left; only they are marked dispatched. */
  const confirmOlderSends = (): void => {
    for (let index = 0; index < live.length - 1; index += 1) ops.spendMarkDispatched(live[index] as string);
  };
  return {
    async charge(inputTokens: number, outputCeilingTokens: number): Promise<WorkerSpendCharge> {
      const sendId = crypto.randomUUID();
      let decision: SpendReservationDecision;
      try {
        decision = await ops.spendReserve({
          sendId,
          scopes,
          inputTokens,
          outputCeilingTokens,
        });
      } catch {
        // The hub could not answer at all. Under a configured ceiling that is the same case
        // as a reservation that could not be journaled: refuse, because admitting would book
        // spend the authority never saw.
        refusals += 1;
        return { ok: false, refusal: workerSpendRefusalResponse(undefined) };
      }
      if (!decision.reserved) {
        refusals += 1;
        const denial = decision.denial;
        if (denial.reason === "reserve-not-durable" || denial.reason === "journal-corrupt") {
          return { ok: false, refusal: workerSpendRefusalResponse(denial) };
        }
        if (denial.reason !== "spend-limit-exceeded") {
          // Capacity and a duplicate send id stay permissive: the ledger cannot account for
          // this send, which is a degradation to report, not an outage to cause.
          return { ok: true };
        }
        return { ok: false, refusal: workerSpendRefusalResponse(denial) };
      }
      live.push(sendId);
      confirmOlderSends();
      return { ok: true };
    },
    resolve(usage: SpendUsage | undefined): void {
      if (resolved) return;
      resolved = true;
      // The terminal usage belongs to the last send that left; every earlier send failed
      // without reporting usage of its own and may still have been billed.
      const terminal = live.pop();
      if (terminal !== undefined) {
        if (usage !== undefined) ops.spendSettle(terminal, usage);
        else ops.spendMarkLost(terminal);
      }
      while (live.length > 0) ops.spendMarkLost(live.pop() as string);
    },
    /** Release the newest reservation for a send that never reached the wire. */
    get refusals(): number { return refusals; },
  };
}
