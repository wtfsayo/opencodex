// Where a Responses body carries the <skills_instructions> catalog, shared by the proxy's session
// snapshot (skills-snapshot.ts) and the Cloudflare Worker's (cloudflare-native-responses.ts).
// Import-free so the Worker can bundle it.

const SKILLS_BLOCK_GLOBAL_REGEX = /<skills_instructions>([\s\S]*?)<\/skills_instructions>/g;

/** One text slot that may carry a catalog: `instructions` or a developer/system text part. */
interface CatalogSlot {
  text: string;
  write(next: string): void;
}

/** Every developer/system text slot, walked the same way the replacement writes. */
function catalogSlots(body: Record<string, unknown>): CatalogSlot[] {
  const slots: CatalogSlot[] = [];
  if (typeof body.instructions === "string") {
    slots.push({ text: body.instructions, write: next => { body.instructions = next; } });
  }
  if (!Array.isArray(body.input)) return slots;
  for (const item of body.input) {
    if (!item || typeof item !== "object") continue;
    const it = item as Record<string, unknown>;
    // Restrict message item type: must be undefined or "message", so role-like tool objects are untouched
    if (it.type !== undefined && it.type !== "message") continue;
    // Only developer and system content is inspected/transformed
    if (it.role !== "developer" && it.role !== "system") continue;
    const content = it.content;
    if (typeof content === "string") {
      slots.push({ text: content, write: next => { it.content = next; } });
    } else if (Array.isArray(content)) {
      for (const part of content) {
        if (!part || typeof part !== "object") continue;
        const p = part as Record<string, unknown>;
        // Restrict text parts to known text / input_text
        if (p.type !== "text" && p.type !== "input_text") continue;
        if (typeof p.text === "string") slots.push({ text: p.text, write: next => { p.text = next; } });
      }
    }
  }
  return slots;
}

export interface SkillsBlockLocation {
  slot: CatalogSlot;
  block: string;
}

/**
 * The body's one catalog block across `instructions` and developer/system text, or undefined when
 * there is none or more than one: with two there is no way to tell which a snapshot stands for.
 */
export function singleSkillsBlock(body: Record<string, unknown>): SkillsBlockLocation | undefined {
  let found: SkillsBlockLocation | undefined;
  let blocks = 0;
  for (const slot of catalogSlots(body)) {
    if (!slot.text.includes("<skills_instructions>")) continue;
    for (const match of slot.text.matchAll(SKILLS_BLOCK_GLOBAL_REGEX)) {
      blocks++;
      found ??= { slot, block: match[0] };
    }
  }
  return blocks === 1 ? found : undefined;
}

/** Puts `replacement` where the located block was. */
export function replaceSkillsBlock(found: SkillsBlockLocation, replacement: string): void {
  const { slot, block } = found;
  const at = slot.text.indexOf(block);
  slot.write(slot.text.slice(0, at) + replacement + slot.text.slice(at + block.length));
}
