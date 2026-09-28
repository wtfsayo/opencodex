// Pre-dispatch input admission (input-admission-core.ts) bound to the native OpenAI provider's
// static limits from the Codex catalog.
import {
  nativeOpenAiContextWindow,
  nativeOpenAiMaxInputTokens,
  nativeOpenAiMaxOutputTokens,
} from "../../codex/catalog/metadata";
import { createInputAdmission } from "./input-admission-core";

export { ADMISSION_TOLERANCE, estimateInputTokens, type InputAdmissionResult } from "./input-admission-core";

export const {
  resolveInputCeiling,
  resolveOutputCeiling,
  checkComboTargetInputAdmission,
  checkInputAdmission,
} = createInputAdmission({
  contextWindow: nativeOpenAiContextWindow,
  maxInputTokens: nativeOpenAiMaxInputTokens,
  maxOutputTokens: nativeOpenAiMaxOutputTokens,
});
