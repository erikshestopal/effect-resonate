import { Effect, Schema } from "effect";
export const ChatMessage = Schema.Struct({ role: Schema.Literals(["user", "assistant"]), content: Schema.String });
export class LlmError extends Schema.TaggedErrorClass<LlmError>()("LlmError", { turnKey: Schema.String }) {}
export const callClaude = (history: ReadonlyArray<typeof ChatMessage.Type>, turnKey: string, isCrashTurn: boolean) =>
  Effect.gen(function* () {
    yield* Effect.logInfo(`[llm] ${turnKey}`);
    if (isCrashTurn) return yield* new LlmError({ turnKey });
    const last = history.at(-1)?.content ?? "hello";
    return `Deterministic assistant response to: ${last}`;
  });
