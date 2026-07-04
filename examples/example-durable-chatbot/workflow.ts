import { Duration, Effect, Schema, SchemaParser } from "effect";
import { Resonate } from "effect-resonate";
import { callClaude, ChatMessage } from "./llm.ts";
export { ChatMessage } from "./llm.ts";
export const repo = "example-durable-chatbot-ts";
export const functionName = "processTurn";
export const sampleArgs = [{ history: ["hello"], turnKey: "turn-1", isCrashTurn: false }] as const;
export const Payload = Schema.Struct({
  history: Schema.Array(Schema.String),
  turnKey: Schema.String,
  isCrashTurn: Schema.Boolean,
});
export const ChatTurn = Schema.Struct({
  turnKey: Schema.String,
  history: Schema.Array(ChatMessage),
  response: Schema.String,
});
export const workflow = Resonate.function({ name: functionName, payload: Payload });
export const App = Resonate.group(workflow);
export const handlers = App.toLayer(
  App.of({
    [functionName]: (input) =>
      Effect.gen(function* (): Effect.fn.Return<typeof ChatTurn.Type, unknown, Resonate.Context> {
        const ctx = yield* Resonate.Context;
        const history = input.history.map((content) => ChatMessage.make({ role: "user", content }));
        const response = yield* ctx
          .run({
            name: `llm-${input.turnKey}`,
            effect: callClaude(history, input.turnKey, input.isCrashTurn),
          })
          .pipe(Effect.flatMap(SchemaParser.decodeUnknownEffect(Schema.String)));
        yield* ctx.sleep({ for: Duration.millis(1) });
        return ChatTurn.make({
          turnKey: input.turnKey,
          history: [...history, ChatMessage.make({ role: "assistant", content: response })],
          response,
        });
      }),
  }),
);
