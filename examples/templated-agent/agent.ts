import { Duration, Effect, SchemaParser } from "effect";
import { Resonate, ResonateContext } from "effect-resonate";
import { calculate, lookup } from "./tools.ts";
import { AgentRequest, AgentResponse, ToolCall } from "./types.ts";
export const repo = "templated-agent-ts";
export const functionName = "agent";
export const sampleArgs = [{ prompt: "hello" }] as const;
export const workflow = Resonate.function({ name: functionName, payload: AgentRequest });
export const App = Resonate.group(workflow);
export const handlers = App.toLayer(
  App.of({
    [functionName]: (input) =>
      Effect.gen(function* (): Effect.fn.Return<typeof AgentResponse.Type, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const facts = yield* ctx
          .run({ name: "lookup", effect: lookup(input.prompt) })
          .pipe(Effect.flatMap(SchemaParser.decodeUnknownEffect(ToolCall)));
        const size = yield* ctx
          .run({ name: "calculate", effect: calculate(input.prompt) })
          .pipe(Effect.flatMap(SchemaParser.decodeUnknownEffect(ToolCall)));
        yield* ctx.sleep({ for: Duration.millis(1) });
        return AgentResponse.make({
          prompt: input.prompt,
          tools: [facts, size],
          answer: `${facts.output}; prompt length ${size.output}`,
        });
      }),
  }),
);
