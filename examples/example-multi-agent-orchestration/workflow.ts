import { Duration, Effect, Schema, SchemaParser } from "effect";
import { Resonate } from "effect-resonate";
import { AgentOutput, researcher, reviewer, writer } from "./agents.ts";
export const repo = "example-multi-agent-orchestration-ts";
export const functionName = "orchestrate";
export const sampleArgs = [{ topic: "resonate", crashOnWriter: false }] as const;
export const Payload = Schema.Struct({ topic: Schema.String, crashOnWriter: Schema.Boolean });
export const OrchestrationResult = Schema.Struct({
  topic: Schema.String,
  research: AgentOutput,
  draft: AgentOutput,
  review: AgentOutput,
});
export const workflow = Resonate.function({ name: functionName, payload: Payload });
export const App = Resonate.group(workflow);
export const handlers = App.toLayer(
  App.of({
    [functionName]: (input) =>
      Effect.gen(function* (): Effect.fn.Return<typeof OrchestrationResult.Type, unknown, Resonate.Context> {
        const ctx = yield* Resonate.Context;
        const research = yield* ctx
          .run({ name: "researcher", effect: researcher(input.topic) })
          .pipe(Effect.flatMap(SchemaParser.decodeUnknownEffect(AgentOutput)));
        const draft = yield* ctx
          .run({ name: "writer", effect: writer(input.topic, research.content, input.crashOnWriter) })
          .pipe(Effect.flatMap(SchemaParser.decodeUnknownEffect(AgentOutput)));
        const review = yield* ctx
          .run({ name: "reviewer", effect: reviewer(draft.content) })
          .pipe(Effect.flatMap(SchemaParser.decodeUnknownEffect(AgentOutput)));
        yield* ctx.sleep({ for: Duration.millis(1) });
        return OrchestrationResult.make({ topic: input.topic, research, draft, review });
      }),
  }),
);
