import { Duration, Effect, Schema } from "effect";
import { Resonate, ResonateContext } from "effect-resonate";

export const repo = "example-durable-sleep-ts";
export const functionName = "sleepingWorkflow";
export const sampleArgs = [1] as const;

export const SleepPayload = Schema.Finite;
export const workflow = Resonate.function({ name: functionName, payload: SleepPayload });
export const App = Resonate.group(workflow);

export const handlers = App.toLayer(
  App.of({
    [functionName]: (ms) =>
      Effect.gen(function* (): Effect.fn.Return<string, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        yield* ctx.run({ effect: Effect.logInfo(`Sleeping for ${ms / 1000} seconds...`) });
        yield* ctx.sleep({ for: Duration.millis(ms) });
        return `Slept for ${ms / 1000} seconds`;
      }),
  }),
);
