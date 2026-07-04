import { Duration, Effect, Schema, SchemaParser } from "effect";
import { Resonate, ResonateContext } from "effect-resonate";
export const repo = "example-distributed-mutex-ts";
export const functionName = "exclusiveResourceAccess";
export const sampleArgs = [
  { resource: "payment-gateway", workers: ["worker-A", "worker-B", "worker-C"], shouldCrash: false },
] as const;
export const Payload = Schema.Struct({
  resource: Schema.String,
  workers: Schema.Array(Schema.String),
  shouldCrash: Schema.Boolean,
});
export const WorkResult = Schema.Struct({ workerId: Schema.String, action: Schema.String, duration: Schema.Finite });
export const MutexResult = Schema.Struct({
  resource: Schema.String,
  processed: Schema.Array(WorkResult),
  totalMs: Schema.Finite,
});
export const accessResource = Effect.fn("DistributedMutex.accessResource")(function* (
  resource: string,
  workerId: string,
) {
  yield* Effect.logInfo(`[${workerId}] acquired ${resource}`);
  yield* Effect.sleep(Duration.millis(10));
  return WorkResult.make({ workerId, action: `updated-${resource}`, duration: 10 });
});
export const workflow = Resonate.function({ name: functionName, payload: Payload });
export const App = Resonate.group(workflow);
export const handlers = App.toLayer(
  App.of({
    [functionName]: (input) =>
      Effect.gen(function* (): Effect.fn.Return<typeof MutexResult.Type, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const processed: Array<typeof WorkResult.Type> = [];
        for (let index = 0; index < input.workers.length; index = index + 1) {
          const workerId = input.workers[index]!;
          processed.push(
            yield* ctx
              .run({ name: `access-${input.resource}-${workerId}`, effect: accessResource(input.resource, workerId) })
              .pipe(Effect.flatMap(SchemaParser.decodeUnknownEffect(WorkResult))),
          );
        }
        return MutexResult.make({
          resource: input.resource,
          processed,
          totalMs: processed.reduce((sum, item) => sum + item.duration, 0),
        });
      }),
  }),
);
