import { Duration, Effect, Schema } from "effect";
import { Resonate } from "effect-resonate";
import { TOPIC_DELETED } from "./kafka-config.ts";
export const repo = "example-kafka-worker-ts";
export const functionName = "workflow";
export const sampleArgs = [{ recordId: "record-1", offset: "1" }] as const;
export const Payload = Schema.Struct({ recordId: Schema.String, offset: Schema.String });
export const WorkflowResult = Schema.Struct({
  recordId: Schema.String,
  offset: Schema.String,
  deletedBatches: Schema.Finite,
  completionTopic: Schema.String,
});
export const deleteBatch = Effect.fn("KafkaWorker.deleteBatch")(function* (recordId: string, batch: number) {
  yield* Effect.logInfo(`deleting batch ${batch} for ${recordId}`);
  yield* Effect.sleep(Duration.millis(10));
  return batch < 2;
});
export const enqueueCompletion = Effect.fn("KafkaWorker.enqueueCompletion")(function* (
  recordId: string,
  offset: string,
) {
  yield* Effect.logInfo(`publish completion for ${recordId}@${offset} to ${TOPIC_DELETED}`);
});
export const workflow = Resonate.function({ name: functionName, payload: Payload });
export const App = Resonate.group(workflow);
export const handlers = App.toLayer(
  App.of({
    [functionName]: (input) =>
      Effect.gen(function* (): Effect.fn.Return<typeof WorkflowResult.Type, unknown, Resonate.Context> {
        const ctx = yield* Resonate.Context;
        let batch = 1;
        let keepGoing = true;
        while (keepGoing) {
          keepGoing = Boolean(
            yield* ctx.run({ name: `delete-${input.recordId}-${batch}`, effect: deleteBatch(input.recordId, batch) }),
          );
          if (keepGoing) yield* ctx.sleep({ for: Duration.millis(10) });
          batch = batch + 1;
        }
        yield* ctx.run({ name: `complete-${input.recordId}`, effect: enqueueCompletion(input.recordId, input.offset) });
        return WorkflowResult.make({
          recordId: input.recordId,
          offset: input.offset,
          deletedBatches: batch - 1,
          completionTopic: TOPIC_DELETED,
        });
      }),
  }),
);
