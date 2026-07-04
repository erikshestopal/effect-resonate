import { Effect, Schema, SchemaParser } from "effect";
import { Resonate, ResonateContext } from "effect-resonate";
import { BatchResult, ImportRecord, generateRecords, processBatchChunk } from "./processor.ts";
export const repo = "example-batch-processor-ts";
export const functionName = "importRecords";
export const sampleArgs = [{ records: generateRecords(4), batchSize: 2 }] as const;
export const Payload = Schema.Struct({ records: Schema.Array(ImportRecord), batchSize: Schema.Finite });
export const ProcessingResult = Schema.Struct({
  totalRecords: Schema.Finite,
  totalProcessed: Schema.Finite,
  totalSkipped: Schema.Finite,
  batchCount: Schema.Finite,
  batches: Schema.Array(BatchResult),
});
export const workflow = Resonate.function({ name: functionName, payload: Payload });
export const App = Resonate.group(workflow);
export const handlers = App.toLayer(
  App.of({
    [functionName]: (input) =>
      Effect.gen(function* (): Effect.fn.Return<
        typeof ProcessingResult.Type,
        unknown,
        ResonateContext.ResonateContext
      > {
        const ctx = yield* ResonateContext.ResonateContext;
        const batches: Array<typeof BatchResult.Type> = [];
        for (let index = 0; index < input.records.length; index = index + input.batchSize) {
          const batchIndex = batches.length;
          const batch = input.records.slice(index, index + input.batchSize);
          batches.push(
            yield* ctx
              .run({ name: `batch-${batchIndex}`, effect: processBatchChunk(batchIndex, batch) })
              .pipe(Effect.flatMap(SchemaParser.decodeUnknownEffect(BatchResult))),
          );
        }
        return ProcessingResult.make({
          totalRecords: input.records.length,
          totalProcessed: batches.reduce((sum, batch) => sum + batch.processed, 0),
          totalSkipped: batches.reduce((sum, batch) => sum + batch.skipped, 0),
          batchCount: batches.length,
          batches,
        });
      }),
  }),
);
