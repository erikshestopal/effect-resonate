import { Duration, Effect, Schema } from "effect";
export const ImportRecord = Schema.Struct({
  id: Schema.String,
  name: Schema.optional(Schema.String),
  email: Schema.optional(Schema.String),
  value: Schema.Finite,
});
export type ImportRecord = typeof ImportRecord.Type;
export const BatchResult = Schema.Struct({
  batchIndex: Schema.Finite,
  processed: Schema.Finite,
  skipped: Schema.Finite,
  durationMs: Schema.Finite,
});
export const generateRecords = (count: number): ReadonlyArray<ImportRecord> =>
  Array.from({ length: count }, (_, index) =>
    ImportRecord.make({
      id: `rec-${index + 1}`,
      name: `User ${index + 1}`,
      email: `user${index + 1}@example.com`,
      value: index + 1,
    }),
  );
export const processBatchChunk = Effect.fn("BatchProcessor.processBatchChunk")(function* (
  batchIndex: number,
  records: ReadonlyArray<ImportRecord>,
) {
  yield* Effect.logInfo(`[batch ${batchIndex}] processing ${records.length}`);
  yield* Effect.sleep(Duration.millis(15));
  const skipped = records.filter((record) => record.value <= 0).length;
  return BatchResult.make({ batchIndex, processed: records.length - skipped, skipped, durationMs: 15 });
});
