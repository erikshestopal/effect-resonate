import { Effect } from "effect";
import { TOPIC_TO_DELETE } from "./kafka-config.ts";
export const produceRecords = Effect.fn("KafkaWorker.produceRecords")(function* (count: number) {
  const records = Array.from({ length: count }, (_, index) => `record-${index + 1}`);
  yield* Effect.logInfo(`produced ${records.length} records to ${TOPIC_TO_DELETE}`);
  return records;
});
