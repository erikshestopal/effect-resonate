import { Effect, Schema } from "effect";
import { CONSUMER_GROUP_ID, TOPIC_TO_DELETE } from "./kafka-config.ts";
export const KafkaRecord = Schema.Struct({ recordId: Schema.String, offset: Schema.String, topic: Schema.String });
export const consumeRecord = Effect.fn("KafkaWorker.consumeRecord")(function* (record: typeof KafkaRecord.Type) {
  yield* Effect.logInfo(`[consumer:${CONSUMER_GROUP_ID}] ${record.topic} ${record.recordId}@${record.offset}`);
  return record;
});
export const sampleRecord = KafkaRecord.make({ recordId: "record-1", offset: "1", topic: TOPIC_TO_DELETE });
