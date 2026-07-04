import { Effect } from "effect";
import { TOPIC_DELETED, TOPIC_TO_DELETE } from "./kafka-config.ts";
export const setupTopics = Effect.fn("KafkaWorker.setupTopics")(function* () {
  yield* Effect.logInfo(`topics ready: ${TOPIC_TO_DELETE}, ${TOPIC_DELETED}`);
  return [TOPIC_TO_DELETE, TOPIC_DELETED] as const;
});
