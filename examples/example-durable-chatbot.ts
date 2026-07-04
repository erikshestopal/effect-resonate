import { BunRuntime } from "@effect/platform-bun";
import { Layer } from "effect";
import { functionName, repo, sampleArgs, worker } from "./example-durable-chatbot/index.ts";
export { functionName, repo, sampleArgs };
if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(worker));
}
