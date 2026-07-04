import { BunRuntime } from "@effect/platform-bun";
import { Layer } from "effect";
import { functionName, repo, sampleArgs, worker } from "./example-ai-image-pipeline/index.ts";

export { functionName, repo, sampleArgs };

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(worker));
}
