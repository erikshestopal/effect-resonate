import { BunRuntime } from "@effect/platform-bun";
import { Layer } from "effect";
import { functionName, repo, sampleArgs, worker } from "./example-multi-agent-orchestration/index.ts";

export { functionName, repo, sampleArgs };

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(worker));
}
