import { BunRuntime } from "@effect/platform-bun";
import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunHttpClient from "@effect/platform-bun/BunHttpClient";
import { Config, Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate } from "effect-resonate";

export const repo = "example-openai-deep-research-agent-supabase-ts";
export const functionName = "research";
export const sampleArgs = ["durable execution", 1] as const;
// Invoke after starting this worker: resonate invoke --server http://127.0.0.1:8001 --target poll://any@example-openai-deep-research-agent-supabase-ts --func research --json-args '["durable execution",1]' example-openai-deep-research-agent-supabase-ts-demo

const Payload = Schema.Tuple([Schema.String, Schema.Finite]);
const workflow = Resonate.function({ name: functionName, payload: Payload });
const App = Resonate.group(workflow);

const handlers = App.toLayer(
  App.of({
    [functionName]: (topic, depth) =>
      Effect.gen(function* () {
        const ctx = yield* Resonate.Context;
        yield* Config.string("OPENAI_API_KEY").pipe(Config.withDefault(""));
        yield* ctx.run({ effect: Effect.logInfo(`prompted research model for ${topic}`) });
        const summaries: Array<string> = [];
        if (depth > 0) {
          for (const subtopic of [`${topic} fundamentals`, `${topic} operations`]) {
            yield* ctx.run({ effect: Effect.logInfo(`research subtopic ${subtopic}`) });
            summaries.push(`researched ${subtopic}`);
          }
        }
        return summaries.length > 0 ? summaries.join("\n") : `Summary for ${topic}`;
      }),
  }),
);

const worker = Layer.unwrap(
  Effect.gen(function* () {
    const url = yield* Config.string("RESONATE_URL").pipe(Config.withDefault("http://127.0.0.1:8001"));
    const groupName = yield* Config.string("RESONATE_GROUP").pipe(Config.withDefault(repo));
    const pidName = yield* Config.string("RESONATE_PID").pipe(Config.withDefault(`${repo}-worker`));
    const group = Protocol.WorkerGroup.make(groupName);
    const pid = Protocol.ProcessId.make(pidName);
    return Resonate.Worker.layerHttp({ group: App, http: { url, group, pid, ttl: Duration.seconds(30) } }).pipe(
      Layer.provideMerge(handlers),
      Layer.provideMerge(BunHttpClient.layer),
      Layer.provideMerge(BunCrypto.layer),
    );
  }),
);

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(worker));
}
