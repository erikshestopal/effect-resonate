import { BunRuntime } from "@effect/platform-bun";
import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunHttpClient from "@effect/platform-bun/BunHttpClient";
import { Config, Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

export const repo = "example-event-sourcing-ts";
export const functionName = "processEventStream";
export const sampleArgs = [
  { userId: "user-1", events: [{ eventId: "event-1", type: "created", payload: { name: "Ada" } }] },
] as const;
// Invoke after starting this worker: resonate invoke --server http://127.0.0.1:8001 --target poll://any@example-event-sourcing-ts --func processEventStream --json-args '[{"userId":"user-1","events":[{"eventId":"event-1","type":"created","payload":{"name":"Ada"}}]}]' example-event-sourcing-ts-demo

const Payload = Schema.Struct({
  userId: Schema.String,
  events: Schema.Array(
    Schema.Struct({
      eventId: Schema.String,
      type: Schema.String,
      payload: Schema.Record(Schema.String, Schema.Unknown),
    }),
  ),
});
const workflow = Resonate.function({ name: functionName, payload: Payload });
const App = Resonate.group(workflow);

const handlers = App.toLayer(
  App.of({
    [functionName]: (input) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const results: Array<unknown> = [];
        results.push(
          yield* ctx.run({
            effect: Effect.logInfo(`processed ${input.events.length} events for ${input.userId}`).pipe(
              Effect.as(`processed ${input.events.length} events for ${input.userId}`),
            ),
          }),
        );
        yield* ctx.sleep({ for: Duration.millis(1) });
        return { repo, functionName, results };
      }),
  }),
);

const worker = Layer.unwrap(
  Effect.gen(function* () {
    const url = yield* Config.string("RESONATE_URL").pipe(Config.withDefault("http://127.0.0.1:8001"));
    const groupName = yield* Config.string("RESONATE_GROUP").pipe(Config.withDefault("example-event-sourcing-ts"));
    const pidName = yield* Config.string("RESONATE_PID").pipe(Config.withDefault("example-event-sourcing-ts-worker"));
    const group = Protocol.WorkerGroup.make(groupName);
    const pid = Protocol.ProcessId.make(pidName);
    return Worker.layerHttp({ group: App, http: { url, group, pid, ttl: Duration.seconds(5) } }).pipe(
      Layer.provideMerge(handlers),
      Layer.provideMerge(BunHttpClient.layer),
      Layer.provideMerge(BunCrypto.layer),
    );
  }),
);

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(worker));
}
