import { BunRuntime } from "@effect/platform-bun";
import { Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

export const repo = "example-durable-entity-ts";
export const functionName = "sessionLifecycle";
export const sampleArgs = [
  { sessionId: "session-1", userId: "user-1", activities: ["click"], idleTimeoutMs: 1 },
] as const;

const url = process.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Protocol.WorkerGroup.make(process.env.RESONATE_GROUP ?? "example-durable-entity-ts");
const pid = Protocol.ProcessId.make(process.env.RESONATE_PID ?? "example-durable-entity-ts-worker");

const Payload = Schema.Struct({
  sessionId: Schema.String,
  userId: Schema.String,
  activities: Schema.Array(Schema.String),
  idleTimeoutMs: Schema.Number,
});
const workflow = Resonate.function(functionName, { payload: Payload });
const App = Resonate.group(workflow);

const handlers = App.toLayer(
  App.of({
    [functionName]: (input) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const results: Array<unknown> = [];
        results.push(
          yield* ctx.run(Effect.logInfo(`login ${input.sessionId}`).pipe(Effect.as(`login ${input.sessionId}`))),
        );
        results.push(
          yield* ctx.run(
            Effect.logInfo(`activity ${input.activities.join(",")}`).pipe(
              Effect.as(`activity ${input.activities.join(",")}`),
            ),
          ),
        );
        results.push(
          yield* ctx.run(Effect.logInfo(`expired ${input.sessionId}`).pipe(Effect.as(`expired ${input.sessionId}`))),
        );
        yield* ctx.sleep(Duration.millis(1));
        return { repo, functionName, results };
      }),
  }),
);

const worker = Worker.layerHttp(App, { url, group, pid, ttl: Duration.seconds(5) }).pipe(Layer.provideMerge(handlers));

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(worker));
}
