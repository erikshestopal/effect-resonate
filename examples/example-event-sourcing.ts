import { BunRuntime } from "@effect/platform-bun";
import { Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

export const repo = "example-event-sourcing-ts";
export const functionName = "processEventStream";
export const sampleArgs = [
  { userId: "user-1", events: [{ eventId: "event-1", type: "created", payload: { name: "Ada" } }] },
] as const;

const url = process.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Protocol.WorkerGroup.make(process.env.RESONATE_GROUP ?? "example-event-sourcing-ts");
const pid = Protocol.ProcessId.make(process.env.RESONATE_PID ?? "example-event-sourcing-ts-worker");

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
const workflow = Resonate.function(functionName, { payload: Payload });
const App = Resonate.group(workflow);

const handlers = App.toLayer(
  App.of({
    [functionName]: (input) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const results: Array<unknown> = [];
        results.push(
          yield* ctx.run(
            Effect.logInfo(`processed ${input.events.length} events for ${input.userId}`).pipe(
              Effect.as(`processed ${input.events.length} events for ${input.userId}`),
            ),
          ),
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
