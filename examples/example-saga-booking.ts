import { BunRuntime } from "@effect/platform-bun";
import { Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

export const repo = "example-saga-booking-ts";
export const functionName = "bookTrip";
export const sampleArgs = [{ tripId: "trip-1", shouldFail: false }] as const;

const url = process.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Protocol.WorkerGroup.make(process.env.RESONATE_GROUP ?? "example-saga-booking-ts");
const pid = Protocol.ProcessId.make(process.env.RESONATE_PID ?? "example-saga-booking-ts-worker");

const Payload = Schema.Struct({ tripId: Schema.String, shouldFail: Schema.Boolean });
const workflow = Resonate.function(functionName, { payload: Payload });
const App = Resonate.group(workflow);

const handlers = App.toLayer(
  App.of({
    [functionName]: (input) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const results: Array<unknown> = [];
        results.push(
          yield* ctx.run(Effect.logInfo(`flight-${input.tripId}`).pipe(Effect.as(`flight-${input.tripId}`))),
        );
        results.push(yield* ctx.run(Effect.logInfo(`hotel-${input.tripId}`).pipe(Effect.as(`hotel-${input.tripId}`))));
        results.push(yield* ctx.run(Effect.logInfo(`car-${input.tripId}`).pipe(Effect.as(`car-${input.tripId}`))));
        yield* ctx.sleep(Duration.millis(1));
        return { repo, functionName, results };
      }),
  }),
);

const worker = Worker.layerHttp(App, { url, group, pid, ttl: Duration.seconds(5) }).pipe(Layer.provideMerge(handlers));

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(worker));
}
