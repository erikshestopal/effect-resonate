import { BunRuntime } from "@effect/platform-bun";
import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunHttpClient from "@effect/platform-bun/BunHttpClient";
import { Config, Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate } from "effect-resonate";

export const repo = "example-fan-out-fan-in-ts";
export const functionName = "notifyAll";
export const sampleArgs = [
  { orderId: "order-1", userId: "user-1", event: "created", message: "order created" },
] as const;
// Invoke after starting this worker: resonate invoke --server http://127.0.0.1:8001 --target poll://any@example-fan-out-fan-in-ts --func notifyAll --json-args '[{"orderId":"order-1","userId":"user-1","event":"created","message":"order created"}]' example-fan-out-fan-in-ts-demo

const Payload = Schema.Struct({
  orderId: Schema.String,
  userId: Schema.String,
  event: Schema.String,
  message: Schema.String,
});
const workflow = Resonate.function({ name: functionName, payload: Payload });
const App = Resonate.group(workflow);

const handlers = App.toLayer(
  App.of({
    [functionName]: (input) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, Resonate.Context> {
        const ctx = yield* Resonate.Context;
        const results: Array<unknown> = [];
        results.push(
          yield* ctx.run({ effect: Effect.logInfo(`email ${input.userId}`).pipe(Effect.as(`email ${input.userId}`)) }),
        );
        results.push(
          yield* ctx.run({ effect: Effect.logInfo(`sms ${input.userId}`).pipe(Effect.as(`sms ${input.userId}`)) }),
        );
        results.push(
          yield* ctx.run({
            effect: Effect.logInfo(`slack ${input.orderId}`).pipe(Effect.as(`slack ${input.orderId}`)),
          }),
        );
        results.push(
          yield* ctx.run({ effect: Effect.logInfo(`push ${input.orderId}`).pipe(Effect.as(`push ${input.orderId}`)) }),
        );
        yield* ctx.sleep({ for: Duration.millis(1) });
        return { repo, functionName, results };
      }),
  }),
);

const worker = Layer.unwrap(
  Effect.gen(function* () {
    const url = yield* Config.string("RESONATE_URL").pipe(Config.withDefault("http://127.0.0.1:8001"));
    const groupName = yield* Config.string("RESONATE_GROUP").pipe(Config.withDefault("example-fan-out-fan-in-ts"));
    const pidName = yield* Config.string("RESONATE_PID").pipe(Config.withDefault("example-fan-out-fan-in-ts-worker"));
    const group = Protocol.WorkerGroup.make(groupName);
    const pid = Protocol.ProcessId.make(pidName);
    return Resonate.Worker.layerHttp({ group: App, http: { url, group, pid, ttl: Duration.seconds(5) } }).pipe(
      Layer.provideMerge(handlers),
      Layer.provideMerge(BunHttpClient.layer),
      Layer.provideMerge(BunCrypto.layer),
    );
  }),
);

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(worker));
}
