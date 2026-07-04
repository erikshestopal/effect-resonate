import { BunRuntime } from "@effect/platform-bun";
import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunHttpClient from "@effect/platform-bun/BunHttpClient";
import { Config, Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate } from "effect-resonate";

export const repo = "example-ecommerce-application-ts";
export const functionName = "checkout";
export const sampleArgs = [{ orderId: "order-1", userId: "user-1", itemId: "item-1" }] as const;
// Invoke after starting this worker: resonate invoke --server http://127.0.0.1:8001 --target poll://any@example-ecommerce-application-ts --func checkout --json-args '[{"orderId":"order-1","userId":"user-1","itemId":"item-1"}]' example-ecommerce-application-ts-demo

const Payload = Schema.Struct({ orderId: Schema.String, userId: Schema.String, itemId: Schema.String });
const workflow = Resonate.function({ name: functionName, payload: Payload });
const App = Resonate.group(workflow);

const handlers = App.toLayer(
  App.of({
    [functionName]: (input) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, Resonate.Context> {
        const ctx = yield* Resonate.Context;
        const results: Array<unknown> = [];
        results.push(
          yield* ctx.run({
            effect: Effect.logInfo(`checkout ${input.orderId}`).pipe(Effect.as(`checkout ${input.orderId}`)),
          }),
        );
        results.push(
          yield* ctx.run({
            effect: Effect.logInfo(`charge ${input.userId}`).pipe(Effect.as(`charge ${input.userId}`)),
          }),
        );
        results.push(
          yield* ctx.run({ effect: Effect.logInfo(`ship ${input.itemId}`).pipe(Effect.as(`ship ${input.itemId}`)) }),
        );
        yield* ctx.sleep({ for: Duration.millis(1) });
        return { repo, functionName, results };
      }),
  }),
);

const worker = Layer.unwrap(
  Effect.gen(function* () {
    const url = yield* Config.string("RESONATE_URL").pipe(Config.withDefault("http://127.0.0.1:8001"));
    const groupName = yield* Config.string("RESONATE_GROUP").pipe(
      Config.withDefault("example-ecommerce-application-ts"),
    );
    const pidName = yield* Config.string("RESONATE_PID").pipe(
      Config.withDefault("example-ecommerce-application-ts-worker"),
    );
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
