import { BunRuntime } from "@effect/platform-bun";
import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunHttpClient from "@effect/platform-bun/BunHttpClient";
import { Config, Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate } from "effect-resonate";

const OrderEvent = Schema.Struct({
  orderId: Schema.String,
  email: Schema.String,
  phone: Schema.String,
});

const notifyAll = Resonate.function({ name: "notifyAll", payload: OrderEvent });

// Invoke after starting this worker: resonate invoke --server http://127.0.0.1:8001 --target poll://any@default --func notifyAll --json-args '[{"orderId":"order-1","email":"ada@example.com","phone":"+15550100"}]' fanout-demo
const App = Resonate.group(notifyAll);

const handlers = App.toLayer(
  App.of({
    notifyAll: (event) =>
      Effect.gen(function* (): Effect.fn.Return<ReadonlyArray<unknown>, unknown, Resonate.Context> {
        const ctx = yield* Resonate.Context;
        const email = yield* ctx.beginRun({
          effect: Effect.logInfo(`email:${event.email}`).pipe(
            Effect.as({ channel: "email", destination: event.email, ok: true }),
          ),
        });
        const sms = yield* ctx.beginRun({
          effect: Effect.logInfo(`sms:${event.phone}`).pipe(
            Effect.as({ channel: "sms", destination: event.phone, ok: true }),
          ),
        });
        const slack = yield* ctx.beginRun({
          effect: Effect.logInfo(`slack:${event.orderId}`).pipe(
            Effect.as({ channel: "slack", destination: event.orderId, ok: true }),
          ),
        });
        const push = yield* ctx.beginRun({
          effect: Effect.logInfo(`push:${event.orderId}`).pipe(
            Effect.as({ channel: "push", destination: event.orderId, ok: true }),
          ),
        });
        return yield* ctx.all([email.await, sms.await, slack.await, push.await]);
      }),
  }),
);

const worker = Layer.unwrap(
  Effect.gen(function* () {
    const url = yield* Config.string("RESONATE_URL").pipe(Config.withDefault("http://127.0.0.1:8001"));
    const groupName = yield* Config.string("RESONATE_GROUP").pipe(Config.withDefault("default"));
    const pidName = yield* Config.string("RESONATE_PID").pipe(Config.withDefault("fanout-worker"));
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
