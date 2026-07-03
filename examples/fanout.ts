import { BunRuntime } from "@effect/platform-bun";
import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunHttpClient from "@effect/platform-bun/BunHttpClient";
import { Duration, Effect, Layer, Schema } from "effect";
import {
  Codec,
  DurablePromise,
  Protocol,
  Resonate,
  ResonateContext,
  ResonateSchedule,
  Task,
  Worker,
} from "effect-resonate";

const url = process.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Protocol.WorkerGroup.make(process.env.RESONATE_GROUP ?? "default");
const pid = Protocol.ProcessId.make(process.env.RESONATE_PID ?? "fanout-worker");

const OrderEvent = Schema.Struct({
  orderId: Schema.String,
  email: Schema.String,
  phone: Schema.String,
});

const notifyAll = Resonate.function("notifyAll", {
  payload: OrderEvent,
});

const App = Resonate.group(notifyAll);

const send = (channel: string, destination: string) =>
  Effect.sync(() => {
    const message = `${channel}:${destination}`;
    console.log(message);
    return { channel, destination, ok: true };
  });

const handlers = App.toLayer(
  App.of({
    notifyAll: (event) =>
      Effect.gen(function* (): Effect.fn.Return<ReadonlyArray<unknown>, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const email = yield* ctx.beginRun(send("email", event.email));
        const sms = yield* ctx.beginRun(send("sms", event.phone));
        const slack = yield* ctx.beginRun(send("slack", event.orderId));
        const push = yield* ctx.beginRun(send("push", event.orderId));
        return yield* ctx.all([email.await, sms.await, slack.await, push.await]);
      }),
  }),
);

const base = Layer.mergeAll(
  Resonate.layerHttp({ url, group, pid }).pipe(Layer.provide(BunHttpClient.layer)),
  BunCrypto.layer,
  Codec.ResonateEncryptor.layerNoop,
);

const services = Layer.mergeAll(
  Codec.ResonateCodec.layerJson,
  DurablePromise.DurablePromises.layer,
  Task.Tasks.layer,
  ResonateSchedule.Schedules.layer,
  handlers,
).pipe(Layer.provideMerge(base));

const client = Resonate.ResonateClient.layer({ group, pid, ttl: Duration.seconds(5) }).pipe(
  Layer.provideMerge(ResonateContext.ExecutionEngine.layer.pipe(Layer.provideMerge(services))),
);

if (import.meta.main) {
  BunRuntime.runMain(
    Layer.launch(Worker.layer(App, { group, pid, ttl: Duration.seconds(5) }).pipe(Layer.provideMerge(client))),
  );
}
