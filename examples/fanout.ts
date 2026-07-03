import { BunRuntime } from "@effect/platform-bun";
import { Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

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
  Effect.gen(function* () {
    const message = `${channel}:${destination}`;
    yield* Effect.logInfo(message);
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

const worker = Worker.layerHttp(App, { url, group, pid, ttl: Duration.seconds(5) }).pipe(Layer.provideMerge(handlers));

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(worker));
}
