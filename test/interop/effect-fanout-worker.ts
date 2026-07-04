import { BunRuntime } from "@effect/platform-bun";
import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunHttpClient from "@effect/platform-bun/BunHttpClient";
import { Config, Duration, Effect, Layer, Schema } from "effect";
import * as Protocol from "../../src/Protocol.ts";
import * as Resonate from "../../src/Resonate.ts";
import { ResonateContext } from "../../src/ResonateContext.ts";
import * as Worker from "../../src/Worker.ts";

const OrderEvent = Schema.Struct({
  orderId: Schema.String,
  email: Schema.String,
  phone: Schema.String,
});

const NotifyAll = Resonate.function({ name: "notifyAll", payload: OrderEvent });

const App = Resonate.group(NotifyAll);

const handlers = App.toLayer(
  App.of({
    notifyAll: (event) =>
      Effect.gen(function* (): Effect.fn.Return<ReadonlyArray<unknown>, unknown, ResonateContext> {
        const ctx = yield* ResonateContext;
        const email = yield* ctx.beginRun({
          name: "sendEmail",
          effect: Effect.succeed({ channel: "email", destination: event.email, ok: true }),
        });
        const sms = yield* ctx.beginRun({
          name: "sendSms",
          effect: Effect.succeed({ channel: "sms", destination: event.phone, ok: true }),
        });
        const slack = yield* ctx.beginRun({
          name: "sendSlack",
          effect: Effect.succeed({ channel: "slack", destination: event.orderId, ok: true }),
        });
        const push = yield* ctx.beginRun({
          name: "sendPush",
          effect: Effect.succeed({ channel: "push", destination: event.orderId, ok: true }),
        });
        return yield* ctx.all([email.await, sms.await, slack.await, push.await]);
      }),
  }),
);

const worker = Layer.unwrap(
  Effect.gen(function* () {
    const url = yield* Config.string("RESONATE_URL").pipe(Config.withDefault("http://127.0.0.1:8001"));
    const groupName = yield* Config.string("RESONATE_GROUP").pipe(Config.withDefault("default"));
    const pidName = yield* Config.string("RESONATE_PID").pipe(Config.withDefault("effect-fanout-worker"));
    const group = Protocol.WorkerGroup.make(groupName);
    const pid = Protocol.ProcessId.make(pidName);
    return Worker.layerHttp({ group: App, http: { url, group, pid, ttl: Duration.seconds(5) } }).pipe(
      Layer.provideMerge(handlers),
      Layer.provideMerge(BunHttpClient.layer),
      Layer.provideMerge(BunCrypto.layer),
    );
  }),
);

BunRuntime.runMain(Layer.launch(worker));
