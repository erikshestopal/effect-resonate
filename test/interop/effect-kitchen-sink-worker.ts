import { BunRuntime } from "@effect/platform-bun";
import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunHttpClient from "@effect/platform-bun/BunHttpClient";
import { Clock, Config, DateTime, Duration, Effect, Layer, Schema } from "effect";
import * as Protocol from "../../src/Protocol.ts";
import * as Resonate from "../../src/Resonate.ts";
import { ResonateContext } from "../../src/ResonateContext.ts";
import * as Worker from "../../src/Worker.ts";

const KitchenSink = Resonate.function({ name: "kitchenSink", payload: Schema.String });
const RemoteChild = Resonate.function({ name: "remoteChild", payload: Schema.String });
const DetachedChild = Resonate.function({ name: "detachedChild", payload: Schema.String });
const ClientEcho = Resonate.function({ name: "clientEcho", payload: Schema.String });
const AwaitApproval = Resonate.function({ name: "awaitApproval", payload: Schema.String });
const RetryOnce = Resonate.function({ name: "retryOnce", payload: Schema.String });
const AlwaysReject = Resonate.function({ name: "alwaysReject", payload: Schema.String });

const Approval = Resonate.promise({ name: "approval", success: Schema.String, error: Schema.String });

const App = Resonate.group(KitchenSink, RemoteChild, DetachedChild, ClientEcho, AwaitApproval, RetryOnce, AlwaysReject);

const handlers = App.toLayer(
  App.of({
    kitchenSink: (input) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext> {
        const ctx = yield* ResonateContext;
        const local = yield* ctx.run({
          name: "localStep",
          effect: Effect.succeed({ step: "localStep", input }),
        });
        const pendingLocal = yield* ctx.beginRun({
          name: "localAsync",
          effect: Effect.succeed({ step: "localAsync", input }),
        });
        const rpc = yield* ctx.rpc({ target: RemoteChild, args: ["rpc"] });
        const pendingRpc = yield* ctx.beginRpc({
          target: RemoteChild,
          args: ["beginRpc"],
        });
        const detached = yield* ctx.detached({
          target: DetachedChild,
          args: ["detached"],
        });
        yield* ctx.sleep({ for: Duration.millis(1) });
        yield* ctx.sleep({ until: DateTime.makeUnsafe((yield* Clock.currentTimeMillis) + 1) });
        return {
          local,
          begunLocal: yield* pendingLocal.await,
          rpc,
          begunRpc: yield* pendingRpc.await,
          detached: yield* detached.await,
        };
      }),
    remoteChild: (input) => Effect.succeed({ step: "remoteChild", input }),
    detachedChild: (input) => Effect.succeed({ step: "detachedChild", input }),
    clientEcho: (input) => Effect.succeed({ step: "clientEcho", input }),
    awaitApproval: (input) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext> {
        const ctx = yield* ResonateContext;
        const approval = yield* ctx.promise({
          declaration: Approval,
          options: { id: Protocol.PromiseId.make(`${ctx.info.id}.0`) },
        });
        return { input, approval: yield* approval.await };
      }),
    retryOnce: (input) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, string, ResonateContext> {
        const ctx = yield* ResonateContext;
        if (ctx.info.attempt === 0) {
          return yield* Effect.fail("retry-once");
        }
        return { step: "retryOnce", input, attempt: ctx.info.attempt };
      }),
    alwaysReject: (input) => Effect.fail({ step: "alwaysReject", input }),
  }),
);

const worker = Layer.unwrap(
  Effect.gen(function* () {
    const url = yield* Config.string("RESONATE_URL").pipe(Config.withDefault("http://127.0.0.1:8001"));
    const groupName = yield* Config.string("RESONATE_GROUP").pipe(Config.withDefault("default"));
    const pidName = yield* Config.string("RESONATE_PID").pipe(Config.withDefault("kitchen-sink-worker"));
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
