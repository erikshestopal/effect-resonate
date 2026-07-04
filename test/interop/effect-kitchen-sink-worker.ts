import { BunRuntime } from "@effect/platform-bun";
import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunHttpClient from "@effect/platform-bun/BunHttpClient";
import { Config, Duration, Effect, Layer, Schema } from "effect";
import * as Protocol from "../../src/Protocol.ts";
import * as Resonate from "../../src/Resonate.ts";
import { ResonateContext } from "../../src/ResonateContext.ts";
import * as Worker from "../../src/Worker.ts";

const KitchenSink = Resonate.function({ name: "kitchenSink", payload: Schema.String });
const RemoteChild = Resonate.function({ name: "remoteChild", payload: Schema.String });
const DetachedChild = Resonate.function({ name: "detachedChild", payload: Schema.String });

const App = Resonate.group(KitchenSink, RemoteChild, DetachedChild);

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
        yield* ctx.sleep(Duration.millis(1));
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
