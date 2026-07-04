import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunHttpClient from "@effect/platform-bun/BunHttpClient";
import { Clock, DateTime, Duration, Effect, Layer, Schema } from "effect";
import * as NetworkHttp from "../../src/network/Http.ts";
import * as Protocol from "../../src/Protocol.ts";
import * as Resonate from "../../src/Resonate.ts";
import { ResonateContext } from "../../src/ResonateContext.ts";
import * as Worker from "../../src/Worker.ts";

const url = Bun.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const groupName = Bun.env.RESONATE_GROUP ?? Bun.env.GROUP ?? "default";
const pidName = Bun.env.RESONATE_PID ?? Bun.env.PID ?? "context-apis-effect";

const group = Protocol.WorkerGroup.make(groupName);
const pid = Protocol.ProcessId.make(pidName);
const request = async (kind: string, data: object = {}) =>
  (
    await (
      await fetch(url, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ kind, head: { corrId: `${kind}-${crypto.randomUUID()}`, version: "2026-04-01" }, data }),
      })
    ).json()
  ).data;
const sleep = (millis: number) => new Promise((resolve) => setTimeout(resolve, millis));
const waitForExternalResumePoint = async (id: string) => {
  for (let attempt = 0; attempt < 40; attempt += 1) {
    const snap = await request("debug.snap");
    const rootSuspended = snap.tasks.some(
      (task: { readonly id: string; readonly state: string }) => task.id === id && task.state === "suspended",
    );
    const afterSleepUntil = snap.promises.some((promise: { readonly id: string }) => promise.id === `${id}.8`);
    if (rootSuspended && afterSleepUntil) {
      return;
    }
    await sleep(100);
  }
  throw new Error(`${id} did not reach external resume point`);
};

const ContextApis = Resonate.function({ name: "contextApis", payload: Schema.String });
const RemoteChild = Resonate.function({ name: "remoteChild", payload: Schema.String });
const DetachedChild = Resonate.function({ name: "detachedChild", payload: Schema.String });
const Approval = Resonate.promise({ name: "approval", success: Schema.String, error: Schema.String });
const App = Resonate.group(ContextApis, RemoteChild, DetachedChild);

const handlers = App.toLayer(
  App.of({
    contextApis: (input) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext> {
        const ctx = yield* ResonateContext;
        const external = yield* ctx.promise({
          declaration: Approval,
          options: {
            tags: Protocol.Tags.make({
              reserved: {},
              unrecognized: {},
              user: { [Protocol.UserTagKey.make("kind")]: "approval" },
            }),
          },
        });
        const local = yield* ctx.run({
          name: "localStep",
          effect: Effect.succeed({ step: "localStep", input: `${input}:run` }),
        });
        const slow = yield* ctx.beginRun({
          name: "localSlow",
          effect: Effect.sleep(Duration.millis(30)).pipe(Effect.as({ step: "localSlow", input: `${input}:slow` })),
        });
        const fast = yield* ctx.beginRun({
          name: "localFast",
          effect: Effect.sleep(Duration.millis(1)).pipe(Effect.as({ step: "localFast", input: `${input}:fast` })),
        });
        const rpc = yield* ctx.rpc({ target: RemoteChild, args: [`${input}:rpc`], options: { target: group } });
        const pendingRpc = yield* ctx.beginRpc({
          target: RemoteChild,
          args: [`${input}:beginRpc`],
          options: { target: group },
        });
        const detached = yield* ctx.detached({
          target: DetachedChild,
          args: [`${input}:detached`],
          options: { target: group },
        });

        yield* ctx.sleep({ for: Duration.millis(1) });
        yield* ctx.sleep({ until: DateTime.makeUnsafe((yield* Clock.currentTimeMillis) + 1) });

        return {
          input,
          local,
          fast: yield* fast.await,
          approval: yield* external.await,
          slow: yield* slow.await,
          rpc,
          beginRpc: yield* pendingRpc.await,
          detached: yield* detached.await,
        };
      }),
    remoteChild: (input) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext> {
        const ctx = yield* ResonateContext;
        return { step: "remoteChild", input, attempt: ctx.info.attempt };
      }),
    detachedChild: (input) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext> {
        const ctx = yield* ResonateContext;
        return { step: "detachedChild", input, parentId: ctx.info.parentId };
      }),
  }),
);

const networkLayer = NetworkHttp.layer({ url, group, pid }).pipe(Layer.provideMerge(BunHttpClient.layer));
const clientLayer = Resonate.Client.layer({ group, pid, ttl: Duration.seconds(5) }).pipe(
  Layer.provideMerge(networkLayer),
  Layer.provideMerge(BunCrypto.layer),
);
const workerLayer = Worker.layer({ group: App, worker: { group, pid, ttl: Duration.seconds(5) } }).pipe(
  Layer.provideMerge(handlers),
  Layer.provideMerge(networkLayer),
  Layer.provideMerge(BunCrypto.layer),
);

const program = Effect.gen(function* () {
  const client = yield* Resonate.Client;
  const run = yield* client.beginRpc({
    targetFunction: ContextApis,
    executionId: Protocol.ExecutionId.make("context-apis"),
    args: ["driver"],
    options: { target: group },
  });
  yield* Effect.promise(() => waitForExternalResumePoint("context-apis"));
  yield* client.resolve({
    declaration: Approval,
    id: Protocol.PromiseId.make("context-apis.0"),
    value: "approved-by-driver",
  });
  const observed = yield* run.await;
  return { scenario: "context-apis", observed };
});

const result = await Effect.runPromise(program.pipe(Effect.provide(Layer.mergeAll(clientLayer, workerLayer))));
console.log(JSON.stringify(result));
