import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunHttpClient from "@effect/platform-bun/BunHttpClient";
import { Duration, Effect, Layer, Schema } from "effect";
import { currentCodec } from "../../src/Codec.ts";
import * as NetworkHttp from "../../src/network/Http.ts";
import * as Protocol from "../../src/Protocol.ts";
import * as Resonate from "../../src/Resonate.ts";
import { ResonateContext } from "../../src/ResonateContext.ts";
import * as RetryPolicy from "../../src/RetryPolicy.ts";
import { Schedules } from "../../src/Schedule.ts";
import * as Worker from "../../src/Worker.ts";

const url = Bun.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const groupName = Bun.env.RESONATE_GROUP ?? "default";
const pidName = Bun.env.RESONATE_PID ?? "client-api-worker";

const group = Protocol.WorkerGroup.make(groupName);
const pid = Protocol.ProcessId.make(pidName);

const ClientEcho = Resonate.function({ name: "clientEcho", payload: Schema.String });
const AwaitApproval = Resonate.function({ name: "awaitApproval", payload: Schema.String });
const RetryOnce = Resonate.function({ name: "retryOnce", payload: Schema.String });
const AlwaysReject = Resonate.function({ name: "alwaysReject", payload: Schema.String });
const Approval = Resonate.promise({ name: "approval", success: Schema.String, error: Schema.String });
const App = Resonate.group(ClientEcho, AwaitApproval, RetryOnce, AlwaysReject);

const handlers = App.toLayer(
  App.of({
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

const networkLayer = NetworkHttp.layer({ url, group, pid }).pipe(Layer.provideMerge(BunHttpClient.layer));

const layer = Resonate.Client.layer({ group, pid, ttl: Duration.seconds(5) }).pipe(
  Layer.provideMerge(networkLayer),
  Layer.provideMerge(BunCrypto.layer),
);

const scheduleLayer = Schedules.layer.pipe(Layer.provideMerge(networkLayer), Layer.provideMerge(BunCrypto.layer));

const worker = Worker.layer({ group: App, worker: { group, pid, ttl: Duration.seconds(5) } }).pipe(
  Layer.provideMerge(handlers),
  Layer.provideMerge(networkLayer),
  Layer.provideMerge(BunCrypto.layer),
);

const program = Effect.gen(function* () {
  const client = yield* Resonate.Client;
  const codec = yield* currentCodec;
  const observed: Record<string, unknown> = {};

  observed.run = yield* client.run({
    targetFunction: ClientEcho,
    executionId: Protocol.ExecutionId.make("client-run"),
    args: ["run"],
  });

  const beginRun = yield* client.beginRun({
    targetFunction: ClientEcho,
    executionId: Protocol.ExecutionId.make("client-begin-run"),
    args: ["beginRun"],
  });
  observed.beginRun = yield* beginRun.await;
  observed.get = yield* (yield* client.get({ fn: ClientEcho, id: Protocol.ExecutionId.make("client-begin-run") }))
    .await;

  observed.rpc = yield* client.rpc({
    targetFunction: ClientEcho,
    executionId: Protocol.ExecutionId.make("client-rpc"),
    args: ["rpc"],
    options: { target: group },
  });

  const beginRpc = yield* client.beginRpc({
    targetFunction: ClientEcho,
    executionId: Protocol.ExecutionId.make("client-begin-rpc"),
    args: ["beginRpc"],
    options: { target: group },
  });
  observed.beginRpc = yield* beginRpc.await;

  const resolving = yield* client.beginRpc({
    targetFunction: AwaitApproval,
    executionId: Protocol.ExecutionId.make("client-resolve"),
    args: ["resolve"],
    options: { target: group },
  });
  yield* Effect.sleep(Duration.millis(500));
  yield* client.resolve({
    declaration: Approval,
    id: Protocol.PromiseId.make("client-resolve.0"),
    value: "approved",
  });
  observed.resolve = yield* resolving.await;

  const rejecting = yield* client.beginRpc({
    targetFunction: AwaitApproval,
    executionId: Protocol.ExecutionId.make("client-reject"),
    args: ["reject"],
    options: { target: group },
  });
  yield* Effect.sleep(Duration.millis(500));
  yield* client.reject({
    declaration: Approval,
    id: Protocol.PromiseId.make("client-reject.0"),
    error: "denied",
  });
  yield* rejecting.await.pipe(Effect.exit);
  observed.reject = "rejected";

  const canceling = yield* client.beginRpc({
    targetFunction: AwaitApproval,
    executionId: Protocol.ExecutionId.make("client-cancel"),
    args: ["cancel"],
    options: { target: group },
  });
  yield* Effect.sleep(Duration.millis(500));
  yield* canceling.cancel;
  observed.cancel = "canceled";

  observed.retry = yield* client.rpc({
    targetFunction: RetryOnce,
    executionId: Protocol.ExecutionId.make("client-retry"),
    args: ["retry"],
    options: { target: group, retryPolicy: RetryPolicy.constant({ delay: Duration.zero, maxRetries: 1 }) },
  });

  yield* client
    .rpc({
      targetFunction: AlwaysReject,
      executionId: Protocol.ExecutionId.make("client-error"),
      args: ["error"],
      options: { target: group },
    })
    .pipe(Effect.exit);
  observed.error = "errored";

  const schedule = Resonate.schedule({
    id: Protocol.ScheduleId.make("client-schedule"),
    cron: "* * * * *",
    function: ClientEcho,
    payload: ["schedule"],
    target: group,
  });
  const scheduleRecord = yield* schedule.create;
  observed.schedule = {
    id: scheduleRecord.id,
    cron: scheduleRecord.cron,
    promiseId: scheduleRecord.promiseId,
    promiseTimeout: Duration.toMillis(scheduleRecord.promiseTimeout),
    promiseParam: yield* codec.decode(scheduleRecord.promiseParam),
    promiseTags: Schema.encodeSync(Protocol.TagsFromWire)(scheduleRecord.promiseTags),
  };
  yield* schedule.delete;

  return observed;
});

const result = await Effect.runPromise(program.pipe(Effect.provide(Layer.mergeAll(layer, scheduleLayer, worker))));

console.log(JSON.stringify(result));
