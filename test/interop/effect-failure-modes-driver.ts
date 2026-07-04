import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunHttpClient from "@effect/platform-bun/BunHttpClient";
import { Duration, Effect, Layer, Schema } from "effect";
import { currentCodec } from "../../src/Codec.ts";
import * as NetworkHttp from "../../src/network/Http.ts";
import * as Protocol from "../../src/Protocol.ts";
import * as Resonate from "../../src/Resonate.ts";
import { ResonateContext } from "../../src/ResonateContext.ts";
import * as RetryPolicy from "../../src/RetryPolicy.ts";
import * as Worker from "../../src/Worker.ts";

const url = Bun.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Protocol.WorkerGroup.make(Bun.env.RESONATE_GROUP ?? "default");
const pid = Protocol.ProcessId.make(Bun.env.RESONATE_PID ?? "failure-modes-worker");
const sleep = (millis: number) => new Promise((resolve) => setTimeout(resolve, millis));

const stableError = (name: string, message: string) => {
  const error = new Error(message);
  error.name = name;
  error.stack = `${name}: ${message}\n    at parity`;
  return error;
};
const stableAggregateError = () => {
  const error = new AggregateError([stableError("Error", "one")], "many");
  error.stack = "AggregateError: many\n    at parity";
  return error;
};
const PendingExternal = Resonate.function({ name: "pendingExternal", payload: Schema.String });
const AttachedParent = Resonate.function({ name: "attachedParent", payload: Schema.String });
const DetachedParent = Resonate.function({ name: "detachedParent", payload: Schema.String });
const Retrying = Resonate.function({ name: "retrying", payload: Schema.Unknown });
const EncodingFailure = Resonate.function({ name: "encodingFailure", payload: Schema.Unknown });
const EncodingValue = Resonate.function({ name: "encodingValue", payload: Schema.Unknown });
const External = Resonate.promise({ name: "nativeTyped", success: Schema.Unknown, error: Schema.Unknown });
const App = Resonate.group(PendingExternal, AttachedParent, DetachedParent, Retrying, EncodingFailure, EncodingValue);
const attempts: Record<string, Array<number>> = {};
const labelTag = Protocol.UserTagKey.make("label");

const handlers = App.toLayer(
  App.of({
    pendingExternal: (label) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext> {
        const ctx = yield* ResonateContext;
        const p = yield* ctx.promise({
          declaration: External,
          options: {
            id: Protocol.PromiseId.make(`${ctx.info.id}.0`),
            tags: Protocol.Tags.make({ reserved: {}, unrecognized: {}, user: { [labelTag]: label } }),
          },
        });
        return yield* p.await;
      }),
    attachedParent: (label) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext> {
        const ctx = yield* ResonateContext;
        const child = yield* ctx.beginRpc({ target: PendingExternal, args: [`${label}-child`] });
        return yield* child.await;
      }),
    detachedParent: (label) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext> {
        const ctx = yield* ResonateContext;
        const child = yield* ctx.detached({ target: PendingExternal, args: [`${label}-detached`] });
        return { detached: child.id };
      }),
    retrying: (spec) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext> {
        const ctx = yield* ResonateContext;
        const value = spec as { readonly label: string; readonly kind: string; readonly error?: unknown };
        attempts[value.label] ??= [];
        attempts[value.label].push(ctx.info.attempt);
        if (value.kind === "nonRetryable") {
          return yield* Effect.fail(stableError("Error", "non-retryable"));
        }
        return yield* Effect.fail(value.error);
      }),
    encodingFailure: (value) => Effect.fail(value),
    encodingValue: (value) => Effect.succeed(value),
  }),
);
const networkLayer = NetworkHttp.layer({ url, group, pid }).pipe(Layer.provideMerge(BunHttpClient.layer));
const layer = Resonate.Client.layer({ group, pid, ttl: Duration.seconds(5) }).pipe(
  Layer.provideMerge(networkLayer),
  Layer.provideMerge(BunCrypto.layer),
);
const worker = Worker.layer({ group: App, worker: { group, pid, ttl: Duration.seconds(5) } }).pipe(
  Layer.provideMerge(handlers),
  Layer.provideMerge(networkLayer),
  Layer.provideMerge(BunCrypto.layer),
);

const program = Effect.gen(function* () {
  const client = yield* Resonate.Client;
  const codec = yield* currentCodec;
  const observed: Record<string, unknown> = {};
  const root = yield* client.beginRpc({
    targetFunction: PendingExternal,
    executionId: Protocol.ExecutionId.make("failure-cancel-root"),
    args: ["root"],
    options: { target: group },
  });
  yield* Effect.promise(() => sleep(250));
  yield* client.cancel(root.id);
  yield* root.await.pipe(Effect.exit);
  const attached = yield* client.beginRpc({
    targetFunction: AttachedParent,
    executionId: Protocol.ExecutionId.make("failure-cancel-attached"),
    args: ["attached"],
    options: { target: group },
  });
  yield* Effect.promise(() => sleep(500));
  yield* client.cancel(Protocol.PromiseId.make("failure-cancel-attached.0"));
  yield* attached.await.pipe(Effect.exit);
  const detached = yield* client.rpc({
    targetFunction: DetachedParent,
    executionId: Protocol.ExecutionId.make("failure-cancel-detached"),
    args: ["detached"],
    options: { target: group },
  });
  yield* client.cancel(Protocol.PromiseId.make((detached as { detached: string }).detached));
  observed.cancelDetachedChild = { detached };
  const external = yield* client.beginRpc({
    targetFunction: PendingExternal,
    executionId: Protocol.ExecutionId.make("failure-cancel-external"),
    args: ["external"],
    options: { target: group },
  });
  yield* Effect.promise(() => sleep(250));
  yield* client.cancel(Protocol.PromiseId.make("failure-cancel-external.0"));
  yield* external.await.pipe(Effect.exit);
  const policies = {
    constant: RetryPolicy.constant({ delay: Duration.zero, maxRetries: 2 }),
    linear: RetryPolicy.linear({ delay: Duration.zero, maxRetries: 2 }),
    exponential: RetryPolicy.exponential({
      delay: Duration.zero,
      factor: 2,
      maxRetries: 2,
      maxDelay: Duration.millis(1),
    }),
    never: RetryPolicy.never(),
    exhausted: RetryPolicy.constant({ delay: Duration.zero, maxRetries: 1 }),
    timeoutExceeded: RetryPolicy.constant({ delay: Duration.seconds(1), maxRetries: 3 }),
  };
  const retryResultStatuses: Record<string, string> = {};
  for (const [label, retryPolicy] of Object.entries(policies)) {
    retryResultStatuses[label] = yield* client
      .rpc({
        targetFunction: Retrying,
        executionId: Protocol.ExecutionId.make(`failure-retry-${label}`),
        args: [{ label, kind: "error", error: `${label}-boom` }],
        options: {
          target: group,
          retryPolicy,
          timeout: label === "timeoutExceeded" ? Duration.millis(250) : Duration.seconds(5),
        },
      })
      .pipe(Effect.match({ onFailure: () => "failure", onSuccess: () => "success" }));
  }
  retryResultStatuses.nonRetryable = yield* client
    .rpc({
      targetFunction: Retrying,
      executionId: Protocol.ExecutionId.make("failure-retry-non-retryable"),
      args: [{ label: "nonRetryable", kind: "nonRetryable" }],
      options: {
        target: group,
        retryPolicy: RetryPolicy.constant({ delay: Duration.zero, maxRetries: 3 }),
        nonRetryableErrors: [],
      },
    })
    .pipe(Effect.match({ onFailure: () => "failure", onSuccess: () => "success" }));
  observed.retryResultStatuses = retryResultStatuses;
  observed.retryAttempts = attempts;
  const values = [
    "string-error",
    { object: true },
    stableError("Error", "native-error"),
    stableAggregateError(),
    undefined,
    null,
    Infinity,
    new Date(0),
    [1, undefined, null],
    { nested: { array: [1, { two: 2 }] } },
  ];
  yield* Effect.forEach(values, (value, index) =>
    client
      .rpc({
        targetFunction: EncodingFailure,
        executionId: Protocol.ExecutionId.make(`failure-encoding-error-${index}`),
        args: [value],
        options: { target: group },
      })
      .pipe(Effect.exit),
  );
  observed.encodingCount = values.length;
  observed.encodedValue = yield* client.rpc({
    targetFunction: EncodingValue,
    executionId: Protocol.ExecutionId.make("failure-encoding-value"),
    args: [
      { undefined, nil: null, infinity: Infinity, date: new Date(0), array: [undefined, null], nested: { ok: true } },
    ],
    options: { target: group },
  });
  observed.typedExternalSchemaHeaders = {
    resolve: { ...(yield* codec.encode("ok")), headers: { "resonate:schema": "nativeTyped" } },
  };
  return observed;
});
const result = await Effect.runPromise(program.pipe(Effect.provide(Layer.mergeAll(layer, worker))));
console.log(JSON.stringify(result));
