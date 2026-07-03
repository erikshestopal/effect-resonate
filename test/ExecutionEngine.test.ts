import { describe, expect, it } from "@effect/vitest";
import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import { DateTime, Duration, Effect, Exit, Layer, Option, Predicate, Schema, SchemaParser } from "effect";
import { TestClock } from "effect/testing";
import { currentCodec, ResonateCodec, ResonateEncryptor } from "../src/Codec.ts";
import { DurablePromises } from "../src/DurablePromise.ts";
import { DurablePromiseTimedOut } from "../src/Errors.ts";
import { makeRequestHead, ResonateNetwork } from "../src/network/network.ts";
import * as NetworkLocal from "../src/network/local.ts";
import * as Protocol from "../src/Protocol.ts";
import * as Resonate from "../src/Resonate.ts";
import { ExecutionEngine, ResonateContext } from "../src/ResonateContext.ts";
import * as RetryPolicy from "../src/RetryPolicy.ts";
import { Tasks } from "../src/Task.ts";

const isDebugSnapSuccess = SchemaParser.is(Protocol.DebugSnapResponse.members[0]);

const Workflow = Resonate.function("Workflow", {
  payload: Schema.Number,
});

const RemoteChild = Resonate.function("RemoteChild", {
  payload: Schema.Number,
});

const RemoteParent = Resonate.function("RemoteParent", {
  payload: Schema.Number,
});

class ApprovalDenied extends Schema.TaggedErrorClass<ApprovalDenied>()("ApprovalDenied", {
  reason: Schema.String,
}) {}

const Approval = Resonate.promise("approval", {
  success: Schema.Struct({ approvedBy: Schema.String }),
  error: ApprovalDenied,
});

const isApprovalDenied = SchemaParser.is(ApprovalDenied);
const isDurablePromiseTimedOut = SchemaParser.is(DurablePromiseTimedOut);

const ExternalWorkflow = Resonate.function("ExternalWorkflow", {
  payload: Schema.String,
});

class CardDeclined extends Schema.TaggedErrorClass<CardDeclined>()("CardDeclined", {
  code: Schema.String,
}) {}

const RetryWorkflow = Resonate.function("RetryWorkflow", {
  payload: Schema.Number,
});

const workflowGroup = Resonate.group(Workflow);
const remoteGroup = Resonate.group(Workflow, RemoteChild, RemoteParent);
const externalGroup = Resonate.group(ExternalWorkflow);
const retryGroup = Resonate.group(RetryWorkflow);

const baseLayer = Layer.mergeAll(
  NetworkLocal.layer({
    group: "workers",
    pid: "engine-1",
    tickInterval: Duration.hours(24),
    retryTimeout: Duration.seconds(5),
  }),
  BunCrypto.layer,
  ResonateEncryptor.layerNoop,
);

const codecLayer = ResonateCodec.layerJson.pipe(Layer.provide(baseLayer));
const protocolLayer = Layer.mergeAll(DurablePromises.layer, Tasks.layer).pipe(
  Layer.provide(Layer.mergeAll(baseLayer, codecLayer)),
);
const clientLayer = Resonate.ResonateClient.layer({
  group: Protocol.WorkerGroup.make("workers"),
  pid: Protocol.ProcessId.make("engine-1"),
  ttl: Duration.seconds(30),
}).pipe(Layer.provide(Layer.mergeAll(baseLayer, codecLayer, protocolLayer)));
const engineLayer = ExecutionEngine.layer.pipe(Layer.provide(Layer.mergeAll(protocolLayer, codecLayer)));
const layer = Layer.mergeAll(baseLayer, codecLayer, protocolLayer, clientLayer, engineLayer);

const snap = Effect.fn("ExecutionEngineTest.snap")(function* () {
  const network = yield* ResonateNetwork;
  const response = yield* network.send(Protocol.DebugSnapRequest.make({ head: yield* makeRequestHead, data: {} }));
  if (!isDebugSnapSuccess(response)) {
    return yield* Effect.die(response.data);
  }
  return response.data;
});

const acquiredRoot = Effect.fn("ExecutionEngineTest.acquiredRoot")(function* (id: Protocol.PromiseId) {
  const state = yield* snap();
  const task = state.tasks.find((task) => task.id === id);
  const promise = state.promises.find((promise) => promise.id === id);
  if (task?.state !== "acquired" || !promise) {
    return yield* Effect.die(`Root '${id}' was not acquired`);
  }
  return { task, promise };
});

describe("ExecutionEngine", () => {
  it.effect("executes local ctx.run steps with deterministic ids and root fulfillment", () =>
    Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const engine = yield* ExecutionEngine;
      const codec = yield* currentCodec;
      const handlers = workflowGroup.toLayer(
        workflowGroup.of({
          Workflow: (value) =>
            Effect.gen(function* (): Effect.fn.Return<number, unknown, ResonateContext> {
              const ctx = yield* ResonateContext;
              const stepped = yield* ctx.run(Effect.succeed(value + 1));
              const explicit = yield* ctx.beginRun(Effect.succeed(value + 2), {
                id: Protocol.PromiseId.make("engine-explicit-local"),
              });
              yield* explicit.await;
              return Number(stepped) + 1;
            }),
        }),
      );
      const registry = yield* workflowGroup.registry().pipe(Effect.provide(handlers));

      const handle = yield* client.beginRun(Workflow, Protocol.ExecutionId.make("engine-root-1"), [1]);
      const root = yield* acquiredRoot(handle.id);
      const outcome = yield* engine.execute({ task: root.task, promise: root.promise, registry });
      expect(Predicate.isTagged(outcome, "Done")).toBe(true);

      const state = yield* snap();
      const child = state.promises.find((promise) => promise.id === "engine-root-1.0");
      const explicit = state.promises.find((promise) => promise.id === "engine-explicit-local");
      const completedRoot = state.promises.find((promise) => promise.id === handle.id);
      expect(child?.state).toBe("resolved");
      expect(child?.tags.reserved["resonate:scope"]).toBe("local");
      expect(child?.tags.reserved["resonate:parent"]).toBe(handle.id);
      expect(explicit?.tags.reserved["resonate:origin"]).toBe("engine-explicit-local");
      expect(explicit?.tags.reserved["resonate:prefix"]).toBe(handle.id);
      expect(completedRoot?.state).toBe("resolved");
      expect(yield* codec.decode(completedRoot?.value ?? Protocol.emptyValue)).toBe(3);
    }).pipe(Effect.provide(layer)),
  );

  it.effect("replays a completed local step from the promise cache without re-executing it", () =>
    Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const engine = yield* ExecutionEngine;
      const promises = yield* DurablePromises;
      const codec = yield* currentCodec;
      let executions = 0;
      const handlers = workflowGroup.toLayer(
        workflowGroup.of({
          Workflow: () =>
            Effect.gen(function* (): Effect.fn.Return<number, unknown, ResonateContext> {
              const ctx = yield* ResonateContext;
              const value = yield* ctx.run(
                Effect.sync(() => {
                  executions = executions + 1;
                  return 41;
                }),
              );
              return Number(value) + 1;
            }),
        }),
      );
      const registry = yield* workflowGroup.registry().pipe(Effect.provide(handlers));

      const handle = yield* client.beginRun(Workflow, Protocol.ExecutionId.make("engine-root-2"), [0]);
      const root = yield* acquiredRoot(handle.id);
      const encoded = yield* codec.encode(41);
      const child = yield* promises.create({
        id: Protocol.PromiseId.make("engine-root-2.0"),
        timeoutAt: root.promise.timeoutAt,
        param: Protocol.emptyValue,
        tags: Protocol.emptyTags,
      });
      const settled = yield* promises.settle({
        id: child.id,
        state: Schema.Literal("resolved").make("resolved"),
        value: encoded,
      });

      yield* engine.execute({ task: root.task, promise: root.promise, registry, preload: [settled] });
      const state = yield* snap();
      const completedRoot = state.promises.find((promise) => promise.id === handle.id);
      expect(executions).toBe(0);
      expect(yield* codec.decode(completedRoot?.value ?? Protocol.emptyValue)).toBe(42);
    }).pipe(Effect.provide(layer)),
  );

  it.effect("creates targeted remote child invocations and suspends while awaiting them", () =>
    Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const engine = yield* ExecutionEngine;
      const codec = yield* currentCodec;
      const handlers = remoteGroup.toLayer(
        remoteGroup.of({
          Workflow: () => Effect.void,
          RemoteChild: (value) => Effect.succeed(Number(value) + 1),
          RemoteParent: (value) =>
            Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext> {
              const ctx = yield* ResonateContext;
              return yield* ctx.rpc(RemoteChild, [Number(value) + 1], {
                target: Protocol.WorkerGroup.make("remote-workers"),
              });
            }),
        }),
      );
      const registry = yield* remoteGroup.registry().pipe(Effect.provide(handlers));

      const handle = yield* client.beginRun(RemoteParent, Protocol.ExecutionId.make("engine-remote-1"), [1]);
      const root = yield* acquiredRoot(handle.id);
      const outcome = yield* engine.execute({ task: root.task, promise: root.promise, registry });
      expect(Predicate.isTagged(outcome, "Suspended")).toBe(true);

      const state = yield* snap();
      const child = state.promises.find((promise) => promise.id === "engine-remote-1.0");
      expect(child?.state).toBe("pending");
      expect(child?.tags.reserved["resonate:scope"]).toBe("global");
      expect(child?.tags.reserved["resonate:target"]?.address).toBe("local://any@remote-workers");
      expect(child?.tags.reserved["resonate:origin"]).toBe(handle.id);
      expect(child?.tags.reserved["resonate:prefix"]).toBe(handle.id);
      expect(child?.tags.reserved["resonate:branch"]).toBe("engine-remote-1.0");
      expect(child?.tags.reserved["resonate:parent"]).toBe(handle.id);
      expect(yield* codec.decode(child?.param ?? Protocol.emptyValue)).toEqual({
        func: "RemoteChild",
        args: [2],
        version: 1,
      });
    }).pipe(Effect.provide(layer)),
  );

  it.effect("suspends parent return until attached unawaited remote children settle", () =>
    Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const engine = yield* ExecutionEngine;
      const promises = yield* DurablePromises;
      const codec = yield* currentCodec;
      const handlers = remoteGroup.toLayer(
        remoteGroup.of({
          Workflow: () => Effect.void,
          RemoteChild: (value) => Effect.succeed(Number(value) + 1),
          RemoteParent: () =>
            Effect.gen(function* (): Effect.fn.Return<string, unknown, ResonateContext> {
              const ctx = yield* ResonateContext;
              yield* ctx.beginRpc(RemoteChild, [1]);
              return "parent-ready";
            }),
        }),
      );
      const registry = yield* remoteGroup.registry().pipe(Effect.provide(handlers));

      const handle = yield* client.beginRun(RemoteParent, Protocol.ExecutionId.make("engine-remote-flush-1"), [0]);
      const root = yield* acquiredRoot(handle.id);
      const outcome = yield* engine.execute({ task: root.task, promise: root.promise, registry });
      expect(Predicate.isTagged(outcome, "Suspended")).toBe(true);
      if (Predicate.isTagged(outcome, "Suspended")) {
        expect(outcome.awaited).toEqual(["engine-remote-flush-1.0"]);
      }

      const settledChild = yield* promises.settle({
        id: Protocol.PromiseId.make("engine-remote-flush-1.0"),
        state: Schema.Literal("resolved").make("resolved"),
        value: yield* codec.encode(2),
      });
      const done = yield* engine.execute({ task: root.task, promise: root.promise, registry, preload: [settledChild] });
      expect(Predicate.isTagged(done, "Done")).toBe(true);

      const completed = yield* promises.get(handle.id);
      expect(completed.state).toBe("resolved");
      expect(yield* codec.decode(completed.value)).toBe("parent-ready");
    }).pipe(Effect.provide(layer)),
  );

  it.effect("fans out multiple attached remote awaits into one suspend", () =>
    Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const engine = yield* ExecutionEngine;
      const handlers = remoteGroup.toLayer(
        remoteGroup.of({
          Workflow: () => Effect.void,
          RemoteChild: (value) => Effect.succeed(Number(value) + 1),
          RemoteParent: () =>
            Effect.gen(function* () {
              const ctx = yield* ResonateContext;
              const left = yield* ctx.beginRpc(RemoteChild, [1]);
              const right = yield* ctx.beginRpc(RemoteChild, [2]);
              return yield* ctx.all([left.await, right.await]);
            }),
        }),
      );
      const registry = yield* remoteGroup.registry().pipe(Effect.provide(handlers));

      const handle = yield* client.beginRun(RemoteParent, Protocol.ExecutionId.make("engine-remote-fanout-1"), [0]);
      const root = yield* acquiredRoot(handle.id);
      const outcome = yield* engine.execute({ task: root.task, promise: root.promise, registry });
      expect(Predicate.isTagged(outcome, "Suspended")).toBe(true);
      if (Predicate.isTagged(outcome, "Suspended")) {
        expect(outcome.awaited).toEqual(["engine-remote-fanout-1.0", "engine-remote-fanout-1.1"]);
      }
    }).pipe(Effect.provide(layer)),
  );

  it.effect("breaks attached lineage when an explicit remote id is supplied", () =>
    Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const engine = yield* ExecutionEngine;
      const handlers = remoteGroup.toLayer(
        remoteGroup.of({
          Workflow: () => Effect.void,
          RemoteChild: (value) => Effect.succeed(Number(value) + 1),
          RemoteParent: () =>
            Effect.gen(function* (): Effect.fn.Return<string, unknown, ResonateContext> {
              const ctx = yield* ResonateContext;
              yield* ctx.beginRpc(RemoteChild, [1], { id: Protocol.PromiseId.make("engine-explicit-child") });
              return "ready";
            }),
        }),
      );
      const registry = yield* remoteGroup.registry().pipe(Effect.provide(handlers));

      const handle = yield* client.beginRun(RemoteParent, Protocol.ExecutionId.make("engine-remote-explicit-1"), [0]);
      const root = yield* acquiredRoot(handle.id);
      const outcome = yield* engine.execute({ task: root.task, promise: root.promise, registry });
      expect(Predicate.isTagged(outcome, "Suspended")).toBe(true);

      const state = yield* snap();
      const child = state.promises.find((promise) => promise.id === "engine-explicit-child");
      expect(child?.tags.reserved["resonate:origin"]).toBe("engine-explicit-child");
      expect(child?.tags.reserved["resonate:prefix"]).toBe(handle.id);
      expect(child?.tags.reserved["resonate:branch"]).toBe("engine-explicit-child");
      expect(child?.tags.reserved["resonate:parent"]).toBe(handle.id);
    }).pipe(Effect.provide(layer)),
  );

  it.effect("detaches remote children as fresh roots without blocking parent completion", () =>
    Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const engine = yield* ExecutionEngine;
      const codec = yield* currentCodec;
      const handlers = remoteGroup.toLayer(
        remoteGroup.of({
          Workflow: () => Effect.void,
          RemoteChild: (value) => Effect.succeed(Number(value) + 1),
          RemoteParent: () =>
            Effect.gen(function* (): Effect.fn.Return<string, unknown, ResonateContext> {
              const ctx = yield* ResonateContext;
              yield* ctx.detached(RemoteChild, [1]);
              return "detached-parent";
            }),
        }),
      );
      const registry = yield* remoteGroup.registry().pipe(Effect.provide(handlers));

      const handle = yield* client.beginRun(RemoteParent, Protocol.ExecutionId.make("engine-remote-detached-1"), [0], {
        timeout: Duration.seconds(30),
      });
      const root = yield* acquiredRoot(handle.id);
      const outcome = yield* engine.execute({ task: root.task, promise: root.promise, registry });
      expect(Predicate.isTagged(outcome, "Done")).toBe(true);

      const state = yield* snap();
      const child = state.promises.find((promise) => promise.id.startsWith("engine-remote-detached-1.d"));
      const completedRoot = state.promises.find((promise) => promise.id === handle.id);
      expect(child?.state).toBe("pending");
      expect(child?.id).toHaveLength("engine-remote-detached-1.d".length + 14);
      expect(child?.tags.reserved["resonate:origin"]).toBe(child?.id);
      expect(child?.tags.reserved["resonate:prefix"]).toBe(handle.id);
      expect(child?.tags.reserved["resonate:branch"]).toBe(child?.id);
      expect(child?.tags.reserved["resonate:parent"]).toBe(handle.id);
      expect(DateTime.toEpochMillis(child?.timeoutAt ?? DateTime.makeUnsafe(-1))).toBe(86_400_000);
      expect(completedRoot?.state).toBe("resolved");
      expect(yield* codec.decode(completedRoot?.value ?? Protocol.emptyValue)).toBe("detached-parent");
    }).pipe(Effect.provide(layer)),
  );

  it.effect("creates named external promises and resumes after typed resolution", () =>
    Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const engine = yield* ExecutionEngine;
      const codec = yield* currentCodec;
      const handlers = externalGroup.toLayer(
        externalGroup.of({
          ExternalWorkflow: () =>
            Effect.gen(function* (): Effect.fn.Return<string, unknown, ResonateContext> {
              const ctx = yield* ResonateContext;
              const approval = yield* ctx.promise(Approval, { timeout: Duration.hours(1) });
              const published = yield* ctx.run(Effect.succeed(approval.id));
              const result = yield* approval.await;
              if (!Predicate.isString(published)) {
                return yield* Effect.die("published id was not a string");
              }
              return `${published}:${result.approvedBy}`;
            }),
        }),
      );
      const registry = yield* externalGroup.registry().pipe(Effect.provide(handlers));

      const handle = yield* client.beginRun(ExternalWorkflow, Protocol.ExecutionId.make("engine-external-1"), ["ok"]);
      const root = yield* acquiredRoot(handle.id);
      const suspended = yield* engine.execute({ task: root.task, promise: root.promise, registry });
      expect(Predicate.isTagged(suspended, "Suspended")).toBe(true);
      if (Predicate.isTagged(suspended, "Suspended")) {
        expect(suspended.awaited).toEqual(["engine-external-1.approval"]);
      }

      yield* client.resolve(Approval, Approval.id(Protocol.ExecutionId.make("engine-external-1")), {
        approvedBy: "erik",
      });
      const state = yield* snap();
      const settled = state.promises.find((promise) => promise.id === "engine-external-1.approval");
      if (Predicate.isUndefined(settled)) {
        return yield* Effect.die("approval promise missing");
      }
      const done = yield* engine.execute({ task: root.task, promise: root.promise, registry, preload: [settled] });
      expect(Predicate.isTagged(done, "Done")).toBe(true);

      const completed = yield* client.get(ExternalWorkflow, Protocol.ExecutionId.make("engine-external-1"));
      const exit = yield* completed.await.pipe(Effect.exit);
      expect(Exit.isSuccess(exit)).toBe(true);
      if (Exit.isSuccess(exit)) {
        expect(exit.value).toBe("engine-external-1.approval:erik");
      }
      const approval = state.promises.find((promise) => promise.id === "engine-external-1.approval");
      expect(yield* codec.decode(approval?.value ?? Protocol.emptyValue)).toEqual({ approvedBy: "erik" });
    }).pipe(Effect.provide(layer)),
  );

  it.effect("decodes typed external promise rejection into the awaiting function", () =>
    Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const engine = yield* ExecutionEngine;
      const handlers = externalGroup.toLayer(
        externalGroup.of({
          ExternalWorkflow: () =>
            Effect.gen(function* (): Effect.fn.Return<string, unknown, ResonateContext> {
              const ctx = yield* ResonateContext;
              const approval = yield* ctx.promise(Approval);
              const result = yield* approval.await.pipe(
                Effect.catch((error) => (isApprovalDenied(error) ? Effect.succeed(error.reason) : Effect.fail(error))),
              );
              if (Predicate.isString(result)) {
                return result;
              }
              return yield* Effect.die("expected approval denial");
            }),
        }),
      );
      const registry = yield* externalGroup.registry().pipe(Effect.provide(handlers));

      const handle = yield* client.beginRun(ExternalWorkflow, Protocol.ExecutionId.make("engine-external-reject-1"), [
        "ok",
      ]);
      const root = yield* acquiredRoot(handle.id);
      yield* engine.execute({ task: root.task, promise: root.promise, registry });
      yield* client.reject(
        Approval,
        Approval.id(Protocol.ExecutionId.make("engine-external-reject-1")),
        new ApprovalDenied({ reason: "nope" }),
      );
      const state = yield* snap();
      const settled = state.promises.find((promise) => promise.id === "engine-external-reject-1.approval");
      if (Predicate.isUndefined(settled)) {
        return yield* Effect.die("approval promise missing");
      }
      yield* engine.execute({ task: root.task, promise: root.promise, registry, preload: [settled] });
      const completed = yield* client.get(ExternalWorkflow, Protocol.ExecutionId.make("engine-external-reject-1"));
      expect(yield* completed.await).toBe("nope");
    }).pipe(Effect.provide(layer)),
  );

  it.effect("surfaces external promise timeout as a typed await error", () =>
    Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const engine = yield* ExecutionEngine;
      const promises = yield* DurablePromises;
      const codec = yield* currentCodec;
      const handlers = externalGroup.toLayer(
        externalGroup.of({
          ExternalWorkflow: () =>
            Effect.gen(function* (): Effect.fn.Return<string, unknown, ResonateContext> {
              const ctx = yield* ResonateContext;
              const approval = yield* ctx.promise(Approval, { timeout: Duration.minutes(30) });
              const result = yield* approval.await.pipe(
                Effect.catch((error) =>
                  isDurablePromiseTimedOut(error) ? Effect.succeed(`timed-out:${error.id}`) : Effect.fail(error),
                ),
              );
              if (Predicate.isString(result)) {
                return result;
              }
              return yield* Effect.die("expected promise timeout");
            }),
        }),
      );
      const registry = yield* externalGroup.registry().pipe(Effect.provide(handlers));

      const handle = yield* client.beginRun(ExternalWorkflow, Protocol.ExecutionId.make("engine-external-timeout-1"), [
        "ok",
      ]);
      const root = yield* acquiredRoot(handle.id);
      yield* engine.execute({ task: root.task, promise: root.promise, registry });
      const state = yield* snap();
      const pending = state.promises.find((promise) => promise.id === "engine-external-timeout-1.approval");
      if (Predicate.isUndefined(pending)) {
        return yield* Effect.die("approval promise missing");
      }
      const approval = new Protocol.PromiseSettled({
        id: pending.id,
        state: Schema.Literal("rejected_timedout").make("rejected_timedout"),
        param: pending.param,
        value: pending.value,
        tags: pending.tags,
        timeoutAt: pending.timeoutAt,
        createdAt: pending.createdAt,
        settledAt: Option.some(pending.timeoutAt),
      });
      yield* engine.execute({ task: root.task, promise: root.promise, registry, preload: [approval] });
      const completed = yield* promises.get(handle.id);
      expect(completed.state).toBe("resolved");
      expect(yield* codec.decode(completed.value)).toBe("timed-out:engine-external-timeout-1.approval");
    }).pipe(Effect.provide(layer)),
  );

  it.effect("keeps named external promise ids stable when earlier steps are inserted", () =>
    Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const engine = yield* ExecutionEngine;
      const handlers = externalGroup.toLayer(
        externalGroup.of({
          ExternalWorkflow: () =>
            Effect.gen(function* (): Effect.fn.Return<string, unknown, ResonateContext> {
              const ctx = yield* ResonateContext;
              yield* ctx.run(Effect.succeed("before"));
              const approval = yield* ctx.promise(Approval);
              return approval.id;
            }),
        }),
      );
      const registry = yield* externalGroup.registry().pipe(Effect.provide(handlers));

      const handle = yield* client.beginRun(ExternalWorkflow, Protocol.ExecutionId.make("engine-external-stable-1"), [
        "ok",
      ]);
      const root = yield* acquiredRoot(handle.id);
      const outcome = yield* engine.execute({ task: root.task, promise: root.promise, registry });
      expect(Predicate.isTagged(outcome, "Suspended")).toBe(true);
      const state = yield* snap();
      expect(state.promises.some((promise) => promise.id === "engine-external-stable-1.0")).toBe(true);
      expect(state.promises.some((promise) => promise.id === "engine-external-stable-1.approval")).toBe(true);
    }).pipe(Effect.provide(layer)),
  );

  it.effect("records ctx.now once and replays the same instant after clock movement", () =>
    Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const engine = yield* ExecutionEngine;
      const promises = yield* DurablePromises;
      const codec = yield* currentCodec;
      const handlers = externalGroup.toLayer(
        externalGroup.of({
          ExternalWorkflow: () =>
            Effect.gen(function* (): Effect.fn.Return<number, unknown, ResonateContext> {
              const ctx = yield* ResonateContext;
              const observed = yield* ctx.now;
              const approval = yield* ctx.promise(Approval);
              yield* approval.await;
              return DateTime.toEpochMillis(observed);
            }),
        }),
      );
      const registry = yield* externalGroup.registry().pipe(Effect.provide(handlers));

      yield* TestClock.setTime(1_000);
      const handle = yield* client.beginRun(ExternalWorkflow, Protocol.ExecutionId.make("engine-now-1"), ["ok"]);
      const root = yield* acquiredRoot(handle.id);
      const suspended = yield* engine.execute({ task: root.task, promise: root.promise, registry });
      expect(Predicate.isTagged(suspended, "Suspended")).toBe(true);

      const state = yield* snap();
      const recordedNow = state.promises.find((promise) => promise.id === "engine-now-1.0");
      expect(recordedNow?.state).toBe("resolved");
      expect(yield* codec.decode(recordedNow?.value ?? Protocol.emptyValue)).toBe(1_000);

      yield* TestClock.setTime(99_000);
      yield* client.resolve(Approval, Approval.id(Protocol.ExecutionId.make("engine-now-1")), {
        approvedBy: "erik",
      });
      const replayState = yield* snap();
      const approval = replayState.promises.find((promise) => promise.id === "engine-now-1.approval");
      if (Predicate.isUndefined(approval) || Predicate.isUndefined(recordedNow)) {
        return yield* Effect.die("recorded promises missing");
      }
      yield* engine.execute({ task: root.task, promise: root.promise, registry, preload: [recordedNow, approval] });

      const completed = yield* promises.get(handle.id);
      expect(completed.state).toBe("resolved");
      expect(yield* codec.decode(completed.value)).toBe(1_000);
    }).pipe(Effect.provide(layer)),
  );

  it.effect("records ctx.random as a local durable step and consumes the sequence slot", () =>
    Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const engine = yield* ExecutionEngine;
      const codec = yield* currentCodec;
      const handlers = workflowGroup.toLayer(
        workflowGroup.of({
          Workflow: () =>
            Effect.gen(function* (): Effect.fn.Return<number, unknown, ResonateContext> {
              const ctx = yield* ResonateContext;
              const observed = yield* ctx.random;
              yield* ctx.run(Effect.succeed("after-random"));
              return observed;
            }),
        }),
      );
      const registry = yield* workflowGroup.registry().pipe(Effect.provide(handlers));

      const handle = yield* client.beginRun(Workflow, Protocol.ExecutionId.make("engine-random-1"), [0]);
      const root = yield* acquiredRoot(handle.id);
      const outcome = yield* engine.execute({ task: root.task, promise: root.promise, registry });
      expect(Predicate.isTagged(outcome, "Done")).toBe(true);

      const state = yield* snap();
      const recordedRandom = state.promises.find((promise) => promise.id === "engine-random-1.0");
      const nextStep = state.promises.find((promise) => promise.id === "engine-random-1.1");
      const value = yield* codec.decode(recordedRandom?.value ?? Protocol.emptyValue);
      expect(recordedRandom?.state).toBe("resolved");
      expect(nextStep?.state).toBe("resolved");
      expect(typeof value).toBe("number");
      if (!Predicate.isNumber(value)) {
        return yield* Effect.die("recorded random was not numeric");
      }
      expect(value).toBeGreaterThanOrEqual(0);
      expect(value).toBeLessThan(1);
    }).pipe(Effect.provide(layer)),
  );

  it.effect("defects on duplicate named external promises without explicit ids", () =>
    Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const engine = yield* ExecutionEngine;
      const handlers = externalGroup.toLayer(
        externalGroup.of({
          ExternalWorkflow: () =>
            Effect.gen(function* (): Effect.fn.Return<string, unknown, ResonateContext> {
              const ctx = yield* ResonateContext;
              yield* ctx.promise(Approval);
              yield* ctx.promise(Approval);
              return "unreachable";
            }),
        }),
      );
      const registry = yield* externalGroup.registry().pipe(Effect.provide(handlers));

      const handle = yield* client.beginRun(
        ExternalWorkflow,
        Protocol.ExecutionId.make("engine-external-duplicate-1"),
        ["ok"],
      );
      const root = yield* acquiredRoot(handle.id);
      const outcome = yield* engine.execute({ task: root.task, promise: root.promise, registry });
      expect(Predicate.isTagged(outcome, "Done")).toBe(true);
      const completed = yield* client.get(ExternalWorkflow, Protocol.ExecutionId.make("engine-external-duplicate-1"));
      const exit = yield* completed.await.pipe(Effect.exit);
      expect(Exit.isFailure(exit)).toBe(true);
    }).pipe(Effect.provide(layer)),
  );

  it.effect("defects when an externally settled value does not match the declaration schema", () =>
    Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const engine = yield* ExecutionEngine;
      const promises = yield* DurablePromises;
      const codec = yield* currentCodec;
      const handlers = externalGroup.toLayer(
        externalGroup.of({
          ExternalWorkflow: () =>
            Effect.gen(function* (): Effect.fn.Return<string, unknown, ResonateContext> {
              const ctx = yield* ResonateContext;
              const approval = yield* ctx.promise(Approval);
              const value = yield* approval.await;
              return value.approvedBy;
            }),
        }),
      );
      const registry = yield* externalGroup.registry().pipe(Effect.provide(handlers));

      const handle = yield* client.beginRun(
        ExternalWorkflow,
        Protocol.ExecutionId.make("engine-external-malformed-1"),
        ["ok"],
      );
      const root = yield* acquiredRoot(handle.id);
      yield* engine.execute({ task: root.task, promise: root.promise, registry });
      const settled = yield* promises.settle({
        id: Approval.id(Protocol.ExecutionId.make("engine-external-malformed-1")),
        state: Schema.Literal("resolved").make("resolved"),
        value: yield* codec.encode({ wrong: true }),
      });
      yield* engine.execute({ task: root.task, promise: root.promise, registry, preload: [settled] });
      const completed = yield* client.get(ExternalWorkflow, Protocol.ExecutionId.make("engine-external-malformed-1"));
      const exit = yield* completed.await.pipe(Effect.exit);
      expect(Exit.isFailure(exit)).toBe(true);
    }).pipe(Effect.provide(layer)),
  );

  it.effect("retries local steps with attempt visible in context", () =>
    Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const engine = yield* ExecutionEngine;
      const codec = yield* currentCodec;
      let calls = 0;
      const handlers = retryGroup.toLayer(
        retryGroup.of({
          RetryWorkflow: () =>
            Effect.gen(function* (): Effect.fn.Return<number, unknown, ResonateContext> {
              const ctx = yield* ResonateContext;
              return Number(
                yield* ctx.run(
                  Effect.gen(function* () {
                    calls = calls + 1;
                    if (calls < 3) {
                      return yield* new CardDeclined({ code: "retry" });
                    }
                    return ctx.info.attempt;
                  }),
                  { retryPolicy: RetryPolicy.exponential({ delay: Duration.millis(0), maxRetries: 3 }) },
                ),
              );
            }),
        }),
      );
      const registry = yield* retryGroup.registry().pipe(Effect.provide(handlers));

      const handle = yield* client.beginRun(RetryWorkflow, Protocol.ExecutionId.make("engine-retry-1"), [0]);
      const root = yield* acquiredRoot(handle.id);
      const outcome = yield* engine.execute({ task: root.task, promise: root.promise, registry });
      expect(Predicate.isTagged(outcome, "Done")).toBe(true);

      const completed = yield* DurablePromises.pipe(Effect.flatMap((promises) => promises.get(handle.id)));
      expect(yield* codec.decode(completed.value)).toBe(2);
      expect(calls).toBe(3);
    }).pipe(Effect.provide(layer)),
  );

  it.effect("does not retry non-retryable tagged errors", () =>
    Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const engine = yield* ExecutionEngine;
      let calls = 0;
      const handlers = retryGroup.toLayer(
        retryGroup.of({
          RetryWorkflow: () =>
            Effect.gen(function* (): Effect.fn.Return<string, unknown, ResonateContext> {
              const ctx = yield* ResonateContext;
              const result = yield* ctx
                .run(
                  Effect.gen(function* () {
                    calls = calls + 1;
                    return yield* new CardDeclined({ code: "stop" });
                  }),
                  {
                    retryPolicy: RetryPolicy.constant({ delay: Duration.seconds(1), maxRetries: 5 }),
                    nonRetryableErrors: [CardDeclined],
                  },
                )
                .pipe(Effect.exit);
              return Exit.isFailure(result) ? "stopped" : "unexpected";
            }),
        }),
      );
      const registry = yield* retryGroup.registry().pipe(Effect.provide(handlers));

      const handle = yield* client.beginRun(
        RetryWorkflow,
        Protocol.ExecutionId.make("engine-retry-nonretryable-1"),
        [0],
      );
      const root = yield* acquiredRoot(handle.id);
      yield* engine.execute({ task: root.task, promise: root.promise, registry });
      expect(calls).toBe(1);
    }).pipe(Effect.provide(layer)),
  );

  it.effect("stops retrying when the next retry would exceed the root timeout", () =>
    Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const engine = yield* ExecutionEngine;
      let calls = 0;
      const handlers = retryGroup.toLayer(
        retryGroup.of({
          RetryWorkflow: () =>
            Effect.gen(function* (): Effect.fn.Return<string, unknown, ResonateContext> {
              const ctx = yield* ResonateContext;
              const result = yield* ctx
                .run(
                  Effect.gen(function* () {
                    calls = calls + 1;
                    return yield* Effect.fail("retryable");
                  }),
                  { retryPolicy: RetryPolicy.constant({ delay: Duration.seconds(10), maxRetries: 5 }) },
                )
                .pipe(Effect.exit);
              return Exit.isFailure(result) ? "bounded" : "unexpected";
            }),
        }),
      );
      const registry = yield* retryGroup.registry().pipe(Effect.provide(handlers));

      const handle = yield* client.beginRun(RetryWorkflow, Protocol.ExecutionId.make("engine-retry-timeout-1"), [0], {
        timeout: Duration.seconds(5),
      });
      const root = yield* acquiredRoot(handle.id);
      yield* engine.execute({ task: root.task, promise: root.promise, registry });
      expect(calls).toBe(2);
    }).pipe(Effect.provide(layer)),
  );
});
