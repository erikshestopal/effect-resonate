import { describe, expect, it } from "@effect/vitest";
import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import { DateTime, Duration, Effect, Layer, Option, SchemaParser, Stream } from "effect";
import { TestClock } from "effect/testing";
import { ResonateNetwork } from "../src/network/network.ts";
import { makeRequestHead } from "../src/testing.ts";
import * as NetworkLocal from "../src/network/local.ts";
import * as Protocol from "../src/Protocol.ts";
import { assertInvariants } from "../src/testing.ts";

const layers = Layer.mergeAll(
  NetworkLocal.layer({ tickInterval: Duration.hours(24), retryTimeout: Duration.seconds(5) }),
  BunCrypto.layer,
);

const pid = (value: string) => Protocol.PromiseId.make(value);

const anycastDefault = Protocol.TargetAddress.pollAny(Protocol.WorkerGroup.make("default"));

const targetTags = Protocol.Tags.make({
  reserved: { "resonate:target": anycastDefault },
  unrecognized: {},
  user: {},
});

const timerTags = Protocol.Tags.make({
  reserved: { "resonate:timer": "true" },
  unrecognized: {},
  user: {},
});

const latentExternalTags = Protocol.Tags.make({
  reserved: { "resonate:scope": "global" },
  unrecognized: {},
  user: {},
});

const delayedTargetTags = (delay: DateTime.Utc) =>
  Protocol.Tags.make({
    reserved: { "resonate:target": anycastDefault, "resonate:delay": delay },
    unrecognized: {},
    user: {},
  });

const branchTargetTags = Protocol.Tags.make({
  reserved: { "resonate:target": anycastDefault, "resonate:branch": pid("branch") },
  unrecognized: {},
  user: {},
});

const branchTags = Protocol.Tags.make({
  reserved: { "resonate:branch": pid("branch") },
  unrecognized: {},
  user: {},
});

const at = (ms: number) => DateTime.makeUnsafe(ms);

const send = Effect.fn(function* <K extends Protocol.RequestKind>(request: Protocol.Request<K>) {
  const network = yield* ResonateNetwork;
  return yield* network.send(request);
});

const isCreated = SchemaParser.is(Protocol.PromiseCreateResponse.members[0]);
const isGot = SchemaParser.is(Protocol.PromiseGetResponse.members[0]);
const isSettled = SchemaParser.is(Protocol.PromiseSettleResponse.members[0]);
const isCallbackOk = SchemaParser.is(Protocol.PromiseRegisterCallbackResponse.members[0]);
const isListenerOk = SchemaParser.is(Protocol.PromiseRegisterListenerResponse.members[0]);
const isSnap = SchemaParser.is(Protocol.DebugSnapResponse.members[0]);
const isTaskCreated = SchemaParser.is(Protocol.TaskCreateResponse.members[0]);
const isTaskAcquired = SchemaParser.is(Protocol.TaskAcquireResponse.members[0]);
const isTaskGot = SchemaParser.is(Protocol.TaskGetResponse.members[0]);
const isTaskSuspended = SchemaParser.is(Protocol.TaskSuspendResponse.members[0]);
const isTaskSuspendPreloaded = SchemaParser.is(Protocol.TaskSuspendResponse.members[1]);
const isTaskFulfilled = SchemaParser.is(Protocol.TaskFulfillResponse.members[0]);
const isTaskFenced = SchemaParser.is(Protocol.TaskFenceResponse.members[0]);
const isScheduleGot = SchemaParser.is(Protocol.ScheduleGetResponse.members[0]);
const isScheduleCreated = SchemaParser.is(Protocol.ScheduleCreateResponse.members[0]);

const workerPid = Protocol.ProcessId.make("worker-a");
const workerPidB = Protocol.ProcessId.make("worker-b");
const lease = Duration.seconds(30);

const sendCreate = Effect.fn(function* (
  id: string,
  timeoutAtMs: number,
  tags = Protocol.emptyTags,
  param: Protocol.Value = { data: "cGFyYW0=" },
) {
  const head = yield* makeRequestHead;
  return yield* send(
    Protocol.PromiseCreateRequest.make({
      head,
      data: { id: pid(id), timeoutAt: at(timeoutAtMs), param, tags },
    }),
  );
});

const create = Effect.fn(function* (id: string, timeoutAtMs: number, tags = Protocol.emptyTags) {
  const response = yield* sendCreate(id, timeoutAtMs, tags);
  if (!isCreated(response)) {
    throw new Error(`promise.create failed: ${JSON.stringify(response.data)}`);
  }
  return response.data.promise;
});

const getPromise = Effect.fn(function* (id: string) {
  const head = yield* makeRequestHead;
  const response = yield* send(Protocol.PromiseGetRequest.make({ head, data: { id: pid(id) } }));
  if (!isGot(response)) {
    throw new Error(`promise.get failed: ${JSON.stringify(response.data)}`);
  }
  return response.data.promise;
});

const settle = Effect.fn(function* (id: string, state: "resolved" | "rejected" | "rejected_canceled", data = "NDI=") {
  const head = yield* makeRequestHead;
  return yield* send(Protocol.PromiseSettleRequest.make({ head, data: { id: pid(id), state, value: { data } } }));
});

const sendCallback = Effect.fn(function* (awaited: string, awaiter: string) {
  const head = yield* makeRequestHead;
  return yield* send(
    Protocol.PromiseRegisterCallbackRequest.make({
      head,
      data: { awaited: pid(awaited), awaiter: pid(awaiter) },
    }),
  );
});

const sendListener = Effect.fn(function* (awaited: string, address = anycastDefault) {
  const head = yield* makeRequestHead;
  return yield* send(
    Protocol.PromiseRegisterListenerRequest.make({
      head,
      data: { awaited: pid(awaited), address },
    }),
  );
});

const snap = Effect.fn(function* () {
  const head = yield* makeRequestHead;
  const response = yield* send(Protocol.DebugSnapRequest.make({ head, data: {} }));
  if (!isSnap(response)) {
    throw new Error("debug.snap failed");
  }
  yield* assertInvariants(response.data);
  return response.data;
});

const tick = Effect.fn(function* (timeMs: number) {
  const head = yield* makeRequestHead;
  return yield* send(Protocol.DebugTickRequest.make({ head, data: { time: at(timeMs) } }));
});

const taskCreate = Effect.fn(function* (id: string, timeoutAtMs: number, tags = targetTags, requestPid = workerPid) {
  const head = yield* makeRequestHead;
  const actionHead = yield* makeRequestHead;
  return yield* send(
    Protocol.TaskCreateRequest.make({
      head,
      data: {
        pid: requestPid,
        ttl: lease,
        action: Protocol.PromiseCreateRequest.make({
          head: actionHead,
          data: { id: pid(id), timeoutAt: at(timeoutAtMs), param: { data: "cGFyYW0=" }, tags },
        }),
      },
    }),
  );
});

const taskAcquire = Effect.fn(function* (id: string, version: number, requestPid = workerPid) {
  const head = yield* makeRequestHead;
  return yield* send(
    Protocol.TaskAcquireRequest.make({
      head,
      data: {
        id: pid(id),
        version: Protocol.TaskVersion.make(version),
        pid: requestPid,
        ttl: lease,
      },
    }),
  );
});

const taskRelease = Effect.fn(function* (id: string, version: number) {
  const head = yield* makeRequestHead;
  return yield* send(
    Protocol.TaskReleaseRequest.make({
      head,
      data: { id: pid(id), version: Protocol.TaskVersion.make(version) },
    }),
  );
});

const taskSuspend = Effect.fn(function* (id: string, version: number, awaited: ReadonlyArray<string>) {
  const head = yield* makeRequestHead;
  const actions = [];
  for (const awaitedId of awaited) {
    const actionHead = yield* makeRequestHead;
    actions.push(
      Protocol.PromiseRegisterCallbackRequest.make({
        head: actionHead,
        data: { awaited: pid(awaitedId), awaiter: pid(id) },
      }),
    );
  }
  return yield* send(
    Protocol.TaskSuspendRequest.make({
      head,
      data: { id: pid(id), version: Protocol.TaskVersion.make(version), actions },
    }),
  );
});

const taskFulfill = Effect.fn(function* (
  id: string,
  version: number,
  state: "resolved" | "rejected" | "rejected_canceled" = "resolved",
) {
  const head = yield* makeRequestHead;
  const actionHead = yield* makeRequestHead;
  return yield* send(
    Protocol.TaskFulfillRequest.make({
      head,
      data: {
        id: pid(id),
        version: Protocol.TaskVersion.make(version),
        action: Protocol.PromiseSettleRequest.make({
          head: actionHead,
          data: { id: pid(id), state, value: { data: "dmFsdWU=" } },
        }),
      },
    }),
  );
});

const taskFenceSettle = Effect.fn(function* (taskId: string, version: number, promiseId: string) {
  const head = yield* makeRequestHead;
  const actionHead = yield* makeRequestHead;
  return yield* send(
    Protocol.TaskFenceRequest.make({
      head,
      data: {
        id: pid(taskId),
        version: Protocol.TaskVersion.make(version),
        action: Protocol.PromiseSettleRequest.make({
          head: actionHead,
          data: { id: pid(promiseId), state: "resolved", value: { data: "ZmVuY2Vk" } },
        }),
      },
    }),
  );
});

const taskHalt = Effect.fn(function* (id: string) {
  const head = yield* makeRequestHead;
  return yield* send(Protocol.TaskHaltRequest.make({ head, data: { id: pid(id) } }));
});

const taskContinue = Effect.fn(function* (id: string) {
  const head = yield* makeRequestHead;
  return yield* send(Protocol.TaskContinueRequest.make({ head, data: { id: pid(id) } }));
});

const taskHeartbeat = Effect.fn(function* (id: string, version: number, requestPid = workerPid) {
  const head = yield* makeRequestHead;
  return yield* send(
    Protocol.TaskHeartbeatRequest.make({
      head,
      data: {
        pid: requestPid,
        tasks: [{ id: pid(id), version: Protocol.TaskVersion.make(version) }],
      },
    }),
  );
});

const scheduleCreate = Effect.fn(function* (
  id: string,
  cron = "* * * * *",
  promiseId = "{{.id}}.{{.timestamp}}",
  promiseTimeout = Duration.seconds(30),
  promiseTags = Protocol.emptyTags,
  promiseParam: Protocol.Value = { data: "c2NoZWR1bGU=" },
) {
  const head = yield* makeRequestHead;
  return yield* send(
    Protocol.ScheduleCreateRequest.make({
      head,
      data: {
        id: Protocol.ScheduleId.make(id),
        cron,
        promiseId,
        promiseTimeout,
        promiseParam,
        promiseTags,
      },
    }),
  );
});

const scheduleGet = Effect.fn(function* (id: string) {
  const head = yield* makeRequestHead;
  return yield* send(Protocol.ScheduleGetRequest.make({ head, data: { id: Protocol.ScheduleId.make(id) } }));
});

const scheduleDelete = Effect.fn(function* (id: string) {
  const head = yield* makeRequestHead;
  return yield* send(Protocol.ScheduleDeleteRequest.make({ head, data: { id: Protocol.ScheduleId.make(id) } }));
});

describe("P-02 promise.create", () => {
  it.effect("creates a pending promise and echoes the record", () =>
    Effect.gen(function* () {
      const promise = yield* create("p1", 10_000);
      expect(promise.state).toBe("pending");
      expect(DateTime.toEpochMillis(promise.createdAt)).toBe(0);
      expect((yield* getPromise("p1")).state).toBe("pending");
    }).pipe(Effect.provide(layers)),
  );

  it.effect("idempotent re-create returns the stored record and ignores the body", () =>
    Effect.gen(function* () {
      yield* create("p1", 10_000);
      const second = yield* sendCreate("p1", 99_999, timerTags, { data: "ZGlmZmVyZW50" });
      if (!isCreated(second)) {
        throw new Error("expected 200");
      }
      expect(DateTime.toEpochMillis(second.data.promise.timeoutAt)).toBe(10_000);
      expect(second.data.promise.param).toEqual({ data: "cGFyYW0=" });
      expect(second.data.promise.tags.isTimer).toBe(false);
    }).pipe(Effect.provide(layers)),
  );

  it.effect("a target tag mints a pending companion task and dispatches execute", () =>
    Effect.gen(function* () {
      const network = yield* ResonateNetwork;
      yield* create("p1", 10_000, targetTags);
      const state = yield* snap();
      expect(state.tasks).toHaveLength(1);
      expect(state.tasks[0]?.state).toBe("pending");
      expect(state.tasks[0]?.version).toBe(0);

      expect(state.taskTimeouts).toEqual([{ id: "p1", type: 0, timeout: at(5_000) }]);
      const [message] = yield* Stream.runCollect(Stream.take(network.messages, 1));
      expect(message?.kind).toBe("execute");
      if (message?.kind === "execute") {
        expect(message.data.task).toEqual({ id: "p1", version: 0 });
      }
    }).pipe(Effect.provide(layers)),
  );

  it.effect("a future resonate:delay defers dispatch to the delay instant", () =>
    Effect.gen(function* () {
      yield* create("p1", 60_000, delayedTargetTags(at(7_000)));
      const state = yield* snap();
      expect(state.taskTimeouts).toEqual([{ id: "p1", type: 0, timeout: at(7_000) }]);
      expect(state.messages).toHaveLength(0);
    }).pipe(Effect.provide(layers)),
  );

  it.effect("a past resonate:delay dispatches immediately", () =>
    Effect.gen(function* () {
      yield* TestClock.adjust(Duration.millis(10_000));
      yield* create("p1", 60_000, delayedTargetTags(at(2_000)));
      const state = yield* snap();
      expect(state.taskTimeouts).toEqual([{ id: "p1", type: 0, timeout: at(15_000) }]);
      expect(state.messages).toHaveLength(1);
    }).pipe(Effect.provide(layers)),
  );

  it.effect("timeoutAt <= now births a settled promise, backdated, without dispatch", () =>
    Effect.gen(function* () {
      yield* TestClock.adjust(Duration.millis(5_000));
      const plain = yield* create("plain", 1_000, targetTags);
      expect(plain.state).toBe("rejected_timedout");
      expect(DateTime.toEpochMillis(plain.createdAt)).toBe(1_000);
      if (plain.state !== "pending") {
        expect(Option.map(plain.settledAt, DateTime.toEpochMillis)).toEqual(Option.some(1_000));
      }
      const timer = yield* create("timer", 1_000, timerTags);
      expect(timer.state).toBe("resolved");
      const state = yield* snap();
      expect(state.tasks).toEqual([{ id: "plain", state: "fulfilled", version: 0, resumes: 0 }]);
      expect(state.promiseTimeouts).toHaveLength(0);
      expect(state.taskTimeouts).toHaveLength(0);
      expect(state.messages).toHaveLength(0);
    }).pipe(Effect.provide(layers)),
  );

  it.effect("only external promises arm a promise timeout", () =>
    Effect.gen(function* () {
      yield* create("internal", 10_000);
      yield* create("latent", 10_000, latentExternalTags);
      yield* create("external", 10_000, targetTags);
      yield* create("timer", 10_000, timerTags);
      const state = yield* snap();
      expect(state.promiseTimeouts.map((entry) => entry.id).sort()).toEqual(["external", "latent", "timer"]);
    }).pipe(Effect.provide(layers)),
  );
});

describe("P-03 promise.settle", () => {
  it.effect("settles a fresh pending promise and clears registrations", () =>
    Effect.gen(function* () {
      yield* create("p1", 10_000, targetTags);
      yield* TestClock.adjust(Duration.millis(1_000));
      const response = yield* settle("p1", "resolved");
      if (!isSettled(response)) {
        throw new Error("expected 200");
      }
      const record = response.data.promise;
      expect(record.state).toBe("resolved");
      if (record.state !== "pending") {
        expect(Option.map(record.settledAt, DateTime.toEpochMillis)).toEqual(Option.some(1_000));
        expect(record.value).toEqual({ data: "NDI=" });
      }
      const state = yield* snap();

      expect(state.tasks).toEqual([{ id: "p1", state: "fulfilled", version: 0, resumes: 0 }]);
      expect(state.taskTimeouts).toHaveLength(0);
      expect(state.promiseTimeouts).toHaveLength(0);
    }).pipe(Effect.provide(layers)),
  );

  it.effect("is idempotent once settled — the second request's body is ignored", () =>
    Effect.gen(function* () {
      yield* create("p1", 10_000);
      yield* settle("p1", "resolved");
      const second = yield* settle("p1", "rejected", "b3RoZXI=");
      if (!isSettled(second)) {
        throw new Error("expected 200");
      }
      expect(second.data.promise.state).toBe("resolved");
      expect(second.data.promise.value).toEqual({ data: "NDI=" });
    }).pipe(Effect.provide(layers)),
  );

  it.effect("a settle racing the timeout returns the projected outcome, unpersisted", () =>
    Effect.gen(function* () {
      yield* create("p1", 10_000);
      yield* TestClock.adjust(Duration.millis(10_000));
      const response = yield* settle("p1", "resolved");
      if (!isSettled(response)) {
        throw new Error("expected 200");
      }
      expect(response.data.promise.state).toBe("rejected_timedout");

      const state = yield* snap();
      expect(state.promises[0]?.state).toBe("pending");
    }).pipe(Effect.provide(layers)),
  );

  it.effect("notifies listeners exactly once and resumes callbacks via the cascade", () =>
    Effect.gen(function* () {
      const network = yield* ResonateNetwork;

      yield* create("w", 60_000, targetTags);
      yield* create("b", 60_000);
      yield* sendCallback("b", "w");
      yield* sendListener("b");
      yield* settle("b", "resolved");

      const messages = yield* Stream.runCollect(Stream.take(network.messages, 2));
      expect(messages.map((message) => message.kind)).toEqual(["execute", "unblock"]);

      const state = yield* snap();
      expect(state.tasks).toEqual([{ id: "w", state: "pending", version: 0, resumes: 1 }]);

      yield* settle("b", "resolved");
      expect((yield* snap()).messages).toHaveLength(2);
    }).pipe(Effect.provide(layers)),
  );
});

describe("P-04 promise.register_callback", () => {
  it.effect("rejects malformed and unresolvable registrations", () =>
    Effect.gen(function* () {
      yield* create("w", 60_000, targetTags);
      yield* create("plain", 60_000);
      yield* create("b", 60_000);

      const self = yield* sendCallback("b", "b");
      expect(self.head.status).toBe(400);

      const missingAwaited = yield* sendCallback("nope", "w");
      expect(missingAwaited.head.status).toBe(404);

      const missingAwaiter = yield* sendCallback("b", "nope");
      expect(missingAwaiter.head.status).toBe(422);

      const targetless = yield* sendCallback("b", "plain");
      expect(targetless.head.status).toBe(422);
    }).pipe(Effect.provide(layers)),
  );

  it.effect("registers only when both sides are pending and fresh", () =>
    Effect.gen(function* () {
      yield* create("w", 60_000, targetTags);
      yield* create("b", 60_000);
      yield* sendCallback("b", "w");
      expect((yield* snap()).callbacks).toEqual([{ awaiter: "w", awaited: "b" }]);

      yield* create("done", 60_000);
      yield* settle("done", "resolved");
      const onSettled = yield* sendCallback("done", "w");
      expect(isCallbackOk(onSettled)).toBe(true);
      expect((yield* snap()).callbacks).toHaveLength(1);

      yield* create("expiring", 5_000, targetTags);
      yield* TestClock.adjust(Duration.millis(5_000));
      yield* create("b2", 60_000);
      const expiredAwaiter = yield* sendCallback("b2", "expiring");
      expect(isCallbackOk(expiredAwaiter)).toBe(true);
      expect((yield* snap()).callbacks).toHaveLength(1);
    }).pipe(Effect.provide(layers)),
  );

  it.effect("an expired awaited returns the projection", () =>
    Effect.gen(function* () {
      yield* create("w", 60_000, targetTags);
      yield* create("b", 5_000);
      yield* TestClock.adjust(Duration.millis(5_000));
      const response = yield* sendCallback("b", "w");
      if (!isCallbackOk(response)) {
        throw new Error("expected 200");
      }
      expect(response.data.promise.state).toBe("rejected_timedout");
    }).pipe(Effect.provide(layers)),
  );
});

describe("P-05 promise.register_listener", () => {
  it.effect("registers listeners keyed by address and 404s on absent promises", () =>
    Effect.gen(function* () {
      const missing = yield* sendListener("nope");
      expect(missing.head.status).toBe(404);

      yield* create("b", 60_000);
      yield* sendListener("b");
      yield* sendListener("b");
      expect((yield* snap()).listeners).toEqual([{ id: "b", address: "poll://any@default" }]);
    }).pipe(Effect.provide(layers)),
  );
});

describe("timeout projection and the tick (P-01 + onPromiseTimeout)", () => {
  it.effect("get observes the projection at the timeout instant, before the tick persists it", () =>
    Effect.gen(function* () {
      yield* create("plain", 10_000, targetTags);
      yield* create("latent", 10_000, latentExternalTags);
      yield* create("timer", 10_000, timerTags);
      yield* TestClock.adjust(Duration.millis(10_000));

      expect((yield* getPromise("plain")).state).toBe("rejected_timedout");
      expect((yield* getPromise("latent")).state).toBe("rejected_timedout");
      expect((yield* getPromise("timer")).state).toBe("resolved");

      const before = yield* snap();
      expect(before.promises.every((promise) => promise.state === "pending")).toBe(true);

      yield* tick(10_000);

      const after = yield* snap();
      expect(after.promises.map((promise) => promise.state).sort()).toEqual([
        "rejected_timedout",
        "rejected_timedout",
        "resolved",
      ]);
      const persistedPlain = yield* getPromise("plain");
      expect(persistedPlain.state).toBe("rejected_timedout");
      if (persistedPlain.state !== "pending") {
        expect(Option.map(persistedPlain.settledAt, DateTime.toEpochMillis)).toEqual(Option.some(10_000));
      }

      expect(after.tasks).toEqual([{ id: "plain", state: "fulfilled", version: 0, resumes: 0 }]);
      expect(after.promiseTimeouts).toHaveLength(0);
    }).pipe(Effect.provide(layers)),
  );

  it.effect("the tick cascade notifies listeners and buffers resumes exactly once", () =>
    Effect.gen(function* () {
      const network = yield* ResonateNetwork;
      yield* create("w", 60_000, targetTags);
      yield* create("b", 10_000, timerTags);
      yield* sendCallback("b", "w");
      yield* sendListener("b");
      yield* TestClock.adjust(Duration.millis(10_000));
      yield* tick(10_000);
      yield* tick(11_000);
      const state = yield* snap();
      expect(state.tasks.find((task) => task.id === "w")?.resumes).toBe(1);
      const messages = yield* Stream.runCollect(Stream.take(network.messages, 2));
      expect(messages.map((message) => message.kind)).toEqual(["execute", "unblock"]);
      expect(state.messages).toHaveLength(2);
    }).pipe(Effect.provide(layers)),
  );

  it.effect("the projection formula is identical across get/create/settle/register paths", () =>
    Effect.gen(function* () {
      yield* create("w", 60_000, targetTags);
      yield* create("b", 10_000);
      yield* TestClock.adjust(Duration.millis(10_000));

      const viaGet = yield* getPromise("b");
      const viaCreate = yield* sendCreate("b", 99_000, Protocol.emptyTags, {});
      const viaSettle = yield* settle("b", "resolved");
      const viaCallback = yield* sendCallback("b", "w");
      const viaListener = yield* sendListener("b");

      for (const response of [viaCreate, viaSettle, viaCallback, viaListener]) {
        expect(response.head.status).toBe(200);
        if (isCreated(viaCreate) && isSettled(viaSettle) && isCallbackOk(viaCallback) && isListenerOk(viaListener)) {
        }
      }
      if (!isCreated(viaCreate) || !isSettled(viaSettle) || !isCallbackOk(viaCallback) || !isListenerOk(viaListener)) {
        throw new Error("expected 200s");
      }
      for (const record of [
        viaGet,
        viaCreate.data.promise,
        viaSettle.data.promise,
        viaCallback.data.promise,
        viaListener.data.promise,
      ]) {
        expect(record.state).toBe("rejected_timedout");
        if (record.state !== "pending") {
          expect(Option.map(record.settledAt, DateTime.toEpochMillis)).toEqual(Option.some(10_000));
        }
      }
    }).pipe(Effect.provide(layers)),
  );
});

describe("T-01…T-10 task state machine", () => {
  it.effect("task.create creates acquired tasks, re-acquires pending tasks, and is idempotent once fulfilled", () =>
    Effect.gen(function* () {
      const first = yield* taskCreate("t1", 60_000);
      if (!isTaskCreated(first)) {
        throw new Error("expected task.create 200");
      }
      expect(first.data.task?.state).toBe("acquired");
      expect(first.data.task?.version).toBe(1);
      expect((yield* snap()).taskTimeouts).toEqual([{ id: "t1", type: 1, timeout: at(30_000) }]);

      yield* taskRelease("t1", 1);
      expect((yield* snap()).tasks[0]).toEqual({
        id: "t1",
        state: "pending",
        version: 1,
        resumes: 0,
      });

      const reacquire = yield* taskCreate("t1", 60_000, targetTags, workerPidB);
      if (!isTaskCreated(reacquire)) {
        throw new Error("expected task.create reacquire 200");
      }
      expect(reacquire.data.task?.state).toBe("acquired");
      expect(reacquire.data.task?.version).toBe(2);

      const fulfilled = yield* taskFulfill("t1", 2);
      if (!isTaskFulfilled(fulfilled)) {
        throw new Error("expected task.fulfill 200");
      }
      const idempotent = yield* taskCreate("t1", 60_000);
      if (!isTaskCreated(idempotent)) {
        throw new Error("expected task.create idempotent 200");
      }
      expect(idempotent.data.task?.state).toBe("fulfilled");
      yield* snap();
    }).pipe(Effect.provide(layers)),
  );

  it.effect("task.acquire fences by version and lease expiry does not bump until the next acquire", () =>
    Effect.gen(function* () {
      yield* create("t1", 60_000, targetTags);
      const acquired = yield* taskAcquire("t1", 0);
      if (!isTaskAcquired(acquired)) {
        throw new Error("expected task.acquire 200");
      }
      expect(acquired.data.task.version).toBe(1);

      yield* TestClock.adjust(Duration.seconds(30));
      yield* tick(30_000);
      const afterLease = yield* snap();
      expect(afterLease.tasks[0]).toEqual({ id: "t1", state: "pending", version: 1, resumes: 0 });

      const stale = yield* taskFulfill("t1", 1);
      expect(stale.head.status).toBe(409);

      const next = yield* taskAcquire("t1", 1, workerPidB);
      if (!isTaskAcquired(next)) {
        throw new Error("expected second acquire 200");
      }
      expect(next.data.task.version).toBe(2);
      const staleAgain = yield* taskFulfill("t1", 1);
      expect(staleAgain.head.status).toBe(409);
      expect((yield* taskFulfill("t1", 2)).head.status).toBe(200);
      yield* snap();
    }).pipe(Effect.provide(layers)),
  );

  it.effect("task.get projects expired tasks to fulfilled without persisting", () =>
    Effect.gen(function* () {
      yield* taskCreate("t1", 10_000);
      yield* TestClock.adjust(Duration.millis(10_000));
      const head = yield* makeRequestHead;
      const got = yield* send(Protocol.TaskGetRequest.make({ head, data: { id: pid("t1") } }));
      if (!isTaskGot(got)) {
        throw new Error("expected task.get 200");
      }
      expect(got.data.task.state).toBe("fulfilled");
      expect((yield* snap()).tasks[0]?.state).toBe("acquired");
    }).pipe(Effect.provide(layers)),
  );

  it.effect("task.suspend registers callbacks atomically and the 300 fast path clears buffered resumes", () =>
    Effect.gen(function* () {
      yield* taskCreate("w", 60_000, branchTargetTags);
      yield* create("a", 60_000, branchTags);
      const suspended = yield* taskSuspend("w", 1, ["a"]);
      expect(isTaskSuspended(suspended)).toBe(true);
      let state = yield* snap();
      expect(state.tasks[0]?.state).toBe("suspended");
      expect(state.callbacks).toEqual([{ awaiter: "w", awaited: "a" }]);
      expect(state.taskTimeouts).toHaveLength(0);

      yield* settle("a", "resolved");
      state = yield* snap();
      expect(state.tasks.find((task) => task.id === "w")).toEqual({
        id: "w",
        state: "pending",
        version: 1,
        resumes: 1,
      });

      const reacquired = yield* taskAcquire("w", 1);
      if (!isTaskAcquired(reacquired)) {
        throw new Error("expected reacquire");
      }
      const fastPath = yield* taskSuspend("w", 2, ["a"]);
      if (!isTaskSuspendPreloaded(fastPath)) {
        throw new Error("expected task.suspend 300");
      }
      expect(fastPath.data.preload.map((promise) => promise.id)).toContain("a");
      expect((yield* snap()).tasks.find((task) => task.id === "w")?.state).toBe("acquired");
    }).pipe(Effect.provide(layers)),
  );

  it.effect("resume buffering while acquired is deduped and causes suspend 300 without registering callbacks", () =>
    Effect.gen(function* () {
      yield* taskCreate("w", 60_000);
      yield* create("a", 60_000);
      yield* sendCallback("a", "w");
      yield* settle("a", "resolved");
      let state = yield* snap();
      expect(state.tasks.find((task) => task.id === "w")?.resumes).toBe(1);
      expect(state.tasks.find((task) => task.id === "w")?.state).toBe("acquired");

      const fastPath = yield* taskSuspend("w", 1, ["a"]);
      expect(isTaskSuspendPreloaded(fastPath)).toBe(true);
      state = yield* snap();
      expect(state.callbacks).toHaveLength(0);
      expect(state.tasks.find((task) => task.id === "w")?.resumes).toBe(0);
    }).pipe(Effect.provide(layers)),
  );

  it.effect("task.fence delegates promise actions under the task version gate", () =>
    Effect.gen(function* () {
      yield* taskCreate("owner", 60_000);
      yield* create("child", 60_000);
      const fenced = yield* taskFenceSettle("owner", 1, "child");
      if (!isTaskFenced(fenced)) {
        throw new Error("expected task.fence 200");
      }
      expect(fenced.data.action.kind).toBe("promise.settle");
      expect((yield* getPromise("child")).state).toBe("resolved");
      expect((yield* taskFenceSettle("owner", 0, "child")).head.status).toBe(409);
      yield* snap();
    }).pipe(Effect.provide(layers)),
  );

  it.effect("heartbeat silently refreshes only matching acquired tasks using stored ttl", () =>
    Effect.gen(function* () {
      yield* taskCreate("t1", 60_000);
      yield* TestClock.adjust(Duration.seconds(5));
      expect((yield* taskHeartbeat("t1", 1, workerPidB)).head.status).toBe(200);
      expect((yield* snap()).taskTimeouts).toEqual([{ id: "t1", type: 1, timeout: at(30_000) }]);
      expect((yield* taskHeartbeat("t1", 1)).head.status).toBe(200);
      expect((yield* snap()).taskTimeouts).toEqual([{ id: "t1", type: 1, timeout: at(35_000) }]);
    }).pipe(Effect.provide(layers)),
  );

  it.effect("halt and continue move through halted without bumping the version", () =>
    Effect.gen(function* () {
      yield* taskCreate("t1", 60_000);
      expect((yield* taskHalt("t1")).head.status).toBe(200);
      let state = yield* snap();
      expect(state.tasks[0]).toEqual({ id: "t1", state: "halted", version: 1, resumes: 0 });
      expect(state.taskTimeouts).toHaveLength(0);

      expect((yield* taskContinue("t1")).head.status).toBe(200);
      state = yield* snap();
      expect(state.tasks[0]).toEqual({ id: "t1", state: "pending", version: 1, resumes: 0 });
      expect(state.taskTimeouts).toEqual([{ id: "t1", type: 0, timeout: at(5_000) }]);
    }).pipe(Effect.provide(layers)),
  );

  it.effect("pending retry timeout redelivers execute and rearms itself", () =>
    Effect.gen(function* () {
      yield* create("t1", 60_000, targetTags);
      yield* TestClock.adjust(Duration.seconds(5));
      const response = yield* tick(5_000);
      expect(response.head.status).toBe(200);
      const state = yield* snap();
      expect(state.taskTimeouts).toEqual([{ id: "t1", type: 0, timeout: at(10_000) }]);
      expect(state.messages).toHaveLength(1);
      expect(state.messages[0]?.message.kind).toBe("execute");
    }).pipe(Effect.provide(layers)),
  );

  it.effect("task.create follows shipped server by allowing a fresh targetless action", () =>
    Effect.gen(function* () {
      const response = yield* taskCreate("local-step", 60_000, Protocol.emptyTags);
      if (!isTaskCreated(response)) {
        throw new Error("expected task.create 200");
      }
      expect(response.data.task?.state).toBe("acquired");
      const promise = (yield* snap()).promises[0];
      expect(promise?.tags.reserved).toEqual({});
      expect(promise?.tags.user).toEqual({});
      expect(promise?.tags.unrecognized).toEqual({});
    }).pipe(Effect.provide(layers)),
  );
});

describe("S-01…S-03 schedules and catch-up", () => {
  it.effect("creates, gets, and idempotently re-creates schedules", () =>
    Effect.gen(function* () {
      const created = yield* scheduleCreate("nightly", "* * * * *", "run-{{.id}}-{{.timestamp}}");
      if (!isScheduleCreated(created)) {
        throw new Error("expected schedule.create 200");
      }
      expect(DateTime.toEpochMillis(created.data.schedule.createdAt)).toBe(0);
      expect(DateTime.toEpochMillis(created.data.schedule.nextRunAt)).toBe(60_000);
      expect(created.data.schedule.lastRunAt).toEqual(Option.none());

      const changed = yield* scheduleCreate("nightly", "*/5 * * * *", "changed");
      if (!isScheduleCreated(changed)) {
        throw new Error("expected idempotent schedule.create 200");
      }
      expect(changed.data.schedule.cron).toBe("* * * * *");
      expect(changed.data.schedule.promiseId).toBe("run-{{.id}}-{{.timestamp}}");

      const got = yield* scheduleGet("nightly");
      if (!isScheduleGot(got)) {
        throw new Error("expected schedule.get 200");
      }
      expect(got.data.schedule).toEqual(changed.data.schedule);
      yield* snap();
    }).pipe(Effect.provide(layers)),
  );

  it.effect("fires a due schedule with expanded id, backdated promise timing, and task dispatch", () =>
    Effect.gen(function* () {
      yield* scheduleCreate("job", "* * * * *", "{{.id}}.{{.timestamp}}", Duration.seconds(30), targetTags);
      yield* TestClock.adjust(Duration.millis(60_000));
      const response = yield* tick(60_000);
      expect(response.head.status).toBe(200);

      const state = yield* snap();
      const promise = state.promises.find((promise) => promise.id === "job.60000");
      expect(promise?.state).toBe("pending");
      expect(DateTime.toEpochMillis(promise?.createdAt ?? at(-1))).toBe(60_000);
      expect(DateTime.toEpochMillis(promise?.timeoutAt ?? at(-1))).toBe(90_000);
      expect(state.tasks.find((task) => task.id === "job.60000")).toEqual({
        id: "job.60000",
        state: "pending",
        version: 0,
        resumes: 0,
      });
      expect(state.taskTimeouts).toContainEqual({ id: "job.60000", type: 0, timeout: at(65_000) });
      expect(state.messages.at(-1)?.message.kind).toBe("execute");

      const got = yield* scheduleGet("job");
      if (!isScheduleGot(got)) {
        throw new Error("expected schedule.get 200");
      }
      expect(Option.map(got.data.schedule.lastRunAt, DateTime.toEpochMillis)).toEqual(Option.some(60_000));
      expect(DateTime.toEpochMillis(got.data.schedule.nextRunAt)).toBe(120_000);
    }).pipe(Effect.provide(layers)),
  );

  it.effect("catches up one promise per missed cron tick at each historical time", () =>
    Effect.gen(function* () {
      yield* scheduleCreate("catch", "* * * * *", "p-{{.timestamp}}", Duration.seconds(10));
      yield* TestClock.adjust(Duration.millis(180_000));
      yield* tick(180_000);
      const state = yield* snap();

      expect(state.promises.map((promise) => promise.id).sort()).toEqual(["p-120000", "p-180000", "p-60000"]);
      expect(state.promises.map((promise) => promise.tags.unrecognized["resonate:schedule"])).toEqual([
        "catch",
        "catch",
        "catch",
      ]);
      expect(
        state.promises
          .map((promise) => ({
            id: promise.id,
            createdAt: DateTime.toEpochMillis(promise.createdAt),
            timeoutAt: DateTime.toEpochMillis(promise.timeoutAt),
          }))
          .sort((left, right) => left.createdAt - right.createdAt),
      ).toEqual([
        { id: "p-60000", createdAt: 60_000, timeoutAt: 70_000 },
        { id: "p-120000", createdAt: 120_000, timeoutAt: 130_000 },
        { id: "p-180000", createdAt: 180_000, timeoutAt: 190_000 },
      ]);
      const got = yield* scheduleGet("catch");
      if (!isScheduleGot(got)) {
        throw new Error("expected schedule.get 200");
      }
      expect(Option.map(got.data.schedule.lastRunAt, DateTime.toEpochMillis)).toEqual(Option.some(180_000));
      expect(DateTime.toEpochMillis(got.data.schedule.nextRunAt)).toBe(240_000);
    }).pipe(Effect.provide(layers)),
  );

  it.effect("deletes schedules and disarms future firing", () =>
    Effect.gen(function* () {
      yield* scheduleCreate("gone");
      expect((yield* scheduleDelete("missing")).head.status).toBe(404);
      expect((yield* scheduleDelete("gone")).head.status).toBe(200);
      expect((yield* scheduleGet("gone")).head.status).toBe(404);

      yield* TestClock.adjust(Duration.millis(180_000));
      yield* tick(180_000);
      expect((yield* snap()).promises).toHaveLength(0);
    }).pipe(Effect.provide(layers)),
  );

  it.effect("accepts five-field cron but rejects six-field seconds", () =>
    Effect.gen(function* () {
      expect((yield* scheduleCreate("five", "*/5 * * * *")).head.status).toBe(200);
      expect((yield* scheduleCreate("six", "0 */5 * * * *")).head.status).toBe(400);
      yield* snap();
    }).pipe(Effect.provide(layers)),
  );
});

describe("unimplemented surfaces", () => {
  it.effect("searches return 501", () =>
    Effect.gen(function* () {
      const head1 = yield* makeRequestHead;
      const search = yield* send(Protocol.PromiseSearchRequest.make({ head: head1, data: {} }));
      expect(search.head.status).toBe(501);
      const head3 = yield* makeRequestHead;
      const schedule = yield* send(
        Protocol.ScheduleSearchRequest.make({
          head: head3,
          data: {},
        }),
      );
      expect(schedule.head.status).toBe(501);
    }).pipe(Effect.provide(layers)),
  );
});
