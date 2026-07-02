import { describe, expect, it } from "@effect/vitest";
import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import { DateTime, Duration, Effect, Layer, Option, SchemaParser, Stream } from "effect";
import { TestClock } from "effect/testing";
import { makeRequestHead, ResonateNetwork } from "../src/Network.ts";
import * as NetworkLocal from "../src/NetworkLocal.ts";
import * as Protocol from "../src/Protocol.ts";

// The tick interval is set far out so tests drive convergence deterministically
// via explicit debug.tick requests; TestClock stays in control of time.
const layers = Layer.mergeAll(
  NetworkLocal.layer({ tickInterval: Duration.hours(24), retryTimeout: Duration.seconds(5) }),
  BunCrypto.layer,
);

const pid = (value: string) => Protocol.PromiseId.make(value);

const anycastDefault = Protocol.TargetAddress.make({
  transport: "poll",
  cast: "any",
  group: Protocol.WorkerGroup.make("default"),
  id: Option.none(),
});

const targetTags = Protocol.Tags.make({
  reserved: { "resonate:target": anycastDefault },
  unrecognized: {},
  user: {},
});

const timerTags = Protocol.Tags.make({ reserved: { "resonate:timer": "true" }, unrecognized: {}, user: {} });

const delayedTargetTags = (delay: DateTime.Utc) =>
  Protocol.Tags.make({
    reserved: { "resonate:target": anycastDefault, "resonate:delay": delay },
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

const sendCreate = Effect.fn(function* (
  id: string,
  timeoutAtMs: number,
  tags = Protocol.emptyTags,
  param: Protocol.Value = { data: "cGFyYW0=" },
) {
  const head = yield* makeRequestHead;
  return yield* send(
    Protocol.PromiseCreateRequest.make({ head, data: { id: pid(id), timeoutAt: at(timeoutAtMs), param, tags } }),
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
    Protocol.PromiseRegisterCallbackRequest.make({ head, data: { awaited: pid(awaited), awaiter: pid(awaiter) } }),
  );
});

const sendListener = Effect.fn(function* (awaited: string, address = anycastDefault) {
  const head = yield* makeRequestHead;
  return yield* send(Protocol.PromiseRegisterListenerRequest.make({ head, data: { awaited: pid(awaited), address } }));
});

const snap = Effect.fn(function* () {
  const head = yield* makeRequestHead;
  const response = yield* send(Protocol.DebugSnapRequest.make({ head, data: {} }));
  if (!isSnap(response)) {
    throw new Error("debug.snap failed");
  }
  return response.data;
});

const tick = Effect.fn(function* (timeMs: number) {
  const head = yield* makeRequestHead;
  return yield* send(Protocol.DebugTickRequest.make({ head, data: { time: at(timeMs) } }));
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
      // Retry timeout armed at now + retryTimeout (5s).
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
      yield* create("external", 10_000, targetTags);
      yield* create("timer", 10_000, timerTags);
      const state = yield* snap();
      expect(state.promiseTimeouts.map((entry) => entry.id).sort()).toEqual(["external", "timer"]);
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
      // Companion task force-fulfilled, its timeout deleted, promise timeout deleted.
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
      // Persistence happens only at the tick: the raw store still says pending.
      const state = yield* snap();
      expect(state.promises[0]?.state).toBe("pending");
    }).pipe(Effect.provide(layers)),
  );

  it.effect("notifies listeners exactly once and resumes callbacks via the cascade", () =>
    Effect.gen(function* () {
      const network = yield* ResonateNetwork;
      // W is an awaiter with a target (its task buffers the resume while pending).
      yield* create("w", 60_000, targetTags);
      yield* create("b", 60_000);
      yield* sendCallback("b", "w");
      yield* sendListener("b");
      yield* settle("b", "resolved");
      // First message is w's create-dispatch; the settle then emits one unblock.
      const messages = yield* Stream.runCollect(Stream.take(network.messages, 2));
      expect(messages.map((message) => message.kind)).toEqual(["execute", "unblock"]);
      // The pending awaiter task buffered the resume instead of re-dispatching.
      const state = yield* snap();
      expect(state.tasks).toEqual([{ id: "w", state: "pending", version: 0, resumes: 1 }]);
      // Idempotent re-settle fires nothing new.
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

      // Settled awaited: 200 with the record, no registration.
      yield* create("done", 60_000);
      yield* settle("done", "resolved");
      const onSettled = yield* sendCallback("done", "w");
      expect(isCallbackOk(onSettled)).toBe(true);
      expect((yield* snap()).callbacks).toHaveLength(1);

      // Expired awaiter: still 200, registration silently skipped.
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
      yield* create("timer", 10_000, timerTags);
      yield* TestClock.adjust(Duration.millis(10_000));

      expect((yield* getPromise("plain")).state).toBe("rejected_timedout");
      expect((yield* getPromise("timer")).state).toBe("resolved");
      // Raw store untouched: projection is logical, not persisted.
      const before = yield* snap();
      expect(before.promises.every((promise) => promise.state === "pending")).toBe(true);

      yield* tick(10_000);

      const after = yield* snap();
      expect(after.promises.map((promise) => promise.state).sort()).toEqual(
        ["pending", "rejected_timedout", "resolved"].filter((s) => s !== "pending"),
      );
      const persistedPlain = yield* getPromise("plain");
      expect(persistedPlain.state).toBe("rejected_timedout");
      if (persistedPlain.state !== "pending") {
        // Backdated to timeoutAt, not the tick time.
        expect(Option.map(persistedPlain.settledAt, DateTime.toEpochMillis)).toEqual(Option.some(10_000));
      }
      // Companion task force-fulfilled by the tick (suspended→fulfilled path guard).
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
          // noop — narrowing happens below per response
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

describe("unimplemented surfaces", () => {
  it.effect("searches and task/schedule ops return 501", () =>
    Effect.gen(function* () {
      const head1 = yield* makeRequestHead;
      const search = yield* send(Protocol.PromiseSearchRequest.make({ head: head1, data: {} }));
      expect(search.head.status).toBe(501);
      const head2 = yield* makeRequestHead;
      const task = yield* send(Protocol.TaskGetRequest.make({ head: head2, data: { id: pid("x") } }));
      expect(task.head.status).toBe(501);
      const head3 = yield* makeRequestHead;
      const schedule = yield* send(
        Protocol.ScheduleGetRequest.make({ head: head3, data: { id: Protocol.ScheduleId.make("s") } }),
      );
      expect(schedule.head.status).toBe(501);
    }).pipe(Effect.provide(layers)),
  );
});
