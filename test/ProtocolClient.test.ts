import { describe, expect, it } from "@effect/vitest";
import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import { Duration, Effect, Fiber, Layer, Predicate, Schema, SchemaParser } from "effect";
import { TestClock } from "effect/testing";
import { DurablePromises } from "../src/DurablePromise.ts";
import { PromiseNotFound, ScheduleNotFound, TaskFenced } from "../src/Errors.ts";
import { ResonateNetwork } from "../src/network/network.ts";
import { makeRequestHead } from "../src/testing.ts";
import * as NetworkLocal from "../src/network/local.ts";
import * as Protocol from "../src/Protocol.ts";
import { Schedules } from "../src/Schedule.ts";
import { Tasks } from "../src/Task.ts";
import { assertInvariants } from "../src/testing.ts";

const isDebugSnapSuccess = SchemaParser.is(Protocol.DebugSnapResponse.members[0]);

const baseLayer = Layer.mergeAll(
  NetworkLocal.layer({ tickInterval: Duration.hours(24), retryTimeout: Duration.seconds(5) }),
  BunCrypto.layer,
);

const clientLayer = Layer.mergeAll(DurablePromises.layer, Tasks.layer, Schedules.layer).pipe(Layer.provide(baseLayer));

const layer = Layer.mergeAll(baseLayer, clientLayer);

const timestamp = (millis: number) => Schema.decodeUnknownSync(Protocol.Timestamp)(millis);

const snap = Effect.fn("ProtocolClientTest.snap")(function* () {
  const network = yield* ResonateNetwork;
  const response = yield* network.send(Protocol.DebugSnapRequest.make({ head: yield* makeRequestHead, data: {} }));
  if (!isDebugSnapSuccess(response)) {
    return yield* Effect.die(response.data);
  }
  yield* assertInvariants(response.data);
  return response.data;
});

const promiseCreateData = (id: string, timeoutAt = 60_000) => ({
  id: Protocol.PromiseId.make(id),
  timeoutAt: timestamp(timeoutAt),
  param: Protocol.emptyValue,
  tags: Protocol.emptyTags,
});

const resolvedState = Schema.Literal("resolved").make("resolved");

const promiseSettleData = (id: string) => ({
  id: Protocol.PromiseId.make(id),
  state: resolvedState,
  value: Protocol.emptyValue,
});

describe("DurablePromises", () => {
  it.effect("maps promise operations and awaitSettled through the local oracle", () =>
    Effect.gen(function* () {
      const promises = yield* DurablePromises;

      const missing = yield* Effect.flip(promises.get(Protocol.PromiseId.make("missing")));
      expect(Predicate.isTagged(missing, "PromiseNotFound")).toBe(true);

      const created = yield* promises.create(promiseCreateData("p1"));
      expect(created.id).toBe("p1");
      yield* snap();

      const got = yield* promises.get(Protocol.PromiseId.make("p1"));
      expect(got.state).toBe("pending");

      const waiter = yield* promises.awaitSettled(Protocol.PromiseId.make("p1")).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;
      const settled = yield* promises.settle(promiseSettleData("p1"));
      expect(settled.state).toBe("resolved");
      const observed = yield* Fiber.join(waiter);
      expect(observed.state).toBe("resolved");
      yield* snap();

      const immediate = yield* promises.awaitSettled(Protocol.PromiseId.make("p1"));
      expect(immediate.state).toBe("resolved");
    }).pipe(Effect.provide(layer)),
  );
});

describe("Tasks", () => {
  it.effect("maps task happy paths, 300 suspend, and 409 fencing", () =>
    Effect.gen(function* () {
      const tasks = yield* Tasks;
      const promises = yield* DurablePromises;
      const pid = Protocol.ProcessId.make("pid-1");
      const ttl = Duration.seconds(30);

      const created = yield* tasks.create({
        pid,
        ttl,
        action: Protocol.PromiseCreateRequest.make({
          head: yield* makeRequestHead,
          data: promiseCreateData("task-1"),
        }),
      });
      expect(created.promise.id).toBe("task-1");
      expect(created.task?.state).toBe("acquired");
      yield* snap();

      const gotTask = yield* tasks.get(Protocol.TaskId.make("task-1"));
      expect(gotTask.state).toBe("acquired");

      const version = Protocol.TaskVersion.make(1);
      yield* tasks.release({ data: { id: Protocol.TaskId.make("task-1"), version } });
      yield* snap();

      const acquired = yield* tasks.acquire({ data: { id: Protocol.TaskId.make("task-1"), version, pid, ttl } });
      expect(acquired.task.state).toBe("acquired");
      yield* snap();

      const fenced = yield* Effect.flip(
        tasks.fulfill({
          data: {
            id: Protocol.TaskId.make("task-1"),
            version,
            action: Protocol.PromiseSettleRequest.make({
              head: yield* makeRequestHead,
              data: promiseSettleData("task-1"),
            }),
          },
        }),
      );
      expect(Predicate.isTagged(fenced, "TaskFenced")).toBe(true);

      yield* promises.create(promiseCreateData("awaited"));
      yield* promises.settle(promiseSettleData("awaited"));
      const refused = yield* tasks.suspend({
        data: {
          id: Protocol.TaskId.make("task-1"),
          version: Protocol.TaskVersion.make(2),
          actions: [
            Protocol.PromiseRegisterCallbackRequest.make({
              head: yield* makeRequestHead,
              data: { awaited: Protocol.PromiseId.make("awaited"), awaiter: Protocol.PromiseId.make("task-1") },
            }),
          ],
        },
      });
      expect(Predicate.isTagged(refused, "SuspendRefused")).toBe(true);
      yield* snap();

      const currentVersion = Protocol.TaskVersion.make(2);
      yield* tasks.heartbeat({ pid, tasks: [{ id: Protocol.TaskId.make("task-1"), version: currentVersion }] });
      yield* snap();

      const fencedCreate = yield* tasks.fence({
        data: {
          id: Protocol.TaskId.make("task-1"),
          version: currentVersion,
          action: Protocol.PromiseCreateRequest.make({
            head: yield* makeRequestHead,
            data: promiseCreateData("child"),
          }),
        },
      });
      expect(fencedCreate.action.head.status).toBe(200);
      yield* snap();

      yield* tasks.halt(Protocol.TaskId.make("task-1"));
      yield* snap();

      yield* tasks.continue(Protocol.TaskId.make("task-1"));
      yield* snap();
    }).pipe(Effect.provide(layer)),
  );
});

describe("Schedules", () => {
  it.effect("maps schedule create/get/delete and not-found errors", () =>
    Effect.gen(function* () {
      const schedules = yield* Schedules;

      const missing = yield* Effect.flip(schedules.get(Protocol.ScheduleId.make("missing")));
      expect(Predicate.isTagged(missing, "ScheduleNotFound")).toBe(true);

      const created = yield* schedules.create({
        id: Protocol.ScheduleId.make("nightly"),
        cron: "* * * * *",
        promiseId: "scheduled-{{.timestamp}}",
        promiseTimeout: Duration.seconds(30),
        promiseParam: Protocol.emptyValue,
        promiseTags: Protocol.emptyTags,
      });
      expect(created.id).toBe("nightly");
      yield* snap();

      const got = yield* schedules.get(Protocol.ScheduleId.make("nightly"));
      expect(got.id).toBe("nightly");

      yield* schedules.delete(Protocol.ScheduleId.make("nightly"));
      yield* snap();

      yield* TestClock.adjust(Duration.seconds(60));
      yield* snap();
    }).pipe(Effect.provide(layer)),
  );
});
