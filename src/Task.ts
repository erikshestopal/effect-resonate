/**
 * Task domain model + typed client ops.
 *
 * See `docs/DESIGN.md` §3.2 (Layer 2 — Protocol client).
 */
import { Context, Crypto, Effect, Layer, Schema, SchemaParser } from "effect";
import {
  InvalidTarget,
  PromiseNotFound,
  TaskFenced,
  type ResonateProtocolError,
  type TransportError,
} from "./Errors.ts";
import { ResonateNetwork } from "./Network.ts";
import * as Protocol from "./Protocol.ts";

export interface TaskCreateResult {
  readonly promise: Protocol.PromiseRecord;
  readonly task?: Protocol.TaskRecord;
  readonly preload: ReadonlyArray<Protocol.PromiseRecord>;
}

export interface TaskClaimResult {
  readonly task: Protocol.TaskRecord;
  readonly promise: Protocol.PromiseRecord;
  readonly preload: ReadonlyArray<Protocol.PromiseRecord>;
}

export class SuspendRefused extends Schema.Class<SuspendRefused>("SuspendRefused")({
  _tag: Schema.tag("SuspendRefused"),
  preload: Schema.Array(Protocol.PromiseRecord),
}) {}

export class SuspendAccepted extends Schema.Class<SuspendAccepted>("SuspendAccepted")({
  _tag: Schema.tag("SuspendAccepted"),
}) {}

export type SuspendResult = SuspendAccepted | SuspendRefused;

export interface FenceResult {
  readonly action: typeof Protocol.PromiseCreateResponse.Type | typeof Protocol.PromiseSettleResponse.Type;
  readonly preload: ReadonlyArray<Protocol.PromiseRecord>;
}

const isGetSuccess = SchemaParser.is(Protocol.TaskGetResponse.members[0]);
const isCreateSuccess = SchemaParser.is(Protocol.TaskCreateResponse.members[0]);
const isAcquireSuccess = SchemaParser.is(Protocol.TaskAcquireResponse.members[0]);
const isReleaseSuccess = SchemaParser.is(Protocol.TaskReleaseResponse.members[0]);
const isSuspendAccepted = SchemaParser.is(Protocol.TaskSuspendResponse.members[0]);
const isSuspendRefused = SchemaParser.is(Protocol.TaskSuspendResponse.members[1]);
const isHaltSuccess = SchemaParser.is(Protocol.TaskHaltResponse.members[0]);
const isContinueSuccess = SchemaParser.is(Protocol.TaskContinueResponse.members[0]);
const isFulfillSuccess = SchemaParser.is(Protocol.TaskFulfillResponse.members[0]);
const isFenceSuccess = SchemaParser.is(Protocol.TaskFenceResponse.members[0]);
const isHeartbeatSuccess = SchemaParser.is(Protocol.TaskHeartbeatResponse.members[0]);

const taskError = (
  id: Protocol.TaskId,
  version: Protocol.TaskVersion,
  status: number,
  message: unknown,
): ResonateProtocolError => {
  if (status === 404) {
    return new PromiseNotFound({ id });
  }
  if (status === 409) {
    return new TaskFenced({ id, version });
  }
  return new InvalidTarget({ message: String(message) });
};

export class Tasks extends Context.Service<
  Tasks,
  {
    readonly get: (id: Protocol.TaskId) => Effect.Effect<Protocol.TaskRecord, ResonateProtocolError | TransportError>;
    readonly create: (
      data: typeof Protocol.TaskCreateRequest.Type.data,
    ) => Effect.Effect<TaskCreateResult, ResonateProtocolError | TransportError>;
    readonly acquire: (
      data: typeof Protocol.TaskAcquireRequest.Type.data,
    ) => Effect.Effect<TaskClaimResult, ResonateProtocolError | TransportError>;
    readonly release: (
      data: typeof Protocol.TaskReleaseRequest.Type.data,
    ) => Effect.Effect<void, ResonateProtocolError | TransportError>;
    readonly suspend: (
      data: typeof Protocol.TaskSuspendRequest.Type.data,
    ) => Effect.Effect<SuspendResult, ResonateProtocolError | TransportError>;
    readonly halt: (id: Protocol.TaskId) => Effect.Effect<void, ResonateProtocolError | TransportError>;
    readonly continue: (id: Protocol.TaskId) => Effect.Effect<void, ResonateProtocolError | TransportError>;
    readonly fulfill: (
      data: typeof Protocol.TaskFulfillRequest.Type.data,
    ) => Effect.Effect<Protocol.PromiseRecord, ResonateProtocolError | TransportError>;
    readonly fence: (
      data: typeof Protocol.TaskFenceRequest.Type.data,
    ) => Effect.Effect<FenceResult, ResonateProtocolError | TransportError>;
    readonly heartbeat: (
      data: typeof Protocol.TaskHeartbeatRequest.Type.data,
    ) => Effect.Effect<void, ResonateProtocolError | TransportError>;
  }
>()("effect-resonate/Tasks") {
  static readonly layer: Layer.Layer<Tasks, never, ResonateNetwork | Crypto.Crypto> = Layer.effect(
    Tasks,
    Effect.gen(function* () {
      const network = yield* ResonateNetwork;
      const crypto = yield* Crypto.Crypto;

      const head = Effect.fn("Tasks.head")(function* () {
        const corrId = Protocol.CorrelationId.make(yield* Effect.orDie(crypto.randomUUIDv4));
        return Protocol.RequestHead.make({ corrId, version: Protocol.protocolVersion });
      });
      const zero = Protocol.TaskVersion.make(0);

      const get = Effect.fn("Tasks.get")(function* (id: Protocol.TaskId) {
        const response = yield* network.send(Protocol.TaskGetRequest.make({ head: yield* head(), data: { id } }));
        if (isGetSuccess(response)) {
          return response.data.task;
        }
        return yield* Effect.fail(taskError(id, zero, response.head.status, response.data));
      });

      const create = Effect.fn("Tasks.create")(function* (data: typeof Protocol.TaskCreateRequest.Type.data) {
        const response = yield* network.send(Protocol.TaskCreateRequest.make({ head: yield* head(), data }));
        if (isCreateSuccess(response)) {
          return { promise: response.data.promise, task: response.data.task, preload: response.data.preload };
        }
        return yield* Effect.fail(taskError(data.action.data.id, zero, response.head.status, response.data));
      });

      const acquire = Effect.fn("Tasks.acquire")(function* (data: typeof Protocol.TaskAcquireRequest.Type.data) {
        const response = yield* network.send(Protocol.TaskAcquireRequest.make({ head: yield* head(), data }));
        if (isAcquireSuccess(response)) {
          return { task: response.data.task, promise: response.data.promise, preload: response.data.preload };
        }
        return yield* Effect.fail(taskError(data.id, data.version, response.head.status, response.data));
      });

      const release = Effect.fn("Tasks.release")(function* (data: typeof Protocol.TaskReleaseRequest.Type.data) {
        const response = yield* network.send(Protocol.TaskReleaseRequest.make({ head: yield* head(), data }));
        if (isReleaseSuccess(response)) {
          return;
        }
        return yield* Effect.fail(taskError(data.id, data.version, response.head.status, response.data));
      });

      const suspend = Effect.fn("Tasks.suspend")(function* (data: typeof Protocol.TaskSuspendRequest.Type.data) {
        const response = yield* network.send(Protocol.TaskSuspendRequest.make({ head: yield* head(), data }));
        if (isSuspendAccepted(response)) {
          return SuspendAccepted.make({});
        }
        if (isSuspendRefused(response)) {
          return SuspendRefused.make({ preload: response.data.preload });
        }
        return yield* Effect.fail(taskError(data.id, data.version, response.head.status, response.data));
      });

      const halt = Effect.fn("Tasks.halt")(function* (id: Protocol.TaskId) {
        const response = yield* network.send(Protocol.TaskHaltRequest.make({ head: yield* head(), data: { id } }));
        if (isHaltSuccess(response)) {
          return;
        }
        return yield* Effect.fail(taskError(id, zero, response.head.status, response.data));
      });

      const continueTask = Effect.fn("Tasks.continue")(function* (id: Protocol.TaskId) {
        const response = yield* network.send(Protocol.TaskContinueRequest.make({ head: yield* head(), data: { id } }));
        if (isContinueSuccess(response)) {
          return;
        }
        return yield* Effect.fail(taskError(id, zero, response.head.status, response.data));
      });

      const fulfill = Effect.fn("Tasks.fulfill")(function* (data: typeof Protocol.TaskFulfillRequest.Type.data) {
        const response = yield* network.send(Protocol.TaskFulfillRequest.make({ head: yield* head(), data }));
        if (isFulfillSuccess(response)) {
          return response.data.promise;
        }
        return yield* Effect.fail(taskError(data.id, data.version, response.head.status, response.data));
      });

      const fence = Effect.fn("Tasks.fence")(function* (data: typeof Protocol.TaskFenceRequest.Type.data) {
        const response = yield* network.send(Protocol.TaskFenceRequest.make({ head: yield* head(), data }));
        if (isFenceSuccess(response)) {
          return { action: response.data.action, preload: response.data.preload };
        }
        return yield* Effect.fail(taskError(data.id, data.version, response.head.status, response.data));
      });

      const heartbeat = Effect.fn("Tasks.heartbeat")(function* (data: typeof Protocol.TaskHeartbeatRequest.Type.data) {
        const response = yield* network.send(Protocol.TaskHeartbeatRequest.make({ head: yield* head(), data }));
        if (isHeartbeatSuccess(response)) {
          return;
        }
        return yield* Effect.fail(new InvalidTarget({ message: "Task heartbeat failed" }));
      });

      return Tasks.of({
        get,
        create,
        acquire,
        release,
        suspend,
        halt,
        continue: continueTask,
        fulfill,
        fence,
        heartbeat,
      });
    }),
  );
}
