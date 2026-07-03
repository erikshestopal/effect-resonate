/**
 * Low-level client service for Resonate task protocol endpoints.
 *
 * Workers use this service to create, acquire, release, suspend, fulfill, fence,
 * and heartbeat tasks while executing durable functions.
 *
 * @since 0.0.0
 */
import { Context, Crypto, Effect, Layer, Predicate, Schema, SchemaParser } from "effect";
import {
  InvalidTarget,
  PromiseNotFound,
  TaskFenced,
  type ResonateProtocolError,
  type TransportError,
} from "./Errors.ts";
import { ResonateNetwork } from "./network/network.ts";
import * as Protocol from "./Protocol.ts";

/**
 * Result returned when creating a task through the protocol service.
 *
 * @category models
 * @since 0.0.0
 */
export class TaskCreateResult extends Schema.Class<TaskCreateResult>("Task/CreateResult")({
  promise: Protocol.PromiseRecord,
  task: Schema.optionalKey(Protocol.TaskRecord),
  preload: Schema.Array(Protocol.PromiseRecord),
}) {}

/**
 * Result returned when acquiring a task for execution.
 *
 * @category models
 * @since 0.0.0
 */
export class TaskClaimResult extends Schema.Class<TaskClaimResult>("Task/ClaimResult")({
  task: Protocol.TaskRecord,
  promise: Protocol.PromiseRecord,
  preload: Schema.Array(Protocol.PromiseRecord),
}) {}

export class SuspendRefused extends Schema.Class<SuspendRefused>("SuspendRefused")({
  _tag: Schema.tag("SuspendRefused"),
  preload: Schema.Array(Protocol.PromiseRecord),
}) {}

export class SuspendAccepted extends Schema.Class<SuspendAccepted>("SuspendAccepted")({
  _tag: Schema.tag("SuspendAccepted"),
}) {}

export type SuspendResult = SuspendAccepted | SuspendRefused;

export class FenceResult extends Schema.Class<FenceResult>("Task/FenceResult")({
  action: Schema.Union([Protocol.PromiseCreateResponse, Protocol.PromiseSettleResponse]),
  preload: Schema.Array(Protocol.PromiseRecord),
}) {}

export interface TaskRequestOptions {
  readonly origin?: string;
}

const isGetSuccess = SchemaParser.is(Protocol.TaskGetSuccessResponse);
const isCreateSuccess = SchemaParser.is(Protocol.TaskCreateSuccessResponse);
const isAcquireSuccess = SchemaParser.is(Protocol.TaskAcquireSuccessResponse);
const isReleaseSuccess = SchemaParser.is(Protocol.TaskReleaseSuccessResponse);
const isSuspendAccepted = SchemaParser.is(Protocol.TaskSuspendAcceptedResponse);
const isSuspendRefused = SchemaParser.is(Protocol.TaskSuspendRefusedResponse);
const isHaltSuccess = SchemaParser.is(Protocol.TaskHaltSuccessResponse);
const isContinueSuccess = SchemaParser.is(Protocol.TaskContinueSuccessResponse);
const isFulfillSuccess = SchemaParser.is(Protocol.TaskFulfillSuccessResponse);
const isFenceSuccess = SchemaParser.is(Protocol.TaskFenceSuccessResponse);
const isHeartbeatSuccess = SchemaParser.is(Protocol.TaskHeartbeatSuccessResponse);

const taskError = (options: {
  readonly id: Protocol.TaskId;
  readonly version: Protocol.TaskVersion;
  readonly status: number;
  readonly message: unknown;
}): ResonateProtocolError => {
  const { id, version, status, message } = options;
  if (status === 404) {
    return new PromiseNotFound({ id });
  }
  if (status === 409) {
    return new TaskFenced({ id, version });
  }
  return new InvalidTarget({ message: String(message) });
};

/**
 * Service interface for task protocol operations.
 *
 * @category models
 * @since 0.0.0
 */
export interface TasksService {
  readonly get: (id: Protocol.TaskId) => Effect.Effect<Protocol.TaskRecord, ResonateProtocolError | TransportError>;
  readonly create: (
    data: Protocol.TaskCreateRequest["data"],
  ) => Effect.Effect<TaskCreateResult, ResonateProtocolError | TransportError>;
  readonly acquire: (request: {
    readonly data: Protocol.TaskAcquireRequest["data"];
    readonly options?: TaskRequestOptions;
  }) => Effect.Effect<TaskClaimResult, ResonateProtocolError | TransportError>;
  readonly release: (request: {
    readonly data: Protocol.TaskReleaseRequest["data"];
    readonly options?: TaskRequestOptions;
  }) => Effect.Effect<void, ResonateProtocolError | TransportError>;
  readonly suspend: (request: {
    readonly data: Protocol.TaskSuspendRequest["data"];
    readonly options?: TaskRequestOptions;
  }) => Effect.Effect<SuspendResult, ResonateProtocolError | TransportError>;
  readonly halt: (id: Protocol.TaskId) => Effect.Effect<void, ResonateProtocolError | TransportError>;
  readonly continue: (id: Protocol.TaskId) => Effect.Effect<void, ResonateProtocolError | TransportError>;
  readonly fulfill: (request: {
    readonly data: Protocol.TaskFulfillRequest["data"];
    readonly options?: TaskRequestOptions;
  }) => Effect.Effect<Protocol.PromiseRecord, ResonateProtocolError | TransportError>;
  readonly fence: (request: {
    readonly data: Protocol.TaskFenceRequest["data"];
    readonly options?: TaskRequestOptions;
  }) => Effect.Effect<FenceResult, ResonateProtocolError | TransportError>;
  readonly heartbeat: (
    data: Protocol.TaskHeartbeatRequest["data"],
  ) => Effect.Effect<void, ResonateProtocolError | TransportError>;
}

/**
 * Protocol client service for worker task operations.
 *
 * @category services
 * @since 0.0.0
 */
export class Tasks extends Context.Service<Tasks, TasksService>()("effect-resonate/Tasks") {
  static readonly layer: Layer.Layer<Tasks, never, ResonateNetwork | Crypto.Crypto> = Layer.effect(
    Tasks,
    Effect.gen(function* () {
      const network = yield* ResonateNetwork;
      const crypto = yield* Crypto.Crypto;

      const head = Effect.fn("Tasks.head")(function* (options?: TaskRequestOptions) {
        const corrId = Protocol.CorrelationId.make(yield* Effect.orDie(crypto.randomUUIDv4));
        return Protocol.RequestHead.make({
          corrId,
          version: Protocol.protocolVersion,
          ...(options?.origin ? { "resonate:origin": options.origin } : {}),
        });
      });
      const zero = Protocol.TaskVersion.make(0);

      return Tasks.of({
        get: Effect.fn("Tasks.get")(function* (id) {
          const response = yield* network.send(Protocol.TaskGetRequest.make({ head: yield* head(), data: { id } }));
          if (isGetSuccess(response)) {
            return response.data.task;
          }
          return yield* taskError({ id, version: zero, status: response.head.status, message: response.data });
        }),
        create: Effect.fn("Tasks.create")(function* (data) {
          const response = yield* network.send(Protocol.TaskCreateRequest.make({ head: yield* head(), data }));
          if (isCreateSuccess(response)) {
            return TaskCreateResult.make({
              promise: response.data.promise,
              ...(Predicate.isNotUndefined(response.data.task) ? { task: response.data.task } : {}),
              preload: response.data.preload,
            });
          }
          return yield* taskError({
            id: data.action.data.id,
            version: zero,
            status: response.head.status,
            message: response.data,
          });
        }),
        acquire: Effect.fn("Tasks.acquire")(function* ({ data, options }) {
          const response = yield* network.send(Protocol.TaskAcquireRequest.make({ head: yield* head(options), data }));
          if (isAcquireSuccess(response)) {
            return TaskClaimResult.make({
              task: response.data.task,
              promise: response.data.promise,
              preload: response.data.preload,
            });
          }
          return yield* taskError({
            id: data.id,
            version: data.version,
            status: response.head.status,
            message: response.data,
          });
        }),
        release: Effect.fn("Tasks.release")(function* ({ data, options }) {
          const response = yield* network.send(Protocol.TaskReleaseRequest.make({ head: yield* head(options), data }));
          if (isReleaseSuccess(response)) {
            return;
          }
          return yield* taskError({
            id: data.id,
            version: data.version,
            status: response.head.status,
            message: response.data,
          });
        }),
        suspend: Effect.fn("Tasks.suspend")(function* ({ data, options }) {
          const response = yield* network.send(Protocol.TaskSuspendRequest.make({ head: yield* head(options), data }));
          if (isSuspendAccepted(response)) {
            return SuspendAccepted.make({});
          }
          if (isSuspendRefused(response)) {
            return SuspendRefused.make({ preload: response.data.preload });
          }
          return yield* taskError({
            id: data.id,
            version: data.version,
            status: response.head.status,
            message: response.data,
          });
        }),
        halt: Effect.fn("Tasks.halt")(function* (id) {
          const response = yield* network.send(Protocol.TaskHaltRequest.make({ head: yield* head(), data: { id } }));
          if (isHaltSuccess(response)) {
            return;
          }
          return yield* taskError({ id, version: zero, status: response.head.status, message: response.data });
        }),
        continue: Effect.fn("Tasks.continue")(function* (id) {
          const response = yield* network.send(
            Protocol.TaskContinueRequest.make({ head: yield* head(), data: { id } }),
          );
          if (isContinueSuccess(response)) {
            return;
          }
          return yield* taskError({ id, version: zero, status: response.head.status, message: response.data });
        }),
        fulfill: Effect.fn("Tasks.fulfill")(function* ({ data, options }) {
          const response = yield* network.send(Protocol.TaskFulfillRequest.make({ head: yield* head(options), data }));
          if (isFulfillSuccess(response)) {
            return response.data.promise;
          }
          return yield* taskError({
            id: data.id,
            version: data.version,
            status: response.head.status,
            message: response.data,
          });
        }),
        fence: Effect.fn("Tasks.fence")(function* ({ data, options }) {
          const response = yield* network.send(Protocol.TaskFenceRequest.make({ head: yield* head(options), data }));
          if (isFenceSuccess(response)) {
            return FenceResult.make({ action: response.data.action, preload: response.data.preload });
          }
          return yield* taskError({
            id: data.id,
            version: data.version,
            status: response.head.status,
            message: response.data,
          });
        }),
        heartbeat: Effect.fn("Tasks.heartbeat")(function* (data) {
          const response = yield* network.send(Protocol.TaskHeartbeatRequest.make({ head: yield* head(), data }));
          if (isHeartbeatSuccess(response)) {
            return;
          }
          return yield* new InvalidTarget({ message: "Task heartbeat failed" });
        }),
      });
    }),
  );
}
