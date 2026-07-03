/**
 * In-memory implementation of the Resonate network service.
 *
 * This module is useful for deterministic local execution, tests, and examples
 * that do not need to talk to a shipped Resonate server.
 *
 * @since 0.0.0
 */
import {
  Array as Arr,
  Cron,
  DateTime,
  Duration,
  Effect,
  HashMap,
  Layer,
  Match,
  Option,
  Predicate,
  Queue,
  Ref,
  Result,
  Schema,
  SchemaParser,
  Stream,
  Number as Num,
  String as Str,
} from "effect";
import * as Protocol from "../Protocol.ts";
import { decodeResponse, ResonateNetwork } from "./network.ts";
export class PromiseObject extends Schema.Class<PromiseObject>("NetworkLocal/PromiseObject")({
  id: Protocol.PromiseId,
  state: Protocol.PromiseState,
  param: Protocol.Value,
  value: Protocol.Value,
  tags: Protocol.Tags,
  timeoutAt: Schema.DateTimeUtc,
  createdAt: Schema.DateTimeUtc,
  settledAt: Schema.Option(Schema.DateTimeUtc),
  callbacks: Schema.Array(Protocol.PromiseId),
  listeners: Schema.Array(Protocol.TargetAddress),
}) {
  get fields() {
    return {
      id: this.id,
      state: this.state,
      param: this.param,
      value: this.value,
      tags: this.tags,
      timeoutAt: this.timeoutAt,
      createdAt: this.createdAt,
      settledAt: this.settledAt,
      callbacks: this.callbacks,
      listeners: this.listeners,
    };
  }
  get isTimer(): boolean {
    return this.tags.isTimer;
  }
  get target(): Option.Option<Protocol.TargetAddress> {
    return Option.fromNullishOr(this.tags.reserved["resonate:target"]);
  }
  get external(): boolean {
    return Option.isSome(this.target) || this.isTimer || this.tags.reserved["resonate:scope"] === "global";
  }
  get timedOutState(): "resolved" | "rejected_timedout" {
    return this.isTimer ? "resolved" : "rejected_timedout";
  }
  projected(now: DateTime.Utc): PromiseObject {
    if (this.state !== "pending" || DateTime.toEpochMillis(this.timeoutAt) > DateTime.toEpochMillis(now)) {
      return this;
    }
    return new PromiseObject({
      ...this.fields,
      state: this.timedOutState,
      settledAt: Option.some(this.timeoutAt),
    });
  }
  toRecord(): Protocol.PromiseRecord {
    if (this.state === "pending") {
      return new Protocol.PromisePending({
        id: this.id,
        param: this.param,
        value: this.value,
        tags: this.tags,
        timeoutAt: this.timeoutAt,
        createdAt: this.createdAt,
      });
    }
    return new Protocol.PromiseSettled({
      state: this.state,
      id: this.id,
      param: this.param,
      value: this.value,
      tags: this.tags,
      timeoutAt: this.timeoutAt,
      createdAt: this.createdAt,
      settledAt: this.settledAt,
    });
  }
}
export class TaskObject extends Schema.Class<TaskObject>("NetworkLocal/TaskObject")({
  id: Protocol.TaskId,
  state: Protocol.TaskState,
  version: Protocol.TaskVersion,
  pid: Schema.Option(Protocol.ProcessId),
  ttl: Schema.Option(Schema.Duration),
  resumes: Schema.Array(Protocol.PromiseId),
}) {
  get fields() {
    return {
      id: this.id,
      state: this.state,
      version: this.version,
      pid: this.pid,
      ttl: this.ttl,
      resumes: this.resumes,
    };
  }
  toRecord(): Protocol.TaskRecord {
    const common = {
      id: this.id,
      version: this.version,
      resumes: this.resumes.length,
    };
    return Match.value(this.state).pipe(
      Match.when("pending", () => new Protocol.TaskPending(common)),
      Match.when("acquired", () => new Protocol.TaskAcquired({ ...common, pid: this.pid, ttl: this.ttl })),
      Match.when("suspended", () => new Protocol.TaskSuspended(common)),
      Match.when("halted", () => new Protocol.TaskHalted(common)),
      Match.when("fulfilled", () => new Protocol.TaskFulfilled(common)),
      Match.exhaustive,
    );
  }
}
export class ScheduleObject extends Schema.Class<ScheduleObject>("NetworkLocal/ScheduleObject")({
  id: Protocol.ScheduleId,
  cron: Schema.String,
  promiseId: Schema.NonEmptyString,
  promiseTimeout: Protocol.Ttl,
  promiseParam: Protocol.Value,
  promiseTags: Protocol.Tags,
  createdAt: Schema.DateTimeUtc,
  nextRunAt: Schema.DateTimeUtc,
  lastRunAt: Schema.Option(Schema.DateTimeUtc),
}) {
  get fields() {
    return {
      id: this.id,
      cron: this.cron,
      promiseId: this.promiseId,
      promiseTimeout: this.promiseTimeout,
      promiseParam: this.promiseParam,
      promiseTags: this.promiseTags,
      createdAt: this.createdAt,
      nextRunAt: this.nextRunAt,
      lastRunAt: this.lastRunAt,
    };
  }
  toRecord(): Protocol.ScheduleRecord {
    return new Protocol.ScheduleRecord({
      id: this.id,
      cron: this.cron,
      promiseId: this.promiseId,
      promiseTimeout: this.promiseTimeout,
      promiseParam: this.promiseParam,
      promiseTags: this.promiseTags,
      createdAt: this.createdAt,
      nextRunAt: this.nextRunAt,
      lastRunAt: this.lastRunAt,
    });
  }
}
interface TaskTimeoutEntry {
  readonly kind: 0 | 1;
  readonly at: DateTime.Utc;
}
interface OutboxEntry {
  readonly address: Protocol.TargetAddress;
  readonly message: Protocol.Message;
}
interface ServerState {
  readonly promises: HashMap.HashMap<Protocol.PromiseId, PromiseObject>;
  readonly tasks: HashMap.HashMap<Protocol.TaskId, TaskObject>;
  readonly schedules: HashMap.HashMap<Protocol.ScheduleId, ScheduleObject>;
  readonly promiseTimeouts: HashMap.HashMap<Protocol.PromiseId, DateTime.Utc>;
  readonly taskTimeouts: HashMap.HashMap<Protocol.TaskId, TaskTimeoutEntry>;
  readonly scheduleTimeouts: HashMap.HashMap<Protocol.ScheduleId, DateTime.Utc>;
  readonly outbox: ReadonlyArray<OutboxEntry>;
}
/**
 * Snapshot of the local in-memory server state.
 *
 * @category models
 * @since 0.0.0
 */
export type DebugState = (typeof Protocol.DebugSnapResponse.members)[0]["Type"]["data"];
const initialState: ServerState = {
  promises: HashMap.empty(),
  tasks: HashMap.empty(),
  schedules: HashMap.empty(),
  promiseTimeouts: HashMap.empty(),
  taskTimeouts: HashMap.empty(),
  scheduleTimeouts: HashMap.empty(),
  outbox: [],
};
interface Emitting {
  readonly state: ServerState;
  readonly emitted: ReadonlyArray<OutboxEntry>;
}
const setPromise = ({
  state,
  promise,
}: {
  readonly state: ServerState;
  readonly promise: PromiseObject;
}): ServerState => ({
  ...state,
  promises: HashMap.set(state.promises, promise.id, promise),
});
const setTask = ({ state, task }: { readonly state: ServerState; readonly task: TaskObject }): ServerState => ({
  ...state,
  tasks: HashMap.set(state.tasks, task.id, task),
});
const setSchedule = ({
  state,
  schedule,
}: {
  readonly state: ServerState;
  readonly schedule: ScheduleObject;
}): ServerState => ({
  ...state,
  schedules: HashMap.set(state.schedules, schedule.id, schedule),
});
const delSchedule = ({
  state,
  id,
}: {
  readonly state: ServerState;
  readonly id: Protocol.ScheduleId;
}): ServerState => ({
  ...state,
  schedules: HashMap.remove(state.schedules, id),
});
const setPromiseTimeout = ({
  state,
  id,
  at,
}: {
  readonly state: ServerState;
  readonly id: Protocol.PromiseId;
  readonly at: DateTime.Utc;
}): ServerState => ({
  ...state,
  promiseTimeouts: HashMap.set(state.promiseTimeouts, id, at),
});
const delPromiseTimeout = ({
  state,
  id,
}: {
  readonly state: ServerState;
  readonly id: Protocol.PromiseId;
}): ServerState => ({
  ...state,
  promiseTimeouts: HashMap.remove(state.promiseTimeouts, id),
});
const setTaskTimeout = ({
  state,
  id,
  entry,
}: {
  readonly state: ServerState;
  readonly id: Protocol.TaskId;
  readonly entry: TaskTimeoutEntry;
}): ServerState => ({
  ...state,
  taskTimeouts: HashMap.set(state.taskTimeouts, id, entry),
});
const delTaskTimeout = ({ state, id }: { readonly state: ServerState; readonly id: Protocol.TaskId }): ServerState => ({
  ...state,
  taskTimeouts: HashMap.remove(state.taskTimeouts, id),
});
const setScheduleTimeout = ({
  state,
  id,
  at,
}: {
  readonly state: ServerState;
  readonly id: Protocol.ScheduleId;
  readonly at: DateTime.Utc;
}): ServerState => ({
  ...state,
  scheduleTimeouts: HashMap.set(state.scheduleTimeouts, id, at),
});
const delScheduleTimeout = ({
  state,
  id,
}: {
  readonly state: ServerState;
  readonly id: Protocol.ScheduleId;
}): ServerState => ({
  ...state,
  scheduleTimeouts: HashMap.remove(state.scheduleTimeouts, id),
});
const outboxKey = (entry: OutboxEntry): string =>
  entry.message.kind === "execute"
    ? `execute:${entry.message.data.task.id}`
    : `unblock:${entry.message.data.promise.id}:${entry.address.address}`;
const setMessage = ({
  input: { emitted, state },
  address,
  message,
}: {
  readonly input: Emitting;
  readonly address: Protocol.TargetAddress;
  readonly message: Protocol.Message;
}): Emitting => {
  const entry: OutboxEntry = { address, message };
  const key = outboxKey(entry);
  const outbox = Arr.findFirstIndex(state.outbox, (existing) => outboxKey(existing) === key).pipe(
    Option.flatMap((index) => Arr.replace(state.outbox, index, entry)),
    Option.getOrElse(() => Arr.append(state.outbox, entry)),
  );
  return { state: { ...state, outbox }, emitted: [...emitted, entry] };
};
const millis = DateTime.toEpochMillis;
const maybeExecute = ({ input, task }: { readonly input: Emitting; readonly task: TaskObject }): Emitting => {
  const promise = HashMap.get(input.state.promises, task.id);
  const target = Option.flatMap(promise, (promise) => promise.target);
  if (Option.isNone(target)) {
    return input;
  }
  return setMessage({
    input: input,
    address: target.value,
    message: Protocol.ExecuteMessage.make({
      head: {},
      data: { task: { id: task.id, version: task.version } },
    }),
  });
};
const preload = ({
  state,
  id,
}: {
  readonly state: ServerState;
  readonly id: Protocol.PromiseId;
}): ReadonlyArray<Protocol.PromiseRecord> => {
  const promise = HashMap.get(state.promises, id);
  if (Option.isNone(promise)) {
    return [];
  }
  const branch = promise.value.tags.reserved["resonate:branch"];
  if (Predicate.isUndefined(branch)) {
    return [];
  }
  return Arr.map(
    Arr.filter(
      Arr.fromIterable(HashMap.values(state.promises)),
      (candidate) => candidate.id !== id && candidate.tags.reserved["resonate:branch"] === branch,
    ),
    (candidate) => candidate.toRecord(),
  );
};
const enqueueResume = ({
  input: { emitted, state },
  awaitedId,
  awaiterId,
  now,
  retryTimeout,
}: {
  readonly input: Emitting;
  readonly awaitedId: Protocol.PromiseId;
  readonly awaiterId: Protocol.PromiseId;
  readonly now: DateTime.Utc;
  readonly retryTimeout: Duration.Duration;
}): Emitting => {
  const task = HashMap.get(state.tasks, awaiterId);
  if (Option.isNone(task)) {
    return { state, emitted };
  }
  return Match.value(task.value).pipe(
    Match.when({ state: "suspended" }, (suspended) => {
      const resumed = new TaskObject({
        ...suspended.fields,
        state: "pending",
        resumes: [awaitedId],
      });
      let next = setTask({ state: state, task: resumed });
      next = setTaskTimeout({
        state: next,
        id: resumed.id,
        entry: {
          kind: 0,
          at: DateTime.addDuration(now, retryTimeout),
        },
      });
      const awaiterPromise = HashMap.get(next.promises, awaiterId);
      const target = Option.flatMap(awaiterPromise, (promise) => promise.target);
      if (Option.isNone(target)) {
        return { state: next, emitted };
      }
      return setMessage({
        input: { state: next, emitted },
        address: target.value,
        message: Protocol.ExecuteMessage.make({
          head: {},
          data: { task: { id: resumed.id, version: resumed.version } },
        }),
      });
    }),
    Match.whenOr({ state: "pending" }, { state: "acquired" }, { state: "halted" }, (buffered) => {
      if (Arr.contains(buffered.resumes, awaitedId)) {
        return { state, emitted };
      }
      return {
        state: setTask({
          state: state,
          task: new TaskObject({ ...buffered.fields, resumes: [...buffered.resumes, awaitedId] }),
        }),
        emitted,
      };
    }),
    Match.when({ state: "fulfilled" }, () => ({ state, emitted })),
    Match.exhaustive,
  );
};
const settlementCascade = ({
  input,
  settled,
  priorCallbacks,
  priorListeners,
  now,
  retryTimeout,
}: {
  readonly input: Emitting;
  readonly settled: PromiseObject;
  readonly priorCallbacks: ReadonlyArray<Protocol.PromiseId>;
  readonly priorListeners: ReadonlyArray<Protocol.TargetAddress>;
  readonly now: DateTime.Utc;
  readonly retryTimeout: Duration.Duration;
}): Emitting => {
  let { emitted, state } = input;
  const task = HashMap.get(state.tasks, settled.id);
  if (Option.isSome(task)) {
    state = setTask({
      state: state,
      task: new TaskObject({
        ...task.value.fields,
        state: "fulfilled",
        pid: Option.none(),
        ttl: Option.none(),
        resumes: [],
      }),
    });
    state = delTaskTimeout({ state: state, id: settled.id });
  }
  state = {
    ...state,
    promises: HashMap.map(state.promises, (promise) =>
      promise.state === "pending" && Arr.contains(promise.callbacks, settled.id)
        ? new PromiseObject({
            ...promise.fields,
            callbacks: Arr.filter(promise.callbacks, (id) => id !== settled.id),
          })
        : promise,
    ),
  };
  let next: Emitting = { state, emitted };
  for (const address of priorListeners) {
    next = setMessage({
      input: next,
      address: address,
      message: Protocol.UnblockMessage.make({ head: {}, data: { promise: settled.toRecord() } }),
    });
  }
  for (const awaiterId of priorCallbacks) {
    next = enqueueResume({
      input: next,
      awaitedId: settled.id,
      awaiterId: awaiterId,
      now: now,
      retryTimeout: retryTimeout,
    });
  }
  return next;
};
interface Transition<R extends Protocol.Response = Protocol.Response> {
  readonly state: ServerState;
  readonly response: R;
  readonly emitted: ReadonlyArray<OutboxEntry>;
}
const promiseGet = ({
  state,
  now,
  request,
}: {
  readonly state: ServerState;
  readonly now: DateTime.Utc;
  readonly request: Protocol.Request<"promise.get">;
}): Transition => {
  const promise = HashMap.get(state.promises, request.data.id);
  if (Option.isNone(promise)) {
    return {
      state,
      response: Protocol.PromiseGetResponse.make({
        kind: "promise.get",
        head: { corrId: request.head.corrId, status: 404, version: request.head.version },
        data: "Promise not found",
      }),
      emitted: [],
    };
  }
  return {
    state,
    response: Protocol.PromiseGetResponse.make({
      kind: "promise.get",
      head: { corrId: request.head.corrId, status: 200, version: request.head.version },
      data: { promise: promise.value.projected(now).toRecord() },
    }),
    emitted: [],
  };
};
const promiseCreate = ({
  state,
  now,
  retryTimeout,
  request,
}: {
  readonly state: ServerState;
  readonly now: DateTime.Utc;
  readonly retryTimeout: Duration.Duration;
  readonly request: Protocol.Request<"promise.create">;
}): Transition<(typeof Protocol.PromiseCreateResponse)["Type"]> => {
  const respond = ({
    next,
    promise,
  }: {
    readonly next: Emitting;
    readonly promise: PromiseObject;
  }): Transition<(typeof Protocol.PromiseCreateResponse)["Type"]> => ({
    state: next.state,
    response: Protocol.PromiseCreateResponse.make({
      kind: "promise.create",
      head: { corrId: request.head.corrId, status: 200, version: request.head.version },
      data: { promise: promise.toRecord() },
    }),
    emitted: next.emitted,
  });
  const existing = HashMap.get(state.promises, request.data.id);
  if (Option.isSome(existing)) {
    return respond({ next: { state, emitted: [] }, promise: existing.value.projected(now) });
  }
  const { id, param, tags, timeoutAt } = request.data;
  if (millis(timeoutAt) > millis(now)) {
    const promise = new PromiseObject({
      id,
      state: "pending",
      param,
      value: Protocol.emptyValue,
      tags,
      timeoutAt,
      createdAt: now,
      settledAt: Option.none(),
      callbacks: [],
      listeners: [],
    });
    let next = setPromise({ state: state, promise: promise });
    if (promise.external) {
      next = setPromiseTimeout({ state: next, id: promise.id, at: promise.timeoutAt });
    }
    const target = promise.target;
    if (Option.isNone(target)) {
      return respond({ next: { state: next, emitted: [] }, promise: promise });
    }
    const task = new TaskObject({
      id: promise.id,
      state: "pending",
      version: Protocol.TaskVersion.make(0),
      pid: Option.none(),
      ttl: Option.none(),
      resumes: [],
    });
    next = setTask({ state: next, task: task });
    const delay = tags.reserved["resonate:delay"];
    if (Predicate.isNotUndefined(delay) && millis(delay) > millis(now)) {
      next = setTaskTimeout({ state: next, id: task.id, entry: { kind: 0, at: delay } });
      return respond({ next: { state: next, emitted: [] }, promise: promise });
    }
    next = setTaskTimeout({
      state: next,
      id: task.id,
      entry: { kind: 0, at: DateTime.addDuration(now, retryTimeout) },
    });
    const dispatched = setMessage({
      input: { state: next, emitted: [] },
      address: target.value,
      message: Protocol.ExecuteMessage.make({
        head: {},
        data: { task: { id: task.id, version: task.version } },
      }),
    });
    return respond({ next: dispatched, promise: promise });
  }
  const promise = new PromiseObject({
    id,
    state: tags.isTimer ? "resolved" : "rejected_timedout",
    param,
    value: Protocol.emptyValue,
    tags,
    timeoutAt,
    createdAt: timeoutAt,
    settledAt: Option.some(timeoutAt),
    callbacks: [],
    listeners: [],
  });
  let next = setPromise({ state: state, promise: promise });
  if (Predicate.isNotUndefined(tags.reserved["resonate:target"])) {
    next = setTask({
      state: next,
      task: new TaskObject({
        id: promise.id,
        state: "fulfilled",
        version: Protocol.TaskVersion.make(0),
        pid: Option.none(),
        ttl: Option.none(),
        resumes: [],
      }),
    });
  }
  return respond({ next: { state: next, emitted: [] }, promise: promise });
};
const promiseSettle = ({
  state,
  now,
  retryTimeout,
  request,
}: {
  readonly state: ServerState;
  readonly now: DateTime.Utc;
  readonly retryTimeout: Duration.Duration;
  readonly request: Protocol.Request<"promise.settle">;
}): Transition<(typeof Protocol.PromiseSettleResponse)["Type"]> => {
  const respond = ({
    next,
    promise,
  }: {
    readonly next: Emitting;
    readonly promise: PromiseObject;
  }): Transition<(typeof Protocol.PromiseSettleResponse)["Type"]> => ({
    state: next.state,
    response: Protocol.PromiseSettleResponse.make({
      kind: "promise.settle",
      head: { corrId: request.head.corrId, status: 200, version: request.head.version },
      data: { promise: promise.toRecord() },
    }),
    emitted: next.emitted,
  });
  const stored = HashMap.get(state.promises, request.data.id);
  if (Option.isNone(stored)) {
    return {
      state,
      response: Protocol.PromiseSettleResponse.make({
        kind: "promise.settle",
        head: { corrId: request.head.corrId, status: 404, version: request.head.version },
        data: "Promise not found",
      }),
      emitted: [],
    };
  }
  const promise = stored.value;
  if (promise.state !== "pending") {
    return respond({ next: { state, emitted: [] }, promise: promise });
  }
  if (millis(promise.timeoutAt) <= millis(now)) {
    return respond({ next: { state, emitted: [] }, promise: promise.projected(now) });
  }
  const settled = new PromiseObject({
    ...promise.fields,
    state: request.data.state,
    value: request.data.value,
    settledAt: Option.some(now),
    callbacks: [],
    listeners: [],
  });
  let next = setPromise({ state: state, promise: settled });
  next = delPromiseTimeout({ state: next, id: settled.id });
  const cascaded = settlementCascade({
    input: { state: next, emitted: [] },
    settled: settled,
    priorCallbacks: promise.callbacks,
    priorListeners: promise.listeners,
    now: now,
    retryTimeout: retryTimeout,
  });
  return respond({ next: cascaded, promise: settled });
};
const promiseRegisterCallback = ({
  state,
  now,
  request,
}: {
  readonly state: ServerState;
  readonly now: DateTime.Utc;
  readonly request: Protocol.Request<"promise.register_callback">;
}): Transition => {
  const fail = ({ status, message }: { readonly status: 400 | 404 | 422; readonly message: string }): Transition => ({
    state,
    response: Protocol.PromiseRegisterCallbackResponse.make({
      kind: "promise.register_callback",
      head: { corrId: request.head.corrId, status, version: request.head.version },
      data: message,
    }),
    emitted: [],
  });
  const respond = ({ next, promise }: { readonly next: ServerState; readonly promise: PromiseObject }): Transition => ({
    state: next,
    response: Protocol.PromiseRegisterCallbackResponse.make({
      kind: "promise.register_callback",
      head: { corrId: request.head.corrId, status: 200, version: request.head.version },
      data: { promise: promise.toRecord() },
    }),
    emitted: [],
  });
  if (request.data.awaited === request.data.awaiter) {
    return fail({ status: 400, message: "Awaited and awaiter must be different" });
  }
  const awaited = HashMap.get(state.promises, request.data.awaited);
  if (Option.isNone(awaited)) {
    return fail({ status: 404, message: "Awaited promise not found" });
  }
  const awaiter = HashMap.get(state.promises, request.data.awaiter);
  if (Option.isNone(awaiter)) {
    return fail({ status: 422, message: "Awaiter promise not found" });
  }
  if (Option.isNone(awaiter.value.target)) {
    return fail({ status: 422, message: "Awaiter has no address" });
  }
  if (awaited.value.state !== "pending") {
    return respond({ next: state, promise: awaited.value });
  }
  if (millis(awaited.value.timeoutAt) <= millis(now)) {
    return respond({ next: state, promise: awaited.value.projected(now) });
  }
  const awaiterFresh = awaiter.value.state === "pending" && millis(awaiter.value.timeoutAt) > millis(now);
  if (!awaiterFresh) {
    return respond({ next: state, promise: awaited.value });
  }
  const registered = Arr.contains(awaited.value.callbacks, request.data.awaiter)
    ? awaited.value
    : new PromiseObject({
        ...awaited.value.fields,
        callbacks: [...awaited.value.callbacks, request.data.awaiter],
      });
  return respond({ next: setPromise({ state: state, promise: registered }), promise: awaited.value });
};
const promiseRegisterListener = ({
  state,
  now,
  request,
}: {
  readonly state: ServerState;
  readonly now: DateTime.Utc;
  readonly request: Protocol.Request<"promise.register_listener">;
}): Transition => {
  const respond = ({ next, promise }: { readonly next: ServerState; readonly promise: PromiseObject }): Transition => ({
    state: next,
    response: Protocol.PromiseRegisterListenerResponse.make({
      kind: "promise.register_listener",
      head: { corrId: request.head.corrId, status: 200, version: request.head.version },
      data: { promise: promise.toRecord() },
    }),
    emitted: [],
  });
  const awaited = HashMap.get(state.promises, request.data.awaited);
  if (Option.isNone(awaited)) {
    return {
      state,
      response: Protocol.PromiseRegisterListenerResponse.make({
        kind: "promise.register_listener",
        head: { corrId: request.head.corrId, status: 404, version: request.head.version },
        data: "Awaited promise not found",
      }),
      emitted: [],
    };
  }
  if (awaited.value.state !== "pending") {
    return respond({ next: state, promise: awaited.value });
  }
  if (millis(awaited.value.timeoutAt) <= millis(now)) {
    return respond({ next: state, promise: awaited.value.projected(now) });
  }
  const address = request.data.address;
  const registered = Arr.some(awaited.value.listeners, (existing) => existing.address === address.address)
    ? awaited.value
    : new PromiseObject({
        ...awaited.value.fields,
        listeners: [...awaited.value.listeners, address],
      });
  return respond({ next: setPromise({ state: state, promise: registered }), promise: awaited.value });
};
const taskFresh = ({
  state,
  task,
  now,
}: {
  readonly state: ServerState;
  readonly task: TaskObject;
  readonly now: DateTime.Utc;
}): Option.Option<PromiseObject> => {
  const promise = HashMap.get(state.promises, task.id);
  return Option.filter(promise, (promise) => promise.state === "pending" && millis(promise.timeoutAt) > millis(now));
};
const taskGet = ({
  state,
  now,
  request,
}: {
  readonly state: ServerState;
  readonly now: DateTime.Utc;
  readonly request: Protocol.Request<"task.get">;
}): Transition => {
  const task = HashMap.get(state.tasks, request.data.id);
  if (Option.isNone(task) || Option.isNone(HashMap.get(state.promises, request.data.id))) {
    return {
      state,
      response: Protocol.TaskGetResponse.make({
        kind: "task.get",
        head: { corrId: request.head.corrId, status: 404, version: request.head.version },
        data: "Task not found",
      }),
      emitted: [],
    };
  }
  const projected = Option.isSome(taskFresh({ state: state, task: task.value, now: now }))
    ? task.value
    : new TaskObject({
        ...task.value.fields,
        state: "fulfilled",
        pid: Option.none(),
        ttl: Option.none(),
        resumes: [],
      });
  return {
    state,
    response: Protocol.TaskGetResponse.make({
      kind: "task.get",
      head: { corrId: request.head.corrId, status: 200, version: request.head.version },
      data: { task: projected.toRecord() },
    }),
    emitted: [],
  };
};
const taskCreate = ({
  state,
  now,
  request,
}: {
  readonly state: ServerState;
  readonly now: DateTime.Utc;
  readonly request: Protocol.Request<"task.create">;
}): Transition => {
  const respond = ({
    next,
    task,
    promise,
  }: {
    readonly next: Emitting;
    readonly task: Option.Option<TaskObject>;
    readonly promise: PromiseObject;
  }): Transition => ({
    state: next.state,
    response: Protocol.TaskCreateResponse.make({
      kind: "task.create",
      head: { corrId: request.head.corrId, status: 200, version: request.head.version },
      data: Option.isSome(task)
        ? {
            task: task.value.toRecord(),
            promise: promise.toRecord(),
            preload: preload({ state: next.state, id: promise.id }),
          }
        : { promise: promise.toRecord(), preload: preload({ state: next.state, id: promise.id }) },
    }),
    emitted: next.emitted,
  });
  const { pid, ttl } = request.data;
  const { id, param, tags, timeoutAt } = request.data.action.data;
  const existingPromise = HashMap.get(state.promises, id);
  if (Option.isSome(existingPromise)) {
    const existingTask = HashMap.get(state.tasks, id);
    if (Option.isNone(existingTask)) {
      return {
        state,
        response: Protocol.TaskCreateResponse.make({
          kind: "task.create",
          head: {
            corrId: request.head.corrId,
            status: Option.isSome(existingPromise.value.target) ? 409 : 422,
            version: request.head.version,
          },
          data: Option.isSome(existingPromise.value.target) ? "Promise already exists" : "Promise has no address",
        }),
        emitted: [],
      };
    }
    return Match.value(existingTask.value).pipe(
      Match.when({ state: "fulfilled" }, (task) =>
        respond({
          next: { state, emitted: [] },
          task: Option.some(task),
          promise: existingPromise.value.projected(now),
        }),
      ),
      Match.when({ state: "pending" }, (task) => {
        const acquired = new TaskObject({
          ...task.fields,
          state: "acquired",
          version: Protocol.TaskVersion.make(Num.increment(task.version)),
          pid: Option.some(pid),
          ttl: Option.some(ttl),
          resumes: [],
        });
        let next = setTask({ state: state, task: acquired });
        next = setTaskTimeout({ state: next, id: acquired.id, entry: { kind: 1, at: DateTime.addDuration(now, ttl) } });
        return respond({
          next: { state: next, emitted: [] },
          task: Option.some(acquired),
          promise: existingPromise.value.projected(now),
        });
      }),
      Match.orElse(() => ({
        state,
        response: Protocol.TaskCreateResponse.make({
          kind: "task.create",
          head: { corrId: request.head.corrId, status: 409, version: request.head.version },
          data: "Task already exists",
        }),
        emitted: [],
      })),
    );
  }
  if (millis(timeoutAt) <= millis(now)) {
    const promise = new PromiseObject({
      id,
      state: tags.isTimer ? "resolved" : "rejected_timedout",
      param,
      value: Protocol.emptyValue,
      tags,
      timeoutAt,
      createdAt: timeoutAt,
      settledAt: Option.some(timeoutAt),
      callbacks: [],
      listeners: [],
    });
    const task = new TaskObject({
      id,
      state: "fulfilled",
      version: Protocol.TaskVersion.make(0),
      pid: Option.none(),
      ttl: Option.none(),
      resumes: [],
    });
    return respond({
      next: { state: setTask({ state: setPromise({ state: state, promise: promise }), task: task }), emitted: [] },
      task: Option.some(task),
      promise: promise,
    });
  }
  const promise = new PromiseObject({
    id,
    state: "pending",
    param,
    value: Protocol.emptyValue,
    tags,
    timeoutAt,
    createdAt: now,
    settledAt: Option.none(),
    callbacks: [],
    listeners: [],
  });
  const task = new TaskObject({
    id,
    state: "acquired",
    version: Protocol.TaskVersion.make(1),
    pid: Option.some(pid),
    ttl: Option.some(ttl),
    resumes: [],
  });
  let next = setPromise({ state: state, promise: promise });
  next = setPromiseTimeout({ state: next, id: promise.id, at: promise.timeoutAt });
  next = setTask({ state: next, task: task });
  next = setTaskTimeout({ state: next, id: task.id, entry: { kind: 1, at: DateTime.addDuration(now, ttl) } });
  return respond({ next: { state: next, emitted: [] }, task: Option.some(task), promise: promise });
};
const taskAcquire = ({
  state,
  now,
  request,
}: {
  readonly state: ServerState;
  readonly now: DateTime.Utc;
  readonly request: Protocol.Request<"task.acquire">;
}): Transition => {
  const task = HashMap.get(state.tasks, request.data.id);
  if (Option.isNone(task)) {
    return {
      state,
      response: Protocol.TaskAcquireResponse.make({
        kind: "task.acquire",
        head: { corrId: request.head.corrId, status: 404, version: request.head.version },
        data: "Task not found",
      }),
      emitted: [],
    };
  }
  const promise = taskFresh({ state: state, task: task.value, now: now });
  if (task.value.state !== "pending" || Option.isNone(promise) || task.value.version !== request.data.version) {
    return {
      state,
      response: Protocol.TaskAcquireResponse.make({
        kind: "task.acquire",
        head: { corrId: request.head.corrId, status: 409, version: request.head.version },
        data: "Task not pending",
      }),
      emitted: [],
    };
  }
  const acquired = new TaskObject({
    ...task.value.fields,
    state: "acquired",
    version: Protocol.TaskVersion.make(Num.increment(task.value.version)),
    pid: Option.some(request.data.pid),
    ttl: Option.some(request.data.ttl),
    resumes: [],
  });
  let next = setTask({ state: state, task: acquired });
  next = setTaskTimeout({
    state: next,
    id: acquired.id,
    entry: {
      kind: 1,
      at: DateTime.addDuration(now, request.data.ttl),
    },
  });
  return {
    state: next,
    response: Protocol.TaskAcquireResponse.make({
      kind: "task.acquire",
      head: { corrId: request.head.corrId, status: 200, version: request.head.version },
      data: {
        task: acquired.toRecord(),
        promise: promise.value.toRecord(),
        preload: preload({ state: next, id: acquired.id }),
      },
    }),
    emitted: [],
  };
};
const taskGate = ({
  state,
  now,
  id,
  version,
}: {
  readonly state: ServerState;
  readonly now: DateTime.Utc;
  readonly id: Protocol.TaskId;
  readonly version: Protocol.TaskVersion;
}): Option.Option<{
  readonly task: TaskObject;
  readonly promise: PromiseObject;
}> => {
  const task = HashMap.get(state.tasks, id);
  if (Option.isNone(task) || task.value.state !== "acquired" || task.value.version !== version) {
    return Option.none();
  }
  return Option.map(taskFresh({ state: state, task: task.value, now: now }), (promise) => ({
    task: task.value,
    promise,
  }));
};
const taskRelease = ({
  state,
  now,
  retryTimeout,
  request,
}: {
  readonly state: ServerState;
  readonly now: DateTime.Utc;
  readonly retryTimeout: Duration.Duration;
  readonly request: Protocol.Request<"task.release">;
}): Transition => {
  const task = HashMap.get(state.tasks, request.data.id);
  if (Option.isNone(task)) {
    return {
      state,
      response: Protocol.TaskReleaseResponse.make({
        kind: "task.release",
        head: { corrId: request.head.corrId, status: 404, version: request.head.version },
        data: "Task not found",
      }),
      emitted: [],
    };
  }
  const gated = taskGate({ state: state, now: now, id: request.data.id, version: request.data.version });
  if (Option.isNone(gated)) {
    return {
      state,
      response: Protocol.TaskReleaseResponse.make({
        kind: "task.release",
        head: { corrId: request.head.corrId, status: 409, version: request.head.version },
        data: "Task not acquired",
      }),
      emitted: [],
    };
  }
  const released = new TaskObject({
    ...gated.value.task.fields,
    state: "pending",
    pid: Option.none(),
    ttl: Option.none(),
  });
  let next = setTask({ state: state, task: released });
  next = setTaskTimeout({
    state: next,
    id: released.id,
    entry: {
      kind: 0,
      at: DateTime.addDuration(now, retryTimeout),
    },
  });
  const output = maybeExecute({ input: { state: next, emitted: [] }, task: released });
  return {
    state: output.state,
    response: Protocol.TaskReleaseResponse.make({
      kind: "task.release",
      head: { corrId: request.head.corrId, status: 200, version: request.head.version },
      data: {},
    }),
    emitted: output.emitted,
  };
};
const taskSuspend = ({
  state,
  now,
  request,
}: {
  readonly state: ServerState;
  readonly now: DateTime.Utc;
  readonly request: Protocol.Request<"task.suspend">;
}): Transition => {
  const task = HashMap.get(state.tasks, request.data.id);
  if (Option.isNone(task)) {
    return {
      state,
      response: Protocol.TaskSuspendResponse.make({
        kind: "task.suspend",
        head: { corrId: request.head.corrId, status: 404, version: request.head.version },
        data: "Task not found",
      }),
      emitted: [],
    };
  }
  const malformed =
    request.data.actions.length === 0 ||
    Arr.some(
      request.data.actions,
      (action) => action.data.awaiter !== request.data.id || action.data.awaited === request.data.id,
    );
  if (malformed) {
    return {
      state,
      response: Protocol.TaskSuspendResponse.make({
        kind: "task.suspend",
        head: { corrId: request.head.corrId, status: 400, version: request.head.version },
        data: "Malformed suspend",
      }),
      emitted: [],
    };
  }
  const gated = taskGate({ state: state, now: now, id: request.data.id, version: request.data.version });
  if (Option.isNone(gated)) {
    return {
      state,
      response: Protocol.TaskSuspendResponse.make({
        kind: "task.suspend",
        head: { corrId: request.head.corrId, status: 409, version: request.head.version },
        data: "Task not acquired",
      }),
      emitted: [],
    };
  }
  let settled = false;
  const pending: Array<PromiseObject> = [];
  for (const action of request.data.actions) {
    const awaited = HashMap.get(state.promises, action.data.awaited);
    if (Option.isNone(awaited)) {
      return {
        state,
        response: Protocol.TaskSuspendResponse.make({
          kind: "task.suspend",
          head: { corrId: request.head.corrId, status: 422, version: request.head.version },
          data: "Awaited promise not found",
        }),
        emitted: [],
      };
    }
    if (awaited.value.projected(now).state === "pending") {
      pending.push(awaited.value);
    } else {
      settled = true;
    }
  }
  if (settled) {
    const cleared = new TaskObject({ ...gated.value.task.fields, resumes: [] });
    const next = setTask({ state: state, task: cleared });
    return {
      state: next,
      response: Protocol.TaskSuspendResponse.make({
        kind: "task.suspend",
        head: { corrId: request.head.corrId, status: 300, version: request.head.version },
        data: { preload: preload({ state: next, id: request.data.id }) },
      }),
      emitted: [],
    };
  }
  let next = state;
  for (const awaited of pending) {
    const registered = Arr.contains(awaited.callbacks, request.data.id)
      ? awaited
      : new PromiseObject({
          ...awaited.fields,
          callbacks: [...awaited.callbacks, request.data.id],
        });
    next = setPromise({ state: next, promise: registered });
  }
  const suspended = new TaskObject({
    ...gated.value.task.fields,
    state: "suspended",
    pid: Option.none(),
    ttl: Option.none(),
    resumes: [],
  });
  next = setTask({ state: next, task: suspended });
  next = delTaskTimeout({ state: next, id: suspended.id });
  return {
    state: next,
    response: Protocol.TaskSuspendResponse.make({
      kind: "task.suspend",
      head: { corrId: request.head.corrId, status: 200, version: request.head.version },
      data: {},
    }),
    emitted: [],
  };
};
const taskHalt = ({
  state,
  request,
}: {
  readonly state: ServerState;
  readonly request: Protocol.Request<"task.halt">;
}): Transition => {
  const task = HashMap.get(state.tasks, request.data.id);
  if (Option.isNone(task)) {
    return {
      state,
      response: Protocol.TaskHaltResponse.make({
        kind: "task.halt",
        head: { corrId: request.head.corrId, status: 404, version: request.head.version },
        data: "Task not found",
      }),
      emitted: [],
    };
  }
  if (task.value.state === "fulfilled") {
    return {
      state,
      response: Protocol.TaskHaltResponse.make({
        kind: "task.halt",
        head: { corrId: request.head.corrId, status: 409, version: request.head.version },
        data: "Task is fulfilled",
      }),
      emitted: [],
    };
  }
  if (task.value.state === "halted") {
    return {
      state,
      response: Protocol.TaskHaltResponse.make({
        kind: "task.halt",
        head: { corrId: request.head.corrId, status: 200, version: request.head.version },
        data: {},
      }),
      emitted: [],
    };
  }
  const halted = new TaskObject({
    ...task.value.fields,
    state: "halted",
    pid: Option.none(),
    ttl: Option.none(),
  });
  return {
    state: delTaskTimeout({ state: setTask({ state: state, task: halted }), id: halted.id }),
    response: Protocol.TaskHaltResponse.make({
      kind: "task.halt",
      head: { corrId: request.head.corrId, status: 200, version: request.head.version },
      data: {},
    }),
    emitted: [],
  };
};
const taskContinue = ({
  state,
  now,
  retryTimeout,
  request,
}: {
  readonly state: ServerState;
  readonly now: DateTime.Utc;
  readonly retryTimeout: Duration.Duration;
  readonly request: Protocol.Request<"task.continue">;
}): Transition => {
  const task = HashMap.get(state.tasks, request.data.id);
  if (Option.isNone(task)) {
    return {
      state,
      response: Protocol.TaskContinueResponse.make({
        kind: "task.continue",
        head: { corrId: request.head.corrId, status: 404, version: request.head.version },
        data: "Task not found",
      }),
      emitted: [],
    };
  }
  if (task.value.state !== "halted") {
    return {
      state,
      response: Protocol.TaskContinueResponse.make({
        kind: "task.continue",
        head: { corrId: request.head.corrId, status: 409, version: request.head.version },
        data: "Task is not halted",
      }),
      emitted: [],
    };
  }
  if (Option.isNone(HashMap.get(state.promises, request.data.id))) {
    return {
      state,
      response: Protocol.TaskContinueResponse.make({
        kind: "task.continue",
        head: { corrId: request.head.corrId, status: 404, version: request.head.version },
        data: "Promise not found",
      }),
      emitted: [],
    };
  }
  const continued = new TaskObject({ ...task.value.fields, state: "pending" });
  let next = setTask({ state: state, task: continued });
  next = setTaskTimeout({
    state: next,
    id: continued.id,
    entry: {
      kind: 0,
      at: DateTime.addDuration(now, retryTimeout),
    },
  });
  const output = maybeExecute({ input: { state: next, emitted: [] }, task: continued });
  return {
    state: output.state,
    response: Protocol.TaskContinueResponse.make({
      kind: "task.continue",
      head: { corrId: request.head.corrId, status: 200, version: request.head.version },
      data: {},
    }),
    emitted: output.emitted,
  };
};
const taskFulfill = ({
  state,
  now,
  retryTimeout,
  request,
}: {
  readonly state: ServerState;
  readonly now: DateTime.Utc;
  readonly retryTimeout: Duration.Duration;
  readonly request: Protocol.Request<"task.fulfill">;
}): Transition => {
  const task = HashMap.get(state.tasks, request.data.id);
  if (Option.isNone(task)) {
    return {
      state,
      response: Protocol.TaskFulfillResponse.make({
        kind: "task.fulfill",
        head: { corrId: request.head.corrId, status: 404, version: request.head.version },
        data: "Task not found",
      }),
      emitted: [],
    };
  }
  const gated = taskGate({ state: state, now: now, id: request.data.id, version: request.data.version });
  if (Option.isNone(gated) || request.data.action.data.id !== request.data.id) {
    return {
      state,
      response: Protocol.TaskFulfillResponse.make({
        kind: "task.fulfill",
        head: { corrId: request.head.corrId, status: 409, version: request.head.version },
        data: "Task not acquired",
      }),
      emitted: [],
    };
  }
  const settled = new PromiseObject({
    ...gated.value.promise.fields,
    state: request.data.action.data.state,
    value: request.data.action.data.value,
    settledAt: Option.some(now),
    callbacks: [],
    listeners: [],
  });
  let next = setPromise({ state: state, promise: settled });
  next = delPromiseTimeout({ state: next, id: settled.id });
  const output = settlementCascade({
    input: { state: next, emitted: [] },
    settled: settled,
    priorCallbacks: gated.value.promise.callbacks,
    priorListeners: gated.value.promise.listeners,
    now: now,
    retryTimeout: retryTimeout,
  });
  return {
    state: output.state,
    response: Protocol.TaskFulfillResponse.make({
      kind: "task.fulfill",
      head: { corrId: request.head.corrId, status: 200, version: request.head.version },
      data: { promise: settled.toRecord() },
    }),
    emitted: output.emitted,
  };
};
const taskFence = ({
  state,
  now,
  retryTimeout,
  request,
}: {
  readonly state: ServerState;
  readonly now: DateTime.Utc;
  readonly retryTimeout: Duration.Duration;
  readonly request: Protocol.Request<"task.fence">;
}): Transition => {
  const task = HashMap.get(state.tasks, request.data.id);
  if (Option.isNone(task)) {
    return {
      state,
      response: Protocol.TaskFenceResponse.make({
        kind: "task.fence",
        head: { corrId: request.head.corrId, status: 404, version: request.head.version },
        data: "Task not found",
      }),
      emitted: [],
    };
  }
  if (Option.isNone(taskGate({ state: state, now: now, id: request.data.id, version: request.data.version }))) {
    return {
      state,
      response: Protocol.TaskFenceResponse.make({
        kind: "task.fence",
        head: { corrId: request.head.corrId, status: 409, version: request.head.version },
        data: "Fence check failed",
      }),
      emitted: [],
    };
  }
  if (request.data.action.kind === "promise.create") {
    const inner = promiseCreate({ state: state, now: now, retryTimeout: retryTimeout, request: request.data.action });
    return {
      state: inner.state,
      response: Protocol.TaskFenceResponse.make({
        kind: "task.fence",
        head: { corrId: request.head.corrId, status: 200, version: request.head.version },
        data: { action: inner.response, preload: preload({ state: inner.state, id: request.data.id }) },
      }),
      emitted: inner.emitted,
    };
  }
  const inner = promiseSettle({ state: state, now: now, retryTimeout: retryTimeout, request: request.data.action });
  return {
    state: inner.state,
    response: Protocol.TaskFenceResponse.make({
      kind: "task.fence",
      head: { corrId: request.head.corrId, status: 200, version: request.head.version },
      data: { action: inner.response, preload: preload({ state: inner.state, id: request.data.id }) },
    }),
    emitted: inner.emitted,
  };
};
const taskHeartbeat = ({
  state,
  now,
  request,
}: {
  readonly state: ServerState;
  readonly now: DateTime.Utc;
  readonly request: Protocol.Request<"task.heartbeat">;
}): Transition => {
  let next = state;
  for (const ref of request.data.tasks) {
    const task = HashMap.get(next.tasks, ref.id);
    if (
      Option.isNone(task) ||
      task.value.state !== "acquired" ||
      task.value.version !== ref.version ||
      !Option.contains(task.value.pid, request.data.pid)
    ) {
      continue;
    }
    const promise = taskFresh({ state: next, task: task.value, now: now });
    if (Option.isNone(promise) || Option.isNone(task.value.ttl)) {
      continue;
    }
    next = setTaskTimeout({
      state: next,
      id: ref.id,
      entry: {
        kind: 1,
        at: DateTime.addDuration(now, task.value.ttl.value),
      },
    });
  }
  return {
    state: next,
    response: Protocol.TaskHeartbeatResponse.make({
      kind: "task.heartbeat",
      head: { corrId: request.head.corrId, status: 200, version: request.head.version },
      data: {},
    }),
    emitted: [],
  };
};
const FiveFieldCronExpression = Schema.String.check(
  Schema.makeFilter(
    (cron) => {
      const segments = Arr.filter(Str.split(" ")(cron), Str.isNonEmpty);
      return segments.length === 5 && Result.isSuccess(Cron.parse(cron));
    },
    { title: "five-field cron expression" },
  ),
);
const isFiveFieldCronExpression = SchemaParser.is(FiveFieldCronExpression);
const parseCron = (cron: string): Option.Option<Cron.Cron> => {
  if (!isFiveFieldCronExpression(cron)) {
    return Option.none();
  }
  const parsed = Cron.parse(cron);
  if (Result.isFailure(parsed)) {
    return Option.none();
  }
  return Option.some(parsed.success);
};
const scheduleGet = ({
  state,
  request,
}: {
  readonly state: ServerState;
  readonly request: Protocol.Request<"schedule.get">;
}): Transition => {
  const schedule = HashMap.get(state.schedules, request.data.id);
  if (Option.isNone(schedule)) {
    return {
      state,
      response: Protocol.ScheduleGetResponse.make({
        kind: "schedule.get",
        head: { corrId: request.head.corrId, status: 404, version: request.head.version },
        data: "Schedule not found",
      }),
      emitted: [],
    };
  }
  return {
    state,
    response: Protocol.ScheduleGetResponse.make({
      kind: "schedule.get",
      head: { corrId: request.head.corrId, status: 200, version: request.head.version },
      data: { schedule: schedule.value.toRecord() },
    }),
    emitted: [],
  };
};
const scheduleCreate = ({
  state,
  now,
  request,
}: {
  readonly state: ServerState;
  readonly now: DateTime.Utc;
  readonly request: Protocol.Request<"schedule.create">;
}): Transition => {
  const existing = HashMap.get(state.schedules, request.data.id);
  if (Option.isSome(existing)) {
    return {
      state,
      response: Protocol.ScheduleCreateResponse.make({
        kind: "schedule.create",
        head: { corrId: request.head.corrId, status: 200, version: request.head.version },
        data: { schedule: existing.value.toRecord() },
      }),
      emitted: [],
    };
  }
  const cron = parseCron(request.data.cron);
  if (Option.isNone(cron)) {
    return {
      state,
      response: Protocol.ScheduleCreateResponse.make({
        kind: "schedule.create",
        head: { corrId: request.head.corrId, status: 400, version: request.head.version },
        data: "Invalid cron expression",
      }),
      emitted: [],
    };
  }
  const schedule = new ScheduleObject({
    id: request.data.id,
    cron: request.data.cron,
    promiseId: request.data.promiseId,
    promiseTimeout: request.data.promiseTimeout,
    promiseParam: request.data.promiseParam,
    promiseTags: request.data.promiseTags,
    createdAt: now,
    nextRunAt: DateTime.makeUnsafe(Cron.next(cron.value, now).getTime()),
    lastRunAt: Option.none(),
  });
  let next = setSchedule({ state: state, schedule: schedule });
  next = setScheduleTimeout({ state: next, id: schedule.id, at: schedule.nextRunAt });
  return {
    state: next,
    response: Protocol.ScheduleCreateResponse.make({
      kind: "schedule.create",
      head: { corrId: request.head.corrId, status: 200, version: request.head.version },
      data: { schedule: schedule.toRecord() },
    }),
    emitted: [],
  };
};
const scheduleDelete = ({
  state,
  request,
}: {
  readonly state: ServerState;
  readonly request: Protocol.Request<"schedule.delete">;
}): Transition => {
  if (Option.isNone(HashMap.get(state.schedules, request.data.id))) {
    return {
      state,
      response: Protocol.ScheduleDeleteResponse.make({
        kind: "schedule.delete",
        head: { corrId: request.head.corrId, status: 404, version: request.head.version },
        data: "Schedule not found",
      }),
      emitted: [],
    };
  }
  const next = delScheduleTimeout({ state: delSchedule({ state: state, id: request.data.id }), id: request.data.id });
  return {
    state: next,
    response: Protocol.ScheduleDeleteResponse.make({
      kind: "schedule.delete",
      head: { corrId: request.head.corrId, status: 200, version: request.head.version },
      data: {},
    }),
    emitted: [],
  };
};
const internalHead = Protocol.RequestHead.make({
  corrId: Protocol.CorrelationId.make("local-schedule"),
  version: Protocol.protocolVersion,
});
const catchUpSchedule = ({
  input,
  schedule,
  now,
  retryTimeout,
}: {
  readonly input: Emitting;
  readonly schedule: ScheduleObject;
  readonly now: DateTime.Utc;
  readonly retryTimeout: Duration.Duration;
}): Emitting => {
  const cron = parseCron(schedule.cron);
  if (Option.isNone(cron)) {
    return input;
  }
  let output = input;
  let current = schedule;
  while (millis(current.nextRunAt) <= millis(now)) {
    const cronTime = current.nextRunAt;
    const promiseId = Protocol.PromiseId.make(
      Str.replaceAll(
        "{{.timestamp}}",
        Str.String(millis(cronTime)),
      )(Str.replaceAll("{{.id}}", current.id)(current.promiseId)),
    );
    const transition = promiseCreate({
      state: output.state,
      now: cronTime,
      retryTimeout: retryTimeout,
      request: Protocol.PromiseCreateRequest.make({
        head: internalHead,
        data: {
          id: promiseId,
          timeoutAt: DateTime.addDuration(cronTime, current.promiseTimeout),
          param: current.promiseParam,
          tags: Protocol.Tags.make({
            reserved: current.promiseTags.reserved,
            unrecognized: { ...current.promiseTags.unrecognized, "resonate:schedule": current.id },
            user: current.promiseTags.user,
          }),
        },
      }),
    });
    output = { state: transition.state, emitted: [...output.emitted, ...transition.emitted] };
    current = new ScheduleObject({
      ...current.fields,
      lastRunAt: Option.some(cronTime),
      nextRunAt: DateTime.makeUnsafe(Cron.next(cron.value, cronTime).getTime()),
    });
  }
  let next = setSchedule({ state: output.state, schedule: current });
  next = setScheduleTimeout({ state: next, id: current.id, at: current.nextRunAt });
  return { state: next, emitted: output.emitted };
};
const tick = ({
  state,
  now,
  retryTimeout,
}: {
  readonly state: ServerState;
  readonly now: DateTime.Utc;
  readonly retryTimeout: Duration.Duration;
}): {
  state: ServerState;
  emitted: ReadonlyArray<OutboxEntry>;
  actions: ReadonlyArray<Protocol.DebugTickAction>;
} => {
  const due: Array<PromiseObject> = [];
  for (const [id, at] of HashMap.entries(state.promiseTimeouts)) {
    if (millis(at) > millis(now)) {
      continue;
    }
    const promise = HashMap.get(state.promises, id);
    if (Option.isSome(promise) && promise.value.state === "pending") {
      due.push(promise.value);
    }
  }
  const actions: Array<Protocol.DebugTickAction> = [];
  let next = state;
  const settled: Array<{
    promise: PromiseObject;
    callbacks: ReadonlyArray<Protocol.PromiseId>;
    listeners: ReadonlyArray<Protocol.TargetAddress>;
  }> = [];
  for (const promise of due) {
    actions.push(
      Protocol.DebugTickAction.make({
        kind: "promise.settle",
        data: { id: promise.id, state: promise.timedOutState },
      }),
    );
    const persisted = new PromiseObject({
      ...promise.fields,
      state: promise.timedOutState,
      value: Protocol.emptyValue,
      settledAt: Option.some(promise.timeoutAt),
      callbacks: [],
      listeners: [],
    });
    next = setPromise({ state: next, promise: persisted });
    next = delPromiseTimeout({ state: next, id: promise.id });
    settled.push({
      promise: persisted,
      callbacks: promise.callbacks,
      listeners: promise.listeners,
    });
  }
  let output: Emitting = { state: next, emitted: [] };
  for (const { promise } of settled) {
    const task = HashMap.get(output.state.tasks, promise.id);
    if (Option.isSome(task) && task.value.state !== "fulfilled") {
      let phase = setTask({
        state: output.state,
        task: new TaskObject({
          ...task.value.fields,
          state: "fulfilled",
          pid: Option.none(),
          ttl: Option.none(),
          resumes: [],
        }),
      });
      phase = delTaskTimeout({ state: phase, id: promise.id });
      output = { state: phase, emitted: output.emitted };
    }
    output = {
      state: {
        ...output.state,
        promises: HashMap.map(output.state.promises, (candidate) =>
          candidate.state === "pending" && Arr.contains(candidate.callbacks, promise.id)
            ? new PromiseObject({
                ...candidate.fields,
                callbacks: Arr.filter(candidate.callbacks, (id) => id !== promise.id),
              })
            : candidate,
        ),
      },
      emitted: output.emitted,
    };
  }
  for (const { callbacks, listeners, promise } of settled) {
    for (const awaiterId of callbacks) {
      output = enqueueResume({
        input: output,
        awaitedId: promise.id,
        awaiterId: awaiterId,
        now: now,
        retryTimeout: retryTimeout,
      });
    }
    for (const address of listeners) {
      output = setMessage({
        input: output,
        address: address,
        message: Protocol.UnblockMessage.make({ head: {}, data: { promise: promise.toRecord() } }),
      });
    }
  }
  const dueTaskTimeouts = Arr.filter(
    Arr.fromIterable(HashMap.entries(output.state.taskTimeouts)),
    ([, entry]) => millis(entry.at) <= millis(now),
  );
  for (const [id, entry] of dueTaskTimeouts) {
    const task = HashMap.get(output.state.tasks, id);
    if (Option.isNone(task)) {
      continue;
    }
    if (entry.kind === 0 && task.value.state === "pending") {
      actions.push(
        Protocol.DebugTickAction.make({
          kind: "task.retry",
          data: { id, version: task.value.version },
        }),
      );
      const taskState = setTaskTimeout({
        state: output.state,
        id: id,
        entry: {
          kind: 0,
          at: DateTime.addDuration(now, retryTimeout),
        },
      });
      output = maybeExecute({ input: { state: taskState, emitted: output.emitted }, task: task.value });
    }
    if (entry.kind === 1 && task.value.state === "acquired") {
      actions.push(
        Protocol.DebugTickAction.make({
          kind: "task.release",
          data: { id, version: task.value.version },
        }),
      );
      const pending = new TaskObject({
        ...task.value.fields,
        state: "pending",
        pid: Option.none(),
        ttl: Option.none(),
      });
      let taskState = setTask({ state: output.state, task: pending });
      taskState = setTaskTimeout({
        state: taskState,
        id: id,
        entry: {
          kind: 0,
          at: DateTime.addDuration(now, retryTimeout),
        },
      });
      output = maybeExecute({ input: { state: taskState, emitted: output.emitted }, task: pending });
    }
  }
  const dueScheduleTimeouts = Arr.filter(
    Arr.fromIterable(HashMap.entries(output.state.scheduleTimeouts)),
    ([, at]) => millis(at) <= millis(now),
  );
  for (const [id] of dueScheduleTimeouts) {
    const schedule = HashMap.get(output.state.schedules, id);
    if (Option.isSome(schedule)) {
      output = catchUpSchedule({ input: output, schedule: schedule.value, now: now, retryTimeout: retryTimeout });
    }
  }
  return { state: output.state, emitted: output.emitted, actions };
};
const apply = ({
  state,
  now,
  retryTimeout,
  request,
}: {
  readonly state: ServerState;
  readonly now: DateTime.Utc;
  readonly retryTimeout: Duration.Duration;
  readonly request: Protocol.Request;
}): Transition => {
  return Match.value(request).pipe(
    Match.discriminatorsExhaustive("kind")({
      "promise.get": (request) => promiseGet({ state: state, now: now, request: request }),
      "promise.create": (request) =>
        promiseCreate({ state: state, now: now, retryTimeout: retryTimeout, request: request }),
      "promise.settle": (request) =>
        promiseSettle({ state: state, now: now, retryTimeout: retryTimeout, request: request }),
      "promise.register_callback": (request) => promiseRegisterCallback({ state: state, now: now, request: request }),
      "promise.register_listener": (request) => promiseRegisterListener({ state: state, now: now, request: request }),
      "promise.search": (request) => ({
        state,
        response: Protocol.PromiseSearchResponse.make({
          kind: "promise.search",
          head: { corrId: request.head.corrId, status: 501, version: request.head.version },
          data: "Not implemented",
        }),
        emitted: [],
      }),
      "debug.start": (request) => ({
        state,
        response: Protocol.DebugStartResponse.make({
          kind: "debug.start",
          head: { corrId: request.head.corrId, status: 200, version: request.head.version },
          data: {},
        }),
        emitted: [],
      }),
      "debug.stop": (request) => ({
        state,
        response: Protocol.DebugStopResponse.make({
          kind: "debug.stop",
          head: { corrId: request.head.corrId, status: 200, version: request.head.version },
          data: {},
        }),
        emitted: [],
      }),
      "debug.reset": (request) => ({
        state: initialState,
        response: Protocol.DebugResetResponse.make({
          kind: "debug.reset",
          head: { corrId: request.head.corrId, status: 200, version: request.head.version },
          data: {},
        }),
        emitted: [],
      }),
      "debug.tick": (request) => {
        const result = tick({ state: state, now: request.data.time, retryTimeout: retryTimeout });
        return {
          state: result.state,
          response: Protocol.DebugTickResponse.make({
            kind: "debug.tick",
            head: { corrId: request.head.corrId, status: 200, version: request.head.version },
            data: result.actions,
          }),
          emitted: result.emitted,
        };
      },
      "debug.snap": (request) => ({
        state,
        response: Protocol.DebugSnapResponse.make({
          kind: "debug.snap",
          head: { corrId: request.head.corrId, status: 200, version: request.head.version },
          data: {
            promises: Arr.map(Arr.fromIterable(HashMap.values(state.promises)), (promise) => promise.toRecord()),
            promiseTimeouts: Arr.map(Arr.fromIterable(HashMap.entries(state.promiseTimeouts)), ([id, at]) => ({
              id,
              timeout: at,
            })),
            callbacks: Arr.flatMap(Arr.fromIterable(HashMap.values(state.promises)), (promise) =>
              Arr.map(promise.callbacks, (awaiter) => ({ awaiter, awaited: promise.id })),
            ),
            listeners: Arr.flatMap(Arr.fromIterable(HashMap.values(state.promises)), (promise) =>
              Arr.map(promise.listeners, (address) => ({ id: promise.id, address: address.address })),
            ),
            tasks: Arr.map(Arr.fromIterable(HashMap.values(state.tasks)), (task) => task.toRecord()),
            taskTimeouts: Arr.map(Arr.fromIterable(HashMap.entries(state.taskTimeouts)), ([id, entry]) => ({
              id,
              type: entry.kind,
              timeout: entry.at,
            })),
            messages: Arr.map(state.outbox, (entry) => ({
              address: entry.address.address,
              message: entry.message,
            })),
          },
        }),
        emitted: [],
      }),
      "task.get": (request) => taskGet({ state: state, now: now, request: request }),
      "task.create": (request) => taskCreate({ state: state, now: now, request: request }),
      "task.acquire": (request) => taskAcquire({ state: state, now: now, request: request }),
      "task.release": (request) =>
        taskRelease({ state: state, now: now, retryTimeout: retryTimeout, request: request }),
      "task.suspend": (request) => taskSuspend({ state: state, now: now, request: request }),
      "task.halt": (request) => taskHalt({ state: state, request: request }),
      "task.continue": (request) =>
        taskContinue({ state: state, now: now, retryTimeout: retryTimeout, request: request }),
      "task.fulfill": (request) =>
        taskFulfill({ state: state, now: now, retryTimeout: retryTimeout, request: request }),
      "task.fence": (request) => taskFence({ state: state, now: now, retryTimeout: retryTimeout, request: request }),
      "task.heartbeat": (request) => taskHeartbeat({ state: state, now: now, request: request }),
      "task.search": (request) => ({
        state,
        response: Protocol.TaskSearchResponse.make({
          kind: "task.search",
          head: { corrId: request.head.corrId, status: 501, version: request.head.version },
          data: "Not implemented",
        }),
        emitted: [],
      }),
      "schedule.get": (request) => scheduleGet({ state: state, request: request }),
      "schedule.create": (request) => scheduleCreate({ state: state, now: now, request: request }),
      "schedule.delete": (request) => scheduleDelete({ state: state, request: request }),
      "schedule.search": (request) => ({
        state,
        response: Protocol.ScheduleSearchResponse.make({
          kind: "schedule.search",
          head: { corrId: request.head.corrId, status: 501, version: request.head.version },
          data: "Not implemented",
        }),
        emitted: [],
      }),
    }),
  );
};
/**
 * Options for the in-memory network implementation.
 *
 * @category models
 * @since 0.0.0
 */
export interface NetworkLocalOptions {
  readonly group?: string;
  readonly pid?: string;
  readonly retryTimeout?: Duration.Duration;
  readonly tickInterval?: Duration.Duration;
}
/**
 * Builds an in-memory Resonate network layer.
 *
 * @category layers
 * @since 0.0.0
 */
export const layer = (options?: NetworkLocalOptions): Layer.Layer<ResonateNetwork> =>
  Layer.effect(
    ResonateNetwork,
    Effect.gen(function* () {
      const group = Protocol.WorkerGroup.make(options?.group ?? "default");
      const pid = Protocol.ProcessId.make(options?.pid ?? "local");
      const retryTimeout = options?.retryTimeout ?? Duration.seconds(5);
      const tickInterval = options?.tickInterval ?? Duration.seconds(1);
      const ref = yield* Ref.make(initialState);
      const queue = yield* Queue.unbounded<Protocol.Message>();
      const applyRequest = Effect.fn("NetworkLocal.apply")(function* (request: Protocol.Request) {
        const now = yield* DateTime.now;
        const [response, emitted] = yield* Ref.modify(ref, (state) => {
          const transition = apply({ state: state, now: now, retryTimeout: retryTimeout, request: request });
          return [[transition.response, transition.emitted] as const, transition.state];
        });
        yield* Effect.forEach(emitted, (entry) => Queue.offer(queue, entry.message), { discard: true });
        return response;
      });
      yield* Effect.gen(function* () {
        const now = yield* DateTime.now;
        const emitted = yield* Ref.modify(ref, (state) => {
          const result = tick({ state: state, now: now, retryTimeout: retryTimeout });
          return [result.emitted, result.state];
        });
        yield* Effect.forEach(emitted, (entry) => Queue.offer(queue, entry.message), { discard: true });
      }).pipe(Effect.delay(tickInterval), Effect.forever, Effect.forkScoped);
      return ResonateNetwork.of({
        send: Effect.fn("NetworkLocal.send")(function* (request) {
          const response = yield* applyRequest(request);
          const wire = yield* Effect.orDie(Schema.encodeUnknownEffect(Protocol.ResponseFromWire)(response));
          return yield* decodeResponse(request)(wire);
        }),
        messages: Stream.fromQueue(queue),
        match: (target) => Protocol.TargetAddress.localAny({ group: target }),
        unicast: Protocol.TargetAddress.localUni({ group, id: pid }),
        anycast: (target) => Protocol.TargetAddress.localAny({ group: target, id: Option.some(pid) }),
      });
    }),
  );
