/**
 * In-memory server (dev + conformance oracle).
 *
 * Implements the Lean abstract machine 1:1 — `spec/02-actions/P-01…P-05`,
 * `00-resume.lean`, `02-timeouts.lean` — following the annotated native
 * reference (`repos/resonate-sdk-ts/src/network/local.ts`) where the Lean
 * model is silent. Written against Effect's `Clock`, so `TestClock` drives
 * all time-dependent behavior in tests and a periodic tick drives dev mode.
 *
 * See `docs/DESIGN.md` §3.1 (`NetworkLocal.layer`) and §8.
 *
 * Spec 04 scope: the promise state machine. Task and schedule operations
 * return `501` until specs 05/06 land.
 */
import {
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
  Schema,
  Stream,
} from "effect";
import { decodeResponse, ResonateNetwork } from "./Network.ts";
import * as Protocol from "./Protocol.ts";

// -----------------------------------------------------------------------------
// Server objects — the Lean `PromiseObject`/`TaskObject` (records + registrations)
// -----------------------------------------------------------------------------

export class PromiseObject extends Schema.Class<PromiseObject>("NetworkLocal/PromiseObject")({
  id: Protocol.PromiseId,
  state: Protocol.PromiseState,
  param: Protocol.Value,
  value: Protocol.Value,
  tags: Protocol.Tags,
  timeoutAt: Schema.DateTimeUtc,
  createdAt: Schema.DateTimeUtc,
  settledAt: Schema.Option(Schema.DateTimeUtc),
  /** Ids of promises (tasks) awaiting this one. */
  callbacks: Schema.Array(Protocol.PromiseId),
  /** Addresses listening for this promise's settlement. */
  listeners: Schema.Array(Protocol.TargetAddress),
}) {
  get isTimer(): boolean {
    return this.tags.isTimer;
  }

  get target(): Option.Option<Protocol.TargetAddress> {
    return Option.fromNullishOr(this.tags.reserved["resonate:target"]);
  }

  /** A promise is external iff it carries a target or is a timer (Lean `external`). */
  get external(): boolean {
    return Option.isSome(this.target) || this.isTimer;
  }

  get timedOutState(): "resolved" | "rejected_timedout" {
    return this.isTimer ? "resolved" : "rejected_timedout";
  }

  /** The Lean timeout projection — the logical view, no persistence. */
  projected(now: DateTime.Utc): PromiseObject {
    if (this.state !== "pending" || DateTime.toEpochMillis(this.timeoutAt) > DateTime.toEpochMillis(now)) {
      return this;
    }
    return new PromiseObject({
      ...this,
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
  /** Awaited ids that settled while the task was pending/acquired/halted (R in the spec). */
  resumes: Schema.Array(Protocol.PromiseId),
}) {
  toRecord(): Protocol.TaskRecord {
    const common = {
      id: this.id,
      version: this.version,
      // Lean `toRecord`: resumes is reported as a count.
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

/** `0` = pending retry, `1` = lease expiration (Lean `TaskTimeout.kind`). */
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
  readonly promiseTimeouts: HashMap.HashMap<Protocol.PromiseId, DateTime.Utc>;
  readonly taskTimeouts: HashMap.HashMap<Protocol.TaskId, TaskTimeoutEntry>;
  readonly outbox: ReadonlyArray<OutboxEntry>;
}

const initialState: ServerState = {
  promises: HashMap.empty(),
  tasks: HashMap.empty(),
  promiseTimeouts: HashMap.empty(),
  taskTimeouts: HashMap.empty(),
  outbox: [],
};

// -----------------------------------------------------------------------------
// Pure state helpers (the Lean StateM primitives)
// -----------------------------------------------------------------------------

interface Emitting {
  readonly state: ServerState;
  readonly emitted: ReadonlyArray<OutboxEntry>;
}

const setPromise = (state: ServerState, promise: PromiseObject): ServerState => ({
  ...state,
  promises: HashMap.set(state.promises, promise.id, promise),
});

const setTask = (state: ServerState, task: TaskObject): ServerState => ({
  ...state,
  tasks: HashMap.set(state.tasks, task.id, task),
});

const setPromiseTimeout = (state: ServerState, id: Protocol.PromiseId, at: DateTime.Utc): ServerState => ({
  ...state,
  promiseTimeouts: HashMap.set(state.promiseTimeouts, id, at),
});

const delPromiseTimeout = (state: ServerState, id: Protocol.PromiseId): ServerState => ({
  ...state,
  promiseTimeouts: HashMap.remove(state.promiseTimeouts, id),
});

const setTaskTimeout = (state: ServerState, id: Protocol.TaskId, entry: TaskTimeoutEntry): ServerState => ({
  ...state,
  taskTimeouts: HashMap.set(state.taskTimeouts, id, entry),
});

const delTaskTimeout = (state: ServerState, id: Protocol.TaskId): ServerState => ({
  ...state,
  taskTimeouts: HashMap.remove(state.taskTimeouts, id),
});

/** Lean `OutboxEntry.key`: one pending execute per task, one unblock per (promise, address). */
const outboxKey = (entry: OutboxEntry): string =>
  entry.message.kind === "execute"
    ? `execute:${entry.message.data.task.id}`
    : `unblock:${entry.message.data.promise.id}:${entry.address.address}`;

/** Coalesce into the outbox and emit for immediate local delivery. */
const setMessage = (
  { emitted, state }: Emitting,
  address: Protocol.TargetAddress,
  message: Protocol.Message,
): Emitting => {
  const entry: OutboxEntry = { address, message };
  const key = outboxKey(entry);
  const index = state.outbox.findIndex((existing) => outboxKey(existing) === key);
  const outbox = index >= 0 ? state.outbox.with(index, entry) : [...state.outbox, entry];
  return { state: { ...state, outbox }, emitted: [...emitted, entry] };
};

const millis = DateTime.toEpochMillis;

// -----------------------------------------------------------------------------
// The resume cascade (spec/02-actions/00-resume.lean)
// -----------------------------------------------------------------------------

const enqueueResume = (
  { emitted, state }: Emitting,
  awaitedId: Protocol.PromiseId,
  awaiterId: Protocol.PromiseId,
  now: DateTime.Utc,
  retryTimeout: Duration.Duration,
): Emitting => {
  const task = HashMap.get(state.tasks, awaiterId);
  if (Option.isNone(task)) {
    return { state, emitted };
  }
  return Match.value(task.value).pipe(
    Match.when({ state: "suspended" }, (suspended) => {
      // Version is bumped ONLY on acquire — the execute is a wake-up hint.
      const resumed = new TaskObject({ ...suspended, state: "pending", resumes: [awaitedId] });
      let next = setTask(state, resumed);
      next = setTaskTimeout(next, resumed.id, { kind: 0, at: DateTime.addDuration(now, retryTimeout) });
      const awaiterPromise = HashMap.get(next.promises, awaiterId);
      const target = Option.flatMap(awaiterPromise, (promise) => promise.target);
      if (Option.isNone(target)) {
        return { state: next, emitted };
      }
      return setMessage(
        { state: next, emitted },
        target.value,
        Protocol.ExecuteMessage.make({ head: {}, data: { task: { id: resumed.id, version: resumed.version } } }),
      );
    }),
    Match.whenOr({ state: "pending" }, { state: "acquired" }, { state: "halted" }, (buffered) => {
      if (buffered.resumes.includes(awaitedId)) {
        return { state, emitted };
      }
      return {
        state: setTask(state, new TaskObject({ ...buffered, resumes: [...buffered.resumes, awaitedId] })),
        emitted,
      };
    }),
    Match.when({ state: "fulfilled" }, () => ({ state, emitted })),
    Match.exhaustive,
  );
};

/**
 * The settlement cascade shared by `promise.settle` and `onPromiseTimeout`:
 * force-fulfill the companion task, scrub the settled id from every pending
 * promise's callbacks, notify listeners, resume callbacks.
 */
const settlementCascade = (
  input: Emitting,
  settled: PromiseObject,
  priorCallbacks: ReadonlyArray<Protocol.PromiseId>,
  priorListeners: ReadonlyArray<Protocol.TargetAddress>,
  now: DateTime.Utc,
  retryTimeout: Duration.Duration,
): Emitting => {
  let { emitted, state } = input;

  const task = HashMap.get(state.tasks, settled.id);
  if (Option.isSome(task)) {
    state = setTask(
      state,
      new TaskObject({ ...task.value, state: "fulfilled", pid: Option.none(), ttl: Option.none(), resumes: [] }),
    );
    state = delTaskTimeout(state, settled.id);
  }

  // Settlement scrub: the settled promise can never be resumed again.
  state = {
    ...state,
    promises: HashMap.map(state.promises, (promise) =>
      promise.state === "pending" && promise.callbacks.includes(settled.id)
        ? new PromiseObject({ ...promise, callbacks: promise.callbacks.filter((id) => id !== settled.id) })
        : promise,
    ),
  };

  let next: Emitting = { state, emitted };
  for (const address of priorListeners) {
    next = setMessage(next, address, Protocol.UnblockMessage.make({ head: {}, data: { promise: settled.toRecord() } }));
  }
  for (const awaiterId of priorCallbacks) {
    next = enqueueResume(next, settled.id, awaiterId, now, retryTimeout);
  }
  return next;
};

// -----------------------------------------------------------------------------
// Promise handlers (spec/02-actions/P-01…P-05)
// -----------------------------------------------------------------------------

interface Transition {
  readonly state: ServerState;
  readonly response: Protocol.Response;
  readonly emitted: ReadonlyArray<OutboxEntry>;
}

const head = (request: Protocol.Request, status: 200) => ({
  corrId: request.head.corrId,
  status,
  version: request.head.version,
});

const errorHead = (request: Protocol.Request, status: 400 | 404 | 422 | 501) => ({
  corrId: request.head.corrId,
  status,
  version: request.head.version,
});

const promiseGet = (state: ServerState, now: DateTime.Utc, request: Protocol.Request<"promise.get">): Transition => {
  const promise = HashMap.get(state.promises, request.data.id);
  if (Option.isNone(promise)) {
    return {
      state,
      response: Protocol.PromiseGetResponse.make({
        kind: "promise.get",
        head: errorHead(request, 404),
        data: "Promise not found",
      }),
      emitted: [],
    };
  }
  return {
    state,
    response: Protocol.PromiseGetResponse.make({
      kind: "promise.get",
      head: head(request, 200),
      data: { promise: promise.value.projected(now).toRecord() },
    }),
    emitted: [],
  };
};

const promiseCreate = (
  state: ServerState,
  now: DateTime.Utc,
  retryTimeout: Duration.Duration,
  request: Protocol.Request<"promise.create">,
): Transition => {
  const respond = (next: Emitting, promise: PromiseObject): Transition => ({
    state: next.state,
    response: Protocol.PromiseCreateResponse.make({
      kind: "promise.create",
      head: head(request, 200),
      data: { promise: promise.toRecord() },
    }),
    emitted: next.emitted,
  });

  const existing = HashMap.get(state.promises, request.data.id);
  if (Option.isSome(existing)) {
    // Idempotent re-create: stored record (projected), body completely ignored.
    return respond({ state, emitted: [] }, existing.value.projected(now));
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
    let next = setPromise(state, promise);
    if (promise.external) {
      next = setPromiseTimeout(next, promise.id, promise.timeoutAt);
    }
    const target = promise.target;
    if (Option.isNone(target)) {
      return respond({ state: next, emitted: [] }, promise);
    }
    const task = new TaskObject({
      id: promise.id,
      state: "pending",
      version: Protocol.TaskVersion.make(0),
      pid: Option.none(),
      ttl: Option.none(),
      resumes: [],
    });
    next = setTask(next, task);
    const delay = tags.reserved["resonate:delay"];
    if (Predicate.isNotUndefined(delay) && millis(delay) > millis(now)) {
      // Deferred dispatch: the retry timeout fires at the delay instant.
      next = setTaskTimeout(next, task.id, { kind: 0, at: delay });
      return respond({ state: next, emitted: [] }, promise);
    }
    next = setTaskTimeout(next, task.id, { kind: 0, at: DateTime.addDuration(now, retryTimeout) });
    const dispatched = setMessage(
      { state: next, emitted: [] },
      target.value,
      Protocol.ExecuteMessage.make({ head: {}, data: { task: { id: task.id, version: task.version } } }),
    );
    return respond(dispatched, promise);
  }

  // Born already settled — backdated createdAt/settledAt, no timeout, no dispatch.
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
  let next = setPromise(state, promise);
  if (Predicate.isNotUndefined(tags.reserved["resonate:target"])) {
    next = setTask(
      next,
      new TaskObject({
        id: promise.id,
        state: "fulfilled",
        version: Protocol.TaskVersion.make(0),
        pid: Option.none(),
        ttl: Option.none(),
        resumes: [],
      }),
    );
  }
  return respond({ state: next, emitted: [] }, promise);
};

const promiseSettle = (
  state: ServerState,
  now: DateTime.Utc,
  retryTimeout: Duration.Duration,
  request: Protocol.Request<"promise.settle">,
): Transition => {
  const respond = (next: Emitting, promise: PromiseObject): Transition => ({
    state: next.state,
    response: Protocol.PromiseSettleResponse.make({
      kind: "promise.settle",
      head: head(request, 200),
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
        head: errorHead(request, 404),
        data: "Promise not found",
      }),
      emitted: [],
    };
  }
  const promise = stored.value;
  if (promise.state !== "pending") {
    // Already settled — idempotent.
    return respond({ state, emitted: [] }, promise);
  }
  if (millis(promise.timeoutAt) <= millis(now)) {
    // Projected-timeout race: the caller's requested state is ignored.
    return respond({ state, emitted: [] }, promise.projected(now));
  }

  const settled = new PromiseObject({
    ...promise,
    state: request.data.state,
    value: request.data.value,
    settledAt: Option.some(now),
    callbacks: [],
    listeners: [],
  });
  let next = setPromise(state, settled);
  next = delPromiseTimeout(next, settled.id);
  const cascaded = settlementCascade(
    { state: next, emitted: [] },
    settled,
    promise.callbacks,
    promise.listeners,
    now,
    retryTimeout,
  );
  return respond(cascaded, settled);
};

const promiseRegisterCallback = (
  state: ServerState,
  now: DateTime.Utc,
  request: Protocol.Request<"promise.register_callback">,
): Transition => {
  const fail = (status: 400 | 404 | 422, message: string): Transition => ({
    state,
    response: Protocol.PromiseRegisterCallbackResponse.make({
      kind: "promise.register_callback",
      head: errorHead(request, status),
      data: message,
    }),
    emitted: [],
  });
  const respond = (next: ServerState, promise: PromiseObject): Transition => ({
    state: next,
    response: Protocol.PromiseRegisterCallbackResponse.make({
      kind: "promise.register_callback",
      head: head(request, 200),
      data: { promise: promise.toRecord() },
    }),
    emitted: [],
  });

  // Native validate(): self-await is malformed.
  if (request.data.awaited === request.data.awaiter) {
    return fail(400, "Awaited and awaiter must be different");
  }
  const awaited = HashMap.get(state.promises, request.data.awaited);
  if (Option.isNone(awaited)) {
    return fail(404, "Awaited promise not found");
  }
  const awaiter = HashMap.get(state.promises, request.data.awaiter);
  if (Option.isNone(awaiter)) {
    return fail(422, "Awaiter promise not found");
  }
  if (Option.isNone(awaiter.value.target)) {
    return fail(422, "Awaiter has no address");
  }
  if (awaited.value.state !== "pending") {
    return respond(state, awaited.value);
  }
  if (millis(awaited.value.timeoutAt) <= millis(now)) {
    return respond(state, awaited.value.projected(now));
  }
  // Registration happens only when BOTH sides are pending and fresh; an
  // expired awaiter is silently skipped (still 200).
  const awaiterFresh = awaiter.value.state === "pending" && millis(awaiter.value.timeoutAt) > millis(now);
  if (!awaiterFresh) {
    return respond(state, awaited.value);
  }
  const registered = awaited.value.callbacks.includes(request.data.awaiter)
    ? awaited.value
    : new PromiseObject({ ...awaited.value, callbacks: [...awaited.value.callbacks, request.data.awaiter] });
  return respond(setPromise(state, registered), awaited.value);
};

const promiseRegisterListener = (
  state: ServerState,
  now: DateTime.Utc,
  request: Protocol.Request<"promise.register_listener">,
): Transition => {
  const respond = (next: ServerState, promise: PromiseObject): Transition => ({
    state: next,
    response: Protocol.PromiseRegisterListenerResponse.make({
      kind: "promise.register_listener",
      head: head(request, 200),
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
        head: errorHead(request, 404),
        data: "Awaited promise not found",
      }),
      emitted: [],
    };
  }
  if (awaited.value.state !== "pending") {
    return respond(state, awaited.value);
  }
  if (millis(awaited.value.timeoutAt) <= millis(now)) {
    return respond(state, awaited.value.projected(now));
  }
  const address = request.data.address;
  const registered = awaited.value.listeners.some((existing) => existing.address === address.address)
    ? awaited.value
    : new PromiseObject({ ...awaited.value, listeners: [...awaited.value.listeners, address] });
  return respond(setPromise(state, registered), awaited.value);
};

// -----------------------------------------------------------------------------
// The tick — native debugTick's three-phase convergence (promise half)
// -----------------------------------------------------------------------------

/**
 * Phase 1: persist the projection for every expired pending promise.
 * Phase 2: force-fulfill their companion tasks (suspended → fulfilled DIRECTLY,
 *          never suspended → pending → fulfilled) and scrub dead callbacks.
 * Phase 3: resume callbacks and notify listeners.
 */
const tick = (
  state: ServerState,
  now: DateTime.Utc,
  retryTimeout: Duration.Duration,
): { state: ServerState; emitted: ReadonlyArray<OutboxEntry>; actions: ReadonlyArray<Protocol.DebugTickAction> } => {
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

  // Phase 1 — settle.
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
      ...promise,
      state: promise.timedOutState,
      value: Protocol.emptyValue,
      settledAt: Option.some(promise.timeoutAt),
      callbacks: [],
      listeners: [],
    });
    next = setPromise(next, persisted);
    next = delPromiseTimeout(next, promise.id);
    settled.push({ promise: persisted, callbacks: promise.callbacks, listeners: promise.listeners });
  }

  // Phase 2 — fulfill companion tasks and scrub.
  let output: Emitting = { state: next, emitted: [] };
  for (const { promise } of settled) {
    const task = HashMap.get(output.state.tasks, promise.id);
    if (Option.isSome(task) && task.value.state !== "fulfilled") {
      let phase = setTask(
        output.state,
        new TaskObject({ ...task.value, state: "fulfilled", pid: Option.none(), ttl: Option.none(), resumes: [] }),
      );
      phase = delTaskTimeout(phase, promise.id);
      output = { state: phase, emitted: output.emitted };
    }
    output = {
      state: {
        ...output.state,
        promises: HashMap.map(output.state.promises, (candidate) =>
          candidate.state === "pending" && candidate.callbacks.includes(promise.id)
            ? new PromiseObject({ ...candidate, callbacks: candidate.callbacks.filter((id) => id !== promise.id) })
            : candidate,
        ),
      },
      emitted: output.emitted,
    };
  }

  // Phase 3 — resume callbacks, notify listeners.
  for (const { callbacks, listeners, promise } of settled) {
    for (const awaiterId of callbacks) {
      output = enqueueResume(output, promise.id, awaiterId, now, retryTimeout);
    }
    for (const address of listeners) {
      output = setMessage(
        output,
        address,
        Protocol.UnblockMessage.make({ head: {}, data: { promise: promise.toRecord() } }),
      );
    }
  }

  return { state: output.state, emitted: output.emitted, actions };
};

// -----------------------------------------------------------------------------
// Request dispatch
// -----------------------------------------------------------------------------

const apply = (
  state: ServerState,
  now: DateTime.Utc,
  retryTimeout: Duration.Duration,
  request: Protocol.Request,
): Transition => {
  return Match.value(request).pipe(
    Match.discriminatorsExhaustive("kind")({
      "promise.get": (request) => promiseGet(state, now, request),
      "promise.create": (request) => promiseCreate(state, now, retryTimeout, request),
      "promise.settle": (request) => promiseSettle(state, now, retryTimeout, request),
      "promise.register_callback": (request) => promiseRegisterCallback(state, now, request),
      "promise.register_listener": (request) => promiseRegisterListener(state, now, request),
      "promise.search": (request) => ({
        state,
        response: Protocol.PromiseSearchResponse.make({
          kind: "promise.search",
          head: errorHead(request, 501),
          data: "Not implemented",
        }),
        emitted: [],
      }),
      "debug.start": (request) => ({
        state,
        response: Protocol.DebugStartResponse.make({ kind: "debug.start", head: head(request, 200), data: {} }),
        emitted: [],
      }),
      "debug.stop": (request) => ({
        state,
        response: Protocol.DebugStopResponse.make({ kind: "debug.stop", head: head(request, 200), data: {} }),
        emitted: [],
      }),
      "debug.reset": (request) => ({
        state: initialState,
        response: Protocol.DebugResetResponse.make({ kind: "debug.reset", head: head(request, 200), data: {} }),
        emitted: [],
      }),
      "debug.tick": (request) => {
        const result = tick(state, request.data.time, retryTimeout);
        return {
          state: result.state,
          response: Protocol.DebugTickResponse.make({
            kind: "debug.tick",
            head: head(request, 200),
            data: result.actions,
          }),
          emitted: result.emitted,
        };
      },
      "debug.snap": (request) => ({
        state,
        response: Protocol.DebugSnapResponse.make({
          kind: "debug.snap",
          head: head(request, 200),
          data: {
            promises: [...HashMap.values(state.promises)].map((promise) => promise.toRecord()),
            promiseTimeouts: [...HashMap.entries(state.promiseTimeouts)].map(([id, at]) => ({ id, timeout: at })),
            callbacks: [...HashMap.values(state.promises)].flatMap((promise) =>
              promise.callbacks.map((awaiter) => ({ awaiter, awaited: promise.id })),
            ),
            listeners: [...HashMap.values(state.promises)].flatMap((promise) =>
              promise.listeners.map((address) => ({ id: promise.id, address: address.address })),
            ),
            tasks: [...HashMap.values(state.tasks)].map((task) => task.toRecord()),
            taskTimeouts: [...HashMap.entries(state.taskTimeouts)].map(([id, entry]) => ({
              id,
              type: entry.kind,
              timeout: entry.at,
            })),
            messages: [...state.outbox].map((entry) => ({ address: entry.address.address, message: entry.message })),
          },
        }),
        emitted: [],
      }),
      "task.get": (request) => ({
        state,
        response: Protocol.TaskGetResponse.make({
          kind: "task.get",
          head: errorHead(request, 501),
          data: "Not implemented",
        }),
        emitted: [],
      }),
      "task.create": (request) => ({
        state,
        response: Protocol.TaskCreateResponse.make({
          kind: "task.create",
          head: errorHead(request, 501),
          data: "Not implemented",
        }),
        emitted: [],
      }),
      "task.acquire": (request) => ({
        state,
        response: Protocol.TaskAcquireResponse.make({
          kind: "task.acquire",
          head: errorHead(request, 501),
          data: "Not implemented",
        }),
        emitted: [],
      }),
      "task.release": (request) => ({
        state,
        response: Protocol.TaskReleaseResponse.make({
          kind: "task.release",
          head: errorHead(request, 501),
          data: "Not implemented",
        }),
        emitted: [],
      }),
      "task.suspend": (request) => ({
        state,
        response: Protocol.TaskSuspendResponse.make({
          kind: "task.suspend",
          head: errorHead(request, 501),
          data: "Not implemented",
        }),
        emitted: [],
      }),
      "task.halt": (request) => ({
        state,
        response: Protocol.TaskHaltResponse.make({
          kind: "task.halt",
          head: errorHead(request, 501),
          data: "Not implemented",
        }),
        emitted: [],
      }),
      "task.continue": (request) => ({
        state,
        response: Protocol.TaskContinueResponse.make({
          kind: "task.continue",
          head: errorHead(request, 501),
          data: "Not implemented",
        }),
        emitted: [],
      }),
      "task.fulfill": (request) => ({
        state,
        response: Protocol.TaskFulfillResponse.make({
          kind: "task.fulfill",
          head: errorHead(request, 501),
          data: "Not implemented",
        }),
        emitted: [],
      }),
      "task.fence": (request) => ({
        state,
        response: Protocol.TaskFenceResponse.make({
          kind: "task.fence",
          head: errorHead(request, 501),
          data: "Not implemented",
        }),
        emitted: [],
      }),
      "task.heartbeat": (request) => ({
        state,
        response: Protocol.TaskHeartbeatResponse.make({
          kind: "task.heartbeat",
          head: errorHead(request, 501),
          data: "Not implemented",
        }),
        emitted: [],
      }),
      "task.search": (request) => ({
        state,
        response: Protocol.TaskSearchResponse.make({
          kind: "task.search",
          head: errorHead(request, 501),
          data: "Not implemented",
        }),
        emitted: [],
      }),
      "schedule.get": (request) => ({
        state,
        response: Protocol.ScheduleGetResponse.make({
          kind: "schedule.get",
          head: errorHead(request, 501),
          data: "Not implemented",
        }),
        emitted: [],
      }),
      "schedule.create": (request) => ({
        state,
        response: Protocol.ScheduleCreateResponse.make({
          kind: "schedule.create",
          head: errorHead(request, 501),
          data: "Not implemented",
        }),
        emitted: [],
      }),
      "schedule.delete": (request) => ({
        state,
        response: Protocol.ScheduleDeleteResponse.make({
          kind: "schedule.delete",
          head: errorHead(request, 501),
          data: "Not implemented",
        }),
        emitted: [],
      }),
      "schedule.search": (request) => ({
        state,
        response: Protocol.ScheduleSearchResponse.make({
          kind: "schedule.search",
          head: errorHead(request, 501),
          data: "Not implemented",
        }),
        emitted: [],
      }),
    }),
  );
};

// -----------------------------------------------------------------------------
// The layer
// -----------------------------------------------------------------------------

export interface NetworkLocalOptions {
  readonly group?: string;
  readonly pid?: string;
  /** Lean `ServerConfig.retryTimeout` — default 5s (native local server uses 30s). */
  readonly retryTimeout?: Duration.Duration;
  /** Dev-mode convergence tick — default 1s; `TestClock.adjust` drives it in tests. */
  readonly tickInterval?: Duration.Duration;
}

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

      const deliver = (emitted: ReadonlyArray<OutboxEntry>) =>
        Effect.forEach(emitted, (entry) => Queue.offer(queue, entry.message), { discard: true });

      const applyRequest = Effect.fn("NetworkLocal.apply")(function* (request: Protocol.Request) {
        const now = yield* DateTime.now;
        const [response, emitted] = yield* Ref.modify(ref, (state) => {
          const transition = apply(state, now, retryTimeout, request);
          return [[transition.response, transition.emitted] as const, transition.state];
        });
        yield* deliver(emitted);
        return response;
      });

      // Dev-mode convergence: the periodic tick persists due timeouts.
      yield* Effect.gen(function* () {
        const now = yield* DateTime.now;
        const emitted = yield* Ref.modify(ref, (state) => {
          const result = tick(state, now, retryTimeout);
          return [result.emitted, result.state];
        });
        yield* deliver(emitted);
      }).pipe(Effect.delay(tickInterval), Effect.forever, Effect.forkScoped);

      return ResonateNetwork.of({
        send: Effect.fn("NetworkLocal.send")(function* (request) {
          const response = yield* applyRequest(request);
          const wire = yield* Effect.orDie(Schema.encodeUnknownEffect(Protocol.ResponseFromWire)(response));
          return yield* decodeResponse(request)(wire);
        }),
        messages: Stream.fromQueue(queue),
        match: (target) =>
          Protocol.TargetAddress.make({ transport: "local", cast: "any", group: target, id: Option.none() }),
        unicast: Protocol.TargetAddress.make({ transport: "local", cast: "uni", group, id: Option.some(pid) }),
        anycast: (target) =>
          Protocol.TargetAddress.make({ transport: "local", cast: "any", group: target, id: Option.some(pid) }),
      });
    }),
  );
