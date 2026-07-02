/**
 * Wire schemas: envelope, requests, responses, messages.
 *
 * See `docs/DESIGN.md` §2 (Protocol Model) and §3.2 (Layer 2 — Protocol client).
 * Wire shapes mirror `repos/resonate-sdk-ts/src/network/types.ts` exactly; field
 * names and enums follow `repos/resonate-specification/spec/01-objects/types.lean`.
 *
 * Strict on construct, lenient on decode: everything this SDK emits satisfies the
 * tight `PromiseRecord`/`TaskRecord`/`Tags` schemas; wire decode paths use the
 * `*FromWire` schemas, which never reject a record the server itself accepts
 * (unrecognized reserved-tag values are preserved raw, structural invariants are
 * checked only on the strict schemas).
 */
import type { Duration } from "effect";
import { DateTime, Option, Predicate, Schema, SchemaParser, SchemaTransformation } from "effect";

// -----------------------------------------------------------------------------
// Branded ids
// -----------------------------------------------------------------------------

export const PromiseId = Schema.NonEmptyString.pipe(Schema.brand("PromiseId"));
export type PromiseId = typeof PromiseId.Type;

export const ExecutionId = Schema.NonEmptyString.pipe(Schema.brand("ExecutionId"));
export type ExecutionId = typeof ExecutionId.Type;

export const ScheduleId = Schema.NonEmptyString.pipe(Schema.brand("ScheduleId"));
export type ScheduleId = typeof ScheduleId.Type;

export const WorkerGroup = Schema.NonEmptyString.pipe(Schema.brand("WorkerGroup"));
export type WorkerGroup = typeof WorkerGroup.Type;

export const ProcessId = Schema.NonEmptyString.pipe(Schema.brand("ProcessId"));
export type ProcessId = typeof ProcessId.Type;

export const CorrelationId = Schema.NonEmptyString.pipe(Schema.brand("CorrelationId"));
export type CorrelationId = typeof CorrelationId.Type;

/** Per spec: a task shares its promise's id. */
export const TaskId = PromiseId;
export type TaskId = PromiseId;

/** Task fencing token — bumped only on acquire; non-negative on the wire. */
export const TaskVersion = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)).pipe(Schema.brand("TaskVersion"));
export type TaskVersion = typeof TaskVersion.Type;

/** Function registry version — strictly positive; the wire magic `0` means "latest". */
export const FunctionVersion = Schema.Int.check(Schema.isGreaterThan(0)).pipe(Schema.brand("FunctionVersion"));
export type FunctionVersion = typeof FunctionVersion.Type;

export const FunctionVersionOrLatest = Schema.Union([Schema.Literal("latest"), FunctionVersion]);
export type FunctionVersionOrLatest = typeof FunctionVersionOrLatest.Type;

const Latest = Schema.Literal("latest");
const WireLatest = Schema.Literal(0);

/** Wire form of {@link FunctionVersionOrLatest}: `0` ⇄ `"latest"`. */
export const FunctionVersionFromWire = Schema.Union([
  WireLatest.pipe(
    Schema.decodeTo(
      Latest,
      SchemaTransformation.transform({
        decode: () => Latest.make("latest"),
        encode: () => WireLatest.make(0),
      }),
    ),
  ),
  FunctionVersion,
]);

// -----------------------------------------------------------------------------
// Protocol constants
// -----------------------------------------------------------------------------

export const ProtocolVersion = Schema.Literal("2026-04-01");
export type ProtocolVersion = typeof ProtocolVersion.Type;
export const protocolVersion = ProtocolVersion.make("2026-04-01");

export const Status = Schema.Literals([200, 300, 400, 401, 403, 404, 409, 422, 429, 500, 501]);
export type Status = typeof Status.Type;

const ErrorStatus = Schema.Literals([400, 401, 403, 404, 409, 422, 429, 500, 501]);
export type ErrorStatus = typeof ErrorStatus.Type;

// -----------------------------------------------------------------------------
// Timestamps and durations
// -----------------------------------------------------------------------------

/** Absolute instants are `DateTime.Utc` domain-side, epoch-ms on the wire. */
export const Timestamp = Schema.DateTimeUtcFromMillis;
export type Timestamp = DateTime.Utc;

/** Lease ttls are `Duration` domain-side, ms on the wire. */
export const Ttl = Schema.DurationFromMillis;
export type Ttl = Duration.Duration;

/** Epoch-ms carried as a string (the `resonate:delay` tag). */
export const TimestampFromString = Schema.FiniteFromString.pipe(Schema.decodeTo(Schema.DateTimeUtcFromMillis));

// -----------------------------------------------------------------------------
// Value — the opaque payload envelope
// -----------------------------------------------------------------------------

export const Value = Schema.Struct({
  headers: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  data: Schema.optionalKey(Schema.String),
});
export type Value = typeof Value.Type;

export const emptyValue: Value = {};

// -----------------------------------------------------------------------------
// Target addresses — `poll://{cast}@{group}[/{id}]`
// -----------------------------------------------------------------------------

export class TargetAddress extends Schema.Class<TargetAddress>("TargetAddress")({
  transport: Schema.Literals(["poll", "local"]),
  /** Unicast vs anycast — invisible in a raw string. */
  cast: Schema.Literals(["uni", "any"]),
  group: WorkerGroup,
  id: Schema.OptionFromOptionalKey(ProcessId),
}) {
  get address(): string {
    const suffix = Option.match(this.id, {
      onNone: () => "",
      onSome: (id) => `/${id}`,
    });
    return `${this.transport}://${this.cast}@${this.group}${suffix}`;
  }
}

const AddressTransport = Schema.Literals(["poll", "local"]);
const AddressCast = Schema.Literals(["uni", "any"]);
const AddressGroup = Schema.NonEmptyString.check(Schema.isPattern(/^[^/@]+$/));

const AddressPartsWithId = Schema.Struct({
  transport: AddressTransport,
  cast: AddressCast,
  group: AddressGroup,
  id: Schema.NonEmptyString,
});

const AddressPartsWithoutId = Schema.Struct({
  transport: AddressTransport,
  cast: AddressCast,
  group: AddressGroup,
});

const AddressParserWithId = Schema.TemplateLiteralParser([
  AddressTransport,
  "://",
  AddressCast,
  "@",
  AddressGroup,
  "/",
  Schema.NonEmptyString,
]);

const AddressParserWithoutId = Schema.TemplateLiteralParser([AddressTransport, "://", AddressCast, "@", AddressGroup]);

const AddressWithIdFromString = AddressParserWithId.pipe(
  Schema.decodeTo(
    AddressPartsWithId,
    SchemaTransformation.transform({
      decode: ([transport, , cast, , group, , id]) => AddressPartsWithId.make({ transport, cast, group, id }),
      encode: (parts) =>
        AddressParserWithId.make([parts.transport, "://", parts.cast, "@", parts.group, "/", parts.id]),
    }),
  ),
);

const AddressWithoutIdFromString = AddressParserWithoutId.pipe(
  Schema.decodeTo(
    AddressPartsWithoutId,
    SchemaTransformation.transform({
      decode: ([transport, , cast, , group]) => AddressPartsWithoutId.make({ transport, cast, group }),
      encode: (parts) => AddressParserWithoutId.make([parts.transport, "://", parts.cast, "@", parts.group]),
    }),
  ),
);

export const TargetAddressFromString = Schema.Union([AddressWithIdFromString, AddressWithoutIdFromString]).pipe(
  Schema.decodeTo(TargetAddress),
);

/** The address grammar as a template-literal string schema, for guarding raw strings. */
export const TargetAddressString = Schema.Union([
  Schema.TemplateLiteral([AddressTransport, "://", AddressCast, "@", AddressGroup, "/", Schema.NonEmptyString]),
  Schema.TemplateLiteral([AddressTransport, "://", AddressCast, "@", AddressGroup]),
]);

export const isTargetAddressString = SchemaParser.is(TargetAddressString);

// -----------------------------------------------------------------------------
// Tags — typed reserved vocabulary + user tags, flat record on the wire
// -----------------------------------------------------------------------------

/** User tag keys are provably outside the `resonate:` namespace. */
export const UserTagKey = Schema.String.check(
  Schema.makeFilter((key: string) => !key.startsWith("resonate:"), {
    title: `a tag key outside the "resonate:" namespace`,
  }),
).pipe(Schema.brand("UserTagKey"));
export type UserTagKey = typeof UserTagKey.Type;

const TimerTagValue = Schema.Literal("true");
const ScopeTagValue = Schema.Literals(["local", "global"]);

export const ReservedTags = Schema.Struct({
  "resonate:timer": Schema.optionalKey(TimerTagValue),
  "resonate:scope": Schema.optionalKey(ScopeTagValue),
  "resonate:target": Schema.optionalKey(TargetAddressFromString),
  "resonate:origin": Schema.optionalKey(PromiseId),
  "resonate:parent": Schema.optionalKey(PromiseId),
  "resonate:branch": Schema.optionalKey(PromiseId),
  "resonate:prefix": Schema.optionalKey(PromiseId),
  // Native parses this with `.toNat!` (crashes on junk); we decode.
  "resonate:delay": Schema.optionalKey(TimestampFromString),
});
export type ReservedTags = typeof ReservedTags.Type;

export class Tags extends Schema.Class<Tags>("Tags")({
  reserved: ReservedTags,
  /**
   * Lenient-decode escape hatch: `resonate:`-prefixed entries whose key is
   * unknown or whose value does not inhabit the reserved key's typed domain are
   * preserved raw here (never failing the record), and re-emitted verbatim.
   */
  unrecognized: Schema.Record(Schema.String, Schema.String),
  user: Schema.Record(UserTagKey, Schema.String),
}) {
  get isTimer(): boolean {
    return this.reserved["resonate:timer"] === "true";
  }
}

export const emptyTags: Tags = Tags.make({ reserved: {}, unrecognized: {}, user: {} });

export const TagsFromWire = Schema.Record(Schema.String, Schema.String).pipe(
  Schema.decodeTo(
    Tags,
    SchemaTransformation.transform({
      decode: (flat) => {
        const timer = flat["resonate:timer"];
        const scope = flat["resonate:scope"];
        const target = flat["resonate:target"];
        const origin = flat["resonate:origin"];
        const parent = flat["resonate:parent"];
        const branch = flat["resonate:branch"];
        const prefix = flat["resonate:prefix"];
        const delay = flat["resonate:delay"];
        // Reserved keys keep only values inhabiting their typed domain; the
        // narrowing conditions mirror the ReservedTags field schemas.
        const reserved = {
          ...(timer === "true" ? { "resonate:timer": TimerTagValue.make(timer) } : {}),
          ...(scope === "local" || scope === "global" ? { "resonate:scope": ScopeTagValue.make(scope) } : {}),
          ...(Predicate.isNotUndefined(target) && isTargetAddressString(target) ? { "resonate:target": target } : {}),
          ...(Predicate.isNotUndefined(origin) && origin.length > 0 ? { "resonate:origin": origin } : {}),
          ...(Predicate.isNotUndefined(parent) && parent.length > 0 ? { "resonate:parent": parent } : {}),
          ...(Predicate.isNotUndefined(branch) && branch.length > 0 ? { "resonate:branch": branch } : {}),
          ...(Predicate.isNotUndefined(prefix) && prefix.length > 0 ? { "resonate:prefix": prefix } : {}),
          ...(Predicate.isNotUndefined(delay) && /^\d+$/.test(delay) ? { "resonate:delay": delay } : {}),
        };
        const unrecognized: Record<string, string> = {};
        const user: Record<string, string> = {};
        for (const [key, value] of Object.entries(flat)) {
          if (key in reserved) {
            continue;
          }
          if (key.startsWith("resonate:")) {
            unrecognized[key] = value;
          } else {
            user[key] = value;
          }
        }
        return { reserved, unrecognized, user };
      },
      encode: (tags) => ({ ...tags.reserved, ...tags.unrecognized, ...tags.user }),
    }),
  ),
);

// -----------------------------------------------------------------------------
// Promise records — state-discriminated
// -----------------------------------------------------------------------------

export const PromiseSettledState = Schema.Literals(["resolved", "rejected", "rejected_canceled", "rejected_timedout"]);
export type PromiseSettledState = typeof PromiseSettledState.Type;

export const PromiseState = Schema.Literals([
  "pending",
  "resolved",
  "rejected",
  "rejected_canceled",
  "rejected_timedout",
]);
export type PromiseState = typeof PromiseState.Type;

export class PromiseSettled extends Schema.Class<PromiseSettled>("PromiseSettled")({
  state: PromiseSettledState,
  id: PromiseId,
  param: Value,
  value: Value,
  tags: TagsFromWire,
  timeoutAt: Timestamp,
  createdAt: Timestamp,
  // Required by the strict PromiseRecord schema; Option here so wire decode
  // stays lenient if a server ever omits it.
  settledAt: Schema.OptionFromOptionalKey(Timestamp),
}) {}

export class PromisePending extends Schema.Class<PromisePending>("PromisePending")({
  state: Schema.tag("pending"),
  id: PromiseId,
  param: Value,
  value: Value,
  tags: TagsFromWire,
  timeoutAt: Timestamp,
  createdAt: Timestamp,
}) {
  /**
   * The spec's timeout projection: a pending promise past its `timeoutAt` reads
   * as settled (`resolved` if tagged `resonate:timer`, else `rejected_timedout`,
   * `settledAt = timeoutAt`) on every read/mutate path, even before the server's
   * timeout transition persists it. See `spec/02-actions/P-01-promise.get.lean`.
   */
  projected(now: DateTime.Utc): PromisePending | PromiseSettled {
    if (DateTime.toEpochMillis(this.timeoutAt) > DateTime.toEpochMillis(now)) {
      return this;
    }
    return new PromiseSettled({
      state: this.tags.isTimer ? "resolved" : "rejected_timedout",
      id: this.id,
      param: this.param,
      value: this.value,
      tags: this.tags,
      timeoutAt: this.timeoutAt,
      createdAt: this.createdAt,
      settledAt: Option.some(this.timeoutAt),
    });
  }
}

/** Lenient wire decode — never rejects a record the server accepts. */
export const PromiseRecordFromWire = Schema.Union([PromisePending, PromiseSettled]);

/** Strict domain schema — settled promises must carry `settledAt`. */
export const PromiseRecord = PromiseRecordFromWire.check(
  Schema.makeFilter(
    (record: PromisePending | PromiseSettled) => record.state === "pending" || Option.isSome(record.settledAt),
    {
      title: "a settled promise carries settledAt",
    },
  ),
);
export type PromiseRecord = typeof PromiseRecord.Type;

// -----------------------------------------------------------------------------
// Task records — state-discriminated
// -----------------------------------------------------------------------------

export const TaskState = Schema.Literals(["pending", "acquired", "suspended", "halted", "fulfilled"]);
export type TaskState = typeof TaskState.Type;

/** Native servers variously report a resume count, list, or flag. */
const TaskResumes = Schema.Union([Schema.Number, Schema.Array(Schema.String), Schema.Boolean]);

export class TaskPending extends Schema.Class<TaskPending>("TaskPending")({
  state: Schema.tag("pending"),
  id: TaskId,
  version: TaskVersion,
  resumes: TaskResumes,
}) {}

export class TaskAcquired extends Schema.Class<TaskAcquired>("TaskAcquired")({
  state: Schema.tag("acquired"),
  id: TaskId,
  version: TaskVersion,
  resumes: TaskResumes,
  // Required by the strict TaskRecord schema (every acquired task holds a
  // lease); Options here so wire decode stays lenient.
  pid: Schema.OptionFromOptionalKey(ProcessId),
  ttl: Schema.OptionFromOptionalKey(Ttl),
}) {}

export class TaskSuspended extends Schema.Class<TaskSuspended>("TaskSuspended")({
  state: Schema.tag("suspended"),
  id: TaskId,
  version: TaskVersion,
  resumes: TaskResumes,
}) {}

export class TaskHalted extends Schema.Class<TaskHalted>("TaskHalted")({
  state: Schema.tag("halted"),
  id: TaskId,
  version: TaskVersion,
  resumes: TaskResumes,
}) {}

export class TaskFulfilled extends Schema.Class<TaskFulfilled>("TaskFulfilled")({
  state: Schema.tag("fulfilled"),
  id: TaskId,
  version: TaskVersion,
  resumes: TaskResumes,
}) {}

/** Lenient wire decode — never rejects a record the server accepts. */
export const TaskRecordFromWire = Schema.Union([TaskPending, TaskAcquired, TaskSuspended, TaskHalted, TaskFulfilled]);

/** Strict domain schema — acquired tasks must hold a lease (`pid` + `ttl`). */
export const TaskRecord = TaskRecordFromWire.check(
  Schema.makeFilter(
    (record: typeof TaskRecordFromWire.Type) =>
      record.state !== "acquired" || (Option.isSome(record.pid) && Option.isSome(record.ttl)),
    { title: "an acquired task carries pid and ttl" },
  ),
);
export type TaskRecord = typeof TaskRecord.Type;

// -----------------------------------------------------------------------------
// Schedule records
// -----------------------------------------------------------------------------

export class ScheduleRecord extends Schema.Class<ScheduleRecord>("ScheduleRecord")({
  id: ScheduleId,
  cron: Schema.String,
  /** Promise-id template, e.g. `{{.id}}.{{.timestamp}}` — not a concrete PromiseId. */
  promiseId: Schema.NonEmptyString,
  /** Per-tick promise timeout, relative to the tick. */
  promiseTimeout: Ttl,
  promiseParam: Value,
  promiseTags: TagsFromWire,
  createdAt: Timestamp,
  nextRunAt: Timestamp,
  lastRunAt: Schema.OptionFromOptionalKey(Timestamp),
}) {}

// -----------------------------------------------------------------------------
// Push messages — execute / unblock
// -----------------------------------------------------------------------------

const MessageHead = Schema.Struct({
  serverUrl: Schema.optionalKey(Schema.String),
});

export const ExecuteMessage = Schema.Struct({
  kind: Schema.tag("execute"),
  head: MessageHead,
  data: Schema.Struct({
    task: Schema.Struct({ id: TaskId, version: TaskVersion }),
  }),
});
export type ExecuteMessage = typeof ExecuteMessage.Type;

export const UnblockMessage = Schema.Struct({
  kind: Schema.tag("unblock"),
  head: MessageHead,
  data: Schema.Struct({ promise: PromiseRecordFromWire }),
});
export type UnblockMessage = typeof UnblockMessage.Type;

export const Message = Schema.Union([ExecuteMessage, UnblockMessage]);
export type Message = typeof Message.Type;

// -----------------------------------------------------------------------------
// Envelope heads
// -----------------------------------------------------------------------------

export const RequestHead = Schema.Struct({
  corrId: CorrelationId,
  version: ProtocolVersion,
  auth: Schema.optionalKey(Schema.String),
  "resonate:debug_time": Schema.optionalKey(Schema.Number),
  "resonate:origin": Schema.optionalKey(Schema.String),
});
export type RequestHead = typeof RequestHead.Type;

const responseHead = <S extends Schema.Top>(status: S) =>
  Schema.Struct({
    corrId: CorrelationId,
    status,
    version: Schema.String,
  });

// -----------------------------------------------------------------------------
// Requests
// -----------------------------------------------------------------------------

const requestEnvelope = <Kind extends string, Data extends Schema.Top>(kind: Kind, data: Data) =>
  Schema.Struct({
    kind: Schema.tag(kind),
    head: RequestHead,
    data,
  });

export const PromiseGetRequest = requestEnvelope("promise.get", Schema.Struct({ id: PromiseId }));

export const PromiseCreateRequest = requestEnvelope(
  "promise.create",
  Schema.Struct({
    id: PromiseId,
    timeoutAt: Timestamp,
    param: Value,
    tags: TagsFromWire,
  }),
);

export const PromiseSettleRequest = requestEnvelope(
  "promise.settle",
  Schema.Struct({
    id: PromiseId,
    state: Schema.Literals(["resolved", "rejected", "rejected_canceled"]),
    value: Value,
  }),
);

export const PromiseRegisterCallbackRequest = requestEnvelope(
  "promise.register_callback",
  Schema.Struct({
    awaited: PromiseId,
    awaiter: PromiseId,
  }),
);

export const PromiseRegisterListenerRequest = requestEnvelope(
  "promise.register_listener",
  Schema.Struct({
    awaited: PromiseId,
    address: TargetAddressFromString,
  }),
);

export const PromiseSearchRequest = requestEnvelope(
  "promise.search",
  Schema.Struct({
    state: Schema.optionalKey(PromiseState),
    tags: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
    limit: Schema.optionalKey(Schema.Number),
    cursor: Schema.optionalKey(Schema.String),
  }),
);

export const TaskGetRequest = requestEnvelope("task.get", Schema.Struct({ id: TaskId }));

export const TaskCreateRequest = requestEnvelope(
  "task.create",
  Schema.Struct({
    pid: ProcessId,
    ttl: Ttl,
    action: PromiseCreateRequest,
  }),
);

export const TaskAcquireRequest = requestEnvelope(
  "task.acquire",
  Schema.Struct({
    id: TaskId,
    version: TaskVersion,
    pid: ProcessId,
    ttl: Ttl,
  }),
);

export const TaskReleaseRequest = requestEnvelope("task.release", Schema.Struct({ id: TaskId, version: TaskVersion }));

export const TaskSuspendRequest = requestEnvelope(
  "task.suspend",
  Schema.Struct({
    id: TaskId,
    version: TaskVersion,
    actions: Schema.Array(PromiseRegisterCallbackRequest),
  }),
);

export const TaskHaltRequest = requestEnvelope("task.halt", Schema.Struct({ id: TaskId }));

export const TaskContinueRequest = requestEnvelope("task.continue", Schema.Struct({ id: TaskId }));

export const TaskFulfillRequest = requestEnvelope(
  "task.fulfill",
  Schema.Struct({
    id: TaskId,
    version: TaskVersion,
    action: PromiseSettleRequest,
  }),
);

export const TaskFenceRequest = requestEnvelope(
  "task.fence",
  Schema.Struct({
    id: TaskId,
    version: TaskVersion,
    action: Schema.Union([PromiseCreateRequest, PromiseSettleRequest]),
  }),
);

export const TaskHeartbeatRequest = requestEnvelope(
  "task.heartbeat",
  Schema.Struct({
    pid: ProcessId,
    tasks: Schema.Array(Schema.Struct({ id: TaskId, version: TaskVersion })),
  }),
);

export const TaskSearchRequest = requestEnvelope(
  "task.search",
  Schema.Struct({
    state: Schema.optionalKey(TaskState),
    limit: Schema.optionalKey(Schema.Number),
    cursor: Schema.optionalKey(Schema.String),
  }),
);

export const ScheduleGetRequest = requestEnvelope("schedule.get", Schema.Struct({ id: ScheduleId }));

export const ScheduleCreateRequest = requestEnvelope(
  "schedule.create",
  Schema.Struct({
    id: ScheduleId,
    cron: Schema.String,
    promiseId: Schema.NonEmptyString,
    promiseTimeout: Ttl,
    promiseParam: Value,
    promiseTags: TagsFromWire,
  }),
);

export const ScheduleDeleteRequest = requestEnvelope("schedule.delete", Schema.Struct({ id: ScheduleId }));

export const ScheduleSearchRequest = requestEnvelope(
  "schedule.search",
  Schema.Struct({
    tags: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
    limit: Schema.optionalKey(Schema.Number),
    cursor: Schema.optionalKey(Schema.String),
  }),
);

const EmptyData = Schema.Struct({});

export const DebugStartRequest = requestEnvelope("debug.start", EmptyData);
export const DebugResetRequest = requestEnvelope("debug.reset", EmptyData);
export const DebugTickRequest = requestEnvelope("debug.tick", Schema.Struct({ time: Timestamp }));
export const DebugSnapRequest = requestEnvelope("debug.snap", EmptyData);
export const DebugStopRequest = requestEnvelope("debug.stop", EmptyData);

export const RequestSchemas = {
  "promise.get": PromiseGetRequest,
  "promise.create": PromiseCreateRequest,
  "promise.settle": PromiseSettleRequest,
  "promise.register_callback": PromiseRegisterCallbackRequest,
  "promise.register_listener": PromiseRegisterListenerRequest,
  "promise.search": PromiseSearchRequest,
  "task.get": TaskGetRequest,
  "task.create": TaskCreateRequest,
  "task.acquire": TaskAcquireRequest,
  "task.release": TaskReleaseRequest,
  "task.suspend": TaskSuspendRequest,
  "task.halt": TaskHaltRequest,
  "task.continue": TaskContinueRequest,
  "task.fulfill": TaskFulfillRequest,
  "task.fence": TaskFenceRequest,
  "task.heartbeat": TaskHeartbeatRequest,
  "task.search": TaskSearchRequest,
  "schedule.get": ScheduleGetRequest,
  "schedule.create": ScheduleCreateRequest,
  "schedule.delete": ScheduleDeleteRequest,
  "schedule.search": ScheduleSearchRequest,
  "debug.start": DebugStartRequest,
  "debug.reset": DebugResetRequest,
  "debug.tick": DebugTickRequest,
  "debug.snap": DebugSnapRequest,
  "debug.stop": DebugStopRequest,
} as const;

export type RequestKind = keyof typeof RequestSchemas;

export type Request<K extends RequestKind = RequestKind> = (typeof RequestSchemas)[K]["Type"];

export const RequestFromWire = Schema.Union([
  PromiseGetRequest,
  PromiseCreateRequest,
  PromiseSettleRequest,
  PromiseRegisterCallbackRequest,
  PromiseRegisterListenerRequest,
  PromiseSearchRequest,
  TaskGetRequest,
  TaskCreateRequest,
  TaskAcquireRequest,
  TaskReleaseRequest,
  TaskSuspendRequest,
  TaskHaltRequest,
  TaskContinueRequest,
  TaskFulfillRequest,
  TaskFenceRequest,
  TaskHeartbeatRequest,
  TaskSearchRequest,
  ScheduleGetRequest,
  ScheduleCreateRequest,
  ScheduleDeleteRequest,
  ScheduleSearchRequest,
  DebugStartRequest,
  DebugResetRequest,
  DebugTickRequest,
  DebugSnapRequest,
  DebugStopRequest,
]);

// -----------------------------------------------------------------------------
// Responses
// -----------------------------------------------------------------------------

const successEnvelope = <Kind extends string, S extends Schema.Top, Data extends Schema.Top>(
  kind: Kind,
  status: S,
  data: Data,
) =>
  Schema.Struct({
    kind: Schema.tag(kind),
    head: responseHead(status),
    data,
  });

/** Non-2xx protocol statuses carry a string message as `data`. */
const errorEnvelope = <Kind extends string>(kind: Kind) =>
  Schema.Struct({
    kind: Schema.tag(kind),
    head: responseHead(ErrorStatus),
    data: Schema.String,
  });

const responseEnvelope = <Kind extends string, Data extends Schema.Top>(kind: Kind, data: Data) =>
  Schema.Union([successEnvelope(kind, Schema.Literal(200), data), errorEnvelope(kind)]);

const PromiseData = Schema.Struct({ promise: PromiseRecordFromWire });
const Preload = Schema.Array(PromiseRecordFromWire);

export const PromiseGetResponse = responseEnvelope("promise.get", PromiseData);
export const PromiseCreateResponse = responseEnvelope("promise.create", PromiseData);
export const PromiseSettleResponse = responseEnvelope("promise.settle", PromiseData);
export const PromiseRegisterCallbackResponse = responseEnvelope("promise.register_callback", PromiseData);
export const PromiseRegisterListenerResponse = responseEnvelope("promise.register_listener", PromiseData);
export const PromiseSearchResponse = responseEnvelope(
  "promise.search",
  Schema.Struct({
    promises: Schema.Array(PromiseRecordFromWire),
    cursor: Schema.optionalKey(Schema.String),
  }),
);

export const TaskGetResponse = responseEnvelope("task.get", Schema.Struct({ task: TaskRecordFromWire }));

export const TaskCreateResponse = responseEnvelope(
  "task.create",
  Schema.Struct({
    task: Schema.optionalKey(TaskRecordFromWire),
    promise: PromiseRecordFromWire,
    preload: Preload,
  }),
);

export const TaskAcquireResponse = responseEnvelope(
  "task.acquire",
  Schema.Struct({
    task: TaskRecordFromWire,
    promise: PromiseRecordFromWire,
    preload: Preload,
  }),
);

export const TaskReleaseResponse = responseEnvelope("task.release", EmptyData);

/** `task.suspend` has a `300` fast path carrying already-settled awaited promises. */
export const TaskSuspendResponse = Schema.Union([
  successEnvelope("task.suspend", Schema.Literal(200), EmptyData),
  successEnvelope("task.suspend", Schema.Literal(300), Schema.Struct({ preload: Preload })),
  errorEnvelope("task.suspend"),
]);

export const TaskHaltResponse = responseEnvelope("task.halt", EmptyData);
export const TaskContinueResponse = responseEnvelope("task.continue", EmptyData);
export const TaskFulfillResponse = responseEnvelope("task.fulfill", PromiseData);

export const TaskFenceResponse = responseEnvelope(
  "task.fence",
  Schema.Struct({
    action: Schema.Union([PromiseCreateResponse, PromiseSettleResponse]),
    preload: Preload,
  }),
);

export const TaskHeartbeatResponse = responseEnvelope("task.heartbeat", EmptyData);

export const TaskSearchResponse = responseEnvelope(
  "task.search",
  Schema.Struct({
    tasks: Schema.Array(TaskRecordFromWire),
    cursor: Schema.optionalKey(Schema.String),
  }),
);

const ScheduleData = Schema.Struct({ schedule: ScheduleRecord });

export const ScheduleGetResponse = responseEnvelope("schedule.get", ScheduleData);
export const ScheduleCreateResponse = responseEnvelope("schedule.create", ScheduleData);
export const ScheduleDeleteResponse = responseEnvelope("schedule.delete", EmptyData);
export const ScheduleSearchResponse = responseEnvelope(
  "schedule.search",
  Schema.Struct({
    schedules: Schema.Array(ScheduleRecord),
    cursor: Schema.optionalKey(Schema.String),
  }),
);

export const DebugTickAction = Schema.Union([
  Schema.Struct({
    kind: Schema.tag("promise.settle"),
    data: Schema.Struct({
      id: PromiseId,
      state: Schema.Literals(["rejected_timedout", "resolved"]),
    }),
  }),
  Schema.Struct({
    kind: Schema.tag("task.release"),
    data: Schema.Struct({ id: TaskId, version: TaskVersion }),
  }),
  Schema.Struct({
    kind: Schema.tag("task.retry"),
    data: Schema.Struct({ id: TaskId, version: TaskVersion }),
  }),
]);
export type DebugTickAction = typeof DebugTickAction.Type;

export const DebugStartResponse = responseEnvelope("debug.start", EmptyData);
export const DebugResetResponse = responseEnvelope("debug.reset", EmptyData);
export const DebugTickResponse = responseEnvelope("debug.tick", Schema.Array(DebugTickAction));
export const DebugSnapResponse = responseEnvelope(
  "debug.snap",
  Schema.Struct({
    promises: Schema.Array(PromiseRecordFromWire),
    promiseTimeouts: Schema.Array(Schema.Struct({ id: PromiseId, timeout: Timestamp })),
    callbacks: Schema.Array(Schema.Struct({ awaiter: PromiseId, awaited: PromiseId })),
    listeners: Schema.optionalKey(Schema.Array(Schema.Struct({ id: PromiseId, address: Schema.String }))),
    tasks: Schema.Array(TaskRecordFromWire),
    taskTimeouts: Schema.Array(Schema.Struct({ id: TaskId, type: Schema.Number, timeout: Timestamp })),
    messages: Schema.Array(Schema.Struct({ address: Schema.String, message: Message })),
  }),
);
export const DebugStopResponse = responseEnvelope("debug.stop", EmptyData);

export const ResponseSchemas = {
  "promise.get": PromiseGetResponse,
  "promise.create": PromiseCreateResponse,
  "promise.settle": PromiseSettleResponse,
  "promise.register_callback": PromiseRegisterCallbackResponse,
  "promise.register_listener": PromiseRegisterListenerResponse,
  "promise.search": PromiseSearchResponse,
  "task.get": TaskGetResponse,
  "task.create": TaskCreateResponse,
  "task.acquire": TaskAcquireResponse,
  "task.release": TaskReleaseResponse,
  "task.suspend": TaskSuspendResponse,
  "task.halt": TaskHaltResponse,
  "task.continue": TaskContinueResponse,
  "task.fulfill": TaskFulfillResponse,
  "task.fence": TaskFenceResponse,
  "task.heartbeat": TaskHeartbeatResponse,
  "task.search": TaskSearchResponse,
  "schedule.get": ScheduleGetResponse,
  "schedule.create": ScheduleCreateResponse,
  "schedule.delete": ScheduleDeleteResponse,
  "schedule.search": ScheduleSearchResponse,
  "debug.start": DebugStartResponse,
  "debug.reset": DebugResetResponse,
  "debug.tick": DebugTickResponse,
  "debug.snap": DebugSnapResponse,
  "debug.stop": DebugStopResponse,
} as const;

export type Response<K extends RequestKind = RequestKind> = (typeof ResponseSchemas)[K]["Type"];
