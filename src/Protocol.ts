/**
 * Schema-first Resonate protocol model and wire codecs.
 *
 * This module owns the branded identifiers, protocol records, request and
 * response schemas, message schemas, target address parsing, and wire
 * transformations used by the SDK network implementations.
 *
 * @since 0.0.0
 */
import type { Duration } from "effect";
import {
  DateTime,
  Option,
  Predicate,
  Record as Rec,
  Schema,
  SchemaParser,
  SchemaTransformation,
  String as Str,
} from "effect";

/**
 * Durable promise identifier.
 *
 * @category identifiers
 * @since 0.0.0
 */
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

export const TaskId = PromiseId;
export type TaskId = PromiseId;

export const TaskVersion = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)).pipe(Schema.brand("TaskVersion"));
export type TaskVersion = typeof TaskVersion.Type;

export const FunctionVersion = Schema.Int.check(Schema.isGreaterThan(0)).pipe(Schema.brand("FunctionVersion"));
export type FunctionVersion = typeof FunctionVersion.Type;

export const FunctionVersionOrLatest = Schema.Union([Schema.Literal("latest"), FunctionVersion]);
export type FunctionVersionOrLatest = typeof FunctionVersionOrLatest.Type;

const Latest = Schema.Literal("latest");
const WireLatest = Schema.Literal(0);

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

export const ProtocolVersion = Schema.Literal("2026-04-01");
export type ProtocolVersion = typeof ProtocolVersion.Type;
export const protocolVersion = ProtocolVersion.make("2026-04-01");

export const Status = Schema.Literals([200, 300, 400, 401, 403, 404, 409, 422, 429, 500, 501]);
export type Status = typeof Status.Type;

const ErrorStatus = Schema.Literals([400, 401, 403, 404, 409, 422, 429, 500, 501]);
export type ErrorStatus = typeof ErrorStatus.Type;

export const Timestamp = Schema.DateTimeUtcFromMillis;
export type Timestamp = DateTime.Utc;

export const Ttl = Schema.DurationFromMillis;
export type Ttl = Duration.Duration;

export const TimestampFromString = Schema.FiniteFromString.pipe(Schema.decodeTo(Schema.DateTimeUtcFromMillis));

export const Value = Schema.Struct({
  headers: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  data: Schema.optionalKey(Schema.String),
});
export type Value = typeof Value.Type;

export const emptyValue: Value = {};

/**
 * Address of a worker target in either poll or local transport.
 *
 * @category models
 * @since 0.0.0
 */
export class TargetAddress extends Schema.Class<TargetAddress>("TargetAddress")({
  transport: Schema.Literals(["poll", "local"]),

  cast: Schema.Literals(["uni", "any"]),
  group: WorkerGroup,
  id: Schema.OptionFromOptionalKey(ProcessId),
}) {
  static pollAny(options: { readonly group: WorkerGroup; readonly id?: Option.Option<ProcessId> }): TargetAddress {
    const id = options.id ?? Option.none<ProcessId>();
    return TargetAddress.make({ transport: "poll", cast: "any", group: options.group, id });
  }

  static pollUni(options: { readonly group: WorkerGroup; readonly id: ProcessId }): TargetAddress {
    return TargetAddress.make({ transport: "poll", cast: "uni", group: options.group, id: Option.some(options.id) });
  }

  static localAny(options: { readonly group: WorkerGroup; readonly id?: Option.Option<ProcessId> }): TargetAddress {
    const id = options.id ?? Option.none<ProcessId>();
    return TargetAddress.make({ transport: "local", cast: "any", group: options.group, id });
  }

  static localUni(options: { readonly group: WorkerGroup; readonly id: ProcessId }): TargetAddress {
    return TargetAddress.make({ transport: "local", cast: "uni", group: options.group, id: Option.some(options.id) });
  }

  get address(): string {
    const suffix = Option.match(this.id, {
      onNone: () => "",
      onSome: (id) => `/${id}`,
    });
    return `${this.transport}://${this.cast}@${this.group}${suffix}`;
  }

  get pollPath(): string {
    const suffix = Option.match(this.id, {
      onNone: () => "",
      onSome: (id) => `/${encodeURIComponent(id)}`,
    });
    return `/poll/${encodeURIComponent(this.group)}${suffix}`;
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

export const TargetAddressString = Schema.Union([
  Schema.TemplateLiteral([AddressTransport, "://", AddressCast, "@", AddressGroup, "/", Schema.NonEmptyString]),
  Schema.TemplateLiteral([AddressTransport, "://", AddressCast, "@", AddressGroup]),
]);

export const isTargetAddressString = SchemaParser.is(TargetAddressString);

export const UserTagKey = Schema.String.check(
  Schema.makeFilter((key: string) => !Str.startsWith("resonate:")(key), {
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

  "resonate:delay": Schema.optionalKey(TimestampFromString),
});
export type ReservedTags = typeof ReservedTags.Type;

/**
 * Structured representation of user, reserved, and unrecognized tags.
 *
 * @category models
 * @since 0.0.0
 */
export class Tags extends Schema.Class<Tags>("Tags")({
  reserved: ReservedTags,

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

        const reserved = {
          ...(timer === "true" ? { "resonate:timer": TimerTagValue.make(timer) } : {}),
          ...(scope === "local" || scope === "global" ? { "resonate:scope": ScopeTagValue.make(scope) } : {}),
          ...(Predicate.isNotUndefined(target) && isTargetAddressString(target) ? { "resonate:target": target } : {}),
          ...(Predicate.isNotUndefined(origin) && Str.isNonEmpty(origin) ? { "resonate:origin": origin } : {}),
          ...(Predicate.isNotUndefined(parent) && Str.isNonEmpty(parent) ? { "resonate:parent": parent } : {}),
          ...(Predicate.isNotUndefined(branch) && Str.isNonEmpty(branch) ? { "resonate:branch": branch } : {}),
          ...(Predicate.isNotUndefined(prefix) && Str.isNonEmpty(prefix) ? { "resonate:prefix": prefix } : {}),
          ...(Predicate.isNotUndefined(delay) && /^\d+$/.test(delay) ? { "resonate:delay": delay } : {}),
        };
        const unrecognized: Record<string, string> = {};
        const user: Record<string, string> = {};
        for (const [key, value] of Rec.toEntries(flat)) {
          if (key in reserved) {
            continue;
          }
          if (Str.startsWith("resonate:")(key)) {
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

/**
 * Settled durable promise record.
 *
 * @category models
 * @since 0.0.0
 */
export class PromiseSettled extends Schema.Class<PromiseSettled>("PromiseSettled")({
  state: PromiseSettledState,
  id: PromiseId,
  param: Value,
  value: Value,
  tags: TagsFromWire,
  timeoutAt: Timestamp,
  createdAt: Timestamp,

  settledAt: Schema.OptionFromOptionalKey(Timestamp),
}) {}

/**
 * Pending durable promise record.
 *
 * @category models
 * @since 0.0.0
 */
export class PromisePending extends Schema.Class<PromisePending>("PromisePending")({
  state: Schema.tag("pending"),
  id: PromiseId,
  param: Value,
  value: Value,
  tags: TagsFromWire,
  timeoutAt: Timestamp,
  createdAt: Timestamp,
}) {
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

export const PromiseRecordFromWire = Schema.Union([PromisePending, PromiseSettled]);

export const PromiseRecord = PromiseRecordFromWire.check(
  Schema.makeFilter(
    (record: PromisePending | PromiseSettled) => record.state === "pending" || Option.isSome(record.settledAt),
    {
      title: "a settled promise carries settledAt",
    },
  ),
);
export type PromiseRecord = typeof PromiseRecord.Type;

/**
 * Returns the lineage origin for a promise, falling back to the promise id.
 *
 * @category combinators
 * @since 0.0.0
 */
export const promiseOrigin = (promise: PromiseRecord): PromiseId =>
  promise.tags.reserved["resonate:origin"] ?? promise.id;

export const TaskState = Schema.Literals(["pending", "acquired", "suspended", "halted", "fulfilled"]);
export type TaskState = typeof TaskState.Type;

const TaskResumes = Schema.Union([Schema.Finite, Schema.Array(Schema.String), Schema.Boolean]);

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

export const TaskRecordFromWire = Schema.Union([TaskPending, TaskAcquired, TaskSuspended, TaskHalted, TaskFulfilled]);

export const TaskRecord = TaskRecordFromWire.check(
  Schema.makeFilter(
    (record: typeof TaskRecordFromWire.Type) =>
      record.state !== "acquired" || (Option.isSome(record.pid) && Option.isSome(record.ttl)),
    { title: "an acquired task carries pid and ttl" },
  ),
);
export type TaskRecord = typeof TaskRecord.Type;

export class ScheduleRecord extends Schema.Class<ScheduleRecord>("ScheduleRecord")({
  id: ScheduleId,
  cron: Schema.String,

  promiseId: Schema.NonEmptyString,

  promiseTimeout: Ttl,
  promiseParam: Value,
  promiseTags: TagsFromWire,
  createdAt: Timestamp,
  nextRunAt: Timestamp,
  lastRunAt: Schema.OptionFromOptionalKey(Timestamp),
}) {}

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

/**
 * Tagged union of worker messages emitted by a Resonate network.
 *
 * @category schemas
 * @since 0.0.0
 */
export const Message = Schema.Union([ExecuteMessage, UnblockMessage]).pipe(Schema.toTaggedUnion("kind"));
export type Message = typeof Message.Type;

export const RequestHead = Schema.Struct({
  corrId: CorrelationId,
  version: ProtocolVersion,
  auth: Schema.optionalKey(Schema.String),
  "resonate:debug_time": Schema.optionalKey(Schema.Finite),
  "resonate:origin": Schema.optionalKey(Schema.String),
});
export type RequestHead = typeof RequestHead.Type;

const SuccessHead = Schema.Struct({ corrId: CorrelationId, status: Schema.Literal(200), version: Schema.String });

const RedirectHead = Schema.Struct({ corrId: CorrelationId, status: Schema.Literal(300), version: Schema.String });

const ErrorHead = Schema.Struct({ corrId: CorrelationId, status: ErrorStatus, version: Schema.String });

export const PromiseGetRequest = Schema.Struct({
  kind: Schema.tag("promise.get"),
  head: RequestHead,
  data: Schema.Struct({ id: PromiseId }),
});
export type PromiseGetRequest = typeof PromiseGetRequest.Type;

export const PromiseCreateRequest = Schema.Struct({
  kind: Schema.tag("promise.create"),
  head: RequestHead,
  data: Schema.Struct({
    id: PromiseId,
    timeoutAt: Timestamp,
    param: Value,
    tags: TagsFromWire,
  }),
});
export type PromiseCreateRequest = typeof PromiseCreateRequest.Type;

export const PromiseSettleRequest = Schema.Struct({
  kind: Schema.tag("promise.settle"),
  head: RequestHead,
  data: Schema.Struct({
    id: PromiseId,
    state: Schema.Literals(["resolved", "rejected", "rejected_canceled"]),
    value: Value,
  }),
});
export type PromiseSettleRequest = typeof PromiseSettleRequest.Type;

export const PromiseRegisterCallbackRequest = Schema.Struct({
  kind: Schema.tag("promise.register_callback"),
  head: RequestHead,
  data: Schema.Struct({
    awaited: PromiseId,
    awaiter: PromiseId,
  }),
});
export type PromiseRegisterCallbackRequest = typeof PromiseRegisterCallbackRequest.Type;

export const PromiseRegisterListenerRequest = Schema.Struct({
  kind: Schema.tag("promise.register_listener"),
  head: RequestHead,
  data: Schema.Struct({
    awaited: PromiseId,
    address: TargetAddressFromString,
  }),
});
export type PromiseRegisterListenerRequest = typeof PromiseRegisterListenerRequest.Type;

export const PromiseSearchRequest = Schema.Struct({
  kind: Schema.tag("promise.search"),
  head: RequestHead,
  data: Schema.Struct({
    state: Schema.optionalKey(PromiseState),
    tags: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
    limit: Schema.optionalKey(Schema.Finite),
    cursor: Schema.optionalKey(Schema.String),
  }),
});
export type PromiseSearchRequest = typeof PromiseSearchRequest.Type;

export const TaskGetRequest = Schema.Struct({
  kind: Schema.tag("task.get"),
  head: RequestHead,
  data: Schema.Struct({ id: TaskId }),
});
export type TaskGetRequest = typeof TaskGetRequest.Type;

export const TaskCreateRequest = Schema.Struct({
  kind: Schema.tag("task.create"),
  head: RequestHead,
  data: Schema.Struct({
    pid: ProcessId,
    ttl: Ttl,
    action: PromiseCreateRequest,
  }),
});
export type TaskCreateRequest = typeof TaskCreateRequest.Type;

export const TaskAcquireRequest = Schema.Struct({
  kind: Schema.tag("task.acquire"),
  head: RequestHead,
  data: Schema.Struct({
    id: TaskId,
    version: TaskVersion,
    pid: ProcessId,
    ttl: Ttl,
  }),
});
export type TaskAcquireRequest = typeof TaskAcquireRequest.Type;

export const TaskReleaseRequest = Schema.Struct({
  kind: Schema.tag("task.release"),
  head: RequestHead,
  data: Schema.Struct({ id: TaskId, version: TaskVersion }),
});
export type TaskReleaseRequest = typeof TaskReleaseRequest.Type;

export const TaskSuspendRequest = Schema.Struct({
  kind: Schema.tag("task.suspend"),
  head: RequestHead,
  data: Schema.Struct({
    id: TaskId,
    version: TaskVersion,
    actions: Schema.Array(PromiseRegisterCallbackRequest),
  }),
});
export type TaskSuspendRequest = typeof TaskSuspendRequest.Type;

export const TaskHaltRequest = Schema.Struct({
  kind: Schema.tag("task.halt"),
  head: RequestHead,
  data: Schema.Struct({ id: TaskId }),
});
export type TaskHaltRequest = typeof TaskHaltRequest.Type;

export const TaskContinueRequest = Schema.Struct({
  kind: Schema.tag("task.continue"),
  head: RequestHead,
  data: Schema.Struct({ id: TaskId }),
});
export type TaskContinueRequest = typeof TaskContinueRequest.Type;

export const TaskFulfillRequest = Schema.Struct({
  kind: Schema.tag("task.fulfill"),
  head: RequestHead,
  data: Schema.Struct({
    id: TaskId,
    version: TaskVersion,
    action: PromiseSettleRequest,
  }),
});
export type TaskFulfillRequest = typeof TaskFulfillRequest.Type;

export const TaskFenceRequest = Schema.Struct({
  kind: Schema.tag("task.fence"),
  head: RequestHead,
  data: Schema.Struct({
    id: TaskId,
    version: TaskVersion,
    action: Schema.Union([PromiseCreateRequest, PromiseSettleRequest]),
  }),
});
export type TaskFenceRequest = typeof TaskFenceRequest.Type;

export const TaskHeartbeatRequest = Schema.Struct({
  kind: Schema.tag("task.heartbeat"),
  head: RequestHead,
  data: Schema.Struct({
    pid: ProcessId,
    tasks: Schema.Array(Schema.Struct({ id: TaskId, version: TaskVersion })),
  }),
});
export type TaskHeartbeatRequest = typeof TaskHeartbeatRequest.Type;

export const TaskSearchRequest = Schema.Struct({
  kind: Schema.tag("task.search"),
  head: RequestHead,
  data: Schema.Struct({
    state: Schema.optionalKey(TaskState),
    limit: Schema.optionalKey(Schema.Finite),
    cursor: Schema.optionalKey(Schema.String),
  }),
});
export type TaskSearchRequest = typeof TaskSearchRequest.Type;

export const ScheduleGetRequest = Schema.Struct({
  kind: Schema.tag("schedule.get"),
  head: RequestHead,
  data: Schema.Struct({ id: ScheduleId }),
});
export type ScheduleGetRequest = typeof ScheduleGetRequest.Type;

export const ScheduleCreateRequest = Schema.Struct({
  kind: Schema.tag("schedule.create"),
  head: RequestHead,
  data: Schema.Struct({
    id: ScheduleId,
    cron: Schema.String,
    promiseId: Schema.NonEmptyString,
    promiseTimeout: Ttl,
    promiseParam: Value,
    promiseTags: TagsFromWire,
  }),
});
export type ScheduleCreateRequest = typeof ScheduleCreateRequest.Type;

export const ScheduleDeleteRequest = Schema.Struct({
  kind: Schema.tag("schedule.delete"),
  head: RequestHead,
  data: Schema.Struct({ id: ScheduleId }),
});
export type ScheduleDeleteRequest = typeof ScheduleDeleteRequest.Type;

export const ScheduleSearchRequest = Schema.Struct({
  kind: Schema.tag("schedule.search"),
  head: RequestHead,
  data: Schema.Struct({
    tags: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
    limit: Schema.optionalKey(Schema.Finite),
    cursor: Schema.optionalKey(Schema.String),
  }),
});
export type ScheduleSearchRequest = typeof ScheduleSearchRequest.Type;

const EmptyData = Schema.Struct({});

export const DebugStartRequest = Schema.Struct({
  kind: Schema.tag("debug.start"),
  head: RequestHead,
  data: EmptyData,
});
export const DebugResetRequest = Schema.Struct({
  kind: Schema.tag("debug.reset"),
  head: RequestHead,
  data: EmptyData,
});
export const DebugTickRequest = Schema.Struct({
  kind: Schema.tag("debug.tick"),
  head: RequestHead,
  data: Schema.Struct({ time: Timestamp }),
});
export const DebugSnapRequest = Schema.Struct({
  kind: Schema.tag("debug.snap"),
  head: RequestHead,
  data: EmptyData,
});
export const DebugStopRequest = Schema.Struct({
  kind: Schema.tag("debug.stop"),
  head: RequestHead,
  data: EmptyData,
});

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

/**
 * Codec for all Resonate protocol requests.
 *
 * @category schemas
 * @since 0.0.0
 */
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

const PromiseData = Schema.Struct({ promise: PromiseRecordFromWire });
const Preload = Schema.Array(PromiseRecordFromWire);

export const PromiseGetResponse = Schema.Union([
  Schema.Struct({
    kind: Schema.tag("promise.get"),
    head: SuccessHead,
    data: PromiseData,
  }),
  Schema.Struct({
    kind: Schema.tag("promise.get"),
    head: ErrorHead,
    data: Schema.String,
  }),
]);
export type PromiseGetResponse = typeof PromiseGetResponse.Type;
export const PromiseGetSuccessResponse = PromiseGetResponse.members[0];

export const PromiseCreateResponse = Schema.Union([
  Schema.Struct({
    kind: Schema.tag("promise.create"),
    head: SuccessHead,
    data: PromiseData,
  }),
  Schema.Struct({
    kind: Schema.tag("promise.create"),
    head: ErrorHead,
    data: Schema.String,
  }),
]);
export type PromiseCreateResponse = typeof PromiseCreateResponse.Type;
export const PromiseCreateSuccessResponse = PromiseCreateResponse.members[0];

export const PromiseSettleResponse = Schema.Union([
  Schema.Struct({
    kind: Schema.tag("promise.settle"),
    head: SuccessHead,
    data: PromiseData,
  }),
  Schema.Struct({
    kind: Schema.tag("promise.settle"),
    head: ErrorHead,
    data: Schema.String,
  }),
]);
export type PromiseSettleResponse = typeof PromiseSettleResponse.Type;
export const PromiseSettleSuccessResponse = PromiseSettleResponse.members[0];

export const PromiseRegisterCallbackResponse = Schema.Union([
  Schema.Struct({
    kind: Schema.tag("promise.register_callback"),
    head: SuccessHead,
    data: PromiseData,
  }),
  Schema.Struct({
    kind: Schema.tag("promise.register_callback"),
    head: ErrorHead,
    data: Schema.String,
  }),
]);
export type PromiseRegisterCallbackResponse = typeof PromiseRegisterCallbackResponse.Type;
export const PromiseRegisterCallbackSuccessResponse = PromiseRegisterCallbackResponse.members[0];

export const PromiseRegisterListenerResponse = Schema.Union([
  Schema.Struct({
    kind: Schema.tag("promise.register_listener"),
    head: SuccessHead,
    data: PromiseData,
  }),
  Schema.Struct({
    kind: Schema.tag("promise.register_listener"),
    head: ErrorHead,
    data: Schema.String,
  }),
]);
export type PromiseRegisterListenerResponse = typeof PromiseRegisterListenerResponse.Type;
export const PromiseRegisterListenerSuccessResponse = PromiseRegisterListenerResponse.members[0];

export const PromiseSearchResponse = Schema.Union([
  Schema.Struct({
    kind: Schema.tag("promise.search"),
    head: SuccessHead,
    data: Schema.Struct({
      promises: Schema.Array(PromiseRecordFromWire),
      cursor: Schema.optionalKey(Schema.String),
    }),
  }),
  Schema.Struct({
    kind: Schema.tag("promise.search"),
    head: ErrorHead,
    data: Schema.String,
  }),
]);
export type PromiseSearchResponse = typeof PromiseSearchResponse.Type;

export const TaskGetResponse = Schema.Union([
  Schema.Struct({
    kind: Schema.tag("task.get"),
    head: SuccessHead,
    data: Schema.Struct({ task: TaskRecordFromWire }),
  }),
  Schema.Struct({
    kind: Schema.tag("task.get"),
    head: ErrorHead,
    data: Schema.String,
  }),
]);
export type TaskGetResponse = typeof TaskGetResponse.Type;
export const TaskGetSuccessResponse = TaskGetResponse.members[0];

export const TaskCreateResponse = Schema.Union([
  Schema.Struct({
    kind: Schema.tag("task.create"),
    head: SuccessHead,
    data: Schema.Struct({
      task: Schema.optionalKey(TaskRecordFromWire),
      promise: PromiseRecordFromWire,
      preload: Preload,
    }),
  }),
  Schema.Struct({
    kind: Schema.tag("task.create"),
    head: ErrorHead,
    data: Schema.String,
  }),
]);
export type TaskCreateResponse = typeof TaskCreateResponse.Type;
export const TaskCreateSuccessResponse = TaskCreateResponse.members[0];

export const TaskAcquireResponse = Schema.Union([
  Schema.Struct({
    kind: Schema.tag("task.acquire"),
    head: SuccessHead,
    data: Schema.Struct({
      task: TaskRecordFromWire,
      promise: PromiseRecordFromWire,
      preload: Preload,
    }),
  }),
  Schema.Struct({
    kind: Schema.tag("task.acquire"),
    head: ErrorHead,
    data: Schema.String,
  }),
]);
export type TaskAcquireResponse = typeof TaskAcquireResponse.Type;
export const TaskAcquireSuccessResponse = TaskAcquireResponse.members[0];

export const TaskReleaseResponse = Schema.Union([
  Schema.Struct({
    kind: Schema.tag("task.release"),
    head: SuccessHead,
    data: EmptyData,
  }),
  Schema.Struct({
    kind: Schema.tag("task.release"),
    head: ErrorHead,
    data: Schema.String,
  }),
]);
export type TaskReleaseResponse = typeof TaskReleaseResponse.Type;
export const TaskReleaseSuccessResponse = TaskReleaseResponse.members[0];

export const TaskSuspendResponse = Schema.Union([
  Schema.Struct({
    kind: Schema.tag("task.suspend"),
    head: SuccessHead,
    data: EmptyData,
  }),
  Schema.Struct({
    kind: Schema.tag("task.suspend"),
    head: RedirectHead,
    data: Schema.Struct({ preload: Preload }),
  }),
  Schema.Struct({
    kind: Schema.tag("task.suspend"),
    head: ErrorHead,
    data: Schema.String,
  }),
]);
export type TaskSuspendResponse = typeof TaskSuspendResponse.Type;
export const TaskSuspendAcceptedResponse = TaskSuspendResponse.members[0];
export const TaskSuspendRefusedResponse = TaskSuspendResponse.members[1];

export const TaskHaltResponse = Schema.Union([
  Schema.Struct({
    kind: Schema.tag("task.halt"),
    head: SuccessHead,
    data: EmptyData,
  }),
  Schema.Struct({
    kind: Schema.tag("task.halt"),
    head: ErrorHead,
    data: Schema.String,
  }),
]);
export type TaskHaltResponse = typeof TaskHaltResponse.Type;
export const TaskHaltSuccessResponse = TaskHaltResponse.members[0];

export const TaskContinueResponse = Schema.Union([
  Schema.Struct({
    kind: Schema.tag("task.continue"),
    head: SuccessHead,
    data: EmptyData,
  }),
  Schema.Struct({
    kind: Schema.tag("task.continue"),
    head: ErrorHead,
    data: Schema.String,
  }),
]);
export type TaskContinueResponse = typeof TaskContinueResponse.Type;
export const TaskContinueSuccessResponse = TaskContinueResponse.members[0];

export const TaskFulfillResponse = Schema.Union([
  Schema.Struct({
    kind: Schema.tag("task.fulfill"),
    head: SuccessHead,
    data: PromiseData,
  }),
  Schema.Struct({
    kind: Schema.tag("task.fulfill"),
    head: ErrorHead,
    data: Schema.String,
  }),
]);
export type TaskFulfillResponse = typeof TaskFulfillResponse.Type;
export const TaskFulfillSuccessResponse = TaskFulfillResponse.members[0];

export const TaskFenceResponse = Schema.Union([
  Schema.Struct({
    kind: Schema.tag("task.fence"),
    head: SuccessHead,
    data: Schema.Struct({
      action: Schema.Union([PromiseCreateResponse, PromiseSettleResponse]),
      preload: Preload,
    }),
  }),
  Schema.Struct({
    kind: Schema.tag("task.fence"),
    head: ErrorHead,
    data: Schema.String,
  }),
]);
export type TaskFenceResponse = typeof TaskFenceResponse.Type;
export const TaskFenceSuccessResponse = TaskFenceResponse.members[0];

export const TaskHeartbeatResponse = Schema.Union([
  Schema.Struct({
    kind: Schema.tag("task.heartbeat"),
    head: SuccessHead,
    data: EmptyData,
  }),
  Schema.Struct({
    kind: Schema.tag("task.heartbeat"),
    head: ErrorHead,
    data: Schema.String,
  }),
]);
export type TaskHeartbeatResponse = typeof TaskHeartbeatResponse.Type;
export const TaskHeartbeatSuccessResponse = TaskHeartbeatResponse.members[0];

export const TaskSearchResponse = Schema.Union([
  Schema.Struct({
    kind: Schema.tag("task.search"),
    head: SuccessHead,
    data: Schema.Struct({
      tasks: Schema.Array(TaskRecordFromWire),
      cursor: Schema.optionalKey(Schema.String),
    }),
  }),
  Schema.Struct({
    kind: Schema.tag("task.search"),
    head: ErrorHead,
    data: Schema.String,
  }),
]);
export type TaskSearchResponse = typeof TaskSearchResponse.Type;

const ScheduleData = Schema.Struct({ schedule: ScheduleRecord });

export const ScheduleGetResponse = Schema.Union([
  Schema.Struct({
    kind: Schema.tag("schedule.get"),
    head: SuccessHead,
    data: ScheduleData,
  }),
  Schema.Struct({
    kind: Schema.tag("schedule.get"),
    head: ErrorHead,
    data: Schema.String,
  }),
]);
export type ScheduleGetResponse = typeof ScheduleGetResponse.Type;
export const ScheduleGetSuccessResponse = ScheduleGetResponse.members[0];

export const ScheduleCreateResponse = Schema.Union([
  Schema.Struct({
    kind: Schema.tag("schedule.create"),
    head: SuccessHead,
    data: ScheduleData,
  }),
  Schema.Struct({
    kind: Schema.tag("schedule.create"),
    head: ErrorHead,
    data: Schema.String,
  }),
]);
export type ScheduleCreateResponse = typeof ScheduleCreateResponse.Type;
export const ScheduleCreateSuccessResponse = ScheduleCreateResponse.members[0];

export const ScheduleDeleteResponse = Schema.Union([
  Schema.Struct({
    kind: Schema.tag("schedule.delete"),
    head: SuccessHead,
    data: EmptyData,
  }),
  Schema.Struct({
    kind: Schema.tag("schedule.delete"),
    head: ErrorHead,
    data: Schema.String,
  }),
]);
export type ScheduleDeleteResponse = typeof ScheduleDeleteResponse.Type;
export const ScheduleDeleteSuccessResponse = ScheduleDeleteResponse.members[0];

export const ScheduleSearchResponse = Schema.Union([
  Schema.Struct({
    kind: Schema.tag("schedule.search"),
    head: SuccessHead,
    data: Schema.Struct({
      schedules: Schema.Array(ScheduleRecord),
      cursor: Schema.optionalKey(Schema.String),
    }),
  }),
  Schema.Struct({
    kind: Schema.tag("schedule.search"),
    head: ErrorHead,
    data: Schema.String,
  }),
]);
export type ScheduleSearchResponse = typeof ScheduleSearchResponse.Type;

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

export const DebugStartResponse = Schema.Union([
  Schema.Struct({
    kind: Schema.tag("debug.start"),
    head: SuccessHead,
    data: EmptyData,
  }),
  Schema.Struct({
    kind: Schema.tag("debug.start"),
    head: ErrorHead,
    data: Schema.String,
  }),
]);
export const DebugResetResponse = Schema.Union([
  Schema.Struct({
    kind: Schema.tag("debug.reset"),
    head: SuccessHead,
    data: EmptyData,
  }),
  Schema.Struct({
    kind: Schema.tag("debug.reset"),
    head: ErrorHead,
    data: Schema.String,
  }),
]);
export const DebugTickResponse = Schema.Union([
  Schema.Struct({
    kind: Schema.tag("debug.tick"),
    head: SuccessHead,
    data: Schema.Array(DebugTickAction),
  }),
  Schema.Struct({
    kind: Schema.tag("debug.tick"),
    head: ErrorHead,
    data: Schema.String,
  }),
]);
export const DebugSnapResponse = Schema.Union([
  Schema.Struct({
    kind: Schema.tag("debug.snap"),
    head: SuccessHead,
    data: Schema.Struct({
      promises: Schema.Array(PromiseRecordFromWire),
      promiseTimeouts: Schema.Array(Schema.Struct({ id: PromiseId, timeout: Timestamp })),
      callbacks: Schema.Array(Schema.Struct({ awaiter: PromiseId, awaited: PromiseId })),
      listeners: Schema.optionalKey(Schema.Array(Schema.Struct({ id: PromiseId, address: Schema.String }))),
      tasks: Schema.Array(TaskRecordFromWire),
      taskTimeouts: Schema.Array(Schema.Struct({ id: TaskId, type: Schema.Finite, timeout: Timestamp })),
      messages: Schema.Array(Schema.Struct({ address: Schema.String, message: Message })),
    }),
  }),
  Schema.Struct({
    kind: Schema.tag("debug.snap"),
    head: ErrorHead,
    data: Schema.String,
  }),
]);
export const DebugSnapSuccessResponse = DebugSnapResponse.members[0];
export const DebugStopResponse = Schema.Union([
  Schema.Struct({
    kind: Schema.tag("debug.stop"),
    head: SuccessHead,
    data: EmptyData,
  }),
  Schema.Struct({
    kind: Schema.tag("debug.stop"),
    head: ErrorHead,
    data: Schema.String,
  }),
]);

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

/**
 * Codec for all Resonate protocol responses.
 *
 * @category schemas
 * @since 0.0.0
 */
export const ResponseFromWire = Schema.Union([
  PromiseGetResponse,
  PromiseCreateResponse,
  PromiseSettleResponse,
  PromiseRegisterCallbackResponse,
  PromiseRegisterListenerResponse,
  PromiseSearchResponse,
  TaskGetResponse,
  TaskCreateResponse,
  TaskAcquireResponse,
  TaskReleaseResponse,
  TaskSuspendResponse,
  TaskHaltResponse,
  TaskContinueResponse,
  TaskFulfillResponse,
  TaskFenceResponse,
  TaskHeartbeatResponse,
  TaskSearchResponse,
  ScheduleGetResponse,
  ScheduleCreateResponse,
  ScheduleDeleteResponse,
  ScheduleSearchResponse,
  DebugStartResponse,
  DebugResetResponse,
  DebugTickResponse,
  DebugSnapResponse,
  DebugStopResponse,
]);
