# Design Decisions of the Effect Resonate SDK

> **Status: DRAFT v2 — awaiting review.** Phase 1 deliverable. No implementation has begun.
>
> Sources of truth, in order of precedence:
>
> 1. `repos/resonate-specification` — the Lean 4 abstract machine (protocol semantics).
> 2. The shipped Resonate server behavior (where the spec is silent or diverges — the spec's own `00-resume.lean` declares itself "oracle-aligned" to implementations).
> 3. `repos/resonate-sdk-ts` — the official TypeScript SDK (API-surface reference and wire-format reference; _not_ a design template).
> 4. `repos/distributed-async-await.io/content/docs` — the SDK-implementation handbook (normative guidance, open questions).

## Resolved design decisions (from review)

- These are **functions** (Resonate Functions), not "workflows" — terminology follows Resonate.
- Standalone API. This SDK has nothing to do with effect-smol's unstable `workflow` module; do not model APIs on it.
- The API surface should look as close as possible to the native TypeScript SDK, translated into Effect idioms — no invented concepts.
- Definition/implementation split is kept; registration follows **`effect/rpc`'s `RpcGroup` pattern** (the ecosystem canon, also used by `effect/cluster` `Entity` and `effect/workflow`): `Resonate.function` definitions (PascalCase names, like `Rpc.make("Increment")`) → `Resonate.group(...)` → `group.toLayer(handlers)` → `Resonate.Worker.layer(group, config)`. (`repos/effect-kafka` is vendored as an additional design reference; its `MessageRouter` shape was considered and rejected for registration.)
- **Guiding principle: keep the API slim, composable, effect-first — no unnecessary abstractions.**
- Payload is schema-typed and maps to the wire's **positional args**: a `Schema.Tuple` maps element-per-arg (`args: [5, 60]`, native/CLI interop); any non-tuple schema is a one-element tuple (`args: [value]`). No invented `success`/`error` schemas: return values and rejections round-trip through the codec exactly as in the native SDK, with **no runtime decoding** — result types flow by TypeScript inference from the implementation (the analog of the native `Return<F>`); string-name invocations return `unknown` and callers may `Schema.decodeUnknown` themselves.
- Step/child ids use the native **sequence counter**, not invented mandatory step names. Fan-out follows the native invoke/await pattern (see §5.4).
- **Strict type checking is a hard requirement — no casts, no manual type annotations.** Where the native SDK relies on an annotation (`ctx.promise<T>()`), we deviate: externally settled promises are schema-declared (`Resonate.promise`) so both sides infer their types (§4.5).
- **All domain ids are Schema-branded strings, never raw strings**: `ExecutionId`, `PromiseId`, `ScheduleId`, `WorkerGroup`, `ProcessId` (`TaskId` is `PromiseId` — the spec mandates a task shares its promise's id). Constructed via `X.make(...)` (defect on invalid) or `Schema.decodeUnknown(X)` at external boundaries; public APIs require the brand, so id-kind mix-ups (passing an execution id where a promise id belongs) are compile errors. Validation is shallow (non-empty) — neither spec nor server constrains id charset, and over-constraining would break interop with ids minted by other SDKs.
- **The domain model is maximally strict wherever the protocol vocabulary is finite** (§3.2): structured `Tags` (typed reserved keys; user-tag keys provably outside `resonate:`), parsed `TargetAddress` (`poll://{cast}@{group}[/{id}]`), **state-discriminated unions** for `TaskRecord`/`PromiseRecord` (spec invariants like "acquired ⇒ pid+ttl" become unrepresentable states), branded `FunctionVersion`/`TaskVersion`/`CorrelationId`, literal protocol version and status codes, `DateTime.Utc`/`Duration` in place of raw ms numbers. Rule: **strict on construct, lenient on decode** — never reject a wire record the server itself accepts (other SDKs mint loose records; rejecting them would be a behavior deviation).
- **Never deviate from native semantics; deviate only to make public APIs more useful or strongly typed.** Concretely: interrupting a fiber awaiting a handle detaches the listener and nothing more (native behavior) — no `cancelOnInterrupt` option; cancellation is always the explicit `cancel` call. `ctx.now`/`ctx.random` as one durable step (server round-trip) each is accepted, matching native `ctx.date.now()`/`ctx.math.random()`.
- Single package.

---

## 1. Goals and Non-Goals

**Goals**

- 100% adherence to the Resonate protocol: the `{kind, head, data}` message envelope (protocol version `2026-04-01`), the promise/task/schedule state machines, version-based task fencing, lease/heartbeat semantics, the `resonate:*` tag vocabulary, and deterministic-replay recovery.
- An SDK that feels native to Effect: `Schema`-backed protocol types, `Context.Service` services, `Layer` wiring, typed errors via `Schema.TaggedErrorClass`, `Duration`/`DateTime`/`Stream` where they fit — while keeping the _surface_ recognizable to anyone who knows the native TypeScript SDK (`run`, `rpc`, `beginRun`, `beginRpc`, `detached`, `sleep`, `promise`, options, registry, schedules).
- Interoperable on the wire with TS/Rust workers (the `{func, args}` invocation core, `resonate:target`/`resonate:origin`/`resonate:branch` tags, `resonate:timer` sleep).

**Non-Goals**

- Wire compatibility with the Python SDK's older REST-shaped protocol.
- Implementing `promise.search` / `task.search` / `schedule.search` (the spec itself returns `501`).
- Runtime detection of determinism violations (no reference SDK does; protection is structural — see §5.5).

---

## 2. Protocol Model (condensed)

The server is a message bus that durably remembers. Three object kinds:

**Durable promise** — `id`, `state`, `param: Value`, `value: Value`, `tags`, `timeoutAt` (absolute epoch-ms), `createdAt`, `settledAt?`. States: `pending | resolved | rejected | rejected_canceled | rejected_timedout`; all non-pending states are terminal and immutable. A pending promise past its `timeoutAt` is _projected_ as settled (`resolved` if tagged `resonate:timer`, else `rejected_timedout`) by every read/mutate path even before the server's timeout transition persists it.

**Task** — the claim on producing a promise's value; shares the promise's `id`. Fields: `state`, `version`, `pid?`, `ttl?`, `resumes`. States: `pending | acquired | suspended | halted | fulfilled` (`fulfilled` absorbing). A task exists iff the promise carries a `resonate:target` tag. `version` is the fencing token — bumped **only** on acquire; every mutating task op must present it or receive `409`. Leases (`ttl` + heartbeat at ~TTL/2) return unrefreshed acquired tasks to `pending` for redelivery.

**Schedule** — cron + promise template; the server materializes a promise per tick (with catch-up for missed ticks), dispatching a task if the template's tags include `resonate:target`.

**Key operations** (each maps to one request kind): `promise.get/create/settle/register_callback/register_listener`, `task.get/create/acquire/fence/heartbeat/suspend/fulfill/release/halt/continue`, `schedule.get/create/delete`. Push messages: `execute` (claim-and-run hint carrying `{taskId, version}`) and `unblock` (settled promise pushed to a registered listener).

**The recovery loop**: a worker acquires a task, re-executes the function from the top; every durable step re-issues `promise.create` with a _deterministically derived id_ — creation is idempotent by id, so completed steps return their recorded results instantly and only new work executes. Blocking on unsettled promises triggers `task.suspend` (atomic multi-callback registration; `300` fast-path with `preload` if something already settled). Completion is one atomic `task.fulfill` (settle + fulfill). Side effects the server can't see are guarded by `task.fence`.

**Reserved tags**: `resonate:target` (delivery address; task-existence trigger), `resonate:origin`, `resonate:parent`, `resonate:branch` (drives preload), `resonate:timer` (resolve-on-timeout sleep), `resonate:delay`. `resonate:scope` (`global`/`local`) is SDK convention.

---

## 3. Architecture

Four layers, each usable without the ones above it:

```
┌────────────────────────────────────────────────────────────┐
│ 4. Function API      Resonate.function / ResonateContext   │  ← what users touch
├────────────────────────────────────────────────────────────┤
│ 3. Runtime           ResonateWorker (task loop, heartbeat, │
│                      replay driver), ResonateClient        │
├────────────────────────────────────────────────────────────┤
│ 2. Protocol client   DurablePromises, Tasks, Schedules     │  ← typed ops, Schema-validated
├────────────────────────────────────────────────────────────┤
│ 1. Transport         ResonateNetwork (send + Stream recv)  │  ← Http+SSE / Local / Test
└────────────────────────────────────────────────────────────┘
```

Proposed module layout (single package):

```
src/
  Resonate.ts           # namespace entry: function, group, layers, client access
  ResonateContext.ts    # the in-function durable-operation service (native SDK's `Context`)
  DurablePromise.ts     # promise domain model + typed client ops
  Task.ts               # task domain model + typed client ops
  Schedule.ts           # schedules
  Protocol.ts           # wire schemas: envelope, requests, responses, messages
  Network.ts            # ResonateNetwork service interface
  NetworkHttp.ts        # HTTP POST + SSE poll transport
  NetworkLocal.ts       # in-memory server (dev + conformance oracle)
  Codec.ts              # value encoding boundary
  Errors.ts             # tagged error taxonomy
  Worker.ts             # worker runtime layer
  testing.ts            # test harness exports (TestClock-driven local server, simulator)
```

### 3.1 Layer 1 — Transport: `ResonateNetwork`

```ts
export class ResonateNetwork extends Context.Service<
  ResonateNetwork,
  {
    /** SDK-initiated request/response. Correlation ids and protocol version handled here. */
    readonly send: <K extends Protocol.RequestKind>(
      request: Protocol.Request<K>,
    ) => Effect.Effect<Protocol.Response<K>, TransportError>;
    /** Server-pushed messages (execute / unblock) for this worker's addresses. */
    readonly messages: Stream.Stream<Protocol.Message, TransportError>;
    /** Translate a logical target into a structured transport address (poll://any@gpu-workers). */
    readonly match: (target: WorkerGroup) => TargetAddress;
    readonly unicast: TargetAddress;
    readonly anycast: (group: WorkerGroup) => TargetAddress;
  }
>()("effect-resonate/Network") {}
```

- `NetworkHttp.layer` — single-endpoint HTTP POST via Effect `HttpClient`; SSE long-poll at `/poll/{group}/{pid}` as a `Stream` with exponential reconnect backoff (capped ~30s, reset on success), per the handbook's MUST.
- `NetworkLocal.layer` — an in-memory implementation of the _server_ state machine (see §8). Zero-install dev mode, and the conformance oracle for tests. Selected automatically when no URL is configured, matching the native SDK's `LocalNetwork` fallback.
- Non-2xx protocol statuses (`300`, `404`, `409`, `422`) are **first-class typed outcomes** in layer 2, not transport errors. Only genuine transport failures (connection loss, malformed frames, corrId mismatch, unexpected status) become `TransportError` — mirroring the native SDK's crisp platform-vs-protocol split.

### 3.2 Layer 2 — Protocol client

All wire types are `Schema` classes in `Protocol.ts` — decoded on receipt, encoded on send. The domain model is deliberately much stricter than the native SDK's: literals, brands, and state-discriminated unions wherever the protocol's vocabulary is finite.

**Branded ids** — every domain id; raw strings never cross a public API:

```ts
export const PromiseId = Schema.NonEmptyString.pipe(Schema.brand("PromiseId"));
export type PromiseId = typeof PromiseId.Type;
export const ExecutionId = Schema.NonEmptyString.pipe(Schema.brand("ExecutionId"));
export const ScheduleId = Schema.NonEmptyString.pipe(Schema.brand("ScheduleId"));
export const WorkerGroup = Schema.NonEmptyString.pipe(Schema.brand("WorkerGroup"));
export const ProcessId = Schema.NonEmptyString.pipe(Schema.brand("ProcessId"));
export const CorrelationId = Schema.NonEmptyString.pipe(Schema.brand("CorrelationId"));
export type TaskId = PromiseId; // per spec: a task shares its promise's id
```

**Tags** — the reserved vocabulary is finite and each reserved key has a typed value domain; user tags are provably outside the `resonate:` namespace (the "users can't override system tags" rule lives in the type, not a runtime check). Domain-side structured, wire-side the flat record, via a transformation schema:

```ts
const ReservedTags = Schema.Struct({
  "resonate:timer": Schema.optional(Schema.Literal("true")), // only ever "true"
  "resonate:scope": Schema.optional(Schema.Literals(["local", "global"])),
  "resonate:target": Schema.optional(TargetAddress),
  "resonate:origin": Schema.optional(PromiseId),
  "resonate:parent": Schema.optional(PromiseId),
  "resonate:branch": Schema.optional(PromiseId),
  "resonate:prefix": Schema.optional(PromiseId),
  "resonate:delay": Schema.optional(EpochMillisFromString), // native parses with .toNat! (crashes); we decode
});

const UserTagKey = Schema.String.pipe(
  Schema.filter((k) => !k.startsWith("resonate:")),
  Schema.brand("UserTagKey"),
);

export class Tags extends Schema.Class<Tags>("Tags")({
  reserved: ReservedTags,
  user: Schema.Record(UserTagKey, Schema.String),
}) {}
```

**Target addresses** — the spec-documented grammar `poll://{cast}@{group}[/{id}]`, parsed:

```ts
export class TargetAddress extends Schema.Class<TargetAddress>("TargetAddress")({
  transport: Schema.Literals(["poll", "local"]),
  cast: Schema.Literals(["uni", "any"]), // unicast vs anycast — invisible in a raw string
  group: WorkerGroup,
  id: Schema.Option(ProcessId),
}) {}
```

**Records are state-discriminated unions** — the spec's structural invariants (every acquired task has pid+ttl+lease; suspended/pending/halted/fulfilled tasks hold no lease; settled promises have `settledAt`, pending ones don't) become unrepresentable states instead of runtime assertions:

```ts
export const TaskRecord = Schema.Union([
  TaskPending, // { state: "pending",   id, version, resumes }        — no pid, no ttl
  TaskAcquired, // { state: "acquired",  id, version, pid, ttl }       — pid/ttl REQUIRED
  TaskSuspended, // { state: "suspended", id, version }
  TaskHalted, // { state: "halted",    id, version }
  TaskFulfilled, // { state: "fulfilled", id, version }                 — absorbing
]);

export const PromiseRecord = Schema.Union([PromisePending, PromiseSettled]);
// PromisePending: { state: "pending", id, param, tags: Tags, timeoutAt: DateTime.Utc, createdAt }  — no settledAt
// PromiseSettled: { state: "resolved" | "rejected" | "rejected_canceled" | "rejected_timedout",
//                   id, param, value, tags, timeoutAt, createdAt, settledAt: DateTime.Utc }        — settledAt REQUIRED
// projected(now) returns PromiseSettled — the spec's timeout projection IS a settled view
```

**No magic values, no raw scalars:**

- Function version: branded positive-int `FunctionVersion`; call sites say `"latest"` — the native magic `0` exists only on the wire.
- Task fencing version: branded `TaskVersion` (non-negative int) — cannot be confused with `FunctionVersion`.
- Protocol version: `Schema.Literal("2026-04-01")`, one constant.
- Envelope status: `Schema.Literals([200, 300, 400, 401, 403, 404, 409, 422, 429, 500, 501])`.
- Timestamps are `DateTime.Utc` domain-side (epoch-ms on the wire); `ttl` is `Duration` (wire ms).

**Strict on construct, lenient on decode.** Everything this SDK _emits_ satisfies the tight types. Records arriving off the wire may have been minted by TS/Python workers that can (and do) put arbitrary values in reserved tags — wire-decode paths use permissive fallbacks (an unrecognized reserved-tag value is preserved raw rather than failing the record) so tightness never rejects records the server itself accepts. Rejecting other SDKs' server-valid records would be a behavior deviation; that is the line.

Typed operation services sit on top:

```ts
export class DurablePromises extends Context.Service<DurablePromises, {
  readonly create: (req: PromiseCreateRequest) => Effect.Effect<PromiseRecord, ResonateProtocolError | TransportError>
  readonly settle: (req: PromiseSettleRequest) => Effect.Effect<PromiseRecord, ...>
  readonly get: (id: PromiseId) => Effect.Effect<PromiseRecord, PromiseNotFound | ...>
  readonly registerCallback: (...) => ...
  readonly registerListener: (...) => ...
  /** Await settlement: register_listener + unblock stream, as a single Effect. */
  readonly awaitSettled: (id: PromiseId) => Effect.Effect<PromiseRecord, ...>
}>()("effect-resonate/DurablePromises") {
  static readonly layer: Layer.Layer<DurablePromises, never, ResonateNetwork>
}
```

`Tasks` and `Schedules` follow the same pattern (`Tasks.acquire/fence/suspend/fulfill/release/heartbeat/...`). This layer is public: power users get a fully typed protocol client even if they never touch registered functions.

### 3.3 Layer 3 — Runtime

- `ResonateClient` — start/attach/observe executions from ordinary (non-durable) code; the Effect analog of the `Resonate` class's `run/rpc/beginRun/beginRpc/get/schedule` methods.
- `ResonateWorker` — a `Layer` that owns the worker loop: consume `execute` messages → `task.acquire` → drive replay → `task.suspend`/`task.fulfill`; plus a single per-process heartbeat fiber refreshing **the actual list of held `(id, version)` pairs** at TTL/2 (the official TS SDK sends an empty list — a known gap we deliberately do not reproduce).

### 3.4 Layer 4 — Function API

Detailed in §4–5.

---

## 4. Public API by Example

### 4.1 Defining and implementing a function

A Resonate Function is defined by name + payload schema, and implemented separately. Registration follows the canonical Effect v4 shape — `effect/rpc`'s `RpcGroup` (the same pattern `effect/cluster` `Entity` and `effect/workflow` use): **definitions → group → `toLayer(handlers)` → worker layer built from the group**. Definition names are PascalCase, like `Rpc.make("Increment")`:

```ts
import { Duration, Effect, Schema } from "effect";
import { Resonate, ResonateContext } from "effect-resonate";

// Definitions — client-importable, no implementation attached.
// A tuple schema maps to the wire's positional args (args: [5, 60]).
export const Countdown = Resonate.function("Countdown", {
  payload: Schema.Tuple([Schema.Number, Schema.Number]),
  version: 1,
});
export const Checkout = Resonate.function("Checkout", { payload: Order });

// The group — like RpcGroup.make
export const AppFns = Resonate.group(Countdown, Checkout);

// Implementations — handler map keyed by function name, payload tuple elements
// arriving as positional parameters; produces Layer<Handler<"Countdown"> | Handler<"Checkout">>
export const HandlersLive = AppFns.toLayer({
  Countdown: Effect.fn("Countdown")(function* (count, delay) {
    const ctx = yield* ResonateContext;

    for (let i = count; i > 0; i--) {
      yield* ctx.run(Effect.sync(() => console.log(`Countdown: ${i}`)));
      yield* ctx.sleep(Duration.seconds(delay));
    }

    yield* Effect.logInfo("Done!");
  }),
  Checkout: (order) =>
    Effect.gen(function* () {
      /* ... */
    }),
});
```

- Like `RpcGroup.toLayer`, the handler map can also be built by an Effect (shared setup state: `AppFns.toLayer(Effect.gen(function*() { const ref = yield* Ref.make(0); return AppFns.of({ ... }) }))`), and `AppFns.toLayerHandler("Countdown", handler)` implements a single function.
- Handler `R` requirements flow through the layer as usual; groups compose with `Resonate.group(...AppFns.fns, ...OtherFns.fns)` or by merging handler layers.
- The worker layer is built _from the group_ (§4.9), so a missing handler is a **compile error** — the native runtime error `REGISTRY_FUNCTION_NOT_REGISTERED` moves to the type level.

- **Payload is positional, schema-typed.** The wire's invocation core is `{func, args}` — literal function application, which is why the protocol (and the CLI: `resonate invoke countdown.1 --func countdown --arg 5 --arg 60`) is positional: it ports across SDKs (JS spread, Python `*args`). A `Schema.Tuple` payload maps element-per-arg, so TS workers and the CLI can invoke us natively and vice versa. Any **non-tuple** schema (a struct, `Schema.String`, …) is treated as a one-element tuple — `args: [value]`, handler receives the single decoded value — exactly how a native function taking one object argument looks on the wire.
- **No success/error schemas, no runtime result decoding.** The native SDK's `Return<F>` is pure type inference — the runtime just passes values through the JSON codec (base64 JSON, `Error` reconstruction; see §4.7). Same here: result types flow by inference wherever the implementation is in scope (native parity — `register` types from the actual function). A bare definition or string-name invocation returns `unknown`, and callers parse with `Schema.decodeUnknown` if they want validation — exactly the native string-dispatch posture.
- **Versioning** mirrors the native registry: name + branded `FunctionVersion`; call sites say `"latest"` (the native magic `0` exists only on the wire).
- Anything can also be invoked **by bare name** (`ctx.rpc("Countdown", [5, 60])`) for cross-service calls where no definition is importable — same as the native SDK's string dispatch.

### 4.2 `ResonateContext` — the native `Context`, as an Effect service

The native SDK passes `Context` as the first function argument. Here it is a `Context.Service` provided to the function's fiber by the worker runtime — same operations, native names:

```ts
export class ResonateContext extends Context.Service<ResonateContext, {
  /** Ids locating this invocation in the promise graph (native: ctx.id, originId, parentId, ...). */
  readonly info: {
    readonly id: PromiseId
    readonly originId: PromiseId
    readonly parentId: PromiseId
    readonly attempt: number
    readonly timeoutAt: DateTime.Utc
    readonly version: number
  }

  // ── Local durable execution (native lfc / lfi) ─────────────────────────
  /** Run an effect as a durable step: executes once, persists the result, replays for free. */
  readonly run: <A, E, R>(effect: Effect.Effect<A, E, R>, options?: Options) =>
    Effect.Effect<A, E | ResonateError, R>
  /** Invoke form: create the durable promise and return a handle without awaiting. */
  readonly beginRun: <A, E, R>(effect: Effect.Effect<A, E, R>, options?: Options) =>
    Effect.Effect<DurableHandle<A, E>, ResonateError, R>

  // ── Remote durable invocation (native rfc / rfi) ───────────────────────
  /** Call a registered function on some worker (target group) and await its result. */
  readonly rpc: {
    <F extends AnyResonateFn>(fn: F, payload: Payload<F>, options?: Options): Effect.Effect<Success<F>, Failure<F> | ResonateError>
    (name: string, payload: unknown, options?: Options): Effect.Effect<unknown, ResonateError>
  }
  /** Invoke form of rpc: dispatch and return a handle. */
  readonly beginRpc: /* same overloads, returning DurableHandle */

  /** Fire-and-forget with an independent lifecycle — a fresh root promise (native detached). */
  readonly detached: <F extends AnyResonateFn>(fn: F, payload: Payload<F>, options?: Options) =>
    Effect.Effect<DurableHandle<Success<F>, Failure<F>>, ResonateError>

  // ── Time and events ─────────────────────────────────────────────────────
  /** Durable sleep — a resonate:timer promise; costs nothing to replay. */
  readonly sleep: (duration: Duration.DurationInput) => Effect.Effect<void, ResonateError>
  readonly sleepUntil: (instant: DateTime.Utc) => Effect.Effect<void, ResonateError>

  /** A latent durable promise settled by external means (native ctx.promise) — see §4.5. */
  readonly promise: <P extends AnyResonatePromise>(declaration: P, options?: PromiseOptions) =>
    Effect.Effect<DurableHandle<PromiseSuccess<P>, PromiseError<P>>, ResonateError>

  // ── Recorded nondeterminism (native ctx.date.now / ctx.math.random) ────
  readonly now: Effect.Effect<DateTime.Utc, ResonateError>
  readonly random: Effect.Effect<number, ResonateError>

  /** Abort the ROOT execution on invariant violation (native ctx.panic / ctx.assert). */
  readonly panic: (message: string) => Effect.Effect<never>
}>()("effect-resonate/ResonateContext") {}
```

`DurableHandle<A, E>` is the Effect analog of the native `Future`:

```ts
interface DurableHandle<A, E = never> {
  readonly id: PromiseId;
  readonly await: Effect.Effect<A, E | ResonateError>; // native `yield* future`
  readonly poll: Effect.Effect<Option.Option<Exit.Exit<A, E>>, ResonateError>;
}
```

`Options` mirrors the native options object: `{ id?, target?, timeout?, tags?, version?, retryPolicy?, nonRetryableErrors? }` — resolved in the same layered way (client defaults → per-call), with `Duration` in place of raw ms. An explicit `id` breaks lineage exactly as in the native SDK.

What is _not_ carried over: `getDependency`/`setDependency` — dependency injection is what Effect layers already do. A function implementation that needs a `Database` just uses it; the requirement accumulates on the registry value and is satisfied where the `serve` layer is composed.

### 4.3 Child invocation ids and determinism

Child ids are derived exactly as in the native SDK: a per-invocation **sequence counter** — the nth durable operation of an execution gets id `{parentId}.{n}` (detached: `{prefixId}.d{hash}`). Replay re-executes the function from the top; the same code path yields the same sequence of durable operations, hence the same ids, hence idempotent `promise.create` dedup. The four lineage tags (`resonate:origin/prefix/branch/parent`) and scope/target tags are emitted per native convention.

This is the load-bearing determinism contract, and it interacts with Effect concurrency — see §5.4 for the fan-out idiom and its rules.

### 4.4 Starting executions from the outside

The `ResonateClient` service is the Effect analog of the native `Resonate` instance methods:

```ts
import { ExecutionId, ResonateClient } from "effect-resonate";

const program = Effect.gen(function* () {
  const client = yield* ResonateClient;

  // Ids are branded — ExecutionId.make validates once, the type carries the proof.
  // Ids from external input decode instead: Schema.decodeUnknown(ExecutionId)(req.params.id)
  const id = ExecutionId.make("countdown.1");

  // Native resonate.run(id, func, ...args): create + claim locally, execute in THIS process
  const result = yield* client.run(Countdown, id, [5, 60]);

  // Native resonate.rpc: create with a target — some worker in the group picks it up
  const receipt = yield* client.rpc(ProcessPayment, ExecutionId.make("pay.42"), [{ orderId: "42", amount: 100 }]);

  // begin* forms return handles without awaiting (native beginRun / beginRpc)
  const handle = yield* client.beginRpc(Countdown, ExecutionId.make("countdown.2"), [3, 10]);
  const done = yield* handle.poll;
  const value = yield* handle.await;

  // Attach to an existing execution by id (native resonate.get)
  const existing = yield* client.get(Countdown, id);
});
```

The execution id is caller-chosen and **is the idempotency key** — invoking `run` twice with `"countdown.1"` attaches to the same durable execution, per protocol. Passing a `PromiseId` (or any other id kind, or a raw string) where an `ExecutionId` belongs is a compile error.

### 4.5 Awaiting external events (native `ctx.promise`)

A durable promise with no `resonate:target` is settled by "other means" — an API handler, a human approval, another system.

Here we deviate from the native SDK deliberately. Native `ctx.promise<T>()` types the value by manual annotation — a cast with no inference and no runtime check, on data supplied by an **external** settler, which is exactly untrusted input. So external promises are declared once, with schemas, and both sides derive their types from the declaration:

```ts
// Declaration — shared by the awaiting function and whoever settles it
export const Approval = Resonate.promise("approval", {
  success: Schema.Struct({ approvedBy: Schema.String }),
  error: ApprovalDenied, // optional Schema.TaggedErrorClass; omitted = no typed rejection
});

// The Onboarding handler (provided in the group's toLayer handler map)
const onboardingHandler = Effect.fn("Onboarding")(function* (user) {
  const ctx = yield* ResonateContext;

  // A latent promise; nobody is dispatched. Named declaration → stable id
  // `{executionId}.approval` (e.g. "onboarding.42.approval"), independent of
  // where in the function body it is created.
  const approval = yield* ctx.promise(Approval, { timeout: Duration.days(7) });
  //    ^ DurableHandle<{ approvedBy: string }, ApprovalDenied>   — inferred, no annotation

  // Primary pattern: the id flows OUTWARD from inside the execution —
  // publish approval.id (an email link, a DB row) via a durable step.
  yield* ctx.run(sendApprovalEmail(user.email, approval.id));

  // Suspends the task; any external settle resumes us. Timeout/cancel are typed errors.
  const { approvedBy } = yield* approval.await.pipe(
    Effect.catchTag("DurablePromiseTimedOut", () => Effect.succeed({ approvedBy: "auto" })),
  );
});

// Elsewhere — an HTTP handler settles it, typed and encoded through the same declaration.
// The id arrived via the published link; or, being name-derived, it is also constructible
// from the execution id alone: Approval.id(executionId) → PromiseId "onboarding.42.approval".
const approve = (promiseId: PromiseId) =>
  Effect.gen(function* () {
    const client = yield* ResonateClient;
    yield* client.resolve(Approval, promiseId, { approvedBy: "erik" }); // payload type-checked
    // client.reject(Approval, promiseId, new ApprovalDenied({ ... }))
  });
```

**Id addressing — a deliberate deviation.** Native `ctx.promise` always uses the sequence-counter id (`{parent}.{n}`) and, unlike `lfi/rfc`, accepts no explicit id — so an external settler must know the promise's _positional index_ in the function body, and inserting a step above it silently shifts the address. Since external settlement is the entire purpose of these promises, declared promises get a **name-derived id**: `{executionId}.{declarationName}` (explicitly overridable via `options.id`). Name-derived ids are just as deterministic under replay as counter ids, and they give external systems a stable address. Creating the same declaration twice in one execution requires an explicit id (collision detected at runtime).

- Awaiting decodes the settled value through the schema — external data is validated, not trusted; a value that fails decode surfaces as a defect pointing at the settler.
- Rejections encode/decode through the `error` schema into the awaiter's typed error channel.
- Raw untyped access remains available at layer 2 (`DurablePromises.settle`) for interop with promises settled by other SDKs.

Why schemas are right here but wrong for function returns (§4.1): a function's return value is produced by our own typed implementation in-process — inference covers it, native parity holds. An external promise's value crosses a trust boundary with no implementation to infer from; the only alternatives are a manual type assertion (native) or `unknown`. Strict typing without annotations is a hard requirement of this SDK, so the declaration carries the schema.

### 4.6 Retry policies

Retry policies follow the native SDK: four encodable policies, persisted on the wire (`param.data.retry`) so retries survive restarts and apply on whichever worker claims the task:

```ts
import { RetryPolicy } from "effect-resonate";

yield *
  ctx.run(chargeCard(order), {
    retryPolicy: RetryPolicy.Exponential({ delay: "1 second", factor: 2, maxDelay: "30 seconds" }),
    nonRetryableErrors: [CardDeclined], // the error classes themselves, like native's Error constructors
  });
```

- Policies: `Constant | Linear | Exponential | Never`, modeled as a Schema tagged union with `Duration` inputs; encoded in the TS SDK's wire format.
- Defaults match the native convention: `Never` for durable (generator-style) functions — replay makes whole-function retry wrong; internal steps carry their own policies — and `Exponential` for plain leaf effects.
- Retries apply around step execution in-process (native `executeWithRetry`), bounded by the invocation's `timeoutAt`.
- `nonRetryableErrors` takes the `Schema.TaggedErrorClass` classes themselves (the direct analog of native's `Error` constructors — no stringly-typed tags). Matching uses each class's `_tag`, so it survives serialization boundaries where constructor identity wouldn't; the array's element type is constrained to classes whose instances occur in the effect's error channel, so listing an error the step can't produce is a compile error.

### 4.7 Errors and cancellation

Failure taxonomy — all `Schema.TaggedErrorClass`:

```ts
// Platform (never stored in promises; retriable at the transport layer)
class TransportError extends Schema.TaggedErrorClass<TransportError>()("TransportError", {
  reason: Schema.Literals(["ConnectionLost", "MalformedResponse", "CorrelationMismatch", "Unauthorized"]),
  cause: Schema.Defect()
}) {}

// Protocol (typed outcomes of well-formed server responses)
class TaskFenced extends Schema.TaggedErrorClass<TaskFenced>()("TaskFenced", { id: PromiseId, version: Schema.Number }) {}   // 409
class PromiseNotFound extends Schema.TaggedErrorClass<PromiseNotFound>()("PromiseNotFound", { id: PromiseId }) {}            // 404
// ... InvalidTarget (422), etc.

// Terminal promise outcomes, surfaced to awaiters as typed errors
class DurablePromiseTimedOut extends Schema.TaggedErrorClass<...>()("DurablePromiseTimedOut", { id: PromiseId }) {}
class DurablePromiseCanceled extends Schema.TaggedErrorClass<...>()("DurablePromiseCanceled", { id: PromiseId }) {}
```

- **User failures** are encoded into the rejected promise's `value` through the codec, exactly like the native SDK (which reconstructs `Error`/`AggregateError` by `__type` markers). Effect failures serialize their tagged structure through the same JSON codec, so a `CardDeclined` failure rejects the promise with data any SDK can read, and decodes back to a failure in the awaiter's error channel. Defects reject with the defect encoding. No schemas involved — parity with native behavior.
- **Cancellation**: `handle.cancel` / `client.cancel` settles the promise `rejected_canceled`; the worker observes settlement (its task is force-fulfilled server-side) and interrupts the executing fiber. Awaiting a canceled promise fails with `DurablePromiseCanceled`; timeout with `DurablePromiseTimedOut` — the native SDK throws generic `Error("Promise canceled")`/`Error("Promise timedout")` here; we make the four terminal states distinguishable in the error channel.
- Effect-interrupting a client fiber that is merely _awaiting_ a handle detaches the listener and nothing more — native semantics (awaiting is a `register_listener` long-poll; abandoning it touches nothing durable). Cancellation is always the explicit `cancel` call; there is no `cancelOnInterrupt` option.

### 4.8 Schedules

A schedule is one value constructed from a single named-options struct (the `ClusterCron.make` shape from effect/cluster), carrying both its declarative layer and its imperative operations — nothing is passed twice:

```ts
import { Cron } from "effect";

const NightlyReport = Resonate.schedule({
  id: ScheduleId.make("nightly-report"),
  cron: Cron.unsafeParse("0 3 * * *"), // effect/Cron — parsed and validated, not a raw string
  function: GenerateReport,
  payload: [{ scope: "all" }], // type-checked against GenerateReport's payload schema
  timeout: Duration.hours(2), // optional, per-tick promise timeout relative to the tick
});

// Declarative (the normal case): existence is part of the app topology
const MainLayer = Layer.mergeAll(WorkerLive, NightlyReport.layer);

// Imperative (control flow around it) — same value
yield * NightlyReport.create;
yield * NightlyReport.get;
yield * NightlyReport.delete;
```

Semantics — native parity, exactly:

- Emits `schedule.create` with the native `promiseId` template `{{.id}}.{{.timestamp}}` and the function's invocation tags; the payload is encoded **once**, at creation, into `promiseParam` — the server stamps it verbatim into every tick's promise (the protocol has no per-tick payload templating; per-tick dynamism comes from the `{{.timestamp}}` in the materialized promise id and whatever the function reads at runtime through durable steps).
- `schedule.create` on an existing id returns the stored record and **ignores the request body** (spec + native behavior). We do **not** add drift detection or any other client-side check on top — changing `cron`/`payload` in code does not update an existing schedule, same as native; updating means explicit `delete` + `create` (or a new id). Documented, not mitigated.
- Schedules created at runtime from dynamic data are the same constructor invoked at runtime — `Resonate.schedule({...}).create` — not a second API.
- On the wire the `Cron` value serializes to the five-field expression; **firing semantics belong to the server** (the spec leaves `nextCron` opaque), so input is restricted to the dialect the shipped server accepts (five-field; six-field/seconds rejected until differential tests prove server support). The same `Cron.next` powers the local server's schedule firing (§8), with the differential suite guarding against server disagreement.

### 4.9 Runtime wiring

The worker layer is built from the group (the `RpcServer.layer(group)` shape) and requires every `Handler<Tag>` the group declares — forgetting one is a type error at composition time:

```ts
// Production worker
const MainLayer = Resonate.Worker.layer(AppFns, { group: "payments" }).pipe(
  Layer.provide(HandlersLive),
  Layer.provide(
    Resonate.layerHttp({
      url: Config.String("RESONATE_URL"),
      auth: Config.Option(Config.Redacted("RESONATE_TOKEN")),
    }),
  ),
);
NodeRuntime.runMain(Layer.launch(MainLayer));

// Local development — no server needed; in-memory protocol implementation (native LocalNetwork)
const DevLayer = Resonate.Worker.layer(AppFns, { group: "default" }).pipe(
  Layer.provide(HandlersLive),
  Layer.provide(Resonate.layerLocal),
);
```

At startup the worker assembles the runtime registry (name + version → handler) from the group's `Handler<Tag>` context entries — the analog of the native SDK's rule "register every function before receiving work". One process can serve several groups by composing multiple worker layers; handler dependencies are ordinary `R` requirements satisfied wherever the layers are composed.

Configuration flows through `Config`; `pid` defaults to a generated UUID, `ttl` defaults to 60s with heartbeat at TTL/2 (TS/Rust convention). The native constructor options (`url, group, pid, ttl, auth, timeout`) map one-to-one onto layer config.

### 4.10 Codecs

Every value that crosses the wire — function args, return values, rejection values, external-promise payloads — passes through one codec boundary producing the protocol's `Value = { headers, data }` shape (`data` is an opaque string the server never inspects; `headers` carry format metadata so decode never guesses). The native SDK layers this as a fixed JSON encoder wrapped by a pluggable `Encryptor`; we keep the same two seams, as services:

```ts
/** Value-level encoding: native JsonEncoder-compatible by default. */
export class ResonateCodec extends Context.Service<
  ResonateCodec,
  {
    readonly encode: (value: unknown) => Effect.Effect<Protocol.Value, EncodingError>;
    readonly decode: (value: Protocol.Value) => Effect.Effect<unknown, EncodingError>;
  }
>()("effect-resonate/Codec") {
  /** Default: byte-compatible with the native TS SDK (see rules below). */
  static readonly layerJson: Layer.Layer<ResonateCodec>;
}

/** Byte-level transform applied after encode / before decode (crypto, compression). */
export class ResonateEncryptor extends Context.Service<
  ResonateEncryptor,
  {
    readonly encrypt: (value: Protocol.Value) => Effect.Effect<Protocol.Value, EncodingError>;
    readonly decrypt: (value: Protocol.Value) => Effect.Effect<Protocol.Value, EncodingError>;
  }
>()("effect-resonate/Encryptor") {
  static readonly layerNoop: Layer.Layer<ResonateEncryptor>; // default
}
```

Rules (handbook MUSTs plus native compatibility):

- **One path for everything.** `param` and `value` — including rejections — encode/decode through the same codec. A codec that forgets rejections corrupts failure round-trips.
- **Default codec is native-compatible**: JSON with the TS SDK's conventions — `__type: "error" | "aggregate_error"` markers reconstructing `Error`/`AggregateError` (message/name/stack), `"__INF__"`/`"__NEG_INF__"` Infinity sentinels, `undefined` → empty data — then base64 into `Value.data`. A payload produced by a TS worker decodes here and vice versa.
- **Headers always accompany data.** The default codec sets content metadata; we additionally write a `resonate:schema` header naming the payload schema where one is known (fn payloads, declared promises) — the payload-versioning gap the handbook flags as cheap insurance. Headers are additive; other SDKs ignore them.
- **Schemas sit above the codec, not inside it.** Where a schema exists (fn payload, `Resonate.promise` declarations), Schema encode/decode runs first and the _encoded_ (plain-JSON) form goes through the codec — so custom codecs/encryptors never interact with Schema internals, and schema-less values (returns, string-name calls) take the identical path with no special casing.
- **Swapping the codec is a layer**: provide a different `ResonateCodec`/`ResonateEncryptor` layer (e.g. encryption at rest, msgpack for a closed fleet — the latter breaking TS-worker interop, which is the user's explicit trade to make).
- Encode/decode failures are typed `EncodingError`s tagged with direction and promise id (native: `ENCODING_ARGS_*`/`ENCODING_RETV_*` error codes), never silent.

### 4.11 Testing

```ts
import { it } from "@effect/vitest";
import { ResonateTest } from "effect-resonate/testing";

it.effect("countdown completes after sleeps", () =>
  Effect.gen(function* () {
    const client = yield* ResonateClient;
    const handle = yield* client.beginRpc(Countdown, "t1", [3, 60]);

    yield* TestClock.adjust(Duration.minutes(3)); // drives the local server's timers

    const exit = yield* handle.poll;
    assert(Option.isSome(exit));
  }).pipe(Effect.provide(ResonateTest.layer(AppFns, HandlersLive))),
);
```

`ResonateTest.layer` = local in-memory server + worker + client, with server timeouts/leases/schedules driven by Effect's `TestClock` (the local server's tick is clock-based, so `TestClock.adjust` replaces the native SDK's `debug.tick` plumbing). Crash/replay is testable directly: `ResonateTest.restartWorker` drops all in-process state and forces re-acquire + replay.

---

## 5. Execution Model

### 5.1 Replay

Identical in substance to the native SDK, re-expressed over Effects:

1. Worker receives `execute {taskId, version}` → `task.acquire(id, version, pid, ttl)` → gets root promise + `preload` (settled siblings under the same `resonate:branch`, cached to avoid per-step round-trips).
2. Decode `param.data` (`{func, args, version?, retry?}`), look up the function in the registry (name + `FunctionVersion`; wire `0` decodes to `"latest"`), decode the args through the payload schema (tuple element-per-arg).
3. Run the implementation effect **from the top** inside a fresh fiber with `ResonateContext` provided. Every durable operation (`ctx.run`, `sleep`, `rpc`, `promise`) issues `promise.create` for its counter-derived id (fenced through the owning task via `task.fence`); if the response is already settled, decode and continue instantly — that's replay.
4. Genuinely new local steps: execute the effect (with retry policy), then `task.fence`-wrapped `promise.settle` with the encoded result.
5. On completion of the root effect: encode the result, one atomic `task.fulfill`.

### 5.2 Suspension

When the function blocks on pending remote promises (an `rpc` await, `ctx.promise` await, `sleep`), the runner must issue one atomic `task.suspend` carrying _all_ currently-awaited promise ids, then drop the execution without blocking a thread.

Mechanism: durable awaits don't block the underlying task — each registers its promise id with the execution's **suspension coordinator** and waits on a local `Deferred`. When every live branch of the execution fiber is parked on durable awaits and no local step is still running (the same bookkeeping the native `Coroutine` does with its `localWork`/`remote` sets), the runner:

1. Sends `task.suspend { actions: [register_callback per awaited id] }`.
2. On `200`: interrupts the execution fiber (safe — replay reconstructs), discards in-memory state. The server later sends `execute` (same version — a wake-up hint, not a new fencing token) when a dependency settles, and we re-enter §5.1.
3. On `300 + preload`: at least one dependency already settled — do **not** suspend; feed the preload into the promise cache and settle the corresponding local Deferreds. The fast path is what makes an SDK "fast, not just correct" (handbook).

### 5.3 Structured concurrency

`beginRun`/`beginRpc` handles are _attached_: the runner tracks unsettled attached children and refuses to fulfill the root until they settle (suspending on them if needed) — the native `internal.return` flush. `detached` opts out and re-roots (fresh `resonate:origin`, unchanged `resonate:prefix`, `{prefix}.d{hash}` ids for bounded growth).

### 5.4 Concurrency and the sequence counter

How the native SDK expresses fan-out (handbook, _Composing concurrent durable calls_): **creation is sequential, execution is concurrent.** There is no `Promise.all` analog — you invoke several handles, then await:

```ts
// Native TS
const a = yield ctx.beginRpc("fib", n - 1); // creates promise .0, returns handle
const b = yield ctx.beginRpc("fib", n - 2); // creates promise .1, returns handle
return (yield a) + (yield b); // both already in flight; await both
```

Because every _creation_ is a sequential yield in a single-threaded generator, the sequence counter is deterministic even though the work overlaps. The handbook's guidance for async-runtime languages (Rust): "the structured-concurrency story is the language's, not the SDK's — your job is only to make the durable task type compose with it."

The same idiom in Effect:

```ts
const a = yield * ctx.beginRpc(Fib, [n - 1]); // sequential creation → deterministic ids
const b = yield * ctx.beginRpc(Fib, [n - 2]);
const [x, y] = yield * Effect.all([a.await, b.await], { concurrency: "unbounded" });
```

Handles compose with all of Effect's own combinators (`Effect.all`, `Effect.race`, `Fiber`) — the awaits can be composed freely; only **creation order** must be deterministic.

**The rule** (documented, same class of contract as the native SDK's determinism requirements): durable operations must be _created_ in a deterministic order — in practice, don't put `ctx.run`/`ctx.rpc` creation itself inside racing fibers (`Effect.all([ctx.run(...), ctx.run(...)], { concurrency: 2 })` races the counter). Create sequentially via the `begin*` forms, then await concurrently. The `options.id` escape hatch gives an explicit id where computed creation order is unavoidable.

**Empirical check against the native SDK** (vendored SDK run on bun against `LocalNetwork`): the `Promise.all` analog is _structurally inexpressible_ natively. `ctx.run(...)` returns an inert `LFC` — un-yielded, it creates nothing and executes nothing; all creation happens when the single-threaded coroutine driver processes the yield, so creation cannot race by construction. Yielding `Promise.all([ctx.run(...), ctx.run(...)])` fails the driver's protocol assertion at runtime (`"Unexpected input to extToInt"`) and the execution never completes (the worker re-attempts in a loop). The sanctioned `beginRun` fan-out showed both children starting within 1ms and running overlapped.

**Therefore `ctx.all` ships in v1** — not as sugar but as the Effect equivalent of the structural guarantee the generator model provides natively: it creates the durable promises sequentially in argument order (deterministic ids), then awaits them concurrently:

```ts
const [x, y] = yield * ctx.all([ctx.run(fetchA), ctx.run(fetchB)]);
```

Additionally, the runtime detects durable-op creation from a fiber other than the one the coordinator expects mid-flight and fails with a defect naming the fix (`ctx.all` / `begin*`) — matching native fail-fast behavior, minus its infinite retry loop.

### 5.5 Determinism contract (documented, not runtime-enforced)

Between durable operations, function code must be a pure function of recorded results:

- Time → `ctx.now`. Randomness → `ctx.random`. External reads → `ctx.run`.
- Branching may only depend on payload + recorded results; creation order of durable ops must be deterministic (§5.4).
- Non-durable effects re-run on every replay — fine when pure, a bug when effectful.

No reference SDK detects violations at runtime, and neither do we in v1 — protection is structural. A dev-mode replay-divergence detector is a possible later milestone.

### 5.6 Local steps, leases, long steps

`ctx.run` promises are tagged `resonate:scope: "local"` with no target — recorded but never independently dispatched, per convention. One production hazard inherited from the protocol: a step whose effect runs longer than the lease TTL on a worker that dies mid-step will be re-dispatched and re-executed. Our worker heartbeats real task lists (unlike TS local-mode, where heartbeat is a no-op), but the docs must still warn that a dead worker's partially-run non-idempotent step re-executes — the reason `task.fence` exists and why money-moving steps should be idempotent or fence-checked.

---

## 6. Wire-Level Conformance Decisions

| Concern          | Decision                                                                                                                                                                                                                                                                                                                                           |
| ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Envelope         | `{kind, head: {corrId, version: "2026-04-01"}, data}` on every request; reject corrId mismatches.                                                                                                                                                                                                                                                  |
| Invocation param | `{func, args}` core; tuple payload maps element-per-arg (`args: [5, 60]`), non-tuple payload maps to `args: [value]`; read/write TS-compatible `version` and `retry`. Unknown extensions ignored, preserved.                                                                                                                                       |
| Tags             | Emit `resonate:origin/parent/branch/prefix/scope/target/timer` exactly as the TS SDK does; user tags may not override `resonate:*` keys (divergence: the TS SDK lets them — we treat that as a footgun and fail fast).                                                                                                                             |
| Timeouts         | Absolute epoch-ms; child `timeoutAt` clamped to parent's; default invocation timeout 24h; `detached` unclamped.                                                                                                                                                                                                                                    |
| Value encoding   | `{headers, data}`; data = base64(JSON) with the native `__type` markers for `Error`/`AggregateError` and Infinity sentinels — byte-compatible with TS workers. We additionally set a `resonate:schema` header naming the payload schema where known (headers are additive; other SDKs ignore them — closes the versioning gap the handbook flags). |
| Idempotency      | By promise id, per the spec. `ikc`/`iku`/`strict` are not in the Lean model and are ignored (resolved in review).                                                                                                                                                                                                                                  |
| Suspend          | Atomic multi-callback form (TS/Rust canonical). Handle `300 + preload`.                                                                                                                                                                                                                                                                            |
| Heartbeat        | Per-process, TTL/2, real `(id, version)` list.                                                                                                                                                                                                                                                                                                     |
| Version fencing  | Every mutating task op carries the version; `409` surfaces as typed `TaskFenced` and stops the worker's claim (never blind-retried).                                                                                                                                                                                                               |
| Sleep            | `resonate:timer: "true"`, `timeoutAt` = wake time, no target.                                                                                                                                                                                                                                                                                      |
| Auth failures    | `401/403` are terminal `TransportError` reasons, never retried (fixing the official SDK's retriable-auth-failure gap).                                                                                                                                                                                                                             |

---

## 7. Divergences from the Native SDK (all deliberate, all minimal)

1. **`ResonateContext` service instead of a `Context` first argument** — carried in the Effect `R` channel; operations otherwise keep native names and semantics (`run`, `beginRun`, `rpc`, `beginRpc`, `detached`, `sleep`, `promise`).
2. **Definition/implementation split with group-based registration** (`Resonate.function` + `Resonate.group` + `group.toLayer(handlers)`, the `effect/rpc` `RpcGroup` pattern) instead of `resonate.register(fn)` — clients import definitions without implementations; the worker layer is built from the group and requires every handler at the type level, turning the native runtime error `REGISTRY_FUNCTION_NOT_REGISTERED` into a compile error.
3. **Payload schema** typing the native positional args (tuple → element-per-arg) — the one serialization boundary made explicit. Return values/errors stay codec-round-tripped like native: no schemas, no runtime decoding, types by inference (`Return<F>` analog).
4. **Typed errors** instead of thrown exceptions; the four terminal promise states are distinguishable in the error channel instead of generic `Error` messages.
5. **Layers instead of `setDependency`**.
6. **`Duration`/`DateTime`/`Config`/`Stream`** replace raw ms numbers, `Date`, constructor options, and EventSource callbacks.
7. **Schema-declared external promises** (`Resonate.promise`) instead of native `ctx.promise<T>()`'s manual type annotation — externally settled values cross a trust boundary, so they are declared once and decoded on await; both settle and await sides are typed from the declaration with no casts (§4.5).
8. **Fixed known reference-SDK gaps**: real heartbeat payloads; non-retriable auth errors; schema-version header reserved; system tags protected from user override.

Not diverging on: the replay model, sequence-counter ids, the invoke/await fan-out idiom, suspension protocol, fencing discipline, tag vocabulary, wire encodings, timeout clamping, retry-policy vocabulary and defaults, structured-concurrency-before-fulfill.

---

## 8. The Local Server (dev mode + conformance oracle)

`NetworkLocal` implements the Lean abstract machine directly: the five promise states, five task states, projection semantics, settlement cascade (fulfill → callbacks → listeners, in the three-phase order the reference implementation documents), retry/lease/schedule timeouts, outbox coalescing (latest `execute` per task; one `unblock` per (promise, address)), and schedule catch-up. It is written against Effect's `Clock`, so `TestClock` drives all time-dependent behavior in tests, and a 1s tick drives it in dev mode.

It doubles as the **conformance oracle**: the seven task invariants from the spec's task model are implemented as an `assertInvariants` check that tests run after every operation:

1. Every task has a corresponding promise.
2. Every pending task has a retry timeout.
3. Every acquired task has a lease.
4. Every suspended task has ≥1 registered callback.
5. No suspended task has an already-consumed callback.
6. No suspended task has a timeout.
7. No fulfilled task has a timeout.

Where the spec model and shipped-server behavior are known to differ (version bump on next-acquire, not on lease-lapse; `task.create` target-tag validation), we follow the **shipped server** and record each case in a `SPEC_DEVIATIONS` list in the test suite for differential verification against a live server.

---

## 9. Open Questions

None remaining. Resolved from review: cancellation-on-interrupt (no option — native semantics, see §4.7); `ctx.now`/`ctx.random` round-trip cost (accepted, native parity); context service name (`ResonateContext`); `ikc`/`iku`/`strict` (ignored — not modeled); `ctx.all` (ships in v1 as the structural-determinism guardrail, see §5.4); registration (`effect/rpc` `RpcGroup` pattern with PascalCase function names, see §4.1/§4.9); **differential testing against the shipped Resonate server from day one** (alongside the local oracle, see §8).

---

## 10. Phase 2 Placeholder

After design approval this document gains: protocol-compliance matrix (spec action → SDK behavior → test), test-harness plan, implementation milestones, progress tracker, and the agent work loop. **Do not begin implementation before the design above is approved.**
