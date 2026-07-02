# 01 — Protocol Domain Model (`Protocol.ts`, `Errors.ts`)

## Objective

Schema-first wire and domain types for the whole protocol: envelope, records,
requests/responses, push messages, tags, addresses, branded ids, error taxonomy.
This is the strict domain model of DESIGN.md §3.2 — the foundation everything
else type-checks against.

## Dependencies

0.

## References

- `docs/DESIGN.md` §3.2 (verbatim — the branded ids, `Tags`, `TargetAddress`,
  state-discriminated unions, no-magic-values list), §4.7 (error taxonomy), §6 (wire table)
- `repos/resonate-specification/spec/01-objects/types.lean` and `state.lean` — field
  names/enums are authoritative (`pending|resolved|rejected|rejected_canceled|rejected_timedout`;
  task `pending|acquired|suspended|halted|fulfilled`)
- `repos/resonate-sdk-ts/src/network/types.ts` — exact wire shapes for every request/response
  kind and the `execute`/`unblock` messages; head `{corrId, version:"2026-04-01", auth?}`,
  response head carries `status`
- Handbook: `talking-to-the-server.mdx`, `protocol-at-a-glance.mdx`

## Key facts to encode

- Branded ids: `PromiseId`, `ExecutionId`, `ScheduleId`, `WorkerGroup`, `ProcessId`,
  `CorrelationId`; `TaskId = PromiseId`. Shallow validation (non-empty) only.
- `Tags`: typed reserved keys (`resonate:timer` → `Literal("true")`; `resonate:scope` →
  `"local"|"global"`; `resonate:target` → `TargetAddress`; lineage tags → `PromiseId`;
  `resonate:delay` → epoch-ms-from-string) + `UserTagKey` (filtered: not `resonate:`-prefixed).
  Transformation schema to/from the flat wire record. **Lenient decode**: unknown reserved
  values preserved raw, never failing the record.
- `TargetAddress`: `{transport: "poll"|"local", cast: "uni"|"any", group, id: Option<ProcessId>}`
  ⇄ `poll://any@group[/id]` string.
- `PromiseRecord = PromisePending | PromiseSettled` (settledAt required iff settled);
  `TaskRecord = TaskPending | TaskAcquired | TaskSuspended | TaskHalted | TaskFulfilled`
  (pid+ttl required iff acquired). `projected(now)` on a pending promise returns
  `PromiseSettled` (`resolved` if timer tag else `rejected_timedout`, settledAt = timeoutAt).
- Timestamps `DateTime.Utc` ⇄ epoch-ms; ttl `Duration` ⇄ ms; wire task version is a
  branded non-negative `TaskVersion`; function version branded positive `FunctionVersion`
  with `"latest"` ⇄ wire `0`.
- Envelope status: `Literals([200,300,400,401,403,404,409,422,429,500,501])`.
- Request/response pairs for all kinds listed in DESIGN.md §2 with a type-level
  `Request<K>` → `Response<K>` mapping.
- Error taxonomy (`Errors.ts`): `TransportError` (reasons: ConnectionLost, MalformedResponse,
  CorrelationMismatch, Unauthorized), protocol errors (`TaskFenced` 409, `PromiseNotFound` 404,
  `InvalidTarget` 422), terminal-outcome errors (`DurablePromiseTimedOut`, `DurablePromiseCanceled`),
  `EncodingError` — all `Schema.TaggedErrorClass`, ids branded.

## Deliverables

- `Protocol.ts` and `Errors.ts` fully implemented; exported from the package entry.

## Tests

- Round-trip decode/encode fixtures for each record and request/response kind,
  including fixtures captured verbatim from `resonate-sdk-ts` wire shapes.
- Tags: reserved/user split round-trips; `resonate:`-prefixed user key rejected at
  construct; junk reserved value survives decode (leniency).
- Union discrimination: an acquired task without pid/ttl fails construct; a settled
  promise without settledAt fails construct; both decode leniently if the server sends them.
- `projected(now)` matches the Lean projection rule for timer and non-timer promises.

## Acceptance

- `vp run check` green; CONFORMANCE.md rows for tag vocabulary marked partial (types exist).

## Notes (implementation decisions)

- **Strict/lenient split**: `PromiseSettled.settledAt` and `TaskAcquired.pid/ttl`
  are `Option`s at the field level; the strict `PromiseRecord`/`TaskRecord`
  schemas add a structural `.check(...)` requiring them (`settledAt` on settled,
  lease on acquired), while the `*FromWire` schemas are the same unions without
  the check — so wire decode never rejects a server-accepted record, and strict
  construct fails exactly as the spec's tests demand. Union membership matches
  DESIGN §3.2 exactly (no extra "raw" members).
- **Tags** carry a third field `unrecognized: Record<string,string>` (beyond
  DESIGN's `reserved`/`user` sketch): `resonate:`-prefixed entries whose key is
  unknown or whose value fails its typed domain are preserved raw there and
  re-emitted verbatim on encode — this is the mechanism satisfying "junk reserved
  value survives decode". "User tags can't override system tags" lives in the
  type (`UserTagKey` brand), per DESIGN; wire decode routes such keys to
  `unrecognized`, never `user`.
- **Response error variants** are modeled per kind as `status ∈ {400..501}` with
  string `data` (one generic error envelope) rather than enumerating the native
  per-kind status subsets — more lenient, identical wire behavior; protocol
  errors map by status in spec 08. `task.suspend` keeps its dedicated `300 +
preload` success variant; `task.create`'s optional `task`, `task.fence`'s
  nested response action, and the `debug.*` shapes follow native exactly.
- Search (`promise/task/schedule.search`) and `debug.*` kinds are included
  (needed by specs 09/22) even though DESIGN §2 lists only the core 18.
