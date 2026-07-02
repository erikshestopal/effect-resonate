# 08 — Protocol Client (`DurablePromise.ts`, `Task.ts`, `Schedule.ts`)

## Objective

Typed layer-2 operation services over `ResonateNetwork`: `DurablePromises`, `Tasks`,
`Schedules` — every protocol action as a typed Effect, protocol statuses mapped to
typed outcomes/errors.

## Dependencies

03 (interface); 04+05 (tests run against the local oracle).

## References

- `docs/DESIGN.md` §3.2 (service sketches)
- `repos/resonate-sdk-ts/src/promises.ts`, `src/schedules.ts`, task ops in
  `src/resonate.ts`/`src/core.ts`

## Key facts

- Status mapping: 404 → `PromiseNotFound`, 409 → `TaskFenced` (or op-appropriate
  conflict error), 422 → `InvalidTarget`, 300 → a TYPED success variant of
  `Tasks.suspend` (`SuspendRefused { preload }`), not an error. 501 → defect
  (unimplemented ops are not called).
- `DurablePromises.awaitSettled(id)`: `promise.register_listener` (address =
  network.unicast) + wait for the matching `unblock` on the message stream, returning
  the settled record; re-registers on transport failure (native: 5s constant backoff,
  infinite — replicate); handles already-settled immediate response. Also the
  60s listener-refresh behavior from native (`resonate.ts` subscription refresh interval).
- All requests/responses pass through the Schema types of spec 01; ids branded.
- `TaskFenced`/409 is a STOP signal for the caller — document on the method; nothing
  in this layer retries a 409.

## Deliverables

- Three services with static layers depending only on `ResonateNetwork` (+ Clock).
- Public export — this layer is a supported API for power users.

## Tests

- Against `NetworkLocal` (oracle): every operation happy path + every mapped error
  status, asserting the seven invariants after each op (helper from spec 05).
- `awaitSettled`: pending → settle from another fiber → resolves via unblock;
  already-settled → immediate; transport blip → re-register and still resolve.
- Suspend 300 surfaces as `SuspendRefused` with decoded preload records.

## Acceptance

- `vp run check` green; CONFORMANCE.md P/T/S rows → done (client side); 409 rule row → done.

## Notes

- `DurablePromises`, `Tasks`, and `Schedules` are public `Context.Service` layers
  that capture `ResonateNetwork` and `Crypto` once, then expose schema-typed request
  methods with no environment requirement at call sites.
- Request construction stays on the spec-01 schemas (`*.make`) and all responses are
  narrowed with `SchemaParser.is(responseSchema.members[n])`; no hand-written wire
  guards are used.
- Protocol statuses are mapped at this layer: `404` to `PromiseNotFound` or
  `ScheduleNotFound`, `409` to `TaskFenced`, `422`/other protocol errors to
  `InvalidTarget`. A `409` is surfaced directly and never retried by the client.
- `Tasks.suspend` models the `300` preload path as `SuspendRefused`, a typed success
  variant rather than an error, matching the local/server fast path.
- `DurablePromises.awaitSettled` registers `network.unicast`, returns immediately for
  already-settled listener responses, otherwise waits for the matching `unblock`
  message. Message stream failures and the native 60s listener-refresh interval both
  lead back through registration before waiting again.
- Spec-08 tests use `NetworkLocal.layer` plus `BunCrypto.layer`; after each mutating
  operation they request `debug.snap` and run the seven-invariant `assertInvariants`
  oracle from spec 05.
