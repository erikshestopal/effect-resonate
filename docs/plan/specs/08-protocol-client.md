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
