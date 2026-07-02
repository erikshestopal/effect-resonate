# 04 — Local Server: Promise State Machine (`NetworkLocal.ts`)

## Objective

The in-memory server's promise half: P-01…P-05 handlers, timeout projection,
`onPromiseTimeout`, settlement cascade skeleton, outbox. Written against Effect's
`Clock` so `TestClock` drives all time. This is both dev mode and the conformance oracle.

## Dependencies

3.

## References

- `repos/resonate-specification/spec/02-actions/P-01…P-05` and `02-timeouts.lean` —
  the authoritative handler semantics; implement them 1:1
- `repos/resonate-sdk-ts/src/network/local.ts` — the annotated reference implementation
  ("spec transition #N" comments); follow it where the Lean model is silent
- `docs/DESIGN.md` §8

## Key facts (from the Lean spec — implement exactly)

- **Projection everywhere**: any read/mutate of a pending promise past `timeoutAt`
  observes it settled (`resolved` if timer tag else `rejected_timedout`,
  `settledAt = timeoutAt` — backdated) without necessarily persisting.
- **promise.create**, new id: `timeoutAt > now` → pending; arm promise timeout **only if
  external** (`resonate:target` present OR timer). With `resonate:target`: companion task
  `pending` version 0 + dispatch (immediate `execute` + retry timeout at `now+retryTimeout`;
  `resonate:delay` in the future defers to a retry timeout at the delay instead). If
  `timeoutAt <= now`: promise born already settled (backdated createdAt/settledAt);
  companion task born `fulfilled` version 0; no timeout, no dispatch.
- **promise.create**, existing id: return stored record (projected), request body
  **completely ignored**, always 200. No strict mode, no comparison.
- **promise.settle**: pending & fresh → apply state/value, settledAt=now, clear
  callbacks/listeners, delete promise timeout, force companion task → fulfilled
  (clear pid/ttl/resumes, delete its timeout), run the **settlement scrub** (strip this id
  from every other pending promise's callbacks), fire `unblock` per listener and
  `enqueueResume` per callback. Pending but past timeoutAt → return the PROJECTED
  outcome (caller's requested state ignored), 200. Already settled → idempotent 200.
  NOTE: promise.settle has NO version fencing (that is task.fulfill's job).
- **register_callback**: 404 absent awaited; 422 if awaiter absent or lacks target;
  registers only if awaited pending&fresh AND awaiter pending&fresh (silently skips
  registration when awaiter expired, still 200); projected/settled awaited → return as-is.
- **register_listener**: same shape, no awaiter constraint, address-keyed.
- **onPromiseTimeout**: persist the projection; identical cascade to settle.
- **Outbox coalescing**: one pending `execute` per taskId (latest wins); one `unblock`
  per (promise, address).
- Server config: `retryTimeout` default 5000ms. Native local server uses 30s
  `PENDING_RETRY_TTL` — pick the spec's 5000 default, make it configurable, note it.
- Timeout firing: a background tick driven by `Clock` (1s in dev; `TestClock.adjust`
  in tests). Follow native `debugTick`'s three-phase order (settle expired → force-fulfill
  tasks → fire callbacks/listeners) — phase order matters (suspended→fulfilled direct,
  never suspended→pending→fulfilled).

## Deliverables

- Promise store, timeout store, outbox; handlers for P-01…P-05 (P-06 → 501);
  `onPromiseTimeout`; the tick loop; wired into a `NetworkLocal.layer` implementing
  `ResonateNetwork` (task ops 501 until spec 05).

## Tests (the oracle starts here)

- Every branch above as a scenario test: create/get/settle/timeout races,
  idempotent re-create ignoring body, born-settled creation, timer-vs-plain projection,
  callback-registration edge cases, cascade firing exactly once, outbox coalescing.
- `TestClock`-driven: a promise with a 10s timeout observes projection at 10s via get
  even BEFORE the tick persists it; after the tick, persisted state matches projection.
- Snapshot invariant: projection formula identical across get/create/settle/register paths.

## Acceptance

- `vp run check` green; CONFORMANCE.md P-01…P-05 rows → partial (oracle side),
  onPromiseTimeout → done.
