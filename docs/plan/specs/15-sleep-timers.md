# 15 — Sleep / Timers (`ctx.sleep`, `ctx.sleepUntil`)

## Objective

Durable sleep as `resonate:timer` promises — free to replay, suspension-friendly.

## Dependencies

12, 14.

## References

- `docs/DESIGN.md` §4.2
- `repos/resonate-sdk-ts/src/context.ts` (sleep), local server `timeoutState`
- Handbook: `time-retries-policies.mdx`

## Key facts

- `ctx.sleep(duration)` / `sleepUntil(instant)`: create a promise with seq id, tags
  `resonate:timer: "true"` + lineage + `resonate:scope: "global"`, NO target,
  `timeoutAt = wake time` (clamped to parent deadline — a sleep never outlives its
  parent). The server RESOLVES (not rejects) timer promises at timeoutAt.
- Await it like any pending remote promise → suspension coordinator (spec 14);
  a due sleep on replay is already-settled → returns instantly.
- Duration inputs via `Duration`/`DateTime.Utc`; wake time computed from `ctx.now`
  semantics? NO — from Clock at creation; the promise id + stored timeoutAt make it
  replay-stable (the record, not recomputation, is authoritative on replay).

## Deliverables

- `ctx.sleep`/`ctx.sleepUntil` on `ResonateContext`.

## Tests (local oracle + TestClock)

- Sleep 1h: task suspends; TestClock +1h → timer promise resolves (not rejects) →
  resume → completes. Oracle asserts the timer projection rule.
- Replay after wake: week-long sleep replays instantly (no new promise, no wait).
- Clamping: child sleep beyond parent timeoutAt clamps; parent timeout fires first
  and the sleep's promise settles per its own record.
- Wire fixture: tags/timeoutAt byte-match native sleep creation.

## Acceptance

- `vp run check` green; CONFORMANCE.md sleep row → done.

## Notes

- Implemented in `ResonateContext.sleep`/`sleepUntil`; timer promises use `resonate:branch` equal to the timer id, matching the native SDK.
- Replay after wake is driven by idempotent `promise.create` for the same timer id returning the already-resolved timer record.
