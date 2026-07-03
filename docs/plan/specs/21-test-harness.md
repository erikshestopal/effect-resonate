# 21 — Public Test Harness (`testing.ts` — `ResonateTest`)

## Objective

The user-facing testing story: one layer bundling local server + worker + client
driven by `TestClock`, plus crash/replay ergonomics.

## Dependencies

13, 14.

## Translation map (native → Effect)

| Native source                                | Symbol                                 | Becomes                                                |
| -------------------------------------------- | -------------------------------------- | ------------------------------------------------------ |
| `repos/resonate-sdk-ts/src/network/local.ts` | `LocalNetwork` + `debug.tick` interval | `NetworkLocal` driven by `Clock`/`TestClock` (spec 04) |
| `repos/resonate-sdk-ts` `sim/` debug ops     | `debug.start/reset/tick/snap`          | `ResonateTest` helpers (snapshot, restartWorker)       |

## References

- `docs/DESIGN.md` §4.11 (binding); effect-smol `ai-docs/src/09_testing`
- effect/cluster `TestRunner.layer` for the shape precedent

## Key facts

- `ResonateTest.layer(group, handlersLayer)`: local server + worker(group) + client,
  all in-process; timeouts/leases/schedules advance with `TestClock.adjust` (no
  wall-clock waits, no debug_time plumbing).
- `ResonateTest.restartWorker`: tears down the worker layer scope (dropping ALL
  in-process execution state, releasing nothing — simulating a crash, not a graceful
  stop) and brings up a fresh worker → forces lease-lapse/re-acquire/replay paths.
- `ResonateTest.snapshot`: read-only server state view for assertions +
  `assertInvariants` re-export (spec 05).

## Deliverables

- `effect-resonate/testing` subpath exports: layer, restartWorker, snapshot,
  assertInvariants.

## Tests

- The DESIGN.md §4.11 example verbatim compiles and passes (countdown + TestClock).
- Crash/replay: start slow function, restartWorker mid-flight, TestClock past lease
  → redispatch → replay completes with exactly-once step effects.
- Docs: testing section in the README written against this API.

## Acceptance

- `vp run check` green.

## Implementation notes

- Done in spec 21: `src/testing.ts` now exports the `ResonateTest` service with `ResonateTest.layer(group, handlersLayer)`, plus top-level `snapshot`, `restartWorker`, and the existing `assertInvariants`. The layer composes the in-memory local server, codec/encryption, protocol clients, public client, engine, handlers, and a restartable worker scope. Tests cover the DESIGN countdown example and worker restart replay with a recorded local step executing exactly once.
