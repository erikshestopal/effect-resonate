# 09 — Differential Harness (shipped server vs local oracle)

## Objective

Day-one differential testing: the same operation sequences run against BOTH the
local oracle and the shipped Resonate server, states snapshotted and diffed. This
suite is the arbiter whenever the Lean spec, the native SDK, and our oracle disagree.

## Dependencies

8.

## References

- Handbook: `testing-against-the-spec.mdx` (differential testing is its strongest
  recommendation; documents the known traps)
- `docs/plan/CONFORMANCE.md` Deviations table (the traps to verify first)
- Shipped server: `brew install resonatehq/tap/resonate`, `resonate dev`
  (or docker image) — pick whichever runs headless in CI; document the choice

## Key facts

- Known deviations to verify against the REAL server (spec's prose is wrong or
  incomplete on these): version bump on next-acquire (not lease-lapse);
  `task.create` without `resonate:target` NOT rejected; `preload` population.
- Comparison must normalize server-generated noise (timestamps within tolerance,
  ordering of unordered collections).
- Tag the suite so it can run locally/CI with the server available and skip
  gracefully (with a loud marker, never silently green) when it is not.

## Deliverables

- A `differential` test suite: a scenario DSL (sequence of layer-2 ops) executed
  against both networks, with state snapshots (`promise.get`/`task.get`/
  `schedule.get` probes) diffed after each step.
- Scenarios covering: each promise/task/schedule op family, the suspend-300 fast
  path, lease expiry + re-acquire fencing, idempotent re-creates, timer promises,
  schedule catch-up.
- CI wiring (`vp` task) that boots the dev server, runs the suite, tears down.
- Findings recorded in `CONFORMANCE.md` Deviations; oracle updated to match the
  server wherever they differ (follow-the-server rule).

## Tests

The suite IS the tests. Acceptance scenario: the full scenario set passes with
zero diffs, or every diff is either fixed in the oracle or documented as a
deliberate deviation.

## Acceptance

- `vp run check` green (differential suite included or skipped-loudly);
  Deviations table rows verified.

## Notes

- `test/Differential.test.ts` is included in the normal `check:test` glob, so
  `vp run check` exercises the differential gate on every run. In this orb the
  `resonate` CLI is not installed, so the suite prints
  `[DIFFERENTIAL SKIPPED] resonate CLI not found; install it to run shipped-server parity.`
  rather than silently passing.
- When the CLI is installed, the test boots `resonate dev` with `Bun.spawn`, runs the
  same layer-2 scenario against `NetworkLocal.layer` and `NetworkHttp.layer`, then
  tears the process down. The scenario currently covers promise create/get/settle,
  targetless task.create, and schedule create/delete with unique ids.
- Vite+ exposes a focused `vp run differential` task for the harness; the normal
  `check:test` task still runs it as part of the full suite.
