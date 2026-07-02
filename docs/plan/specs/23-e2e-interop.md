# 23 — End-to-End + Cross-SDK Interop (integration gate)

## Objective

Prove the whole SDK against the shipped server and against the native TS SDK:
quickstart parity, cross-SDK invocation both directions, CLI interop.

## Dependencies

09, 16, 17, 20.

## References

- `repos/resonate-sdk-ts/README.md` quickstart (countdown) — the parity target
- `docs/DESIGN.md` §6 (wire-conformance table — every row must be exercised E2E)
- Native SDK runnable from the vendored source (bun) — see the session experiment
  pattern: `bun` + `repos/resonate-sdk-ts/src` + `eventsource`/`cron-parser` deps

## Scenarios

1. **Quickstart parity**: our countdown (DESIGN.md §4.1) against `resonate dev`;
   invoke via `resonate invoke countdown.1 --func Countdown --arg 5 --arg 60`;
   kill worker mid-countdown, restart, completes exactly like native's README demo.
   `resonate tree` shows the same promise-graph shape as the native countdown.
2. **We call native**: our `ctx.rpc("nativeFn", [args])` dispatched to a native TS
   worker in another process/group → result decodes correctly (incl. a thrown Error).
3. **Native calls us**: native `resonate.rpc` invoking our registered function by
   name → our worker claims, executes, native caller receives the value.
4. **External promise interop**: native SDK resolves a promise our function awaits;
   our client resolves one a native function awaits.
5. **Sleep/timer + schedule** against the shipped server (not just the oracle).
6. **Fencing E2E** on the shipped server: two of our workers, forced lease expiry,
   exactly one fulfill wins.

## Deliverables

- E2E suite (server-gated like spec 09) + a runnable `examples/` directory
  (countdown, approval flow, fan-out) that doubles as documentation.
- Final pass over `docs/plan/CONFORMANCE.md`: every row `done` or explicitly
  deferred with a reason. This spec is the completion gate for the plan.

## Acceptance

- `vp run check` green; all six scenario groups pass against the shipped server;
  CONFORMANCE.md complete.
