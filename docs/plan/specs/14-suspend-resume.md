# 14 — Suspend / Resume (suspension coordinator)

## Objective

Never block a thread on a durable wait: when the execution blocks only on pending
remote promises, issue one atomic `task.suspend`, drop the execution, and resume by
replay on the next `execute`. Handle the `300 + preload` fast path.

## Dependencies

13.

## References

- `docs/DESIGN.md` §5.2 (binding)
- `repos/resonate-sdk-ts/src/coroutine.ts` (`flushLocalWork`, Suspended aggregation),
  `src/core.ts` (`suspendTask`, 300 handling)
- Handbook: `suspend-resume-settlement.mdx`

## Key facts

- Durable awaits park on a local Deferred registered with the execution's
  **suspension coordinator** (awaited PromiseId + Deferred). The engine suspends when:
  every live branch is parked on durable awaits AND no local step fiber is running
  AND local work has been flushed (nested local children that themselves suspended
  contribute their awaited ids — native `flushLocalWork`).
- Suspend = `task.suspend { id, version, actions: [register_callback(awaited: X,
awaiter: taskId) per awaited id] }` — ONE atomic round trip for ALL awaited ids.
- `200` → interrupt the execution fiber (replay reconstructs everything), clear the
  held-task entry (suspended tasks hold no lease — stop heartbeating it).
- `300 + preload` (SuspendRefused) → do NOT suspend: seed the preload into the
  promise cache, settle the matching parked Deferreds, continue executing. Loop —
  a second suspend attempt may also 300.
- Resume: the server's `execute` (same version — wake-up hint) arrives → normal
  worker path (spec 13) re-acquires? NO — the task is `pending` after resume cascade;
  worker does `task.acquire` presenting the version FROM THE MESSAGE, getting a
  bumped version + preload → replay from the top (spec 12); previously-settled steps
  dedup instantly; the formerly-awaited promise is now settled so the await passes.
- Exactly-once resumption is structural (server clears callbacks atomically with
  settlement) — do NOT add client-side dedup on resume messages.

## Deliverables

- Suspension coordinator wired into engine awaits (`handle.await` on pending remote
  promises, later reused by sleep/rpc/external promises); Suspended outcome path in
  the worker; 300 fast-path loop.

## Tests (against local oracle)

- Function awaits an unsettled external promise → task suspends (oracle: state
  suspended, callbacks registered, no lease, invariants hold); settle it → execute
  arrives → replay → completes. Assert the function body ran twice (replay) but
  each durable step's effect ran once.
- Multi-await (two pending promises via ctx.all/begin\*) → ONE suspend with two
  actions; settling one → resume → re-suspend on the remaining one.
- Fast path: promise settles between check and suspend → 300 → no suspension,
  execution continues with preload-fed value; wire shows no second create/get for
  preloaded siblings.
- Suspend-resume across "process death": kill worker after suspend, new worker
  (fresh engine) handles the resume execute.

## Acceptance

- `vp run check` green; CONFORMANCE.md T-06, resume cascade (worker side),
  never-block + 300-fast-path rows → done.
