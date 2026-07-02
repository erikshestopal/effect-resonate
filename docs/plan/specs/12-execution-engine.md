# 12 — Execution Engine (replay driver, `ResonateContext` core)

## Objective

The heart of the SDK: drive a registered function's Effect from the top with replay
semantics — sequence-counter child ids, fenced durable steps (`ctx.run`/`beginRun`),
preload cache, structured-concurrency bookkeeping, `ctx.all`, `ctx.panic`. This spec
covers LOCAL steps and the engine skeleton; remote invocation, suspension, sleep,
retries, and nondeterminism ops layer on in specs 13–19.

## Dependencies

02, 08, 10.

## References

- `docs/DESIGN.md` §4.2, §4.3, §5.1, §5.3, §5.4 (binding — especially the ctx.all
  requirement and the empirical findings), §5.6
- `repos/resonate-sdk-ts/src/coroutine.ts`, `src/computation.ts`, `src/decorator.ts`,
  `src/context.ts` — the semantics to replicate over fibers instead of generators
- Handbook: `coroutines.mdx`, `replay-and-determinism.mdx`

## Key facts

- **Ids**: per-execution mutable seq counter; nth durable op = `{parentId}.{n}`
  (0-based, native parity). Explicit `options.id` overrides and breaks lineage
  (resets `resonate:origin` to the new id; `resonate:prefix` NEVER changes).
- **ctx.run** (lfc) = create local promise (tags: `resonate:scope:"local"`, lineage
  tags, NO target; `timeoutAt = min(now+opts.timeout, parent.timeoutAt)`) via
  **task.fence-wrapped** promise.create; if response settled → decode and return
  (replay); else execute the effect, then fence-wrapped promise.settle with the
  encoded result. Errors in the effect settle `rejected` with the codec-encoded error.
- **ctx.beginRun** (lfi) = same create, but returns a `DurableHandle` immediately;
  the effect runs concurrently in a child fiber; parent awaits via `handle.await`.
- **Preload cache**: settled sibling records from task.create/acquire/suspend-300
  responses seed a per-execution promise cache; fence/create/settle consult it before
  hitting the network (native `effects` cache).
- **Structured concurrency**: track attached children (begun-but-unawaited handles +
  in-flight local fibers); the root may not fulfill until all settle. On root effect
  completion, drain local work first (native `internal.return` flush).
- **ctx.all(entries)**: assigns ids by creating the durable promises sequentially in
  argument order, then runs/awaits concurrently — the v1-required guardrail (§5.4).
- **Creation-order guard**: durable-op creation from concurrently racing fibers
  (i.e. not serialized through the engine's creation lock while another creation is
  mid-flight in a different branch) → defect with a message naming `ctx.all`/`begin*`.
  Fail-fast, no infinite retry (native fails with "Unexpected input to extToInt" + loop).
- **ctx.panic / assert**: reject the ROOT promise (native PANIC), abort execution.
- **Dedup on entry**: root promise already settled → return its value, no execution
  (native `processGenerator` short-circuit).
- Engine entry: `(task: TaskAcquired, rootPromise, registry, context) => Effect<Outcome>`
  where Outcome = Done(exit) | Suspended(awaitedIds) — Suspended is realized in spec 14;
  here it may be a stub that only Done paths exercise.

## Deliverables

- Engine module (internal), `ResonateContext` service implementation for: info, run,
  beginRun, all, panic; `DurableHandle` for local children. Options resolution
  (per-call over context defaults).

## Tests (against local oracle; TestNetwork for wire-shape assertions)

- Sequential steps produce `.0`, `.1`, ... ids; wire bytes for create/settle match
  native fixtures (tags, fence wrapping, timeout clamping).
- Crash/replay: run 3 steps, kill engine, re-run from scratch → steps 1–3 replay from
  recorded results (no re-execution — assert side-effect counters), step 4 executes.
- beginRun fan-out: creation order deterministic, execution overlaps, results correct.
- ctx.all: same guarantees through the combinator; `Effect.all` around raw ctx.run
  creations → the guard defect.
- Structured concurrency: root with un-awaited begun child does not fulfill until the
  child settles; local error settles child rejected and propagates per Effect semantics.
- Fencing: engine holding a stale task version gets 409 on step create → typed
  TaskFenced, execution aborts without settling root.

## Acceptance

- `vp run check` green; CONFORMANCE.md rows: deterministic ids, structured
  concurrency, timeout clamping, T-04 (engine side) → done.

## Notes

- Implemented the first Effect-native engine slice in `src/ResonateContext.ts`: `ExecutionEngine` decodes the root invocation param, resolves the registered handler, provides `ResonateContext`, drains local children, and fulfills the root task through `task.fulfill`.
- Implemented local durable operations only: `ctx.run`/`ctx.beginRun` create child promises with deterministic `{root}.{seq}` ids, local lineage tags, parent-timeout clamping, `task.fence`-wrapped `promise.create`/`promise.settle`, and cache/preload replay of settled child records. Remote calls, sleep, suspension, retries, and recorded nondeterminism remain in specs 14–19.
- Registry handler effects may now require `ResonateContext`; direct registry tests provide a dummy context layer. Child effects accepted by `ctx.run`/`beginRun` are service-free in this slice; widening child effect environments should be done deliberately when worker dependency injection lands.
- Tests cover root execution, local child tag/id emission, root fulfillment decode, and replay from a preloaded settled child without re-executing the local side effect.
