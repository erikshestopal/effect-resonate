# 13 — Worker Runtime (`Worker.ts`)

## Objective

`Resonate.Worker.layer(group, config)`: consume `execute` messages, acquire tasks,
decode invocations, drive the engine, fulfill/release — plus the per-process
heartbeat with REAL task lists.

## Dependencies

05 (oracle for tests), 12.

## References

- `docs/DESIGN.md` §3.3, §4.9, §5.1
- `repos/resonate-sdk-ts/src/core.ts` (`executeUntilBlocked`, `onMessage`,
  fulfill/release paths), `src/heartbeat.ts` (including its empty-tasks bug — do NOT
  replicate)
- Handbook: `tasks-and-the-worker-loop.mdx`, `production-concerns.mdx`

## Key facts

- Message loop: for each `execute {taskId, version}` → `task.acquire(id, version,
pid, ttl)`; 409 → someone else owns it, drop silently (stale wake-up hint is
  normal); success → seed preload cache, decode `param.data` ({func, args, version},
  registry lookup — unregistered function: log the native REGISTRY_FUNCTION_NOT_REGISTERED
  equivalent and DROP the task ("Will drop"), never crash the worker), schema-decode
  args, run engine.
- Engine Done(exit) → single atomic `task.fulfill` embedding promise.settle with the
  codec-encoded result (resolved/rejected). Engine failure (platform error, defect) →
  `task.release` (another worker/attempt takes over) and log; never leave a task
  acquired on abandon.
- **Heartbeat**: ONE fiber per process, interval = min held ttl / 2 (default 60s ttl
  → 30s), sending `task.heartbeat { pid, tasks: [ACTUAL held (id, version) pairs] }`.
  Held-set updated on acquire/fulfill/release/suspend. (Native sends `[]` — a known
  gap we fix; the FIX is wire-compatible since the server iterates the list.)
- Config: `{ group, pid?: generated UUID, ttl?: 60s }` via Config; worker registers
  the group's functions into the runtime registry at layer build (before consuming
  messages — handbook MUST).
- Concurrency: multiple tasks execute concurrently (each its own fiber, supervised by
  the layer scope); graceful shutdown releases all held tasks.

## Deliverables

- `Resonate.Worker.layer(group, config)` per DESIGN.md §4.9; internal held-task
  registry; heartbeat fiber; supervised execution fibers.

## Tests (against local oracle)

- End-to-end: client.rpc → execute message → acquire → run function → fulfill →
  client handle resolves. Root/leaf tag correctness on all created promises.
- Stale execute (already re-acquired elsewhere) → 409 → dropped, no crash, no retry.
- Unregistered function name → task dropped + logged, worker continues.
- Heartbeat: TestClock advance → heartbeat request carries exactly the held
  (id, version) pairs; after fulfill the pair disappears; lease never expires while
  heartbeating; killing the heartbeat → lease lapses → oracle redispatches → second
  worker acquires at bumped version → first worker's fulfill 409s (fencing E2E).
- Graceful shutdown: scope close releases held tasks (oracle shows pending + redispatch).

## Acceptance

- `vp run check` green; CONFORMANCE.md heartbeat + T-03/T-07/T-08 (worker side) +
  register-before-work rows → done.

## Notes

- Implemented `Worker.layer(group, config)` as a scoped background layer: it builds the function registry before consuming messages, starts a process heartbeat fiber, and consumes `ResonateNetwork.messages` with one supervised fiber per layer scope.
- `execute` messages call `Tasks.acquire`; `TaskFenced` stale wakeups are dropped, acquired tasks are added to the held set, then `ExecutionEngine.execute` drives the function. Engine failures release the task and remove it from the held set; successful engine completion removes it after root fulfillment.
- Heartbeat sends the actual current held task list (`[{ id, version }]`) every `ttl / 2`, intentionally fixing the native SDK's empty-list bug. `test/Worker.test.ts` advances `TestClock` past the original lease and proves the task remains acquired only because the heartbeat extended the real held pair.
- Local `NetworkLocal.messages` is a single queue in tests, so the worker E2E test polls the root promise through `DurablePromises.get` instead of racing `handle.await`'s listener consumer against the worker's execute consumer.
