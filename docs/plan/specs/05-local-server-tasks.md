# 05 — Local Server: Task State Machine + Invariant Oracle (`NetworkLocal.ts`)

## Objective

The task half of the local server: T-01…T-10 handlers, the resume cascade,
lease/retry timeouts — plus the **seven-invariant oracle** (`assertInvariants`)
that all conformance tests call after every operation.

## Dependencies

4.

## References

- `repos/resonate-specification/spec/02-actions/T-01…T-10`, `00-resume.lean`,
  `02-timeouts.lean` (task timeouts)
- `repos/resonate-sdk-ts/src/network/local.ts` (`triggerSettlement` phases,
  task handlers)
- `docs/DESIGN.md` §8 (the seven invariants)

## Key facts (implement exactly)

- Task id == promise id, always. Task timeouts keyed `(id, kind)`: kind 0 = pending
  retry, kind 1 = lease.
- **task.acquire**: 404 absent; 409 if not pending / promise not fresh / **version
  mismatch**. Success: → acquired, `version+1` (the ONLY place version bumps, plus
  task.create's re-acquire), set pid/ttl, clear resumes, replace retry timeout with
  lease timeout at now+ttl.
- **task.create**: promise absent + fresh → promise pending (timeout armed
  unconditionally here) + task born **acquired** version 1 + lease. Promise absent +
  expired → both born dead (task fulfilled v0). Promise exists: 422 without target;
  task fulfilled → idempotent 200; task pending → re-acquire (version+1); task in any
  other state → 409; target-tagged promise with no task → 409.
- **task.fence**: gate (acquired + promise fresh + version match) then delegate to the
  inner promise.create/settle handler, wrapping the response.
- **task.heartbeat**: bulk; per-task silent skip unless acquired + version AND pid match
  - promise fresh; extends by the task's STORED ttl; blanket 200 always.
- **task.suspend**: gate as fence. Two-phase: check ALL awaited first (404→422 abort if
  any awaited missing; note any settled), register NOTHING if any settled → clear
  resumes, stay acquired, return **300**. All pending → register each callback, task →
  suspended with pid/ttl cleared, resumes `[]`, lease timeout deleted (suspended tasks
  hold no lease), 200.
- **task.fulfill**: gate as fence, then the full settlement sequence inline (like
  promise.settle's success path) + task → fulfilled.
- **task.release**: gate; → pending, pid/ttl cleared (**resumes NOT cleared**), lease
  timeout deleted, retry timeout armed, `execute` redispatched.
- **task.halt**: NO version fence; 409 if fulfilled; idempotent 200 if halted; else →
  halted, pid/ttl cleared, lease timeout deleted.
- **task.continue**: 409 unless halted; → pending + retry timeout + execute; NO version bump.
- **resume cascade** (`enqueueResume`): awaiter suspended → pending, `resumes=[awaitedId]`
  (replacing), retry timeout, `execute` with **current version** (wake-up hint, not a new
  fencing token); awaiter pending/acquired/halted → append to resumes (dedup), nothing else;
  fulfilled/missing → no-op.
- **onTaskRetryTimeout**: only if still pending; re-arm self at now+retryTimeout and
  re-send `execute` (at-least-once redelivery loop).
- **onTaskLeaseTimeout**: only if acquired; → pending, pid/ttl cleared, retry timeout,
  execute. **No version bump on lease expiry** (shipped-server behavior: bump happens on
  next acquire).

## Deliverables

- All task handlers (T-11 → 501), resume cascade, task timeout transitions wired into
  the tick; `assertInvariants(state)` exported from `testing.ts` checking the seven
  invariants; a test helper that snapshots server state after each op and asserts them.

## Tests

- Scenario per handler branch above, asserting invariants after EVERY op.
- The fencing walkthrough: A acquires v3 → lease lapses (still v3) → B acquires (v4)
  → A's fulfill at v3 → 409; B's fulfill succeeds.
- Suspend racing settlement: settle between check and suspend → 300, task stays acquired.
- Resume buffering: settle an awaited promise while awaiter is acquired → resumes
  buffered, no execute; then suspend returns 300 (already-settled dependency).
- Redelivery loop: unacquired pending task re-sends execute every retryTimeout tick;
  coalesced in outbox.

## Acceptance

- `vp run check` green; CONFORMANCE.md T-rows + resume + task timeouts + all seven
  invariant rows → done (oracle side).

## Notes (implementation decisions)

- Task handlers are pure `NetworkLocal` transitions over the same immutable
  `ServerState` as spec 04. T-01…T-10 are implemented; T-11 remains 501.
- `task.create` intentionally allows a fresh action without `resonate:target` to
  match the shipped server deviation. Existing targetless promises still return
  422 on task.create, matching the spec branch for already-existing promises.
- Version bumps happen only on acquire/re-acquire. Lease timeout moves acquired →
  pending with the same version; the next `task.acquire` bumps it. The fencing
  walkthrough test locks this shipped-server behavior in before spec 09.
- `preload` is populated for create/acquire/suspend-300/fence from settled or
  pending siblings sharing `resonate:branch`, matching the native/shipped-server
  behavior rather than the Lean model's empty preload.
- `assertInvariants(state)` is exported from `src/testing.ts` over the debug-snap
  state. The local `snap()` test helper calls it automatically, so every test
  snapshot checks the seven invariants after the preceding operation.
