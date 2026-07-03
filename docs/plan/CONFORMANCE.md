# Protocol Conformance Matrix

Maps every protocol behavior to the implementing spec and the tests that prove it.
Fill the Tests column as slices land; a row is conformant only when its tests pass
against BOTH the local oracle (specs 04–06) and the shipped server (spec 09).

Statuses: `todo` | `partial` | `done`.

## Promise actions

| Spec action                                                                            | Lean source                             | Impl spec | Tests                                                                                    | Status  |
| -------------------------------------------------------------------------------------- | --------------------------------------- | --------- | ---------------------------------------------------------------------------------------- | ------- |
| P-01 promise.get (incl. timeout projection)                                            | `spec/02-actions/P-01-promise.get.lean` | 04, 08    | `test/NetworkLocal.test.ts` (oracle side); `test/ProtocolClient.test.ts` DurablePromises | partial |
| P-02 promise.create (new / born-settled / idempotent re-create / target→task / delay)  | `P-02-promise.create.lean`              | 04, 08    | `test/NetworkLocal.test.ts` "P-02"; `test/ProtocolClient.test.ts` DurablePromises        | partial |
| P-03 promise.settle (settle / projected-timeout race / idempotent re-settle / cascade) | `P-03-promise.settle.lean`              | 04, 08    | `test/NetworkLocal.test.ts` "P-03"; `test/ProtocolClient.test.ts` awaitSettled           | partial |
| P-04 promise.register_callback (422 non-target awaiter; skip-if-awaiter-expired)       | `P-04-promise.register_callback.lean`   | 04, 08    | `test/NetworkLocal.test.ts` "P-04"; typed client method in `DurablePromises`             | partial |
| P-05 promise.register_listener                                                         | `P-05-promise.register_listener.lean`   | 04, 08    | `test/NetworkLocal.test.ts` "P-05"; `test/ProtocolClient.test.ts` awaitSettled           | partial |
| P-06 promise.search — NOT IMPLEMENTED (501 per spec)                                   | `P-06-promise.search.lean`              | —         | n/a                                                                                      | done    |

## Task actions

| Spec action                                                                           | Lean source                | Impl spec  | Tests                                                                                                                              | Status  |
| ------------------------------------------------------------------------------------- | -------------------------- | ---------- | ---------------------------------------------------------------------------------------------------------------------------------- | ------- |
| T-01 task.get (projection to fulfilled)                                               | `T-01-task.get.lean`       | 05, 08     | `test/NetworkLocal.test.ts` "T-01…T-10"; typed client method in `Tasks`                                                            | partial |
| T-02 task.create (create+acquire / born-fulfilled / re-acquire / 422 no-target / 409) | `T-02-task.create.lean`    | 05, 08, 11 | `test/NetworkLocal.test.ts` "T-01…T-10"; `test/ProtocolClient.test.ts` Tasks; `test/ResonateClient.test.ts` beginRun root create   | partial |
| T-03 task.acquire (version fence, bump, lease arm)                                    | `T-03-task.acquire.lean`   | 05, 08, 13 | `test/NetworkLocal.test.ts` "T-01…T-10"; `test/ProtocolClient.test.ts` Tasks; `test/Worker.test.ts` execute acquire                | partial |
| T-04 task.fence (fenced create/settle)                                                | `T-04-task.fence.lean`     | 05, 08, 12 | `test/NetworkLocal.test.ts` "T-01…T-10"; `test/ProtocolClient.test.ts` Tasks; `test/ExecutionEngine.test.ts` local steps           | partial |
| T-05 task.heartbeat (bulk, silent per-task no-op, stored-ttl extension)               | `T-05-task.heartbeat.lean` | 05, 08, 13 | `test/NetworkLocal.test.ts` "T-01…T-10"; `test/ProtocolClient.test.ts` Tasks                                                       | partial |
| T-06 task.suspend (atomic multi-callback; 300 fast path; check-then-register)         | `T-06-task.suspend.lean`   | 05, 08, 14 | `test/NetworkLocal.test.ts` "T-01…T-10"; `test/ProtocolClient.test.ts` SuspendRefused; `test/Worker.test.ts` suspend/resume replay | done    |
| T-07 task.fulfill (atomic settle+fulfill, cascade)                                    | `T-07-task.fulfill.lean`   | 05, 08, 13 | `test/NetworkLocal.test.ts` "T-01…T-10"; `test/ProtocolClient.test.ts` TaskFenced; `test/Worker.test.ts` root fulfillment          | partial |
| T-08 task.release (→pending, retry timeout, redispatch; resumes NOT cleared)          | `T-08-task.release.lean`   | 05, 08, 13 | `test/NetworkLocal.test.ts` "T-01…T-10"; `test/ProtocolClient.test.ts` Tasks; worker failure path in `src/Worker.ts`               | partial |
| T-09 task.halt (no version fence; 409 if fulfilled; idempotent)                       | `T-09-task.halt.lean`      | 05, 08     | `test/NetworkLocal.test.ts` "T-01…T-10"; `test/ProtocolClient.test.ts` Tasks                                                       | partial |
| T-10 task.continue (409 unless halted; no version bump)                               | `T-10-task.continue.lean`  | 05, 08     | `test/NetworkLocal.test.ts` "T-01…T-10"; `test/ProtocolClient.test.ts` Tasks                                                       | partial |
| T-11 task.search — NOT IMPLEMENTED (501 per spec)                                     | `T-11-task.search.lean`    | —          | n/a                                                                                                                                | done    |

## Schedule actions

| Spec action                                                       | Lean source                 | Impl spec | Tests                                                 | Status |
| ----------------------------------------------------------------- | --------------------------- | --------- | ----------------------------------------------------- | ------ |
| S-01 schedule.get                                                 | `S-01-schedule.get.lean`    | 06, 20    | `test/NetworkLocal.test.ts` schedules/get             | done   |
| S-02 schedule.create (idempotent-by-id, body ignored on existing) | `S-02-schedule.create.lean` | 06, 20    | `test/NetworkLocal.test.ts` schedules/create/catch-up | done   |
| S-03 schedule.delete                                              | `S-03-schedule.delete.lean` | 06, 20    | `test/NetworkLocal.test.ts` schedules/delete          | done   |
| S-04 schedule.search — NOT IMPLEMENTED (501 per spec)             | `S-04-schedule.search.lean` | —         | n/a                                                   | done   |

## Environment transitions

| Transition                                                                       | Lean source                        | Impl spec | Tests                                                                                                              | Status |
| -------------------------------------------------------------------------------- | ---------------------------------- | --------- | ------------------------------------------------------------------------------------------------------------------ | ------ |
| resume cascade (suspended→pending; buffer for non-suspended; version NOT bumped) | `spec/02-actions/00-resume.lean`   | 05, 14    | `test/NetworkLocal.test.ts` suspend/resume buffering (oracle side); `test/Worker.test.ts` worker re-acquire/replay | done   |
| onPromiseTimeout (persist projection; cascade; backdated settledAt)              | `spec/02-actions/02-timeouts.lean` | 04        | `test/NetworkLocal.test.ts` "timeout projection and the tick"                                                      | done   |
| onTaskRetryTimeout (self-rescheduling execute redelivery)                        | `02-timeouts.lean`                 | 05        | `test/NetworkLocal.test.ts` "pending retry timeout redelivers"                                                     | done   |
| onTaskLeaseTimeout (→pending; no version bump)                                   | `02-timeouts.lean`                 | 05, 13    | `test/NetworkLocal.test.ts` "lease expiry does not bump"                                                           | done   |
| schedule catchUp (one promiseCreate per missed tick, backdated)                  | `02-timeouts.lean`                 | 06        | `test/NetworkLocal.test.ts` schedule catch-up                                                                      | done   |

## Structural invariants (the test oracle — assert after EVERY op in oracle tests)

| #   | Invariant                                          | Impl spec | Status |
| --- | -------------------------------------------------- | --------- | ------ |
| 1   | Every task has a corresponding promise             | 05        | done   |
| 2   | Every pending task has a retry timeout             | 05        | done   |
| 3   | Every acquired task has a lease                    | 05        | done   |
| 4   | Every suspended task has ≥1 registered callback    | 05        | done   |
| 5   | No suspended task has an already-consumed callback | 05        | done   |
| 6   | No suspended task has a timeout                    | 05        | done   |
| 7   | No fulfilled task has a timeout                    | 05        | done   |

## Handbook MUSTs (wire/worker behavior)

| Requirement                                                                                             | Impl spec  | Tests                                                                                                                                                                   | Status  |
| ------------------------------------------------------------------------------------------------------- | ---------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| Envelope `{kind, head:{corrId, version:"2026-04-01"}, data}`; reject corrId mismatch                    | 03, 07     | `test/Network.test.ts` "envelope helpers"; `test/NetworkHttp.test.ts` send-path envelope/status/corrId                                                                  | done    |
| SSE reconnect with exponential backoff (cap ~30s, reset on success)                                     | 07         | `test/NetworkHttp.test.ts` poll decode/auth/reconnect backoff                                                                                                           | done    |
| Never block a thread on a durable wait (suspend instead)                                                | 14         | `test/Worker.test.ts` pending external await parks task with callback and releases lease                                                                                | done    |
| Heartbeat per-process at TTL/2 with REAL `(id, version)` list                                           | 13         | `test/Worker.test.ts` heartbeat keeps held task acquired past original lease                                                                                            | done    |
| Same codec path for param AND value, including rejections                                               | 02         | `test/Codec.test.ts` (native byte fixtures, rejection round-trips)                                                                                                      | done    |
| Headers always accompany data; `resonate:schema` header written                                         | 02, 11     | `test/Codec.test.ts` (headers on every encode); `test/ResonateClient.test.ts` invocation params                                                                         | partial |
| Child timeoutAt clamped to parent's (absolute epoch-ms); detached unclamped                             | 12, 16     | `test/ExecutionEngine.test.ts` local child create path + detached unclamped timeout                                                                                     | done    |
| `409` = stop claiming, never blind-retry; auth failures never retried                                   | 07, 08, 13 | `test/NetworkHttp.test.ts` auth terminal; `test/ProtocolClient.test.ts` TaskFenced                                                                                      | partial |
| Tag vocabulary: `resonate:target/origin/parent/branch/prefix/scope/timer` (TS/Rust consensus)           | 01, 11, 12 | `test/Protocol.test.ts` "tags"; `test/ResonateClient.test.ts` root target/lineage emission; `test/ExecutionEngine.test.ts` remote/detached lineage                      | done    |
| Sleep = `resonate:timer:"true"`, timeoutAt = wake time, no target                                       | 15         | `test/Worker.test.ts` durable sleep/resume + parent-timeout clamp                                                                                                       | done    |
| Schema-declared external promises use stable ids, no target, typed settle/await, and timeout projection | 17         | `test/ExecutionEngine.test.ts` external promise resolve/reject/timeout/malformed/duplicate/stable-id; `test/NetworkLocal.test.ts` latent timeout                        | done    |
| Retry policies encode on invocation params and drive bounded in-process step retry                      | 18         | `test/RetryPolicy.test.ts` native math/wire fixtures; `test/ExecutionEngine.test.ts` retry/non-retryable/timeout-bound steps; `test/ResonateClient.test.ts` retry param | done    |
| Recorded nondeterminism uses durable local steps with native-compatible wire values                     | 19         | `test/ExecutionEngine.test.ts` `ctx.now` replay after clock movement, `ctx.random` numeric wire value, and sequence slot consumption                                    | done    |
| Schedule API creates native schedule records, dispatches fired promises, and keeps create idempotent    | 20         | `test/Worker.test.ts` `Resonate.schedule.layer` startup/fire/execute and no-drift idempotent recreate/delete                                                            | done    |
| Public test harness bundles local server, worker, client, snapshots, invariants, and restart replay     | 21         | `test/TestHarness.test.ts` DESIGN countdown example and worker restart replay with exactly-once recorded local step                                                     | done    |
| Deterministic child ids: per-invocation sequence counter (`{parent}.{n}`; detached `{prefix}.d{hash}`)  | 12, 16     | `test/ExecutionEngine.test.ts` local `ctx.run` child id + attached fan-out + detached id shape                                                                          | done    |
| Structured concurrency: root does not fulfill while attached children unsettled                         | 12, 16     | `test/ExecutionEngine.test.ts` drains local children before root fulfill and suspends on attached unawaited remote children                                             | done    |
| 300+preload fast path on suspend handled (no suspend; warm cache)                                       | 14         | `Worker.executeUntilBlocked` loops on `Tasks.SuspendRefused`; `test/ProtocolClient.test.ts` covers `300` typed response                                                 | done    |
| Register all functions before receiving work                                                            | 10, 13     | `test/ResonateRegistry.test.ts` handler layer completeness/duplicates; `Worker.layer` builds registry before stream consumption                                         | done    |

## Known spec ↔ shipped-server deviations (follow the SERVER)

| Area                                                       | Spec says                             | Shipped server does                                                         | Verified by                                                                                          |
| ---------------------------------------------------------- | ------------------------------------- | --------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| Version bump timing                                        | (diagram implies) bump on lease lapse | bump on NEXT acquire                                                        | local oracle test in spec 05; spec 09 harness added (skips loudly until `resonate` CLI is available) |
| task.create without `resonate:target`                      | TS local-mode rejects                 | shipped server does NOT reject (validates address format only when present) | local oracle test in spec 05; `test/Differential.test.ts` scenario                                   |
| `preload` fields on create/acquire/suspend/fence responses | never populated in Lean model         | populated (siblings by `resonate:branch`)                                   | local oracle test in spec 05; spec 09 harness added (skips loudly until `resonate` CLI is available) |
| Idempotency keys (`ikc`/`iku`/`strict`)                    | absent from Lean model                | (ignored by decision — not modeled)                                         | n/a                                                                                                  |
