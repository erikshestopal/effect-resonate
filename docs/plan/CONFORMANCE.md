# Protocol Conformance Matrix

Maps every protocol behavior to the implementing spec and the tests that prove it.
Fill the Tests column as slices land; a row is conformant only when its tests pass
against BOTH the local oracle (specs 04–06) and the shipped server (spec 09).

Statuses: `todo` | `partial` | `done`.

## Promise actions

| Spec action                                                                            | Lean source                             | Impl spec | Tests | Status |
| -------------------------------------------------------------------------------------- | --------------------------------------- | --------- | ----- | ------ |
| P-01 promise.get (incl. timeout projection)                                            | `spec/02-actions/P-01-promise.get.lean` | 04, 08    |       | todo   |
| P-02 promise.create (new / born-settled / idempotent re-create / target→task / delay)  | `P-02-promise.create.lean`              | 04, 08    |       | todo   |
| P-03 promise.settle (settle / projected-timeout race / idempotent re-settle / cascade) | `P-03-promise.settle.lean`              | 04, 08    |       | todo   |
| P-04 promise.register_callback (422 non-target awaiter; skip-if-awaiter-expired)       | `P-04-promise.register_callback.lean`   | 04, 08    |       | todo   |
| P-05 promise.register_listener                                                         | `P-05-promise.register_listener.lean`   | 04, 08    |       | todo   |
| P-06 promise.search — NOT IMPLEMENTED (501 per spec)                                   | `P-06-promise.search.lean`              | —         | n/a   | done   |

## Task actions

| Spec action                                                                           | Lean source                | Impl spec  | Tests | Status |
| ------------------------------------------------------------------------------------- | -------------------------- | ---------- | ----- | ------ |
| T-01 task.get (projection to fulfilled)                                               | `T-01-task.get.lean`       | 05, 08     |       | todo   |
| T-02 task.create (create+acquire / born-fulfilled / re-acquire / 422 no-target / 409) | `T-02-task.create.lean`    | 05, 08, 11 |       | todo   |
| T-03 task.acquire (version fence, bump, lease arm)                                    | `T-03-task.acquire.lean`   | 05, 13     |       | todo   |
| T-04 task.fence (fenced create/settle)                                                | `T-04-task.fence.lean`     | 05, 12     |       | todo   |
| T-05 task.heartbeat (bulk, silent per-task no-op, stored-ttl extension)               | `T-05-task.heartbeat.lean` | 05, 13     |       | todo   |
| T-06 task.suspend (atomic multi-callback; 300 fast path; check-then-register)         | `T-06-task.suspend.lean`   | 05, 14     |       | todo   |
| T-07 task.fulfill (atomic settle+fulfill, cascade)                                    | `T-07-task.fulfill.lean`   | 05, 13     |       | todo   |
| T-08 task.release (→pending, retry timeout, redispatch; resumes NOT cleared)          | `T-08-task.release.lean`   | 05, 13     |       | todo   |
| T-09 task.halt (no version fence; 409 if fulfilled; idempotent)                       | `T-09-task.halt.lean`      | 05, 08     |       | todo   |
| T-10 task.continue (409 unless halted; no version bump)                               | `T-10-task.continue.lean`  | 05, 08     |       | todo   |
| T-11 task.search — NOT IMPLEMENTED (501 per spec)                                     | `T-11-task.search.lean`    | —          | n/a   | done   |

## Schedule actions

| Spec action                                                       | Lean source                 | Impl spec | Tests | Status |
| ----------------------------------------------------------------- | --------------------------- | --------- | ----- | ------ |
| S-01 schedule.get                                                 | `S-01-schedule.get.lean`    | 06, 20    |       | todo   |
| S-02 schedule.create (idempotent-by-id, body ignored on existing) | `S-02-schedule.create.lean` | 06, 20    |       | todo   |
| S-03 schedule.delete                                              | `S-03-schedule.delete.lean` | 06, 20    |       | todo   |
| S-04 schedule.search — NOT IMPLEMENTED (501 per spec)             | `S-04-schedule.search.lean` | —         | n/a   | done   |

## Environment transitions

| Transition                                                                       | Lean source                        | Impl spec | Tests | Status |
| -------------------------------------------------------------------------------- | ---------------------------------- | --------- | ----- | ------ |
| resume cascade (suspended→pending; buffer for non-suspended; version NOT bumped) | `spec/02-actions/00-resume.lean`   | 05, 14    |       | todo   |
| onPromiseTimeout (persist projection; cascade; backdated settledAt)              | `spec/02-actions/02-timeouts.lean` | 04        |       | todo   |
| onTaskRetryTimeout (self-rescheduling execute redelivery)                        | `02-timeouts.lean`                 | 05        |       | todo   |
| onTaskLeaseTimeout (→pending; no version bump)                                   | `02-timeouts.lean`                 | 05, 13    |       | todo   |
| schedule catchUp (one promiseCreate per missed tick, backdated)                  | `02-timeouts.lean`                 | 06        |       | todo   |

## Structural invariants (the test oracle — assert after EVERY op in oracle tests)

| #   | Invariant                                          | Impl spec | Status |
| --- | -------------------------------------------------- | --------- | ------ |
| 1   | Every task has a corresponding promise             | 05        | todo   |
| 2   | Every pending task has a retry timeout             | 05        | todo   |
| 3   | Every acquired task has a lease                    | 05        | todo   |
| 4   | Every suspended task has ≥1 registered callback    | 05        | todo   |
| 5   | No suspended task has an already-consumed callback | 05        | todo   |
| 6   | No suspended task has a timeout                    | 05        | todo   |
| 7   | No fulfilled task has a timeout                    | 05        | todo   |

## Handbook MUSTs (wire/worker behavior)

| Requirement                                                                                            | Impl spec | Tests                                                                                                  | Status  |
| ------------------------------------------------------------------------------------------------------ | --------- | ------------------------------------------------------------------------------------------------------ | ------- |
| Envelope `{kind, head:{corrId, version:"2026-04-01"}, data}`; reject corrId mismatch                   | 03, 07    |                                                                                                        | todo    |
| SSE reconnect with exponential backoff (cap ~30s, reset on success)                                    | 07        |                                                                                                        | todo    |
| Never block a thread on a durable wait (suspend instead)                                               | 14        |                                                                                                        | todo    |
| Heartbeat per-process at TTL/2 with REAL `(id, version)` list                                          | 13        |                                                                                                        | todo    |
| Same codec path for param AND value, including rejections                                              | 02        | `test/Codec.test.ts` (native byte fixtures, rejection round-trips)                                     | done    |
| Headers always accompany data; `resonate:schema` header written                                        | 02        | `test/Codec.test.ts` (headers on every encode; `withSchemaHeader` — written at call sites in 11/12/17) | partial |
| Child timeoutAt clamped to parent's (absolute epoch-ms); detached unclamped                            | 12, 16    |                                                                                                        | todo    |
| `409` = stop claiming, never blind-retry; auth failures never retried                                  | 08, 13    |                                                                                                        | todo    |
| Tag vocabulary: `resonate:target/origin/parent/branch/prefix/scope/timer` (TS/Rust consensus)          | 01, 12    | `test/Protocol.test.ts` "tags" (types + wire transform; emission in 12)                                | partial |
| Sleep = `resonate:timer:"true"`, timeoutAt = wake time, no target                                      | 15        |                                                                                                        | todo    |
| Deterministic child ids: per-invocation sequence counter (`{parent}.{n}`; detached `{prefix}.d{hash}`) | 12        |                                                                                                        | todo    |
| Structured concurrency: root does not fulfill while attached children unsettled                        | 12, 16    |                                                                                                        | todo    |
| 300+preload fast path on suspend handled (no suspend; warm cache)                                      | 14        |                                                                                                        | todo    |
| Register all functions before receiving work                                                           | 10, 13    |                                                                                                        | todo    |

## Known spec ↔ shipped-server deviations (follow the SERVER)

| Area                                                       | Spec says                             | Shipped server does                                                         | Verified by                      |
| ---------------------------------------------------------- | ------------------------------------- | --------------------------------------------------------------------------- | -------------------------------- |
| Version bump timing                                        | (diagram implies) bump on lease lapse | bump on NEXT acquire                                                        | spec 09 differential test (todo) |
| task.create without `resonate:target`                      | TS local-mode rejects                 | shipped server does NOT reject (validates address format only when present) | spec 09 (todo)                   |
| `preload` fields on create/acquire/suspend/fence responses | never populated in Lean model         | populated (siblings by `resonate:branch`)                                   | spec 09 (todo)                   |
| Idempotency keys (`ikc`/`iku`/`strict`)                    | absent from Lean model                | (ignored by decision — not modeled)                                         | n/a                              |
