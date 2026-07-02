# 22 — Deterministic Simulation Testing (DST)

## Objective

Seeded chaos simulation: server + N workers as a discrete-event loop over a seeded
random source and stepped clock — drop/delay/reorder messages, kill workers — and
assert outcome invariance. The handbook calls this "the highest-value test you can
write for a durable SDK".

## Dependencies

14, 21.

## Translation map (native → Effect)

| Native source                                                                                             | Symbol                                                                            | Becomes                                              |
| --------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------- | ---------------------------------------------------- |
| `resonate-sdk-py` `resonate/simulator.py` (concept; described in handbook `testing-against-the-spec.mdx`) | seeded discrete-event sim                                                         | Effect-native simulator over TestClock + seeded PRNG |
| `repos/resonate-sdk-ts` `sim/main.ts`                                                                     | DST entry                                                                         | `vp` task `dst`                                      |
| `repos/resonate-sdk-ts/src/trace.ts`                                                                      | well-formedness predicates (`isWellFormed`, `uniqueSpawn`, `terminalIsLast`, ...) | optional trace assertions on engine events           |

## Key facts

- All chaos from ONE seed (reproducible): message drop probability, delivery delay/
  reorder (shuffled inboxes), worker kill/restart at random points. Print the seed on
  failure; accept a seed to replay.
- The chaos sits in a fault-injecting `ResonateNetwork` wrapper around `NetworkLocal`
  — transports are the seam (that's why spec 03 is an interface).
- Assertion: **outcome invariance** — for a corpus of programs (fan-out, sleeps,
  external promises, detached, retries), the final root promise value equals the
  chaos-free run's value, for every seed batch; plus the seven invariants hold at
  every server step; plus exactly-once observable side effects (counters).
- Random-but-valid op-stream fuzzing against the oracle (generate valid op sequences,
  assert invariants after each) complements the program corpus.

## Deliverables

- Fault-injection network wrapper; simulator runner (`vp` task, seed batch in CI,
  single-seed repro mode); program corpus; fuzzer for raw protocol op streams.

## Tests

The simulator IS the test. CI runs a fixed seed batch; a nightly/extended task runs
larger batches.

## Acceptance

- `vp run check` green including a small seed batch; failures reproduce from
  printed seed.
