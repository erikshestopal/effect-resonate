# 19 — Recorded Nondeterminism (`ctx.now`, `ctx.random`)

## Objective

Time and randomness as durable steps, replay-identical — the native
`ctx.date.now()` / `ctx.math.random()` as Effect values.

## Dependencies

12.

## Translation map (native → Effect)

| Native source                          | Symbol                                                                   | Becomes                                                       |
| -------------------------------------- | ------------------------------------------------------------------------ | ------------------------------------------------------------- |
| `repos/resonate-sdk-ts/src/context.ts` | `date.now()` (LFC over Date.now), `math.random()` (LFC over Math.random) | `ctx.now: Effect<DateTime.Utc>`, `ctx.random: Effect<number>` |

## References

- `docs/DESIGN.md` §4.2, §5.5
- Handbook: `replay-and-determinism.mdx` (why these must be durable steps)

## Key facts

- Each access = one local durable step (fenced create → execute Clock.now /
  Random.next → fenced settle); replay returns the recorded value. One server
  round-trip per access — accepted (resolved decision).
- `ctx.now` returns `DateTime.Utc` (epoch-ms recorded on the wire for native
  interop); underlying source is Effect `Clock` (so TestClock works in tests of
  the FIRST execution; replay ignores the clock entirely).
- We do NOT hijack ambient `Clock`/`Random` services inside functions — documented
  determinism rule instead (DESIGN.md §5.5).

## Deliverables

- `ctx.now`, `ctx.random` on `ResonateContext`; determinism contract section in the
  package docs (time/random/external reads through durable ops; branching only on
  payload + recorded results).

## Tests

- Value recorded once: crash after `ctx.now`, replay → same instant even though
  TestClock advanced.
- Wire: the recorded value's encoding matches native (`Date.now()` number; random
  float) so a native worker replaying our execution branch would read it.
- Sequence ids: `ctx.now` consumes a seq slot like any step.

## Acceptance

- `vp run check` green.
