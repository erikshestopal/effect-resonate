# 18 — Retry Policies (`RetryPolicy`)

## Objective

The four wire-encodable retry policies, in-process retry execution around steps,
and `nonRetryableErrors` via tagged error classes.

## Dependencies

12.

## Translation map (native → Effect)

| Native source                              | Symbol                                              | Becomes                                        |
| ------------------------------------------ | --------------------------------------------------- | ---------------------------------------------- |
| `repos/resonate-sdk-ts/src/retries.ts`     | `Constant/Exponential/Linear/Never`, `encode()`     | `RetryPolicy` Schema tagged union + wire codec |
| `repos/resonate-sdk-ts/src/util.ts`        | `executeWithRetry` (~:139)                          | retry loop around step effects                 |
| `repos/resonate-sdk-ts/src/computation.ts` | default-policy selection, `retry` decode from param | engine policy resolution                       |

## References

- `docs/DESIGN.md` §4.6 (binding)
- Handbook: `time-retries-policies.mdx` (documents that defaults are TS/Python
  convention, not spec-normative — keep them, note it)

## Key facts

- Policies (exact native math): Constant(delay=1000, maxRetries=∞);
  Exponential(delay=1000, factor=2, maxRetries=∞, maxDelay=30000 —
  `min(delay*factor^attempt, maxDelay)`); Linear(delay\*attempt); Never (attempt 0 only).
  Attempt 0 always delays 0.
- Wire encoding `{type, data}` exactly as native `encode()` produces — carried in
  `param.data.retry` so the policy survives restarts and applies on whichever worker
  claims the task; decode on the executing side.
- Defaults: `Never` for durable functions; `Exponential` for plain leaf effects
  (`ctx.run` steps' internal retry).
- Retry loop: in-process around step execution; stop when `next(attempt) === null`
  OR next attempt would exceed the invocation's timeoutAt; `attempt` surfaces in
  `ctx.info.attempt`.
- `nonRetryableErrors: [ErrorClass]` — matches on `_tag` (survives serialization),
  element type constrained to the effect's error channel.
- Durations as `Duration` inputs in the public constructors; ms on the wire.

## Deliverables

- `RetryPolicy` module (tagged union, constructors, wire codec); engine integration;
  `nonRetryableErrors` option.

## Tests

- Delay-sequence table tests matching native math exactly (fixture-compare against
  values computed from native retries.ts).
- Wire fixture: encoded `param.data.retry` byte-matches native for each policy.
- TestClock retry run: failing step with Exponential retries at 0, 1s, 2s, 4s...;
  succeeds on attempt N → settled once; attempt visible in ctx.info.
- Non-retryable: CardDeclined thrown → no retry, step settles rejected immediately.
- Timeout bound: retries stop when next attempt would pass timeoutAt.

## Acceptance

- `vp run check` green.
