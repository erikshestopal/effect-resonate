# 20 — Schedules API (`Resonate.schedule`)

## Objective

The unified schedule value: single options struct → `.layer` / `.create` / `.get` /
`.delete`, native semantics exactly.

## Dependencies

06 (oracle), 11.

## Translation map (native → Effect)

| Native source                                 | Symbol                                           | Becomes                                                        |
| --------------------------------------------- | ------------------------------------------------ | -------------------------------------------------------------- |
| `repos/resonate-sdk-ts/src/resonate.ts`       | `schedule(name, cron, func, ...args)` (:446-478) | `Resonate.schedule({ id, cron, function, payload, timeout? })` |
| `repos/resonate-sdk-ts/src/schedules.ts`      | `Schedules.create/get/delete`                    | `.create` / `.get` / `.delete` on the value                    |
| effect-smol `unstable/cluster/ClusterCron.ts` | `make(options): Layer`                           | the `.layer` shape                                             |

## References

- `docs/DESIGN.md` §4.8 (binding — including the no-drift-check rule)

## Key facts

- Emits `schedule.create` with promiseId template `{idPrefix}{{.id}}.{{.timestamp}}`,
  promiseParam = codec-encoded `{func, args, version}` (encoded ONCE, static forever),
  promiseTags = invocation tags incl. `resonate:target` (so ticks dispatch), and the
  relative `promiseTimeout`.
- `cron: Cron.Cron` (effect/Cron), serialized to the five-field expression;
  six-field/seconds rejected at the boundary.
- Native parity, no additions: re-create on existing id returns stored record, body
  ignored — **NO drift detection**; update = explicit delete + create; runtime-dynamic
  schedules = the same constructor invoked at runtime.
- `.layer` ensures existence at startup (idempotent create); NO delete on scope close.

## Deliverables

- `Resonate.schedule` constructor returning the value; layer + imperative ops.

## Tests (local oracle)

- `.layer` startup creates the schedule; tick materializes the promise; the target
  worker executes the function with the decoded payload; template expansion matches
  native format (wire fixture).
- Re-create with changed cron/payload → stored record returned unchanged (assert we
  do NOT error, do NOT update — native parity).
- `.delete` then tick → nothing fires.
- Layer restart (recreate) → single schedule, no duplicates.

## Acceptance

- `vp run check` green; CONFORMANCE.md S-rows (API side) → done.

## Implementation notes

- Done in spec 20: `Resonate.schedule` constructs a reusable schedule value exposing `.create`, `.get`, `.delete`, and `.layer`. Creation encodes invocation params once, writes the native `{{.id}}.{{.timestamp}}` promise id template, serializes five-field cron values, emits target/global tags, preserves native idempotent re-create behavior without drift checks, and leaves deletion explicit.
