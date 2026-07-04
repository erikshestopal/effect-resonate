# 06 — Local Server: Schedules (`NetworkLocal.ts`)

## Objective

Schedule handlers (S-01…S-03) and the firing/catch-up transition, using `effect/Cron`
as the `nextCron` implementation.

## Dependencies

04 (schedules materialize promises via the promise.create handler).

## References

- `repos/resonate-specification/spec/02-actions/S-01…S-03`, `02-timeouts.lean`
  (`catchUp`, `onScheduleTimeout`)
- `repos/resonate-sdk-ts/src/network/Local.ts` (`scheduleCreate`, cron-parser usage,
  template expansion `{{.id}}`/`{{.timestamp}}`)
- `docs/DESIGN.md` §4.8

## Key facts

- **schedule.create**: idempotent by id — existing schedule returned as-is, body
  ignored, 200. New: `nextRunAt = Cron.next(cron, now)` (strictly after now),
  `lastRunAt = none`, arm schedule timeout at nextRunAt.
- **schedule.delete**: 404 or delete schedule + its timeout.
- **catchUp**: while `nextRunAt <= now`: expand promiseId template (`{{.id}}` →
  schedule id, `{{.timestamp}}` → the cron tick time), `promiseCreate` with
  `timeoutAt = cronTime + promiseTimeout`, `param`/`tags` from the template, executed
  AS OF the historical cronTime (backdated — matters for the created promise's own
  timeout math and dispatch); advance `lastRunAt = cronTime`,
  `nextRunAt = Cron.next(cron, cronTime)`. **One promiseCreate per missed tick.**
- **onScheduleTimeout**: run catchUp, persist schedule, re-arm at new nextRunAt.
- If the template tags carry `resonate:target`, the materialized promise dispatches a
  task exactly like any promise.create (already handled by spec 04's handler).

## Deliverables

- Schedule store + handlers (S-04 → 501) + catchUp wired into the tick.
- Template expansion helper (exact native `{{.id}}`/`{{.timestamp}}` syntax).

## Tests

- Create → tick past nextRunAt → promise materialized with expanded id, backdated
  timing, task dispatched when target-tagged; lastRunAt/nextRunAt advanced.
- Catch-up: server "down" 3 ticks (TestClock jump) → exactly 3 promises, each at its
  historical tick time.
- Idempotent re-create ignores changed cron/payload (native parity — no drift checks).
- Delete disarms firing.
- Cron dialect: five-field accepted; six-field/seconds rejected at the API boundary.

## Acceptance

- `vp run check` green; CONFORMANCE.md S-rows + catchUp → done (oracle side).

## Notes

- `NetworkLocal.layer` now stores schedules separately from schedule timeouts; the
  schedule record carries `nextRunAt`/`lastRunAt`, and the timeout map is the armed
  firing index used by `debug.tick`.
- Cron parsing is expressed as a Schema filter over the shipped-server dialect we
  support locally: five fields only. `effect/Cron` accepts six fields, but the local
  server rejects seconds at `schedule.create` with `400` until differential tests
  prove server support.
- Catch-up invokes the existing `promiseCreate` handler once per due tick using the
  historical cron time as `now`; this preserves backdated `createdAt`, promise
  timeout math, task dispatch, and shipped-server/idempotent promise behavior.
- Template expansion intentionally matches native syntax only: `{{.id}}` and
  `{{.timestamp}}` are replaced with the schedule id and epoch-ms cron tick time.
