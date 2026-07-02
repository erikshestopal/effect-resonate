# 11 — Client Runtime (`ResonateClient`)

## Objective

The outside-in API: `run`, `rpc`, `beginRun`, `beginRpc`, `get`, `cancel`,
`resolve`/`reject` (for declared external promises) — native `Resonate`-class
semantics as an Effect service.

## Dependencies

02, 08, 10.

## References

- `docs/DESIGN.md` §4.4, §4.7 (cancellation), §4.5 (resolve/reject)
- `repos/resonate-sdk-ts/src/resonate.ts` (`run`/`rpc`/`beginRun`/`beginRpc`/`get`,
  root tags at :333-343, handle/subscribe flow)

## Key facts

- **beginRun** (and `run` = beginRun + await): `task.create` embedding the
  promise.create action, pid/ttl = this client's; if response task is `acquired`,
  hand off to the execution engine (spec 12/13) fire-and-forget in this process;
  if the promise already existed, fall back to listener await. Root promise tags:
  `resonate:origin/prefix/branch/parent` all = id (self-referential root),
  `resonate:scope: "global"`, `resonate:target` = anycast(this group).
- **beginRpc** (and `rpc`): bare `promise.create` with `resonate:target` =
  `match(options.target ?? default)`; no local execution.
- Handles: `{ id, await, poll, cancel }` — `await` via
  `DurablePromises.awaitSettled` + codec decode; terminal states map to typed
  errors (`DurablePromiseTimedOut`/`Canceled`); rejection value decodes through the
  codec (Error reconstruction). Interrupting `await` detaches only (native semantics,
  no cancel-on-interrupt).
- `cancel` = `promise.settle { state: rejected_canceled }`.
- Invocation param encoding: `{func, args, version}` (+ `retry` when set) through
  the codec; args = tuple elements schema-encoded (spec 01/02).
- Ids: `ExecutionId` required; `idPrefix` config prepended as in native (`prefix:`).
- Options resolution: client defaults → per-call, native layering, `Duration` inputs.
- Timeout default 24h; `timeoutAt = now + timeout` absolute.

## Deliverables

- `ResonateClient` service + layer (depends: DurablePromises/Tasks, Codec, Network,
  Registry for local-run dispatch); typed by definition reference; string-name
  overloads returning `unknown`.

## Tests

- Against local oracle + a stub engine: run creates acquired task with correct root
  tags and param encoding (assert exact wire bytes vs native fixtures); rpc creates
  target-tagged promise, no task claim by client; begin\* handles poll/await/cancel;
  duplicate id attaches (idempotency); rejected execution surfaces decoded Error;
  canceled/timed-out map to typed errors.

## Acceptance

- `vp run check` green; CONFORMANCE.md T-02 client row → done.

## Notes

- Implemented `ResonateClient` in `src/Resonate.ts` as a service over `DurablePromises`, `Tasks`, `ResonateCodec`, and `ResonateNetwork`; it intentionally does not start the execution engine yet because engine/worker dispatch belongs to specs 12/13.
- `beginRpc` uses bare `promise.create`; `beginRun` uses `task.create` embedding `promise.create`. Both stamp root lineage tags (`origin`, `prefix`, `branch`, `parent`, `scope`, `target`) and encode invocation params through the same codec/header path as values.
- Function definitions schema-encode arguments before invocation encoding; string-name overloads preserve native remote-call behavior by encoding raw positional args with default version 1 when no version is supplied.
- Handle `await` delegates to `DurablePromises.awaitSettled`; `poll` is non-blocking via `promise.get`; `cancel` settles `rejected_canceled`. Canceled and timed-out terminal states map to typed errors, while rejected values decode through the codec into the error channel.
- External promise declarations plus `client.resolve`/`client.reject` remain deferred to spec 17, which owns schema-declared external promises in the plan.
