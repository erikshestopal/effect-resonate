# 16 — Remote Invocation from Context (`ctx.rpc`, `ctx.beginRpc`, `ctx.detached`)

## Objective

Durable child invocations dispatched to workers: attached rfc/rfi and detached
fire-and-forget with independent lifecycle.

## Dependencies

12, 14.

## References

- `docs/DESIGN.md` §4.2, §5.3
- `repos/resonate-sdk-ts/src/context.ts` (rfi/rfc/detached, prefixId vs originId
  comments at :322-329 and :518-605), `src/util.ts` (`detachedId`, cyrb53)

## Key facts

- `ctx.beginRpc(Fn, payload, opts?)`: seq id (or explicit id → lineage break),
  fenced promise.create with `resonate:scope:"global"`, lineage tags,
  `resonate:target` = match(opts.target ?? default group), param = encoded
  `{func, args, version}`, timeoutAt clamped to parent. Returns handle; awaiting a
  pending one goes through the suspension coordinator. `ctx.rpc` = begin + await.
  String-name overload → `unknown` result.
- **Attached**: recorded in the structured-concurrency set — root cannot fulfill
  while unsettled (even if never awaited).
- **ctx.detached**: fresh root — `resonate:origin` = own id (lineage break),
  `resonate:prefix` carried UNCHANGED, id = `{prefixId}.d{cyrb53(seqid)}` (bounded
  growth for recursive detached chains), timeout NOT clamped (MAX_SAFE_INTEGER
  default per native), NOT in the attached set — parent completes independently.
  This is the native fix for unbounded replay in forever-loops; document that use.
- Cross-worker correctness: the child may be claimed by this same process or any
  worker in the target group — no local execution by the caller.

## Deliverables

- `ctx.rpc`/`beginRpc`/`detached` on `ResonateContext`.

## Tests (local oracle, two worker layers in-process)

- Parent on group A calls child registered on group B → child executes on B's
  worker, parent suspends then resumes with the value. Tag/param wire fixtures
  byte-match native.
- Attached-but-unawaited child: parent return waits (suspends on it) until child
  settles — native internal.return flush semantics.
- Detached: parent fulfills immediately; detached child runs to completion after;
  id shape `{prefix}.d{hash}`; recursive detached chain keeps ids bounded.
- Fan-out: two beginRpc + Effect.all awaits → single suspend with both callbacks
  (spec 14 path), deterministic ids.
- Lineage break via explicit id: origin resets, prefix does not.

## Acceptance

- `vp run check` green; CONFORMANCE.md structured-concurrency + tag-vocabulary
  rows → done.
