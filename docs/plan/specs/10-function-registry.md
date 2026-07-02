# 10 — Function Definitions, Group, Registry (`Resonate.ts`)

## Objective

`Resonate.function` definitions, `Resonate.group`, `group.toLayer(handlers)` /
`toLayerHandler` / `of` — the RpcGroup-shaped registration machinery producing
`Handler<Tag>` context entries, plus the runtime registry (name+version → handler).

## Dependencies

01 (branded types, FunctionVersion). (Parallel-safe with 02–08.)

## References

- `docs/DESIGN.md` §4.1 (binding), §4.9
- `repos/effect-smol/packages/effect/src/unstable/rpc/RpcGroup.ts` — the pattern to
  mirror (toLayer accepting handlers or Effect-of-handlers, ToHandler context entries,
  HandlersFrom inference); `ai-docs/src/80_cluster/10_entities.ts` for usage shape
- `repos/resonate-sdk-ts/src/registry.ts` — native registry semantics (bidirectional,
  versioned, duplicate rejection)

## Key facts

- `Resonate.function("PascalName", { payload, version? })`: payload any Schema;
  tuple → positional handler params; non-tuple → single param. Version branded
  `FunctionVersion`, default 1.
- Handler map inference: key per function name; handler `(...payloadElements) =>
Effect<Success, Error, R>`; R accumulates through the layer (minus
  `ResonateContext`, provided by the runtime).
- Result types flow by inference where the implementation is in scope; NO success/
  error schemas (DESIGN.md resolved decision).
- Registry semantics (native parity): duplicate name+version registration is a
  defect at layer build; version resolution `"latest"` = max registered version
  (wire 0 ⇄ "latest").
- Two versions of one function = two definitions, same name, different version,
  both in the group.

## Deliverables

- Definition constructor, group, toLayer/toLayerHandler/of, `Handler<Tag>` context
  entries, runtime `Registry` assembled from a group + context (consumed by spec 13).

## Tests

- Type-level: handler map completeness enforced (missing key = compile error —
  assert via test-d style type tests if the toolchain supports, else documented
  compile fixture); payload tuple inference; R propagation.
- Runtime: duplicate name+version → defect; "latest" resolves max; group of two
  versions dispatches correctly; toLayer(Effect) variant builds handlers with
  shared setup state.

## Acceptance

- `vp run check` green; CONFORMANCE.md "register before work" row → partial.
