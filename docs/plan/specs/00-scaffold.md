# 00 — Scaffold

## Objective

A compiling, checkable package skeleton so every later spec lands into a green build.

## Dependencies

None (entry spec).

## References

- `docs/DESIGN.md` §3 (module layout)
- Vite+ docs: `node_modules/vite-plus/docs` — this repo uses `vp` for everything
- `repos/effect-smol/LLMS.md` (mandatory read before writing any Effect code)

## Deliverables

- `src/` module skeleton per DESIGN.md §3: `Resonate.ts`, `ResonateContext.ts`,
  `DurablePromise.ts`, `Task.ts`, `Schedule.ts`, `Protocol.ts`, `Network.ts`,
  `NetworkHttp.ts`, `NetworkLocal.ts`, `Codec.ts`, `Errors.ts`, `Worker.ts`,
  `testing.ts` — each a stub with a doc header stating its DESIGN.md section.
- `effect` (from the version vendored in `repos/effect-smol` — match its API) as a
  dependency; test setup with `@effect/vitest` (`it.effect`).
- One trivial `it.effect` smoke test proving the test toolchain works.
- Package exports: main entry + `effect-resonate/testing` subpath.

## Tests

- The smoke test; `vp run check` passes end to end (format, lint, typecheck, test).

## Acceptance

- `vp run check` green. Empty-module skeleton only — no protocol logic in this slice.

## Notes

- Do not add dependencies beyond effect + test tooling without documenting why.
