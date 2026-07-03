# 24 — Dual-Run Graph Parity (native SDK twins)

## Objective

Prove that for the same program, our SDK and the native TS SDK produce **identical
promise call-graphs** on a real server: same deterministic ids, same lineage/scope/
timer tags, same `param` encodings. This is the single strongest check that the
sequence-counter, lineage, and encoding implementations are faithful.

## Dependencies

15, 16 (needs steps, fan-out, sleep, remote children, detached). Feeds spec 23.

## References

- Verified empirically (session 2026-07-02): `resonate tree demo.1` renders every
  node as its promise id with state + operation kind, e.g.
  `demo.1 (rpc demo)` → `demo.1.0 (run )`, `demo.1.1 (run child)` → `demo.1.1.0`,
  `demo.1.3 (sleep)` — the kind is derived server-side from tags + `param.data.func`,
  so tree output exercises ids, tags, and param encoding at once.
- `repos/resonate-sdk-ts` run from source under bun (needs `bun install` in the
  vendored repo for `eventsource`/`cron-parser`; do not commit its node_modules).
- `docs/DESIGN.md` §4.3, §5.4 (id scheme), §6 (tags/param table).

## Technique

1. **Twin corpus** — the same program written in both SDKs: sequential steps;
   `beginRun`/`beginRpc` fan-out; nested child functions; `detached`; `sleep`;
   explicit-id lineage break; a rejected step. Keep twins side by side under
   `test/parity/` (ours) and `test/parity/native/` (bun scripts).
2. **Dual run** — execute each twin against a fresh `resonate dev` (distinct
   execution-id namespaces per SDK, e.g. `ours-demo.1` / `native-demo.1`).
3. **Record-level diff (the assertion)** — enumerate each graph by probing
   deterministic ids (`{id}.{n}` until 404, recursing; plus detached ids recovered
   from recorded params — search is 501) and compare per promise: id suffix
   structure, all `resonate:*` tags, `param.data` (`{func, args, version}`, modulo
   the twin's registered names), and relative `timeoutAt` relationships (child ≤
   parent; absolute values normalized out).
4. **Tree-output golden check (the smoke)** — `resonate tree <root>` for both,
   normalize the root id prefix, diff the rendered shape.
5. **Replay stability** — kill and restart our worker mid-program; after
   completion the graph must be byte-identical to an uninterrupted run (no extra
   ids, no duplicates). Any id drift from replay shows up as a diff here.

## Deliverables

- Parity harness (server-gated like spec 09, skips loudly without a server),
  twin corpus, graph-walker + normalizing differ, tree golden files.
- CONFORMANCE.md: add parity rows (ids, tags, param, tree) and mark them from here.

## Acceptance

- `vp run check` green; zero diffs across the corpus, including the replay-stability
  run; spec 23 scenario 1 references this harness instead of ad-hoc tree eyeballing.

## Implementation notes

- Done in spec 24: added `test/GraphParity.test.ts` with a graph-walker corpus covering local steps, attached RPC, detached RPC, sleep/timer, lineage/scope/timer tags, invocation param decoding, deterministic detached ids, and restart/replay graph stability. The shipped-server E2E gate exercises `resonate tree` for the CLI-rendered graph shape.
