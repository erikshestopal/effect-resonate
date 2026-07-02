# 17 — External Promises (`Resonate.promise`, `ctx.promise`, resolve/reject)

## Objective

Schema-declared, externally-settled durable promises: the human-in-the-loop
primitive, typed on both the awaiting and settling sides.

## Dependencies

11, 12.

## Translation map (native → Effect)

| Native source                           | Symbol                                                 | Becomes                                         |
| --------------------------------------- | ------------------------------------------------------ | ----------------------------------------------- |
| `repos/resonate-sdk-ts/src/context.ts`  | `promise({timeout, data, tags})` (RFI, func:"unknown") | `ctx.promise(Declaration, options)`             |
| `repos/resonate-sdk-ts/src/resonate.ts` | promises resolve path via `promise.settle`             | `client.resolve/reject(Declaration, id, value)` |

## References

- `docs/DESIGN.md` §4.5 (binding — including the name-derived id deviation and its
  rationale), §4.7
- Handbook: human-in-the-loop pattern in `promise-lifecycle.mdx` /
  `suspend-resume-settlement.mdx` (bare promise + external settle + `unblock`)

## Key facts

- Declaration: `Resonate.promise("name", { success: Schema, error?: TaggedErrorClass })`.
- `ctx.promise(Approval, { timeout?, tags? })`: creates promise with **name-derived id**
  `{executionId}.{declarationName}` (deliberate deviation from native's seq id —
  external settlers need a stable address; `options.id` overrides; duplicate
  declaration use in one execution without explicit id → defect). No `resonate:target`
  (nobody dispatched); lineage tags present; timeout clamped to parent.
- `handle.await`: suspension coordinator (spec 14); value decodes through the SUCCESS
  schema (external data is untrusted — decode failure = defect naming the settler);
  rejection decodes through the error schema into the typed error channel; timeout →
  `DurablePromiseTimedOut`; canceled → `DurablePromiseCanceled`.
- `client.resolve(Decl, promiseId, value)` / `client.reject(Decl, promiseId, err)`:
  schema-ENCODE then `promise.settle` (resolved / rejected). `Approval.id(executionId)`
  helper returns the branded PromiseId.
- Raw untyped settle stays available at layer 2 for interop.

## Deliverables

- `Resonate.promise` declaration constructor; `ctx.promise`; client resolve/reject;
  `Declaration.id(executionId)`.

## Tests (local oracle)

- Full loop: function creates approval, publishes id via a step, suspends; external
  `client.resolve` with typed payload → resume → decoded value in the function.
- Reject path → typed error in the awaiting function's error channel.
- Timeout: TestClock past the promise timeout → `DurablePromiseTimedOut` (non-timer
  promise rejects on timeout — assert against oracle projection).
- Malformed external settle (raw layer-2 settle with junk) → decode defect naming
  the promise id.
- Name-derived id stability: adding a step above `ctx.promise` does NOT change the id.
- Duplicate declaration in one execution without explicit id → defect.

## Acceptance

- `vp run check` green.
