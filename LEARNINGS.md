# Learnings — corrections from code review

Binding lessons from the maintainer's review of earlier slices. Read this before
writing any code; violating these gets the work rewritten.

## 1. Express codecs and parsers as Schema, not hand-rolled functions

Hand-rolled `JSON.stringify` replacer/reviver functions, `Predicate.isObject` +
bracket-access poking, and regex `exec` parsing were all rejected ("complete
BS"). If a value has a wire form and a domain form, the mapping IS a Schema:

- Recursive tree substitutions → recursive `Schema.Union` + `Schema.suspend`
  (see `src/Codec.ts` `NativeJsonValue`).
- Class instances in trees → `Schema.instanceOf` members with
  `SchemaTransformation.transform`.
- String grammars → `Schema.TemplateLiteralParser` (see `src/Protocol.ts`
  `TargetAddressFromString`), not regex + narrowing.
- Base64/JSON pipelines → compose `Schema.StringFromBase64` +
  `Schema.fromJsonString`, don't call `btoa`/`JSON.parse` yourself.
- Type guards → `SchemaParser.is(schema)`, never a hand-written
  `(v): v is T =>` function.

## 2. Construct schema values with `.make()` — never annotate or cast

Manual return-type annotations (`(): typeof X.Type => ({...})`), `as typeof
X.Encoded`, `as "literal"` on regex groups, and `value as SomeType` are all
banned. Instead:

- Build values through the schema instance: `SomeStruct.make({...})`,
  `SomeLiteral.make("x")`; for encoded-side values use `Schema.flip(s).make`.
- When TypeScript widens a narrowed literal (e.g. in conditional spreads),
  route the value through the field schema's `.make` to pin the type — do not
  add an annotation.

## 3. Services own their composition

Standalone exported glue functions around services (e.g. an `encodeValue`
helper that yields two services and chains them) were rejected. If behavior is
the composition of services, it belongs INSIDE the service implementation:
`ResonateCodec.layerJson` consumes `ResonateEncryptor` from context and applies
it around the JSON codec (the native `Codec`-class analog). The layer's `R`
channel advertises the dependency; callers touch one service.

## 4. Process

- Run `vp run check` BEFORE every commit — including docs-only changes. Never
  rely on the pre-commit hook to catch it.

## 5. General

- Consult `repos/effect-smol/SCHEMA.md` for the v4 Schema API before inventing
  a mechanism — the building block usually exists (flipped constructors,
  decoding defaults, template literals, `OptionFromOptionalKey`, …).
- Keep native-SDK behavioral quirks (truthiness coercions, `Object.assign`
  marker copying) inside the Schema transformation functions, documented with
  a comment pointing at the native source.

## 6. No factory helpers for schema shapes

Local functions that stamp out schema structs (`requestEnvelope(kind, data)`,
`responseEnvelope(...)`, `responseHead(...)`) were rejected as useless
indirection. Write each schema out literally, inline. Shared _values_ (a named
`SuccessHead`/`ErrorHead` struct reused by reference) are fine; shared
_functions_ that build schemas are not.

Avoid tiny one-use helpers for simple expression logic. Inline the expression or
move construction onto the domain model when it represents a named concept, like
`TargetAddress.pollAny(...)` instead of ad hoc `{ transport, cast, group, id }`
objects.

Do not model ordinary data containers as classes with private constructors and
static factories. Follow Effect module style: export an interface for the value
and a same-named module/object with constructors and operations.

## 7. Repo lint conventions to know up front

- `switch` statements are banned — use `Match` (`Match.value(...).pipe(Match.discriminatorsExhaustive("kind")({...}))` for wide discriminated unions, `Match.when/whenOr` + `Match.exhaustive` otherwise).
- Direct `undefined` equality is banned — `Predicate.isUndefined`/`isNotUndefined`.
- `instanceof` checks are banned — use Effect predicates like `Predicate.isError`, `Predicate.isTagged`, `Exit.isFailure`, or schema-backed predicates via `SchemaParser.is(...)`.
- `new Error(...)` is banned outside typed-error construction — `Schema.TaggedErrorClass`; suppress with `// ast-grep-ignore: no-new-error` only for native-wire Error reconstruction.
- WARNINGS are failures too. `vp run check` printing "Checks passed" with
  lint/type warnings does not count as green — fix every warning (e.g.
  `no-misused-spread` on Schema class instances: expose a `fields` getter and
  spread that, never the instance).

## 8. Let schemas and Effect combinators own control flow

- For schema-backed tagged unions, prefer
  `Schema.Union([...]).pipe(Schema.toTaggedUnion("_tag" | "kind"))` and use the
  derived `.match` / `.guards` instead of manually checking tags or retyping
  stream filters in handlers.
- Export named schemas for reusable union members (`TaskGetSuccessResponse`,
  `TaskSuspendRefusedResponse`, …). Consumers must not depend on positional
  `.members[0]` / `.members[1]`; keep that wiring at the schema definition site
  or write the member schema as a named value up front.
- Do not encode loops with `Ref` state plus booleans consumed by
  `Effect.repeat`. If each iteration carries the next state, write an
  `Effect.fn` recursion and pass the state as a parameter.
- Use `Option` for absence, not `null` sentinels. Use `Option.filter` for
  follow-up state guards when the value remains optional.
- Use `Effect.onError` and `Effect.ensuring` for cleanup/finalizers instead of
  manually inspecting `Exit` and then removing state. This keeps cleanup
  interruption-safe.
- Repeated background cadence should use `Schedule`/`Effect.schedule`; prefer
  `Duration` arithmetic (`Duration.max`, `Duration.divideUnsafe`) over
  round-tripping through raw milliseconds.
- Repeated domain fallbacks belong on the domain model, e.g.
  `Protocol.promiseOrigin(promise)` instead of retyping the reserved-tag
  fallback rule at call sites.
