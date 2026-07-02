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

## 3. Process

- Run `vp run check` BEFORE every commit — including docs-only changes. Never
  rely on the pre-commit hook to catch it.

## 4. General

- Consult `repos/effect-smol/SCHEMA.md` for the v4 Schema API before inventing
  a mechanism — the building block usually exists (flipped constructors,
  decoding defaults, template literals, `OptionFromOptionalKey`, …).
- Keep native-SDK behavioral quirks (truthiness coercions, `Object.assign`
  marker copying) inside the Schema transformation functions, documented with
  a comment pointing at the native source.
