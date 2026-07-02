# 02 — Codec (`Codec.ts`)

## Objective

The value-encoding boundary: `ResonateCodec` + `ResonateEncryptor` services with a
default codec byte-compatible with the native TS SDK.

## Dependencies

01 (Value type, EncodingError).

## References

- `docs/DESIGN.md` §4.10 (binding)
- `repos/resonate-sdk-ts/src/codec.ts`, `src/encryptor.ts`, `src/util.ts`
  (`base64Encode`/`base64Decode` chunking) — replicate exactly
- Handbook: `encoding-and-codecs.mdx`

## Key facts

- Native encoding: `undefined` → `{data:"", headers:{}}`; else JSON.stringify with a
  replacer special-casing `Number.POSITIVE_INFINITY`/`NEGATIVE_INFINITY` →
  `"__INF__"`/`"__NEG_INF__"`, `Error` → `{__type:"error", message, stack, name}`,
  `AggregateError` → `{__type:"aggregate_error", message, stack, name, errors}` —
  then base64 of the whole JSON string into `Value.data`. Decode reverses with a
  reviver reconstructing real `Error`/`AggregateError` instances.
- MUST: same codec path for `param` AND `value` including rejections.
- MUST: headers accompany data. Additionally write `resonate:schema` header naming
  the payload schema where known (additive; other SDKs ignore).
- Encryptor: byte-level transform after encode / before decode; `layerNoop` default.
- Schemas sit ABOVE the codec: schema-encode first, plain JSON through the codec.

## Deliverables

- `ResonateCodec` service + `layerJson` (native-compatible default).
- `ResonateEncryptor` service + `layerNoop`.
- Typed `EncodingError` failures tagged with direction (encode/decode) and context.

## Tests

- Byte-compatibility fixtures: encode values here, decode with expectations captured
  from the native codec (numbers, strings, objects, undefined, ±Infinity, Error,
  AggregateError, nested errors) — and the reverse direction.
- Rejection round-trip: an encoded Error decodes to an Error instance with
  message/name/stack preserved.
- Property: for JSON-representable values, decode(encode(v)) deep-equals v.
- Custom encryptor layer applies around the codec in both directions.

## Acceptance

- `vp run check` green; CONFORMANCE.md codec MUST rows → done.
