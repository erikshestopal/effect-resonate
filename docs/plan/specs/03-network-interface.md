# 03 — Network Interface (`Network.ts`)

## Objective

The transport seam: `ResonateNetwork` service (`send` + `messages` Stream + address
helpers) with envelope plumbing (corrId generation/matching, protocol version) so
every transport implementation behaves identically at the boundary.

## Dependencies

1.

## References

- `docs/DESIGN.md` §3.1 (binding)
- `repos/resonate-sdk-ts/src/network/network.ts` (Send/Recv interfaces),
  `src/network/http.ts` (the protocol-status allowlist idea)
- Handbook: `talking-to-the-server.mdx` (envelope, corrId multiplexing)

## Key facts

- `send: <K>(Request<K>) => Effect<Response<K>, TransportError>`; implementations get
  corrId assignment (branded `CorrelationId`, UUID) and response-matching from shared
  helpers in this module, not reimplemented per transport.
- Responses with non-matching corrId or kind → `TransportError("CorrelationMismatch")`.
- Protocol statuses (200, 300, 404, 409, 422, and other spec-listed codes) are DATA —
  returned to layer 2 for interpretation. Only genuine transport failures become
  `TransportError`. 401/403 → `Unauthorized`, never retried.
- `messages: Stream<Protocol.Message, TransportError>` — `execute` and `unblock`.
- `match(WorkerGroup) => TargetAddress`, `unicast: TargetAddress`,
  `anycast(WorkerGroup) => TargetAddress`.

## Deliverables

- `ResonateNetwork` service definition + shared envelope helpers.
- A `TestNetwork` stub implementation (scripted request→response table, manual message
  push) for unit-testing layers 2–4 without the local server — mirrors the role of
  Rust's `StubNetwork`.

## Tests

- Envelope helper: assigns fresh corrIds; carries `version: "2026-04-01"`; rejects
  mismatched corrId/kind responses as CorrelationMismatch.
- TestNetwork: scripted exchange round-trips typed request/response; pushed messages
  arrive on the stream in order.

## Acceptance

- `vp run check` green; CONFORMANCE.md envelope row → partial (interface level).

## Notes (implementation decisions)

- corrId generation lives in `makeRequestHead` (backed by `effect/Crypto`'s
  `randomUUIDv4`; provide `BunCrypto.layer`), used by layer 2 when building
  requests — the native split, where `promises.ts` assigns `randomUUID()` and
  the transport verifies. `checkEnvelope`/`decodeResponse` are the shared
  verification helpers every transport uses.
- `anycast(group)` includes the process id (native derives
  `poll://any@{group}/{pid}`); `match(target)` does not (`poll://any@{target}`)
  — both mirror `network/http.ts` exactly.
- `TestNetwork` (in `testing.ts`) encodes each scripted response through
  `Protocol.ResponseFromWire` and decodes it via `decodeResponse`, so stubbed
  exchanges exercise the exact transport decode/validate path (and stay
  cast-free); it exposes `push` (message stream) and `requests` (send log) as
  its own service alongside `ResonateNetwork`.
