# 07 — HTTP Transport (`NetworkHttp.ts`)

## Objective

The production transport: single-endpoint HTTP POST for `send`, SSE long-poll
stream for `messages`, with mandatory reconnect backoff.

## Dependencies

3.  (Parallel-safe with 04–06.)

## References

- `repos/resonate-sdk-ts/src/network/http.ts` (`HttpNetwork`, `PollMessageSource`) —
  wire behavior to replicate
- Handbook: `talking-to-the-server.mdx`, `production-concerns.mdx`
- effect-smol `HttpClient` + `ai-docs/src/50_http-client`, `Stream` docs

## Key facts

- `send`: POST the whole request JSON to `${url}` (one endpoint; `kind` dispatches
  server-side). Allowlist of protocol statuses (200, 300, 404, 409, 422, 501) passes
  through as typed responses; anything else — network error, malformed JSON, missing
  corrId match, unexpected status — normalizes to `TransportError` (native treats these
  as platform failures). 401/403 → `Unauthorized`, terminal, never retried (deliberate
  fix of native's retriable-auth gap — documented in DESIGN.md §6).
- `messages`: SSE `GET ${url}/poll/{group}/{pid}` (`data: {json}` frames decoding to
  `execute`/`unblock`). Reconnect with `Schedule.exponential` (1s base, ~30s cap,
  reset on success). Model as a `Stream` that never terminates on connection loss —
  only on scope close.
- Config via `Config`: url, auth token (`Redacted`), group, pid.

## Deliverables

- `NetworkHttp.layer` implementing `ResonateNetwork` over effect `HttpClient` +
  an SSE frame decoder (no external eventsource dependency; parse the byte stream).
- `Resonate.layerHttp(config)` public wiring per DESIGN.md §4.9.

## Tests

- Against a scripted in-process HTTP test server: request envelope round-trip;
  each allowlisted status passes through typed; 500/garbage → TransportError;
  401 → Unauthorized without retry.
- SSE: frames decode to typed messages; dropped connection → reconnect with backoff
  (TestClock-observable delays); backoff resets after a successful frame.

## Acceptance

- `vp run check` green; CONFORMANCE.md envelope + SSE-backoff rows → done.

## Notes

- `NetworkHttp.layer` is an Effect `HttpClient` transport. `send` posts the encoded
  protocol request to the configured URL, lets only protocol statuses
  `200/300/404/409/422/501` reach `decodeResponse`, maps `401/403` to terminal
  `Unauthorized`, and normalizes other HTTP/platform/body failures to
  `TransportError`.
- The poll stream uses a real Bun in-process server in tests (`BunHttpServer` +
  `BunHttpClient`), per maintainer direction; no Node HTTP server or recorder is
  used. Vite+'s `check:test` task now runs Vitest through `bunx --bun` so Bun globals
  and platform layers are available.
- SSE parsing is byte-stream based: decode UTF-8, split lines, keep `data:` frames
  with `Filter.fromPredicateOption`, then decode frame JSON via
  `Schema.fromJsonString(Protocol.Message)`. This follows the v4 Schema/Filter APIs
  rather than hand-rolled guards.
- Poll `401/403` fails terminally and is not retried. Other poll connection/status
  failures retry with Effect `Schedule.exponential(Duration.seconds(1))`, capped at
  30s; the test observes the retry under `TestClock`.
