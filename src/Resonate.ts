/**
 * Namespace entry: function, group, layers, client access.
 *
 * See `docs/DESIGN.md` §3.4 (Layer 4 — Function API) and §4 (Public API by Example).
 */
import type { Layer } from "effect";
import type * as HttpClient from "effect/unstable/http/HttpClient";
import * as NetworkHttp from "./NetworkHttp.ts";
import type { ResonateNetwork } from "./Network.ts";

export const layerHttp = (
  options: NetworkHttp.NetworkHttpOptions,
): Layer.Layer<ResonateNetwork, never, HttpClient.HttpClient> => NetworkHttp.layer(options);
