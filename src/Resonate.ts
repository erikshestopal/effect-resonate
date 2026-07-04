/**
 * High-level API for defining and invoking durable Resonate functions.
 *
 * This module contains the public authoring surface: define typed function
 * declarations with {@link function}, group them into handler registries with
 * {@link group}, create schedules and external promises, and use
 * {@link Client} to start, await, resolve, reject, or cancel durable
 * executions.
 *
 * @example
 * ```ts
 * import { Effect, Schema } from "effect"
 * import { Resonate } from "effect-resonate"
 *
 * const greet = Resonate.function({
 *   name: "greet",
 *   payload: Schema.String
 * })
 *
 * const App = Resonate.group(greet)
 *
 * const handlers = App.toLayer(
 *   App.of({
 *     greet: (name) => Effect.succeed(`Hello, ${name}!`)
 *   })
 * )
 * ```
 *
 * @since 0.0.0
 */
import { Layer } from "effect";
import type * as HttpClient from "effect/unstable/http/HttpClient";
import * as NetworkHttp from "./network/http.ts";
import { ResonateNetwork } from "./network/network.ts";

export * as Worker from "./Worker.ts";

/**
 * Builds the HTTP network layer for a Resonate server.
 *
 * The layer requires an Effect `HttpClient` implementation from the application
 * runtime, such as Bun, Node, or another platform package.
 *
 * @category layers
 * @since 0.0.0
 */
export const layerHttp = (
  options: NetworkHttp.NetworkHttpOptions,
): Layer.Layer<ResonateNetwork, never, HttpClient.HttpClient> => NetworkHttp.layer(options);

export {
  defineFunction,
  defineFunction as function,
  group,
  Handler,
  type AnyFunction,
  type Definition,
  type FunctionGroup,
  type HandlerFunction,
  type HandlersFrom,
  type PayloadArgs,
} from "./FunctionDefinition.ts";
export { Registry } from "./Registry.ts";
export type { RegistryItem } from "./Registry.ts";
export { promise } from "./PromiseDefinition.ts";
export type { PromiseDeclaration, PromiseFailure, PromiseSuccess } from "./PromiseDefinition.ts";
export { schedule } from "./ScheduleDefinition.ts";
export type { ScheduleOptions, ScheduleValue } from "./ScheduleDefinition.ts";
export { ResonateClient as Client } from "./ResonateClient.ts";
export type {
  AwaitInvocationMethods,
  ResonateClientOptions as ClientOptions,
  ResonateClientService as ClientService,
  DurableHandle,
  InvocationMethods,
  InvocationOptions,
} from "./ResonateClient.ts";
