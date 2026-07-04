/**
 * External durable promise declarations.
 *
 * @since 0.0.0
 */
import { Schema } from "effect";
import * as Protocol from "./Protocol.ts";

/**
 * Schema-backed declaration for an externally resolvable durable promise.
 *
 * @category models
 * @since 0.0.0
 */
export interface PromiseDeclaration<
  Name extends string = string,
  Success extends Schema.Codec<unknown, unknown, never, never> = Schema.Codec<unknown, unknown, never, never>,
  Failure extends Schema.Codec<unknown, unknown, never, never> | undefined =
    | Schema.Codec<unknown, unknown, never, never>
    | undefined,
> {
  readonly name: Name;
  readonly success: Success;
  readonly error: Failure;
  readonly id: (executionId: Protocol.ExecutionId | Protocol.PromiseId) => Protocol.PromiseId;
}

export type PromiseSuccess<P extends PromiseDeclaration> = P["success"]["Type"];
export type PromiseFailure<P extends PromiseDeclaration> =
  P["error"] extends Schema.Codec<unknown, unknown, never, never> ? P["error"]["Type"] : never;

/**
 * Defines an externally resolvable durable promise.
 *
 * @category constructors
 * @since 0.0.0
 */
export function promise<
  const Name extends string,
  Success extends Schema.Codec<unknown, unknown, never, never>,
>(options: { readonly name: Name; readonly success: Success }): PromiseDeclaration<Name, Success, undefined>;
export function promise<
  const Name extends string,
  Success extends Schema.Codec<unknown, unknown, never, never>,
  Failure extends Schema.Codec<unknown, unknown, never, never>,
>(options: {
  readonly name: Name;
  readonly success: Success;
  readonly error: Failure;
}): PromiseDeclaration<Name, Success, Failure>;
export function promise<
  const Name extends string,
  Success extends Schema.Codec<unknown, unknown, never, never>,
  Failure extends Schema.Codec<unknown, unknown, never, never>,
>(options: { readonly name: Name; readonly success: Success; readonly error?: Failure }) {
  return {
    name: options.name,
    success: options.success,
    error: options.error,
    id: (executionId: Protocol.ExecutionId | Protocol.PromiseId) =>
      Protocol.PromiseId.make(`${executionId}.${options.name}`),
  };
}
