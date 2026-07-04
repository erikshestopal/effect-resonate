/**
 * Invocation payload schemas shared by clients, schedules, and workers.
 *
 * @since 0.0.0
 */
import { Schema } from "effect";
import * as Protocol from "./Protocol.ts";
import * as RetryPolicy from "./RetryPolicy.ts";

/**
 * Encoded durable function invocation payload.
 *
 * @category schemas
 * @since 0.0.0
 */
export const InvocationParam = Schema.Struct({
  func: Schema.String,
  args: Schema.Array(Schema.Unknown),
  version: Protocol.FunctionVersionFromWire,
  retry: Schema.optionalKey(RetryPolicy.RetryPolicyFromWire),
});
