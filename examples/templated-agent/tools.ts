import { Effect } from "effect";
import { ToolCall } from "./types.ts";
export const lookup = (input: string) =>
  Effect.succeed(ToolCall.make({ tool: "lookup", input, output: `facts about ${input}` }));
export const calculate = (input: string) =>
  Effect.succeed(ToolCall.make({ tool: "calculate", input, output: `${input.length}` }));
