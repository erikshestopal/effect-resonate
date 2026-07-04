import { Schema } from "effect";
export const AgentRequest = Schema.Struct({ prompt: Schema.String });
export const ToolCall = Schema.Struct({ tool: Schema.String, input: Schema.String, output: Schema.String });
export const AgentResponse = Schema.Struct({
  prompt: Schema.String,
  tools: Schema.Array(ToolCall),
  answer: Schema.String,
});
