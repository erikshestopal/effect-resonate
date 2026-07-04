import { Effect, Schema } from "effect";
export const AgentOutput = Schema.Struct({ role: Schema.String, content: Schema.String });
export class AgentError extends Schema.TaggedErrorClass<AgentError>()("AgentError", {
  role: Schema.String,
  topic: Schema.String,
}) {}
export const researcher = (topic: string) =>
  Effect.logInfo(`[researcher] ${topic}`).pipe(
    Effect.as(
      AgentOutput.make({
        role: "researcher",
        content: `1. ${topic} has durable workflows. 2. Deterministic replay matters. 3. Effects model failures.`,
      }),
    ),
  );
export const writer = (topic: string, research: string, crashOnWriter: boolean) =>
  Effect.gen(function* () {
    yield* Effect.logInfo(`[writer] ${topic}`);
    if (crashOnWriter) return yield* new AgentError({ role: "writer", topic });
    return AgentOutput.make({
      role: "writer",
      content: `# ${topic}

${research}

A concise article draft composed from the findings.`,
    });
  });
export const reviewer = (draft: string) =>
  Effect.logInfo("[reviewer] draft").pipe(
    Effect.as(
      AgentOutput.make({
        role: "reviewer",
        content: `APPROVED: draft length ${draft.length} is suitable for publication.`,
      }),
    ),
  );
