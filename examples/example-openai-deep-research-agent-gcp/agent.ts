import { Duration, Effect, Schema, SchemaParser } from "effect";
import { Resonate, ResonateContext } from "effect-resonate";
export const repo = "example-openai-deep-research-agent-gcp-ts";
export const functionName = "research";
export const sampleArgs = [{ topic: "resonate" }] as const;
export const SearchResult = Schema.Struct({ title: Schema.String, url: Schema.String, snippet: Schema.String });
export const Report = Schema.Struct({
  topic: Schema.String,
  query: Schema.String,
  sources: Schema.Array(SearchResult),
  summary: Schema.String,
});
export const Payload = Schema.Struct({ topic: Schema.String });
export const workflow = Resonate.function({ name: functionName, payload: Payload });
export const App = Resonate.group(workflow);
export const planQuery = (topic: string) => Effect.succeed(`deep research brief for ${topic}`);
export const searchWeb = (query: string) =>
  Effect.succeed([
    SearchResult.make({ title: `${query} source`, url: "memory://search/1", snippet: "deterministic provider result" }),
  ]);
export const writeReport = (topic: string, query: string, sources: ReadonlyArray<typeof SearchResult.Type>) =>
  Effect.succeed(
    Report.make({
      topic,
      query,
      sources: [...sources],
      summary: `Report on ${topic} using ${sources.length} deterministic sources.`,
    }),
  );
export const handlers = App.toLayer(
  App.of({
    [functionName]: (input) =>
      Effect.gen(function* (): Effect.fn.Return<typeof Report.Type, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const query = yield* ctx
          .run({ name: "plan-query", effect: planQuery(input.topic) })
          .pipe(Effect.flatMap(SchemaParser.decodeUnknownEffect(Schema.String)));
        const sources = yield* ctx
          .run({ name: "search-web", effect: searchWeb(query) })
          .pipe(Effect.flatMap(SchemaParser.decodeUnknownEffect(Schema.Array(SearchResult))));
        yield* ctx.sleep({ for: Duration.millis(1) });
        return yield* ctx
          .run({ name: "write-report", effect: writeReport(input.topic, query, sources) })
          .pipe(Effect.flatMap(SchemaParser.decodeUnknownEffect(Report)));
      }),
  }),
);
