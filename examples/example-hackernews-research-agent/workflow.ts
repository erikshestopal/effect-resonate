import { Config, Duration, Effect, Schema, SchemaParser } from "effect";
import { Resonate, ResonateContext } from "effect-resonate";
import { analyzeStory, notifySlack, searchStories } from "./agent.ts";
import { Analysis, ScanResult, Story } from "./types.ts";
export const repo = "example-hackernews-research-agent-ts";
export const functionName = "scanKeyword";
export const sampleArgs = ["durable execution", []] as const;
export const Payload = Schema.Tuple([Schema.String, Schema.Array(Schema.String)]);
export const workflow = Resonate.function({ name: functionName, payload: Payload });
export const App = Resonate.group(workflow);
export const handlers = App.toLayer(
  App.of({
    [functionName]: (keyword, seenIds) =>
      Effect.gen(function* (): Effect.fn.Return<typeof ScanResult.Type, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const slackWebhook = yield* Config.string("SLACK_WEBHOOK").pipe(Config.withDefault(""));
        const stories = yield* ctx
          .run({ name: "search-stories", effect: searchStories(keyword) })
          .pipe(Effect.flatMap(SchemaParser.decodeUnknownEffect(Schema.Array(Story))));
        const newlyAnalyzed: Array<typeof Analysis.Type> = [];
        for (const story of stories.filter((candidate) => !seenIds.includes(candidate.objectID))) {
          newlyAnalyzed.push(
            yield* ctx
              .run({ name: `analyze-${story.objectID}`, effect: analyzeStory(keyword, story) })
              .pipe(Effect.flatMap(SchemaParser.decodeUnknownEffect(Analysis))),
          );
        }
        if (slackWebhook.length > 0 && newlyAnalyzed.length > 0) {
          yield* ctx.run({ name: "notify-slack", effect: notifySlack(newlyAnalyzed.length) });
        }
        yield* ctx.sleep({ for: Duration.millis(1) });
        return ScanResult.make({ keyword, storiesFound: stories.length, newlyAnalyzed });
      }),
  }),
);
