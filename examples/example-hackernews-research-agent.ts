import { BunRuntime } from "@effect/platform-bun";
import { Config, Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

export const repo = "example-hackernews-research-agent-ts";
export const functionName = "scanKeyword";
export const sampleArgs = ["durable execution", []] as const;
// Invoke after starting this worker: resonate invoke --server http://127.0.0.1:8001 --target poll://any@example-hackernews-research-agent-ts --func scanKeyword --json-args '["durable execution",[]]' example-hackernews-research-agent-ts-demo

const Payload = Schema.Tuple([Schema.String, Schema.Array(Schema.String)]);
const workflow = Resonate.function({ name: functionName, payload: Payload });
const App = Resonate.group(workflow);

const handlers = App.toLayer(
  App.of({
    [functionName]: (keyword, seenIds) =>
      Effect.gen(function* () {
        const ctx = yield* ResonateContext.ResonateContext;
        const slackWebhook = yield* Config.string("SLACK_WEBHOOK").pipe(Config.withDefault(""));
        yield* Config.string("OPENAI_API_KEY").pipe(Config.withDefault(""));
        yield* ctx.run({ effect: Effect.logInfo(`searched Hacker News for ${keyword}`) });
        const stories = [{ objectID: "1", title: `${keyword} on Hacker News`, url: "" }];
        const newlyAnalyzed: Array<unknown> = [];
        for (const story of stories.filter((candidate) => !seenIds.includes(candidate.objectID))) {
          yield* ctx.run({ effect: Effect.logInfo(`analyzed ${story.objectID}`) });
          newlyAnalyzed.push({
            storyId: story.objectID,
            title: story.title,
            url: story.url,
            hnUrl: `https://news.ycombinator.com/item?id=${story.objectID}`,
          });
        }
        if (slackWebhook.length > 0 && newlyAnalyzed.length > 0) {
          yield* ctx.run({ effect: Effect.logInfo(`notified Slack ${slackWebhook}`) });
        }
        return { keyword, storiesFound: stories.length, newlyAnalyzed };
      }),
  }),
);

const worker = Layer.unwrap(
  Effect.gen(function* () {
    const url = yield* Config.string("RESONATE_URL").pipe(Config.withDefault("http://127.0.0.1:8001"));
    const groupName = yield* Config.string("RESONATE_GROUP").pipe(Config.withDefault(repo));
    const pidName = yield* Config.string("RESONATE_PID").pipe(Config.withDefault(`${repo}-worker`));
    const group = Protocol.WorkerGroup.make(groupName);
    const pid = Protocol.ProcessId.make(pidName);
    return Worker.layerHttp({ group: App, http: { url, group, pid, ttl: Duration.seconds(30) } }).pipe(
      Layer.provideMerge(handlers),
    );
  }),
);

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(worker));
}
