import { BunRuntime } from "@effect/platform-bun";
import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunHttpClient from "@effect/platform-bun/BunHttpClient";
import { Config, Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate } from "effect-resonate";

export const repo = "example-bluesky-scraper-ts";
export const functionName = "scrape";
export const sampleArgs = ["resonatehq.bsky.social", 1] as const;
// Invoke after starting this worker: resonate invoke --server http://127.0.0.1:8001 --target poll://any@example-bluesky-scraper-ts --func scrape --json-args '["resonatehq.bsky.social",1]' example-bluesky-scraper-ts-demo

const Payload = Schema.Tuple([Schema.String, Schema.Finite]);
const workflow = Resonate.function({ name: functionName, payload: Payload });
const App = Resonate.group(workflow);

const handlers = App.toLayer(
  App.of({
    [functionName]: (actor, depth) =>
      Effect.gen(function* () {
        const ctx = yield* Resonate.Context;
        const profile = { did: `did:plc:${actor}`, handle: actor };
        yield* ctx.run({ effect: Effect.logInfo(`fetched profile ${actor}`) });
        const followers: Array<string> = [];
        if (depth > 0) {
          for (let page = 0; page < 2; page += 1) {
            yield* ctx.run({ effect: Effect.logInfo(`fetched followers page ${page + 1} for ${profile.handle}`) });
            followers.push(`${profile.did}:follower:${page + 1}`);
            yield* ctx.sleep({ for: Duration.millis(500) });
          }
        }
        return { profile, followersQueued: followers };
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
    return Resonate.Worker.layerHttp({ group: App, http: { url, group, pid, ttl: Duration.seconds(30) } }).pipe(
      Layer.provideMerge(handlers),
      Layer.provideMerge(BunHttpClient.layer),
      Layer.provideMerge(BunCrypto.layer),
    );
  }),
);

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(worker));
}
