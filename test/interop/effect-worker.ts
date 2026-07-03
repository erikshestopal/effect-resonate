import { BunRuntime } from "@effect/platform-bun";
import { Config, Duration, Effect, Layer, Schema } from "effect";
import * as Protocol from "../../src/Protocol.ts";
import * as Resonate from "../../src/Resonate.ts";
import { ResonateContext } from "../../src/ResonateContext.ts";
import * as Worker from "../../src/Worker.ts";

const Countdown = Resonate.function({ name: "Countdown", payload: Schema.Tuple([Schema.Number, Schema.Number]) });
const EffectEcho = Resonate.function({ name: "EffectEcho", payload: Schema.String });
const EffectCallsNative = Resonate.function({ name: "EffectCallsNative", payload: Schema.String });
const EffectAwaitsExternal = Resonate.function({ name: "EffectAwaitsExternal", payload: Schema.String });

const Approval = Resonate.promise({ name: "approval", success: Schema.String });

const Fns = Resonate.group(Countdown, EffectEcho, EffectCallsNative, EffectAwaitsExternal);

const worker = Layer.unwrap(
  Effect.gen(function* () {
    const url = yield* Config.string("RESONATE_URL").pipe(Config.withDefault("http://127.0.0.1:8001"));
    const groupName = yield* Config.string("RESONATE_GROUP").pipe(Config.withDefault("default"));
    const pidName = yield* Config.string("RESONATE_PID").pipe(Config.withDefault("effect-worker"));
    const group = Protocol.WorkerGroup.make(groupName);
    const pid = Protocol.ProcessId.make(pidName);
    const handlers = Fns.toLayer(
      Fns.of({
        Countdown: (count, seconds) =>
          Effect.gen(function* (): Effect.fn.Return<string, unknown, ResonateContext> {
            const ctx = yield* ResonateContext;
            for (let remaining = count; remaining > 0; remaining = remaining - 1) {
              yield* ctx.run({ effect: Effect.logInfo(`Countdown: ${remaining}`) });
              yield* ctx.sleep(Duration.seconds(seconds));
            }
            return "done";
          }),
        EffectEcho: (value) => Effect.succeed(`effect:${value}`),
        EffectCallsNative: (value) =>
          Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext> {
            const ctx = yield* ResonateContext;
            return yield* ctx.rpc({ target: "NativeEcho", args: [value], options: { target: group } });
          }),
        EffectAwaitsExternal: () =>
          Effect.gen(function* (): Effect.fn.Return<string, unknown, ResonateContext> {
            const ctx = yield* ResonateContext;
            return yield* (yield* ctx.promise({ declaration: Approval })).await;
          }),
      }),
    );
    return Worker.layerHttp({ group: Fns, http: { url, group, pid, ttl: Duration.seconds(5) } }).pipe(
      Layer.provideMerge(handlers),
    );
  }),
);

BunRuntime.runMain(Layer.launch(worker));
