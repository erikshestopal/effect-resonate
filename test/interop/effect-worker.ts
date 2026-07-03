import { BunRuntime } from "@effect/platform-bun";
import { Duration, Effect, Layer, Schema } from "effect";
import * as Protocol from "../../src/Protocol.ts";
import * as Resonate from "../../src/Resonate.ts";
import { ResonateContext } from "../../src/ResonateContext.ts";
import * as Worker from "../../src/Worker.ts";

const url = process.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Protocol.WorkerGroup.make(process.env.RESONATE_GROUP ?? "default");
const pid = Protocol.ProcessId.make(process.env.RESONATE_PID ?? "effect-worker");

const Countdown = Resonate.function("Countdown", { payload: Schema.Tuple([Schema.Number, Schema.Number]) });
const EffectEcho = Resonate.function("EffectEcho", { payload: Schema.String });
const EffectCallsNative = Resonate.function("EffectCallsNative", { payload: Schema.String });
const EffectAwaitsExternal = Resonate.function("EffectAwaitsExternal", { payload: Schema.String });

const Approval = Resonate.promise("approval", { success: Schema.String });

const Fns = Resonate.group(Countdown, EffectEcho, EffectCallsNative, EffectAwaitsExternal);

const handlers = Fns.toLayer(
  Fns.of({
    Countdown: (count, seconds) =>
      Effect.gen(function* (): Effect.fn.Return<string, unknown, ResonateContext> {
        const ctx = yield* ResonateContext;
        for (let remaining = count; remaining > 0; remaining = remaining - 1) {
          yield* ctx.run(Effect.sync(() => console.log(`Countdown: ${remaining}`)));
          yield* ctx.sleep(Duration.seconds(seconds));
        }
        return "done";
      }),
    EffectEcho: (value) => Effect.succeed(`effect:${value}`),
    EffectCallsNative: (value) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext> {
        const ctx = yield* ResonateContext;
        return yield* ctx.rpc("NativeEcho", [value], { target: group });
      }),
    EffectAwaitsExternal: () =>
      Effect.gen(function* (): Effect.fn.Return<string, unknown, ResonateContext> {
        const ctx = yield* ResonateContext;
        return yield* (yield* ctx.promise(Approval)).await;
      }),
  }),
);

BunRuntime.runMain(
  Layer.launch(Worker.layerHttp(Fns, { url, group, pid, ttl: Duration.seconds(5) }).pipe(Layer.provideMerge(handlers))),
);
