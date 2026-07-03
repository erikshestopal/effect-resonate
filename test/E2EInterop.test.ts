import { describe, expect, it } from "@effect/vitest";
import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunHttpClient from "@effect/platform-bun/BunHttpClient";
import { Duration, Effect, Layer, Schema } from "effect";
import { ResonateCodec, ResonateEncryptor } from "../src/Codec.ts";
import { DurablePromises } from "../src/DurablePromise.ts";
import * as NetworkHttp from "../src/NetworkHttp.ts";
import * as Protocol from "../src/Protocol.ts";
import * as Resonate from "../src/Resonate.ts";
import { ExecutionEngine, ResonateContext } from "../src/ResonateContext.ts";
import { Schedules } from "../src/Schedule.ts";
import { Tasks } from "../src/Task.ts";
import * as Worker from "../src/Worker.ts";

const serverUrl = "http://127.0.0.1:8001";

const Countdown = Resonate.function("Countdown", {
  payload: Schema.Tuple([Schema.Number, Schema.Number]),
});

const Echo = Resonate.function("InteropEcho", {
  payload: Schema.String,
});

const E2EFns = Resonate.group(Countdown, Echo);

const handlers = E2EFns.toLayer(
  E2EFns.of({
    Countdown: (count, seconds) =>
      Effect.gen(function* (): Effect.fn.Return<string, unknown, ResonateContext> {
        const ctx = yield* ResonateContext;
        for (let remaining = count; remaining > 0; remaining = remaining - 1) {
          yield* ctx.sleep(Duration.seconds(seconds));
        }
        return "done";
      }),
    InteropEcho: (value) => Effect.succeed(`echo:${value}`),
  }),
);

const liveLayer = (group: Protocol.WorkerGroup, pid: Protocol.ProcessId) => {
  const base = Layer.mergeAll(
    NetworkHttp.layer({ url: serverUrl, group, pid }).pipe(Layer.provide(BunHttpClient.layer)),
    BunCrypto.layer,
    ResonateEncryptor.layerNoop,
  );
  const core = Layer.mergeAll(
    ResonateCodec.layerJson,
    DurablePromises.layer,
    Tasks.layer,
    Schedules.layer,
    handlers,
  ).pipe(Layer.provideMerge(base));
  const engine = ExecutionEngine.layer.pipe(Layer.provideMerge(core));
  const client = Resonate.ResonateClient.layer({ group, pid, ttl: Duration.seconds(5) }).pipe(
    Layer.provideMerge(engine),
  );
  return Worker.layer(E2EFns, { group, pid, ttl: Duration.seconds(5) }).pipe(Layer.provideMerge(client));
};

const withResonateDev = <A, E>(effect: Effect.Effect<A, E>): Effect.Effect<A, E> =>
  Effect.acquireUseRelease(
    Effect.sync(() => Bun.spawn(["resonate", "dev"], { stdout: "pipe", stderr: "pipe" })),
    () => Effect.sleep(Duration.seconds(2)).pipe(Effect.andThen(effect)),
    (process) => Effect.sync(() => process.kill()),
  );

describe("E2E interop", () => {
  it.live(
    "runs shipped-server quickstart and interop gate when resonate is installed",
    () =>
      Effect.gen(function* () {
        const resonate = Bun.which("resonate");
        if (resonate === null) {
          console.warn("[E2E SKIPPED] resonate CLI not found; install it to run shipped-server interop.");
          expect(resonate).toBeNull();
          return;
        }

        const group = Protocol.WorkerGroup.make(`e2e-${Date.now()}`);
        const pid = Protocol.ProcessId.make("worker-1");
        yield* withResonateDev(
          Effect.gen(function* () {
            const client = yield* Resonate.ResonateClient;
            const countdown = yield* client.beginRpc(
              Countdown,
              Protocol.ExecutionId.make(`${group}-countdown`),
              [1, 1],
            );
            const echo = yield* client.beginRpc(Echo, Protocol.ExecutionId.make(`${group}-echo`), ["ok"]);

            yield* Effect.sleep(Duration.seconds(3));
            expect(yield* countdown.await).toBe("done");
            expect(yield* echo.await).toBe("echo:ok");
          }).pipe(Effect.provide(liveLayer(group, pid))),
        );
      }),
    30_000,
  );
});
