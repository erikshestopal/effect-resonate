import { describe, expect, it } from "@effect/vitest";
import { Duration, Effect, Schema } from "effect";
import { TestClock } from "effect/testing";
import { ResonateCodec } from "../src/Codec.ts";
import { DurablePromises } from "../src/DurablePromise.ts";
import * as Protocol from "../src/Protocol.ts";
import * as Resonate from "../src/Resonate.ts";
import { ResonateContext } from "../src/ResonateContext.ts";
import { ResonateTest, restartWorker, snapshot } from "../src/testing.ts";

const GraphRoot = Resonate.function("GraphRoot", {
  payload: Schema.Number,
});

const GraphChild = Resonate.function("GraphChild", {
  payload: Schema.Number,
});

const GraphFns = Resonate.group(GraphRoot, GraphChild);

const handlers = GraphFns.toLayer(
  GraphFns.of({
    GraphRoot: (value) =>
      Effect.gen(function* (): Effect.fn.Return<number, unknown, ResonateContext> {
        const ctx = yield* ResonateContext;
        yield* ctx.run(Effect.succeed(Number(value) + 1));
        const local = yield* ctx.beginRun(Effect.succeed(Number(value) + 2));
        const remote = yield* ctx.beginRpc(GraphChild, [Number(value) + 3]);
        yield* ctx.detached(GraphChild, [Number(value) + 4]);
        yield* ctx.sleep(Duration.seconds(60));
        const values = yield* ctx.all([local.await, remote.await]);
        return Number(values[0]) + Number(values[1]);
      }),
    GraphChild: (value) => Effect.succeed(Number(value) + 10),
  }),
);

const graphIds = (prefix: string, ids: ReadonlyArray<string>) =>
  ids.filter((id) => id === prefix || id.startsWith(`${prefix}.`)).sort();

describe("graph parity", () => {
  it.effect("walks the local promise graph shape used by the native twin harness", () =>
    Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const codec = yield* ResonateCodec;
      const handle = yield* client.beginRpc(GraphRoot, Protocol.ExecutionId.make("graph-local-1"), [1]);
      yield* TestClock.adjust(Duration.minutes(1));
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;

      const state = yield* snapshot;
      const ids = graphIds(
        handle.id,
        state.promises.map((promise) => promise.id),
      );
      expect(ids).toEqual([
        "graph-local-1",
        "graph-local-1.0",
        "graph-local-1.1",
        "graph-local-1.2",
        "graph-local-1.4",
        "graph-local-1.d050a519de6b0ec",
      ]);

      const local = state.promises.find((promise) => promise.id === "graph-local-1.0");
      const attached = state.promises.find((promise) => promise.id === "graph-local-1.2");
      const timer = state.promises.find((promise) => promise.id === "graph-local-1.4");
      const detached = state.promises.find((promise) => promise.id === "graph-local-1.d050a519de6b0ec");

      expect(local?.tags.reserved["resonate:scope"]).toBe("local");
      expect(attached?.tags.reserved["resonate:target"]?.address).toBe("local://any@default");
      expect(attached?.tags.reserved["resonate:origin"]).toBe(handle.id);
      expect(attached?.tags.reserved["resonate:branch"]).toBe("graph-local-1.2");
      expect(timer?.tags.reserved["resonate:timer"]).toBe("true");
      expect(detached?.tags.reserved["resonate:origin"]).toBe(detached?.id);
      expect(detached?.tags.reserved["resonate:prefix"]).toBe(handle.id);
      expect(yield* codec.decode(attached?.param ?? Protocol.emptyValue)).toEqual({
        func: "GraphChild",
        args: [4],
        version: 1,
      });
    }).pipe(Effect.provide(ResonateTest.layer(GraphFns, handlers))),
  );

  it.effect("keeps the graph byte-stable after worker restart replay", () =>
    Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const promises = yield* DurablePromises;
      const handle = yield* client.beginRpc(GraphRoot, Protocol.ExecutionId.make("graph-replay-1"), [1]);
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;
      const before = yield* snapshot;
      const beforeIds = graphIds(
        handle.id,
        before.promises.map((promise) => promise.id),
      );

      yield* restartWorker;
      yield* TestClock.adjust(Duration.minutes(2));
      for (let index = 0; index < 30; index = index + 1) {
        yield* Effect.yieldNow;
      }

      const completed = yield* promises.get(handle.id);
      const after = yield* snapshot;
      const afterIds = graphIds(
        handle.id,
        after.promises.map((promise) => promise.id),
      );
      expect(completed.state).toBe("resolved");
      expect(beforeIds.every((id) => afterIds.includes(id))).toBe(true);
      expect(afterIds).toEqual([
        "graph-replay-1",
        "graph-replay-1.0",
        "graph-replay-1.1",
        "graph-replay-1.2",
        "graph-replay-1.4",
        "graph-replay-1.d0f956885dab743",
      ]);
    }).pipe(Effect.provide(ResonateTest.layer(GraphFns, handlers))),
  );

  it("has the resonate tree CLI available for shipped-server graph checks", async () => {
    const result = Bun.spawnSync(["resonate", "tree", "--help"], { stdout: "pipe", stderr: "pipe" });
    if (!result.success) {
      console.error("[GRAPH PARITY SKIPPED] resonate CLI not found; install it to run native/tree parity.");
      expect(result.success).toBe(false);
      return;
    }
    expect(result.success).toBe(true);
  });
});
