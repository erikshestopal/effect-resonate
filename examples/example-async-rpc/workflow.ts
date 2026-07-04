import { Config, Effect, Schema } from "effect";
import { Protocol, Resonate } from "effect-resonate";

export const repo = "example-async-rpc-ts";
export const functionName = "foo";
export const sampleArgs = ["ping"] as const;

const Unit = Schema.Tuple([]);
const NumberArg = Schema.Tuple([Schema.Finite]);
export const foo = Resonate.function({ name: "foo", payload: Schema.Tuple([Schema.String]) });
export const bar = Resonate.function({ name: "bar", payload: Unit });
export const baz = Resonate.function({ name: "baz", payload: Unit });
export const qux = Resonate.function({ name: "qux", payload: NumberArg });
export const quz = Resonate.function({ name: "quz", payload: NumberArg });
export const cog = Resonate.function({ name: "cog", payload: NumberArg });
export const zim = Resonate.function({ name: "zim", payload: NumberArg });
export const rax = Resonate.function({ name: "rax", payload: Unit });
export const dop = Resonate.function({ name: "dop", payload: Unit });

export const App = Resonate.group(foo, bar, baz, qux, quz, cog, zim, rax, dop);

export const targetGroup = (name: string) =>
  Config.string("RESONATE_GROUP").pipe(
    Config.withDefault(name),
    Effect.map((group) => Protocol.WorkerGroup.make(group)),
  );

export const handlers = App.toLayer(
  App.of({
    foo: (_input) =>
      Effect.gen(function* (): Effect.fn.Return<number, unknown, Resonate.Context> {
        const ctx = yield* Resonate.Context;
        yield* ctx.run({ name: "log-foo", effect: Effect.logInfo("running function foo") });
        const result = yield* ctx
          .rpc({ target: bar, args: [], options: { target: yield* targetGroup("service-b") } })
          .pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Finite)));
        return result + 1;
      }),
    bar: () =>
      Effect.gen(function* (): Effect.fn.Return<number, unknown, Resonate.Context> {
        const ctx = yield* Resonate.Context;
        yield* ctx.run({ name: "log-bar", effect: Effect.logInfo("running function bar") });
        const result = yield* ctx
          .rpc({ target: baz, args: [], options: { target: yield* targetGroup("service-c") } })
          .pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Finite)));
        return result + 1;
      }),
    baz: () =>
      Effect.gen(function* (): Effect.fn.Return<number, unknown, Resonate.Context> {
        const ctx = yield* Resonate.Context;
        yield* ctx.run({ name: "log-baz", effect: Effect.logInfo("running function baz") });
        return 1;
      }),
    qux: (arg) =>
      Effect.gen(function* (): Effect.fn.Return<void, unknown, Resonate.Context> {
        const ctx = yield* Resonate.Context;
        yield* ctx.run({ name: "log-qux", effect: Effect.logInfo("running function qux") });
        yield* ctx.detached({ target: quz, args: [arg + 1], options: { target: yield* targetGroup("service-e") } });
      }),
    quz: (arg) =>
      Effect.gen(function* (): Effect.fn.Return<void, unknown, Resonate.Context> {
        const ctx = yield* Resonate.Context;
        yield* ctx.run({ name: "log-quz", effect: Effect.logInfo("running function quz") });
        yield* ctx.detached({ target: cog, args: [arg + 1], options: { target: yield* targetGroup("service-f") } });
      }),
    cog: (arg) =>
      Effect.gen(function* (): Effect.fn.Return<void, unknown, Resonate.Context> {
        const ctx = yield* Resonate.Context;
        yield* ctx.run({ name: "log-cog", effect: Effect.logInfo(`running function cog ${arg}`) });
      }),
    zim: (arg) =>
      Effect.gen(function* (): Effect.fn.Return<number, unknown, Resonate.Context> {
        const ctx = yield* Resonate.Context;
        yield* ctx.run({ name: "log-zim", effect: Effect.logInfo("running function zim") });
        const futureRax = yield* ctx.beginRpc({
          target: rax,
          args: [],
          options: { target: yield* targetGroup("service-h") },
        });
        const futureDop = yield* ctx.beginRpc({
          target: dop,
          args: [],
          options: { target: yield* targetGroup("service-i") },
        });
        const resultRax = yield* futureRax.await.pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Finite)));
        const resultDop = yield* futureDop.await.pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Finite)));
        return resultRax + resultDop + arg;
      }),
    rax: () =>
      Effect.gen(function* (): Effect.fn.Return<number, unknown, Resonate.Context> {
        const ctx = yield* Resonate.Context;
        yield* ctx.run({ name: "log-rax", effect: Effect.logInfo("running function rax") });
        return 1;
      }),
    dop: () =>
      Effect.gen(function* (): Effect.fn.Return<number, unknown, Resonate.Context> {
        const ctx = yield* Resonate.Context;
        yield* ctx.run({ name: "log-dop", effect: Effect.logInfo("running function dop") });
        return 1;
      }),
  }),
);
