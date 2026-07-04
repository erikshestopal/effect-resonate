import { Effect, Schema } from "effect";
import { Resonate, ResonateContext } from "effect-resonate";
export const repo = "example-hello-world-ts";
export const functionName = "foo";
export const sampleArgs = ["World"] as const;
export const Payload = Schema.String;
export const Greeting = Schema.String;
const bar = (greetee: string) => Effect.logInfo("running bar").pipe(Effect.as(`Hello ${greetee} from bar!`));
const baz = (greetee: string) => Effect.logInfo("running baz").pipe(Effect.as(`Hello ${greetee} from baz!`));
export const workflow = Resonate.function({ name: functionName, payload: Payload });
export const App = Resonate.group(workflow);
export const handlers = App.toLayer(
  App.of({
    [functionName]: (greetee) =>
      Effect.gen(function* (): Effect.fn.Return<string, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        yield* Effect.logInfo("running foo");
        const fooGreeting = `Hello ${greetee} from foo!`;
        const barGreeting = yield* ctx.run({ name: "bar", effect: bar(greetee) });
        const bazGreeting = yield* ctx.run({ name: "baz", effect: baz(greetee) });
        return `${fooGreeting} ${barGreeting} ${bazGreeting}`;
      }),
  }),
);
