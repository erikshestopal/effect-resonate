import { Duration, Effect, Schema, SchemaParser } from "effect";
import { Resonate, ResonateContext } from "effect-resonate";

export const repo = "example-countdown-cloudflare-ts";
export const functionName = "countdown";
export const sampleArgs = [2, 1, "https://ntfy.sh/effect-resonate-demo"] as const;

export const Payload = Schema.Tuple([Schema.Finite, Schema.Finite, Schema.String]);
export const Notification = Schema.Struct({ url: Schema.String, message: Schema.String });
export const CountdownResult = Schema.Struct({ notifications: Schema.Array(Notification) });

const notify = (url: string, message: string) =>
  Effect.logInfo(`notify: ${message}`).pipe(Effect.as(Notification.make({ url, message })));

export const workflow = Resonate.function({ name: functionName, payload: Payload });
export const App = Resonate.group(workflow);
export const handlers = App.toLayer(
  App.of({
    [functionName]: (count, delay, url) =>
      Effect.gen(function* (): Effect.fn.Return<typeof CountdownResult.Type, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const notifications: Array<typeof Notification.Type> = [];
        for (let index = count; index > 0; index = index - 1) {
          notifications.push(
            yield* ctx
              .run({ name: `notify-${index}`, effect: notify(url, `Countdown: ${index}`) })
              .pipe(Effect.flatMap(SchemaParser.decodeUnknownEffect(Notification))),
          );
          yield* ctx.sleep({ for: Duration.millis(delay) });
        }
        notifications.push(
          yield* ctx
            .run({ name: "notify-done", effect: notify(url, "Done") })
            .pipe(Effect.flatMap(SchemaParser.decodeUnknownEffect(Notification))),
        );
        return CountdownResult.make({ notifications });
      }),
  }),
);
