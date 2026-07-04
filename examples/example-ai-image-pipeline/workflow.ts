import { Duration, Effect, Schema, SchemaParser } from "effect";
import { Resonate } from "effect-resonate";
import { GeneratedImage, generateImage, ImageStyle } from "./providers.ts";
export const repo = "example-ai-image-pipeline-ts";
export const functionName = "runImagePipeline";
export const sampleArgs = [{ prompt: "cat", crashMode: "none" }] as const;
export const Payload = Schema.Struct({ prompt: Schema.String, crashMode: Schema.String });
export const PipelineResult = Schema.Struct({
  prompt: Schema.String,
  images: Schema.Array(GeneratedImage),
  totalMs: Schema.Finite,
});
export const workflow = Resonate.function({ name: functionName, payload: Payload });
export const App = Resonate.group(workflow);
const styles = [ImageStyle.make("photorealistic"), ImageStyle.make("cartoon"), ImageStyle.make("abstract")];
export const handlers = App.toLayer(
  App.of({
    [functionName]: (input) =>
      Effect.gen(function* (): Effect.fn.Return<typeof PipelineResult.Type, unknown, Resonate.Context> {
        const ctx = yield* Resonate.Context;
        const images: Array<typeof GeneratedImage.Type> = [];
        for (const style of styles) {
          images.push(
            yield* ctx
              .run({ name: `generate-${style}`, effect: generateImage(input.prompt, style, input.crashMode) })
              .pipe(Effect.flatMap(SchemaParser.decodeUnknownEffect(GeneratedImage))),
          );
        }
        yield* ctx.sleep({ for: Duration.millis(1) });
        return PipelineResult.make({
          prompt: input.prompt,
          images,
          totalMs: images.reduce((sum, image) => sum + image.durationMs, 0),
        });
      }),
  }),
);
