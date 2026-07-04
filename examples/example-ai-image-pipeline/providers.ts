import { Effect, Schema } from "effect";

export const ImageStyle = Schema.Literals(["photorealistic", "cartoon", "abstract"]);
export const GeneratedImage = Schema.Struct({
  style: ImageStyle,
  prompt: Schema.String,
  url: Schema.String,
  durationMs: Schema.Finite,
});
export class ImageGenerationError extends Schema.TaggedErrorClass<ImageGenerationError>()("ImageGenerationError", {
  style: ImageStyle,
  prompt: Schema.String,
}) {}
export const generateImage = (prompt: string, style: typeof ImageStyle.Type, crashMode: string) =>
  Effect.gen(function* () {
    yield* Effect.logInfo(`[image:${style}] generating ${prompt}`);
    if (crashMode === style) return yield* new ImageGenerationError({ style, prompt });
    return GeneratedImage.make({
      style,
      prompt,
      url: `memory://images/${style}/${encodeURIComponent(prompt)}`,
      durationMs: 100 + prompt.length + style.length,
    });
  });
