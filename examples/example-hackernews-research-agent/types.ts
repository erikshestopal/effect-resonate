import { Schema } from "effect";
export const Story = Schema.Struct({ objectID: Schema.String, title: Schema.String, url: Schema.String });
export const Analysis = Schema.Struct({
  storyId: Schema.String,
  title: Schema.String,
  url: Schema.String,
  hnUrl: Schema.String,
  relevance: Schema.String,
});
export const ScanResult = Schema.Struct({
  keyword: Schema.String,
  storiesFound: Schema.Finite,
  newlyAnalyzed: Schema.Array(Analysis),
});
