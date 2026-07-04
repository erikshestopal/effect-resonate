import { Effect } from "effect";
import { Analysis, Story } from "./types.ts";
export const searchStories = (keyword: string) =>
  Effect.succeed([
    Story.make({ objectID: "1", title: `${keyword} on Hacker News`, url: "memory://hn/1" }),
    Story.make({ objectID: "2", title: `Ask HN: ${keyword}`, url: "memory://hn/2" }),
  ]);
export const analyzeStory = (keyword: string, story: typeof Story.Type) =>
  Effect.succeed(
    Analysis.make({
      storyId: story.objectID,
      title: story.title,
      url: story.url,
      hnUrl: `https://news.ycombinator.com/item?id=${story.objectID}`,
      relevance: `matches ${keyword}`,
    }),
  );
export const notifySlack = (count: number) => Effect.logInfo(`[slack] notified about ${count} stories`);
