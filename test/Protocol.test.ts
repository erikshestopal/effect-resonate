import { describe, expect, it } from "@effect/vitest";
import { DateTime, Option, Schema } from "effect";
import * as Protocol from "../src/Protocol.ts";

const decode = <T, E>(schema: Schema.Codec<T, E>, input: unknown): T => Schema.decodeUnknownSync(schema)(input);

const encode = <T, E>(schema: Schema.Codec<T, E>, value: T): E => Schema.encodeUnknownSync(schema)(value);

const roundTrip = (schema: Schema.Codec<unknown, unknown>, fixture: unknown): void => {
  expect(encode(schema, decode(schema, fixture))).toEqual(fixture);
};

const head = { corrId: "corr-1", version: "2026-04-01" };

const pendingPromise = {
  id: "foo.1",
  state: "pending",
  param: { headers: { "content-type": "application/json" }, data: "eyJmdW5jIjoiZm9vIn0=" },
  value: {},
  tags: { "resonate:target": "poll://any@default", "resonate:scope": "global", region: "us-east" },
  timeoutAt: 1750000060000,
  createdAt: 1750000000000,
};

const resolvedPromise = {
  id: "foo.1",
  state: "resolved",
  param: {},
  value: { data: "NDI=" },
  tags: {},
  timeoutAt: 1750000060000,
  createdAt: 1750000000000,
  settledAt: 1750000030000,
};

const acquiredTask = { id: "foo.1", state: "acquired", version: 2, resumes: 0, pid: "pid-1", ttl: 60000 };

const schedule = {
  id: "nightly",
  cron: "0 3 * * *",
  promiseId: "{{.id}}.{{.timestamp}}",
  promiseTimeout: 3600000,
  promiseParam: { data: "e30=" },
  promiseTags: { "resonate:target": "poll://any@reports" },
  createdAt: 1750000000000,
  nextRunAt: 1750003600000,
};

describe("identifiers", () => {
  it("derives native-compatible detached promise ids", () => {
    expect(
      Protocol.detachedPromiseId({
        prefix: Protocol.PromiseId.make("root"),
        seqid: Protocol.PromiseId.make("root.0"),
      }),
    ).toBe("root.d06d7462a6ced99");
  });

  it("keeps recursive detached ids bounded under the stable prefix", () => {
    const id = Protocol.detachedPromiseId({
      prefix: Protocol.PromiseId.make("top"),
      seqid: Protocol.PromiseId.make("top.deadbeefdeadbe.0"),
    });

    expect(id.startsWith("top.d")).toBe(true);
    expect(id).toHaveLength("top.d".length + 14);
  });
});

describe("records", () => {
  it("round-trips promise records", () => {
    roundTrip(Protocol.PromiseRecordFromWire, pendingPromise);
    roundTrip(Protocol.PromiseRecordFromWire, resolvedPromise);
    roundTrip(Protocol.PromiseRecordFromWire, { ...resolvedPromise, state: "rejected" });
    roundTrip(Protocol.PromiseRecordFromWire, { ...resolvedPromise, state: "rejected_canceled" });
    roundTrip(Protocol.PromiseRecordFromWire, { ...resolvedPromise, state: "rejected_timedout" });
  });

  it("round-trips task records in all five states", () => {
    roundTrip(Protocol.TaskRecordFromWire, { id: "foo.1", state: "pending", version: 0, resumes: 0 });
    roundTrip(Protocol.TaskRecordFromWire, acquiredTask);
    roundTrip(Protocol.TaskRecordFromWire, { id: "foo.1", state: "suspended", version: 2, resumes: 1 });
    roundTrip(Protocol.TaskRecordFromWire, { id: "foo.1", state: "halted", version: 2, resumes: 0 });
    roundTrip(Protocol.TaskRecordFromWire, { id: "foo.1", state: "fulfilled", version: 2, resumes: 0 });
  });

  it("round-trips schedule records with and without lastRunAt", () => {
    roundTrip(Protocol.ScheduleRecord, schedule);
    roundTrip(Protocol.ScheduleRecord, { ...schedule, lastRunAt: 1750000000000 });
  });

  it("decodes native lenient resumes encodings", () => {
    roundTrip(Protocol.TaskRecordFromWire, { id: "t", state: "pending", version: 0, resumes: ["a", "b"] });
    roundTrip(Protocol.TaskRecordFromWire, { id: "t", state: "pending", version: 0, resumes: false });
  });
});

describe("union discrimination — strict on construct, lenient on decode", () => {
  it("an acquired task without pid/ttl fails strict construct", () => {
    const lenient = decode(Protocol.TaskRecordFromWire, { id: "t", state: "acquired", version: 1, resumes: 0 });
    expect(lenient.state).toBe("acquired");
    expect(() => Protocol.TaskRecord.make(lenient)).toThrow();
    expect(() => decode(Protocol.TaskRecord, { id: "t", state: "acquired", version: 1, resumes: 0 })).toThrow();

    expect(() => Protocol.TaskRecord.make(decode(Protocol.TaskRecordFromWire, acquiredTask))).not.toThrow();
  });

  it("a settled promise without settledAt fails strict construct", () => {
    const { settledAt: _, ...withoutSettledAt } = resolvedPromise;
    const lenient = decode(Protocol.PromiseRecordFromWire, withoutSettledAt);
    expect(lenient.state).toBe("resolved");
    expect(() => Protocol.PromiseRecord.make(lenient)).toThrow();
    expect(() => Protocol.PromiseRecord.make(decode(Protocol.PromiseRecordFromWire, resolvedPromise))).not.toThrow();
  });
});

describe("tags", () => {
  it("splits reserved and user tags and round-trips the flat record", () => {
    const tags = decode(Protocol.TagsFromWire, pendingPromise.tags);
    expect(tags.reserved["resonate:scope"]).toBe("global");
    expect(Option.isNone(tags.reserved["resonate:target"]?.id ?? Option.none())).toBe(true);
    expect(tags.user).toEqual({ region: "us-east" });
    expect(encode(Protocol.TagsFromWire, tags)).toEqual(pendingPromise.tags);
  });

  it("rejects a resonate:-prefixed user key at construct", () => {
    expect(() => Protocol.UserTagKey.make("resonate:evil")).toThrow();
    const tags = decode(Protocol.TagsFromWire, { "resonate:evil": "x" });
    expect(tags.user).toEqual({});
    expect(tags.unrecognized).toEqual({ "resonate:evil": "x" });
  });

  it("preserves junk reserved values raw instead of failing the record (lenient decode)", () => {
    const flat = { "resonate:timer": "banana", "resonate:frobnicate": "x", plain: "ok" };
    const tags = decode(Protocol.TagsFromWire, flat);
    expect(tags.reserved["resonate:timer"]).toBeUndefined();
    expect(tags.unrecognized).toEqual({ "resonate:timer": "banana", "resonate:frobnicate": "x" });
    expect(tags.user).toEqual({ plain: "ok" });
    expect(encode(Protocol.TagsFromWire, tags)).toEqual(flat);
    expect(tags.isTimer).toBe(false);
  });

  it("decodes the reserved delay tag from an epoch-ms string", () => {
    const tags = decode(Protocol.TagsFromWire, { "resonate:delay": "1750000000000" });
    const delay = tags.reserved["resonate:delay"];
    expect(delay && DateTime.toEpochMillis(delay)).toBe(1750000000000);
  });
});

describe("target addresses", () => {
  it("parses and formats poll://{cast}@{group}[/{id}]", () => {
    const anycast = decode(Protocol.TargetAddressFromString, "poll://any@default");
    expect(anycast.cast).toBe("any");
    expect(anycast.group).toBe("default");
    expect(Option.isNone(anycast.id)).toBe(true);
    expect(anycast.address).toBe("poll://any@default");

    const unicast = decode(Protocol.TargetAddressFromString, "poll://uni@gpu-workers/pid-1");
    expect(unicast.cast).toBe("uni");
    expect(Option.getOrNull(unicast.id)).toBe("pid-1");
    expect(encode(Protocol.TargetAddressFromString, unicast)).toBe("poll://uni@gpu-workers/pid-1");
  });

  it("rejects strings outside the address grammar", () => {
    expect(() => decode(Protocol.TargetAddressFromString, "not-an-address")).toThrow();
  });
});

describe("projection — the Lean timeout rule", () => {
  const base = {
    id: "sleep.1",
    state: "pending",
    param: {},
    value: {},
    timeoutAt: 1000,
    createdAt: 0,
  };

  it("keeps a pending promise before its timeout", () => {
    const pending = decode(Protocol.PromiseRecordFromWire, { ...base, tags: {} });
    expect(pending.state).toBe("pending");
    if (pending.state !== "pending") {
      return;
    }
    expect(pending.projected(DateTime.makeUnsafe(999))).toBe(pending);
  });

  it("projects an expired timer promise as resolved at timeoutAt", () => {
    const pending = decode(Protocol.PromiseRecordFromWire, { ...base, tags: { "resonate:timer": "true" } });
    if (pending.state !== "pending") {
      throw new Error("expected pending");
    }
    const projected = pending.projected(DateTime.makeUnsafe(1000));
    expect(projected.state).toBe("resolved");
    if (projected.state === "pending") {
      return;
    }
    expect(Option.map(projected.settledAt, DateTime.toEpochMillis)).toEqual(Option.some(1000));
  });

  it("projects an expired plain promise as rejected_timedout at timeoutAt", () => {
    const pending = decode(Protocol.PromiseRecordFromWire, { ...base, tags: {} });
    if (pending.state !== "pending") {
      throw new Error("expected pending");
    }
    const projected = pending.projected(DateTime.makeUnsafe(5000));
    expect(projected.state).toBe("rejected_timedout");
    if (projected.state === "pending") {
      return;
    }
    expect(Option.map(projected.settledAt, DateTime.toEpochMillis)).toEqual(Option.some(1000));
  });
});

describe("function versions", () => {
  it("maps the wire magic 0 to latest and back", () => {
    expect(decode(Protocol.FunctionVersionFromWire, 0)).toBe("latest");
    expect(decode(Protocol.FunctionVersionFromWire, 3)).toBe(3);
    expect(encode(Protocol.FunctionVersionFromWire, "latest")).toBe(0);
    expect(encode(Protocol.FunctionVersionFromWire, Protocol.FunctionVersion.make(3))).toBe(3);
    expect(() => decode(Protocol.FunctionVersionFromWire, -1)).toThrow();
  });
});

describe("requests — wire shapes per native network/types.ts", () => {
  const promiseCreate = {
    kind: "promise.create",
    head,
    data: {
      id: "foo.1",
      timeoutAt: 1750000060000,
      param: { data: "e30=" },
      tags: { "resonate:target": "poll://any@default" },
    },
  };
  const promiseSettle = {
    kind: "promise.settle",
    head,
    data: { id: "foo.1", state: "resolved", value: { data: "NDI=" } },
  };
  const registerCallback = {
    kind: "promise.register_callback",
    head,
    data: { awaited: "foo.1.2", awaiter: "foo.1" },
  };

  const fixtures: ReadonlyArray<{ kind: Protocol.RequestKind; fixture: unknown }> = [
    { kind: "promise.get", fixture: { kind: "promise.get", head, data: { id: "foo.1" } } },
    { kind: "promise.create", fixture: promiseCreate },
    { kind: "promise.settle", fixture: promiseSettle },
    { kind: "promise.register_callback", fixture: registerCallback },
    {
      kind: "promise.register_listener",
      fixture: {
        kind: "promise.register_listener",
        head,
        data: { awaited: "foo.1", address: "poll://uni@default/pid-1" },
      },
    },
    {
      kind: "promise.search",
      fixture: { kind: "promise.search", head, data: { state: "pending", tags: { a: "b" }, limit: 10, cursor: "c" } },
    },
    { kind: "task.get", fixture: { kind: "task.get", head, data: { id: "foo.1" } } },
    {
      kind: "task.create",
      fixture: { kind: "task.create", head, data: { pid: "pid-1", ttl: 60000, action: promiseCreate } },
    },
    {
      kind: "task.acquire",
      fixture: { kind: "task.acquire", head, data: { id: "foo.1", version: 1, pid: "pid-1", ttl: 60000 } },
    },
    { kind: "task.release", fixture: { kind: "task.release", head, data: { id: "foo.1", version: 1 } } },
    {
      kind: "task.suspend",
      fixture: { kind: "task.suspend", head, data: { id: "foo.1", version: 1, actions: [registerCallback] } },
    },
    { kind: "task.halt", fixture: { kind: "task.halt", head, data: { id: "foo.1" } } },
    { kind: "task.continue", fixture: { kind: "task.continue", head, data: { id: "foo.1" } } },
    {
      kind: "task.fulfill",
      fixture: { kind: "task.fulfill", head, data: { id: "foo.1", version: 1, action: promiseSettle } },
    },
    {
      kind: "task.fence",
      fixture: { kind: "task.fence", head, data: { id: "foo.1", version: 1, action: promiseCreate } },
    },
    {
      kind: "task.heartbeat",
      fixture: { kind: "task.heartbeat", head, data: { pid: "pid-1", tasks: [{ id: "foo.1", version: 1 }] } },
    },
    { kind: "task.search", fixture: { kind: "task.search", head, data: { state: "acquired", limit: 5 } } },
    { kind: "schedule.get", fixture: { kind: "schedule.get", head, data: { id: "nightly" } } },
    {
      kind: "schedule.create",
      fixture: {
        kind: "schedule.create",
        head,
        data: {
          id: "nightly",
          cron: "0 3 * * *",
          promiseId: "{{.id}}.{{.timestamp}}",
          promiseTimeout: 3600000,
          promiseParam: { data: "e30=" },
          promiseTags: { "resonate:target": "poll://any@reports" },
        },
      },
    },
    { kind: "schedule.delete", fixture: { kind: "schedule.delete", head, data: { id: "nightly" } } },
    { kind: "schedule.search", fixture: { kind: "schedule.search", head, data: { limit: 100 } } },
    { kind: "debug.start", fixture: { kind: "debug.start", head, data: {} } },
    { kind: "debug.reset", fixture: { kind: "debug.reset", head, data: {} } },
    { kind: "debug.tick", fixture: { kind: "debug.tick", head, data: { time: 1750000000000 } } },
    { kind: "debug.snap", fixture: { kind: "debug.snap", head, data: {} } },
    { kind: "debug.stop", fixture: { kind: "debug.stop", head, data: {} } },
  ];

  it.each(fixtures)("round-trips $kind", ({ fixture, kind }) => {
    roundTrip(Protocol.RequestSchemas[kind], fixture);

    roundTrip(Protocol.RequestFromWire, fixture);
  });
});

describe("responses — wire shapes per native network/types.ts", () => {
  const ok = (status: number) => ({ corrId: "corr-1", status, version: "2026-04-01" });

  const promiseCreateSuccess = {
    kind: "promise.create",
    head: ok(200),
    data: { promise: pendingPromise },
  };

  const fixtures: ReadonlyArray<{ kind: Protocol.RequestKind; fixture: unknown }> = [
    { kind: "promise.get", fixture: { kind: "promise.get", head: ok(200), data: { promise: resolvedPromise } } },
    { kind: "promise.create", fixture: promiseCreateSuccess },
    { kind: "promise.settle", fixture: { kind: "promise.settle", head: ok(200), data: { promise: resolvedPromise } } },
    {
      kind: "promise.register_callback",
      fixture: { kind: "promise.register_callback", head: ok(200), data: { promise: pendingPromise } },
    },
    {
      kind: "promise.register_listener",
      fixture: { kind: "promise.register_listener", head: ok(200), data: { promise: pendingPromise } },
    },
    {
      kind: "promise.search",
      fixture: { kind: "promise.search", head: ok(200), data: { promises: [pendingPromise], cursor: "next" } },
    },
    { kind: "task.get", fixture: { kind: "task.get", head: ok(200), data: { task: acquiredTask } } },
    {
      kind: "task.create",
      fixture: {
        kind: "task.create",
        head: ok(200),
        data: { task: { id: "foo.1", state: "pending", version: 0, resumes: 0 }, promise: pendingPromise, preload: [] },
      },
    },
    {
      kind: "task.acquire",
      fixture: {
        kind: "task.acquire",
        head: ok(200),
        data: { task: acquiredTask, promise: pendingPromise, preload: [resolvedPromise] },
      },
    },
    { kind: "task.release", fixture: { kind: "task.release", head: ok(200), data: {} } },
    { kind: "task.suspend", fixture: { kind: "task.suspend", head: ok(200), data: {} } },
    { kind: "task.halt", fixture: { kind: "task.halt", head: ok(200), data: {} } },
    { kind: "task.continue", fixture: { kind: "task.continue", head: ok(200), data: {} } },
    { kind: "task.fulfill", fixture: { kind: "task.fulfill", head: ok(200), data: { promise: resolvedPromise } } },
    {
      kind: "task.fence",
      fixture: { kind: "task.fence", head: ok(200), data: { action: promiseCreateSuccess, preload: [] } },
    },
    { kind: "task.heartbeat", fixture: { kind: "task.heartbeat", head: ok(200), data: {} } },
    {
      kind: "task.search",
      fixture: { kind: "task.search", head: ok(200), data: { tasks: [acquiredTask] } },
    },
    { kind: "schedule.get", fixture: { kind: "schedule.get", head: ok(200), data: { schedule } } },
    { kind: "schedule.create", fixture: { kind: "schedule.create", head: ok(200), data: { schedule } } },
    { kind: "schedule.delete", fixture: { kind: "schedule.delete", head: ok(200), data: {} } },
    {
      kind: "schedule.search",
      fixture: { kind: "schedule.search", head: ok(200), data: { schedules: [schedule] } },
    },
    { kind: "debug.start", fixture: { kind: "debug.start", head: ok(200), data: {} } },
    { kind: "debug.reset", fixture: { kind: "debug.reset", head: ok(200), data: {} } },
    {
      kind: "debug.tick",
      fixture: {
        kind: "debug.tick",
        head: ok(200),
        data: [
          { kind: "promise.settle", data: { id: "foo.1", state: "rejected_timedout" } },
          { kind: "task.release", data: { id: "foo.1", version: 1 } },
          { kind: "task.retry", data: { id: "foo.1", version: 1 } },
        ],
      },
    },
    {
      kind: "debug.snap",
      fixture: {
        kind: "debug.snap",
        head: ok(200),
        data: {
          promises: [pendingPromise],
          promiseTimeouts: [{ id: "foo.1", timeout: 1750000060000 }],
          callbacks: [{ awaiter: "foo.1", awaited: "foo.1.2" }],
          listeners: [{ id: "foo.1", address: "poll://uni@default/pid-1" }],
          tasks: [acquiredTask],
          taskTimeouts: [{ id: "foo.1", type: 1, timeout: 1750000060000 }],
          messages: [
            {
              address: "poll://any@default",
              message: { kind: "execute", head: {}, data: { task: { id: "foo.1", version: 1 } } },
            },
          ],
        },
      },
    },
    { kind: "debug.stop", fixture: { kind: "debug.stop", head: ok(200), data: {} } },
  ];

  it.each(fixtures)("round-trips $kind success", ({ fixture, kind }) => {
    roundTrip(Protocol.ResponseSchemas[kind], fixture);
  });

  it("round-trips error responses (string data, per-status head)", () => {
    for (const status of [400, 401, 403, 404, 409, 422, 429, 500, 501]) {
      roundTrip(Protocol.PromiseGetResponse, { kind: "promise.get", head: ok(status), data: "boom" });
    }
  });

  it("round-trips the task.suspend 300 fast path with preload", () => {
    roundTrip(Protocol.TaskSuspendResponse, {
      kind: "task.suspend",
      head: ok(300),
      data: { preload: [resolvedPromise] },
    });
  });
});

describe("messages", () => {
  it("round-trips execute and unblock", () => {
    roundTrip(Protocol.Message, {
      kind: "execute",
      head: { serverUrl: "http://localhost:8001" },
      data: { task: { id: "foo.1", version: 1 } },
    });
    roundTrip(Protocol.Message, { kind: "unblock", head: {}, data: { promise: resolvedPromise } });
  });
});
