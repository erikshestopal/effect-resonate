# effect-resonate

`effect-resonate` is an Effect-native SDK for the Resonate durable execution
protocol. It exposes protocol models as `Schema` values, composes runtime
dependencies with `Layer`, and keeps platform implementations such as Bun or
Node at the application edge.

## Installation

```bash
bun add effect-resonate effect
```

Choose and install the Effect platform package for your runtime when using HTTP
transport. For Bun-based applications:

```bash
bun add @effect/platform-bun
```

## Define functions

Functions are declared with a name, version, and payload schema. The schema is
the source of truth for durable payload encoding and handler argument types.

```ts
import { Effect, Schema } from "effect";
import { Resonate } from "effect-resonate";

const greet = Resonate.function({
  name: "greet",
  payload: Schema.String,
});

const App = Resonate.group(greet);

const handlers = App.toLayer(
  App.of({
    greet: (name) => Effect.succeed(`Hello, ${name}!`),
  }),
);
```

## Run a worker

`Worker.layerHttp` builds against Effect's abstract `HttpClient` and `Crypto`
services. The SDK does not provide a Bun or Node implementation internally; the
application supplies the runtime implementation at composition time.

```ts
import { BunRuntime } from "@effect/platform-bun";
import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunHttpClient from "@effect/platform-bun/BunHttpClient";
import { Duration, Layer } from "effect";
import { Worker } from "effect-resonate";

const worker = Worker.layerHttp({
  group: App,
  http: {
    url: "http://127.0.0.1:8001",
    group: "default",
    ttl: Duration.seconds(30),
  },
}).pipe(Layer.provideMerge(handlers), Layer.provideMerge(BunHttpClient.layer), Layer.provideMerge(BunCrypto.layer));

BunRuntime.runMain(Layer.launch(worker));
```

## Use the client

The client is an Effect service. It can start durable executions, await handles,
resolve or reject external promises, inspect executions, and cancel promises.

```ts
import { Effect, Layer } from "effect";
import { Resonate, Worker } from "effect-resonate";

const program = Effect.gen(function* () {
  const client = yield* Resonate.Client;
  const handle = yield* client.beginRpc({
    targetFunction: greet,
    executionId: "greet-ada",
    args: ["Ada"],
  });
  return yield* handle.await;
});
```

## Public modules

| Module         | Purpose                                                    |
| -------------- | ---------------------------------------------------------- |
| `Resonate`     | High-level function, schedule, promise, and client APIs.   |
| `Worker`       | Worker layers for running registered handlers.             |
| `Protocol`     | Schema-first Resonate protocol model and wire codecs.      |
| `Codec`        | Durable payload encoding and optional encryption services. |
| `Network`      | Runtime-neutral network service interface.                 |
| `NetworkHttp`  | HTTP network implementation over Effect `HttpClient`.      |
| `NetworkLocal` | In-memory network implementation for local execution.      |
| `RetryPolicy`  | Retry policy constructors and wire codecs.                 |

## Examples

The `examples/` directory contains standalone Effect ports of the official
Resonate TypeScript examples. Each file includes an invocation comment showing
how to run it against a local Resonate server.
