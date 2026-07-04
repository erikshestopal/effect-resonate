import { spawn as nodeSpawn, spawnSync as nodeSpawnSync } from "node:child_process";
import { accessSync, constants } from "node:fs";
import { delimiter, join } from "node:path";
import { Predicate } from "effect";

export interface Subprocess {
  readonly stdout: NodeJS.ReadableStream;
  readonly stderr: NodeJS.ReadableStream;
  readonly exited: Promise<number>;
  kill(): boolean;
}

export const spawn = (command: ReadonlyArray<string>, env: Record<string, string> = {}): Subprocess => {
  const [file, ...args] = command;
  if (Predicate.isUndefined(file)) {
    throw new Error("Cannot spawn an empty command");
  }
  const process = nodeSpawn(file, args, {
    env: { ...globalThis.process.env, ...env },
    detached: true,
    stdio: ["ignore", "pipe", "pipe"],
  });
  return {
    stdout: process.stdout,
    stderr: process.stderr,
    exited: new Promise((resolve) => {
      process.once("error", () => resolve(1));
      process.once("close", (code) => resolve(code ?? 1));
    }),
    kill: () => {
      if (Predicate.isNotUndefined(process.pid)) {
        try {
          globalThis.process.kill(-process.pid, "SIGKILL");
          return true;
        } catch {
          return process.kill("SIGKILL");
        }
      }
      return process.kill("SIGKILL");
    },
  };
};

export const spawnSync = (command: ReadonlyArray<string>) => {
  const [file, ...args] = command;
  if (Predicate.isUndefined(file)) {
    throw new Error("Cannot spawn an empty command");
  }
  return nodeSpawnSync(file, args, { stdio: "pipe" });
};

export const commandExists = (command: string): boolean => {
  for (const directory of (globalThis.process.env.PATH ?? "").split(delimiter)) {
    try {
      accessSync(join(directory, command), constants.X_OK);
      return true;
    } catch {
      // keep scanning PATH
    }
  }
  return false;
};

export const streamText = async (stream: NodeJS.ReadableStream): Promise<string> => {
  let text = "";
  stream.setEncoding("utf8");
  for await (const chunk of stream) {
    text = text + chunk;
  }
  return text;
};
