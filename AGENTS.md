# Project Description

`effect-resonate` is an Effect v4 based SDK implementation of the Resonate protocol.

# Vendored Repos

You have source access to the following vendored repos that you MUST consult instead of searching node_modules:

- @repos/effect-smol - effect v4 codebase
- @repos/resonate-specification - the official Resonate specification
- @repos/resonate-sdk-ts - the Resonate TypeScript SDK implementing the Resonate specification
- @repos/distributed-async-await.io/content/docs - handbook for implementing the Resonate protocol in SDKs

# Write Effect Code

- When writing Effect code, you MUST load @repos/effect-smol/LLMS.md and apply the guidelines in that document.
- Use the entire Effect module space and combinators ALWAYS, like Predicate, Option, Schema, Order, etc

# Review Checklist

- Run `vp run check` to run our quality checks after every turn. This includes formatting, linting, type checking, and testing.

<!--VITE PLUS START-->

# Using Vite+, the Unified Toolchain for the Web

This project is using Vite+, a unified toolchain built on top of Vite, Rolldown, Vitest, tsdown, Oxlint, Oxfmt, and Vite Task. Vite+ wraps runtime management, package management, and frontend tooling in a single global CLI called `vp`. Vite+ is distinct from Vite, and it invokes Vite through `vp dev` and `vp build`. Run `vp help` to print a list of commands and `vp <command> --help` for information about a specific command.

Docs are local at `node_modules/vite-plus/docs` or online at https://viteplus.dev/guide/.

<!--VITE PLUS END-->
