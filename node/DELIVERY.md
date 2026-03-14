# Node SDK Delivery Notes

This document captures the Node.js package-delivery concerns that sit adjacent to
the SDK implementation itself.

## Current State

- The package is authored in TypeScript under `src/`.
- Public exports are rooted at `src/index.ts`.
- Declaration output is enabled in `tsconfig.json`.
- Runtime and protocol behavior are currently validated with:
  - `npm test`
  - `npm run check`
  - repository-level `just check`

## Already Landed

- `node/.gitignore` ignores generated artifacts:
  - `dist/`
  - `dist-test/`
  - `node_modules/`
- `node/examples/` now provides minimal statement / table / batch examples.
- `node/README.md` now points at the in-repo examples and documents the current
  development commands.

## Recommended Follow-ups

### 1. Tighten package metadata

The package should eventually carry a fuller npm-facing metadata set:

- `repository`
- `homepage`
- `bugs`
- `keywords`
- `sideEffects`
- `publishConfig`

This is low-risk but best done once the initial package surface is more stable.

### 2. Harden exports / declaration delivery

The package already points `types` at `dist/index.d.ts`, but the final delivery
shape should be reviewed as a Node package rather than just a TypeScript repo:

- keep `src/index.ts` as the single declaration root
- ensure `exports` explicitly includes the type path alongside the runtime entry
- decide whether `package.json` itself should be exported
- confirm the emitted declaration tree matches the intended public surface

### 3. Add a publish-time build hook

Before the package is actually published, add a build hook so npm tarballs always
contain fresh runtime and declaration artifacts.

Typical options:

- `prepack`
- `prepare`

`prepack` is usually the safer default for library publishing.

### 4. Decide CI boundaries

For a production-ready Node package, the expected CI floor is:

- `npm test`
- `npm run build`
- repository-level `just check`

If a separate Node-specific workflow is introduced later, these commands should
be the first candidates.

### 5. Revisit example execution ergonomics

The current examples are intentionally close to `src/` and serve as in-repo
reference code. If the package becomes more user-facing, decide whether examples
should instead run against built `dist/` output or a dedicated example runner.
