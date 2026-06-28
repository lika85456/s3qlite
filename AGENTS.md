# Commands

## Development

- `bun run dev` - Start all apps in watch mode
- `bun run dev --filter=docs` - Start docs app only

## Testing

- `bun test` - Run all tests in watch mode
- `bun test:ci` - Run all tests with coverage (no watch)

## Linting/Formatting

- `bun run lint` - Lint all code
- `bun run format` - Format all code
- `oxlint src/` - Lint specific directory
- `oxfmt src/` - Format specific directory

# Code Style

## Imports

- Use `import type { }` for type-only imports
- Separate type imports from value imports
- Imports sorted: builtin/external → internal → parent → sibling/index
- No duplicate imports

## Formatting (oxfmt)

- Use tabs, 4-space width
- Double quotes, single quotes for JSX attributes
- Semicolons required
- Trailing commas (all)
- 100 character line width
- Bracket same line
- Single attribute per line in JSX

## TypeScript

- Strict mode enabled
- `unknown` over `any`
- no any, properly type everything
- Explicit return types on public APIs
- No enums, use const objects/unions
- Discriminated unions for variants
- `as const` for literal types
- `satisfies` for type checking

## Naming

- Types: nouns (Task, User)
- Functions: verbs (getTask, createUser)
- Interfaces: describe what they use (DrizzleTaskApi, FetchTaskApi)
- Implementations: prefix with tech (MockTaskApi, HttpTaskApi)
- Unused vars: prefix with underscore (`_unused`)

## Error Handling

- Never throw in Effect code
- Model domain errors with Data.TaggedEnum/TaggedClass
- Handle all paths explicitly
- Use Effect.catchTag for specific errors

## Function Design

- Max 4 parameters
- Single responsibility
- Early returns for validation
- Prefer arrow functions
- No var (const/let only)

## Testing

- Tests in `tests/` subfolder
- Files use `.test.ts` or `.test.tsx` suffix
- Use globals (describe, it, expect)
- Mock with Effect layers, not imports
- Run tests in parallel where possible

## Comments

Code explains "how", comments explain "why". Comment only non-obvious decisions, workarounds, or complex algorithms.

# Architecture

## Feature-Sliced Design

Each feature is self-contained in one folder. Keep files flat unless 10+ files. Tests always in `tests/` subfolder.

## Effect Ecosystem

All business logic uses Effect. Services defined as layers for dependency injection. Use `pipe` for composition. Handle success, failure, and interruption paths explicitly.

# Stack

- **Testing**: Vitest with Bun runtime
- **Build**: Turbo for monorepo orchestration
- **Lint**: oxlint, Format: oxfmt
