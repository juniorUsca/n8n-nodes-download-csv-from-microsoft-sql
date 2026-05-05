# AGENTS guide for this repository

## Project purpose
This package provides one n8n community node:

- `Microsoft SQL to CSV` (`nodes/MicrosoftSqlToCsv/MicrosoftSqlToCsv.node.ts`)

The node executes a SQL Server query in streaming mode and outputs a CSV file as
binary data.

## Repository layout
- `nodes/MicrosoftSqlToCsv/` — node implementation + icon
- `credentials/MicrosoftSqlCsvApi.credentials.ts` — SQL Server credentials
- `package.json` — scripts + `n8n.credentials` and `n8n.nodes` dist entries

## Working rules
1. Keep this node **programmatic-style** (`execute`) because it manages SQL
   streaming, CSV serialization, and file/binary handling.
2. Preserve support for very large exports (millions of rows) and avoid
   introducing artificial row/file-size limits.
3. Prefer performance-safe changes:
   - maintain streaming behavior;
   - avoid loading full result sets in memory;
   - keep explicit cleanup of temp files/directories.
4. If output shape changes, update `README.md` accordingly.
5. If package version changes, update `CHANGELOG.md`.

## Commands
- Build: `npm run build`
- Lint: `npm run lint`
- Dev mode (external n8n): `npm run dev`

## Documentation checklist
Whenever behavior/parameters/credentials change, keep these in sync:
- `README.md` (operations, credentials, usage, troubleshooting)
- `AGENTS.md` (this file)
- `CHANGELOG.md` (only when version is bumped)

## Context docs
Use `.agents/*` docs as implementation reference:
- `.agents/workflow.md`
- `.agents/nodes.md`
- `.agents/properties.md`
- `.agents/nodes-programmatic.md`
