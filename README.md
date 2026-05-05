# n8n-nodes-stream-csv-from-microsoft-sql

n8n community node to execute a Microsoft SQL Server query and return the result
as a CSV binary file.

The node is optimized for streaming large result sets (including millions of
rows) without loading the full query result into memory.

## Installation

In n8n, install the community package:

```bash
n8n-nodes-stream-csv-from-microsoft-sql
```

Or follow the official guide:
https://docs.n8n.io/integrations/community-nodes/installation/

## Operations

### Microsoft SQL to CSV
- Executes a SQL query against SQL Server.
- Streams rows to a temporary CSV file.
- Returns the CSV as binary output.
- Adds execution metadata in JSON:
  - `microsoftSqlToCsv.rowCount`
  - `microsoftSqlToCsv.columnCount`
  - `microsoftSqlToCsv.fileName`

## Credentials

Credential type: `Microsoft SQL` (`microsoftSqlCsvApi`)

Required:
- `Server`
- `Database`
- `Username`
- `Password`

Optional:
- `Port` (default `1433`)
- `Instance Name`
- `Encrypt`
- `Trust Server Certificate`
- `Connection Timeout (ms)`
- `Request Timeout (ms)`

## Node parameters

- `Query` (required): SQL statement to execute.
- `Include Headers`: include column names as CSV header row.
- `Delimiter`: CSV delimiter (default `,`).
- `File Name`: output CSV file name.
- `Binary Property`: output binary field name (default `data`).
- `Include Input JSON`: preserve incoming JSON fields in output.

## Usage notes

- Prefer `SELECT` statements for export workflows.
- For very large exports, keep only required upstream fields and disable
  `Include Input JSON` when you don't need pass-through data.
- If your query returns multiple recordsets, the node fails intentionally
  because CSV export supports a single result set per execution item.

## Troubleshooting

### `SQLITE_BUSY` / `database is locked` in n8n

This node does not write directly to SQLite. The lock usually happens when n8n
persists execution and binary data under concurrent load.

Recommendations:
- Reduce concurrent executions writing large binaries at the same time.
- Configure workflow execution-data retention according to your auditing needs.
- Keep n8n and this node on current versions to benefit from persistence fixes.

## Development

```bash
npm run build
npm run lint
npm run dev
```

## Resources

- n8n community nodes docs: https://docs.n8n.io/integrations/#community-nodes
- SQL Server docs: https://learn.microsoft.com/sql/sql-server/
