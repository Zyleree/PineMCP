# Troubleshooting

Common issues and resolutions when running PineMCP v2.1.0+.

## Node or Runtime Issues
- Error: "Unexpected token" or ESM import errors
  - Ensure Node.js >= 18. Check with `node -v`.
- Permission denied writing config (2.0.0+)
  - PineMCP no longer writes `config/`. Define connections in your MCP client.
  - Only `data/` is written (query history/templates). For Docker, mount a writable data volume, e.g.: `-v "$PWD/data:/app/data"`.

## v2.1.0 Bug Fixes
- **MSSQL Transactions**: Fixed broken transaction support - now properly implements `beginTransaction()`, `commitTransaction()`, and `rollbackTransaction()`
- **PostgreSQL Field Types**: Fixed field type mapping showing OID numbers instead of readable names
- **Redis Transactions**: Implemented proper MULTI/EXEC transaction support
- **MongoDB Sessions**: Fixed queries not using sessions during transactions
- **Error Handling**: All services now use structured error classes with proper context
- **Type Safety**: Eliminated all unsafe type assertions and improved type safety

## Connection Problems
- "No database connection configured"
  - Define connections in your MCP client configuration (see MCP Integration). CLI setup is disabled.
- "Connection '<name>' not found"
  - Verify the connection name in your MCP client configuration (see MCP Integration). Optionally, use the `list_connections` MCP tool from your client to inspect active names.
- Invalid credentials / SSL errors
  - Update your MCP client configuration for the connection and restart the client/server. Then validate with:
    - CLI: `pinemcp test-connection --name <name>`
    - MCP tool: `validate_connection` with `{ "connection": "<name>" }`
- SQLite path not found
  - Use an absolute path or ensure the file exists. For in-memory: `:memory:`.

## MCP Stdio Integration
- Client shows no tools/resources
  - Ensure the server is started via stdio and the MCP client points to the `pinemcp` binary.
- Tables not listed as resources
  - Verify an active connection exists and the adapter supports `getTables()`.

## Windows Notes
- PowerShell piping may buffer stdio in some shells. Append `| cat` to avoid paging.
- Paths use `\\` in JSON; verify file paths are valid.

## Logs & Debugging
- Increase verbosity by observing CLI output and adapter errors.
- If schema operations fail, try validating connections first: `pinemcp test-connection --name <name>` or call the `validate_connection` tool.

If issues persist, open an issue with logs and reproduction steps: https://github.com/Zyleree/PineMCP/issues
