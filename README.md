# Airflow MCP Server

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)

A [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server for Apache Airflow that gives AI assistants access to Airflow's REST API. Built with [FastMCP](https://github.com/jlowin/fastmcp).

Supports Airflow 2.x and 3.x with automatic version detection.

## Installation

```bash
pip install git+https://github.com/zachliu/astro-airflow-mcp.git
```

## Setup for Claude Code

### 1. Get credentials

Get the following values from your team lead or secrets manager (e.g., HashiCorp Vault):

- `AUTH0_DOMAIN` - Auth0 tenant domain for your environment
- `AUTH0_CLIENT_ID` - Auth0 application client ID
- `AIRFLOW_API_URL` - Your Airflow instance URL
- `AUTH0_CALLBACK_URL` - OAuth callback URL on your Airflow instance

### 2. Add MCP config

Add to your project's `.mcp.json` (or `~/.claude/.mcp.json` for global access):

```json
{
  "mcpServers": {
    "airflow": {
      "command": "astro-airflow-mcp",
      "args": ["--transport", "stdio"],
      "env": {
        "AIRFLOW_API_URL": "<your-airflow-url>",
        "AUTH0_DOMAIN": "<your-auth0-domain>",
        "AUTH0_CLIENT_ID": "<your-auth0-client-id>"
      }
    }
  }
}
```

### 3. Authenticate

Run once (opens browser for Auth0 login):

```bash
astro-airflow-mcp-login \
  --auth0-domain <your-auth0-domain> \
  --auth0-client-id <your-auth0-client-id> \
  --auth0-callback-url <your-auth0-callback-url>
```

After logging in, copy the token from the browser page and paste it into the CLI.

### 3. Restart Claude Code

The MCP server will be available. Try asking: "List my DAGs" or "Show Airflow system health".

## Multi-Environment Support

Tokens are stored per Auth0 domain, so you can authenticate to multiple environments simultaneously. Just run the login command once per environment with the corresponding credentials:

```bash
# Environment A (e.g., integration)
astro-airflow-mcp-login \
  --auth0-domain <env-a-auth0-domain> \
  --auth0-client-id <env-a-client-id> \
  --auth0-callback-url <env-a-callback-url>

# Environment B (e.g., production)
astro-airflow-mcp-login \
  --auth0-domain <env-b-auth0-domain> \
  --auth0-client-id <env-b-client-id> \
  --auth0-callback-url <env-b-callback-url>
```

Each environment gets its own token file at `~/.config/astro-airflow-mcp/tokens/`.

### Token management

```bash
# List all stored tokens and their status
astro-airflow-mcp-login --list

# Check a specific environment
astro-airflow-mcp-login --status --auth0-domain mycompany-dev.us.auth0.com
```

Tokens last 24 hours. Re-run the login command when expired.

### Environment awareness

The MCP server includes a `get_current_environment` tool that returns which Airflow instance is connected. Destructive operations (pause, unpause, trigger) include the environment label in their output so you always know which environment was affected.

## Other MCP Clients

### VS Code / Cursor

Add to `.vscode/mcp.json` or `~/.cursor/mcp.json` (same config format as above):

```json
{
  "mcpServers": {
    "airflow": {
      "command": "astro-airflow-mcp",
      "args": ["--transport", "stdio"],
      "env": {
        "AIRFLOW_API_URL": "<your-airflow-url>",
        "AUTH0_DOMAIN": "<your-auth0-domain>",
        "AUTH0_CLIENT_ID": "<your-auth0-client-id>"
      }
    }
  }
}
```

### HTTP mode

For connecting multiple clients to one server:

```bash
astro-airflow-mcp --transport http --host localhost --port 8000
```

Connect clients to: `http://localhost:8000/mcp`

## Authentication

This server uses Auth0 with an Airflow OAuth plugin. The flow:

1. You run `astro-airflow-mcp-login` which opens Auth0 in your browser
2. After authenticating, the Airflow plugin exchanges the Auth0 code for an Airflow API JWT
3. You paste the JWT back into the CLI, which stores it locally
4. The MCP server uses the stored JWT for API calls (valid for 24 hours)

## Available Tools

### Consolidated Tools (Agent-Optimized)

| Tool | Description |
|------|-------------|
| `explore_dag` | Get comprehensive DAG info: metadata, tasks, recent runs, source code |
| `diagnose_dag_run` | Debug a DAG run: run details, failed task instances, logs |
| `get_system_health` | System overview: health status, import errors, warnings, DAG stats |
| `get_current_environment` | Show which Airflow instance is connected |

### Core Tools

| Tool | Description |
|------|-------------|
| `list_dags` | Get all DAGs and their metadata |
| `get_dag_details` | Get detailed info about a specific DAG |
| `get_dag_source` | Get the source code of a DAG |
| `get_dag_stats` | Get DAG run statistics (Airflow 3.x only) |
| `list_dag_warnings` | Get DAG import warnings |
| `list_import_errors` | Get import errors from DAG files that failed to parse |
| `list_dag_runs` | Get DAG run history |
| `get_dag_run` | Get specific DAG run details |
| `trigger_dag` | Trigger a new DAG run |
| `trigger_dag_and_wait` | Trigger a DAG run and poll until completion |
| `pause_dag` | Pause a DAG to prevent new scheduled runs |
| `unpause_dag` | Unpause a DAG to resume scheduled runs |
| `list_tasks` | Get all tasks in a DAG |
| `get_task` | Get details about a specific task |
| `get_task_instance` | Get task instance execution details |
| `get_task_logs` | Get logs for a specific task instance |
| `list_pools` | Get all resource pools |
| `get_pool` | Get details about a specific pool |
| `list_variables` | Get all Airflow variables |
| `get_variable` | Get a specific variable by key |
| `list_connections` | Get all connections (credentials excluded) |
| `list_assets` | Get assets/datasets (unified naming across Airflow versions) |
| `list_plugins` | Get installed Airflow plugins |
| `list_providers` | Get installed provider packages |
| `get_airflow_config` | Get Airflow configuration |
| `get_airflow_version` | Get Airflow version information |

### MCP Resources

| Resource URI | Description |
|--------------|-------------|
| `airflow://version` | Airflow version information |
| `airflow://providers` | Installed provider packages |
| `airflow://plugins` | Installed Airflow plugins |
| `airflow://config` | Airflow configuration |

### MCP Prompts

| Prompt | Description |
|--------|-------------|
| `troubleshoot_failed_dag` | Guided workflow for diagnosing DAG failures |
| `daily_health_check` | Morning health check routine |
| `onboard_new_dag` | Guide for understanding a new DAG |

## CLI Options

### `astro-airflow-mcp` (server)

| Environment Variable | Description |
|---------------------|-------------|
| `AIRFLOW_API_URL` | Airflow webserver URL |
| `AUTH0_DOMAIN` | Auth0 tenant domain |
| `AUTH0_CLIENT_ID` | Auth0 client ID |

### `astro-airflow-mcp-login` (authentication)

| Flag | Description |
|------|-------------|
| `--auth0-domain` | Auth0 tenant domain |
| `--auth0-client-id` | Auth0 client ID |
| `--auth0-callback-url` | OAuth callback URL on your Airflow instance |
| `--status` | Check if current token is valid |
| `--list` | List all stored environment tokens |

## Architecture

```
src/astro_airflow_mcp/
├── server.py          # MCP tools, resources, prompts (FastMCP)
├── auth.py            # Auth0 login flow + per-environment token storage
├── auth_cli.py        # `astro-airflow-mcp-login` CLI
├── adapters/
│   ├── base.py        # Abstract adapter interface
│   ├── airflow_v2.py  # Airflow 2.x API (/api/v1)
│   └── airflow_v3.py  # Airflow 3.x API (/api/v2)
├── models.py          # Pydantic models (type reference)
└── plugin.py          # Airflow 3.x plugin integration
```

- **Adapter pattern**: Version-specific API implementations behind a common interface
- **Auto-detection**: Probes API endpoints at startup to determine Airflow version
- **Per-environment tokens**: Keyed by Auth0 domain so multiple environments coexist

## Development

```bash
# Setup
make install-dev

# Run tests
make test

# Run all checks
make check

# Local testing with Astro CLI
astro dev start
make run
```

## Contributing

Contributions welcome! Please ensure:
- All tests pass (`make test`)
- Code passes linting (`make check`)
