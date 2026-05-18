"""System information tools: variables, version, config, environment."""

import json

from astro_airflow_mcp.server import _config, mcp
from astro_airflow_mcp.tools._common import _environment_label, _get_adapter, _wrap_list_response

DEFAULT_LIMIT = 100
DEFAULT_OFFSET = 0


def _get_variable_impl(variable_key: str) -> str:
    try:
        adapter = _get_adapter()
        data = adapter.get_variable(variable_key)
        return json.dumps(data, indent=2)
    except Exception as e:
        return str(e)


def _list_variables_impl(
    limit: int = DEFAULT_LIMIT,
    offset: int = DEFAULT_OFFSET,
) -> str:
    try:
        adapter = _get_adapter()
        data = adapter.list_variables(limit=limit, offset=offset)

        if "variables" in data:
            return _wrap_list_response(data["variables"], "variables", data)
        return f"No variables found. Response: {data}"
    except Exception as e:
        return str(e)


def _get_version_impl() -> str:
    try:
        adapter = _get_adapter()
        data = adapter.get_version()
        return json.dumps(data, indent=2)
    except Exception as e:
        return str(e)


def _get_config_impl() -> str:
    try:
        adapter = _get_adapter()
        data = adapter.get_config()

        if "sections" in data:
            result = {"total_sections": len(data["sections"]), "sections": data["sections"]}
            return json.dumps(result, indent=2)
        return f"No configuration found. Response: {data}"
    except Exception as e:
        return str(e)


@mcp.tool()
def get_variable(variable_key: str) -> str:
    """Get a specific Airflow variable by key.

    Use this tool when the user asks about:
    - "What's the value of variable X?" or "Show me variable Y"
    - "Get variable Z" or "What does variable A contain?"
    - "What's stored in variable B?" or "Look up variable C"

    Variables are key-value pairs stored in Airflow's metadata database that
    can be accessed by DAGs at runtime. They're commonly used for configuration
    values, API keys, or other settings that need to be shared across DAGs.

    Returns variable information including:
    - key: The variable's key/name
    - value: The variable's value (may be masked if marked as sensitive)
    - description: Optional description of the variable's purpose

    Args:
        variable_key: The key/name of the variable to retrieve

    Returns:
        JSON with the variable's key, value, and metadata
    """
    return _get_variable_impl(variable_key=variable_key)


@mcp.tool()
def list_variables() -> str:
    """Get all Airflow variables (key-value configuration pairs).

    Use this tool when the user asks about:
    - "What variables are configured?" or "List all variables"
    - "Show me the variables" or "What variables exist?"
    - "What configuration variables are available?"
    - "Show me all variable keys"

    Variables are key-value pairs stored in Airflow's metadata database that
    can be accessed by DAGs at runtime. They're commonly used for configuration
    values, environment-specific settings, or other data that needs to be
    shared across DAGs without hardcoding in the DAG files.

    Returns variable information including:
    - key: The variable's key/name
    - value: The variable's value (may be masked if marked as sensitive)
    - description: Optional description of the variable's purpose

    IMPORTANT: Sensitive variables (like passwords, API keys) may have their
    values masked in the response for security reasons.

    Returns:
        JSON with list of all variables and their values
    """
    return _list_variables_impl()


@mcp.tool()
def get_airflow_version() -> str:
    """Get version information for the Airflow instance.

    Use this tool when the user asks about:
    - "What version of Airflow is running?" or "Show me the Airflow version"
    - "What's the Airflow version?" or "Which Airflow release is this?"
    - "What version is installed?" or "Check Airflow version"
    - "Is this Airflow 2 or 3?" or "What's the version number?"

    Returns version information including:
    - version: The Airflow version string (e.g., "2.8.0", "3.0.0")
    - git_version: Git commit hash if available

    This is useful for:
    - Determining API compatibility
    - Checking if features are available in this version
    - Troubleshooting version-specific issues
    - Verifying upgrade success

    Returns:
        JSON with Airflow version information
    """
    return _get_version_impl()


@mcp.tool()
def get_airflow_config() -> str:
    """Get Airflow instance configuration and settings.

    Use this tool when the user asks about:
    - "What's the Airflow configuration?" or "Show me Airflow settings"
    - "What's the executor type?" or "How is Airflow configured?"
    - "What's the parallelism setting?"
    - Database connection, logging, or scheduler settings
    - Finding specific configuration values

    Returns all Airflow configuration organized by sections:
    - [core]: Basic Airflow settings (executor, dags_folder, parallelism)
    - [database]: Database connection and settings
    - [webserver]: Web UI configuration (port, workers, auth)
    - [scheduler]: Scheduler behavior and intervals
    - [logging]: Log locations and formatting
    - [api]: REST API configuration
    - [operators]: Default operator settings
    - And many more sections...

    Each setting includes:
    - key: Configuration parameter name
    - value: Current value
    - source: Where the value came from (default, env var, config file)

    Returns:
        JSON with complete Airflow configuration organized by sections
    """
    return _get_config_impl()


@mcp.tool()
def get_current_environment() -> str:
    """Show which Airflow environment this MCP server is connected to.

    Use this tool when:
    - The user asks "which Airflow am I connected to?"
    - Before performing destructive operations to confirm the target
    - The user is unsure whether they're on integration or production

    Returns:
        JSON with the current Airflow URL and environment label
    """
    result = {
        "airflow_url": _config.url,
        "environment": _environment_label(),
    }
    try:
        adapter = _get_adapter()
        version_info = adapter.get_version()
        result["airflow_version"] = version_info.get("version", "unknown")
    except Exception:
        result["airflow_version"] = "unavailable"
    return json.dumps(result, indent=2)
