"""Admin tools: connections, pools, plugins, providers."""

import json

from astro_airflow_mcp.server import mcp
from astro_airflow_mcp.tools._common import _get_adapter, _wrap_list_response

DEFAULT_LIMIT = 100
DEFAULT_OFFSET = 0


def _list_connections_impl(
    limit: int = DEFAULT_LIMIT,
    offset: int = DEFAULT_OFFSET,
) -> str:
    try:
        adapter = _get_adapter()
        data = adapter.list_connections(limit=limit, offset=offset)

        if "connections" in data:
            connections = data["connections"]
            total_entries = data.get("total_entries", len(connections))

            filtered_connections = [
                {
                    "connection_id": conn.get("connection_id"),
                    "conn_type": conn.get("conn_type"),
                    "description": conn.get("description"),
                    "host": conn.get("host"),
                    "port": conn.get("port"),
                    "schema": conn.get("schema"),
                    "login": conn.get("login"),
                    "extra": conn.get("extra"),
                }
                for conn in connections
            ]

            result = {
                "total_connections": total_entries,
                "returned_count": len(filtered_connections),
                "connections": filtered_connections,
            }

            return json.dumps(result, indent=2)
        return f"No connections found. Response: {data}"
    except Exception as e:
        return str(e)


@mcp.tool()
def list_connections() -> str:
    """Get connection configurations for external systems (databases, APIs, services).

    Use this tool when the user asks about:
    - "What connections are configured?" or "List all connections"
    - "How do I connect to database X?"
    - "What's the connection string for Y?"
    - "Which databases/services are available?"
    - Finding connection details by name or type

    Connections store credentials and connection info for external systems
    that DAGs interact with (databases, S3, APIs, etc.).

    Returns connection metadata including:
    - connection_id: Unique name for this connection
    - conn_type: Type (postgres, mysql, s3, http, etc.)
    - description: Human-readable description
    - host: Server hostname or IP
    - port: Port number
    - schema: Database schema or path
    - login: Username (passwords excluded for security)
    - extra: Additional connection parameters as JSON

    IMPORTANT: Passwords are NEVER returned for security reasons.

    Returns:
        JSON with list of all connections (credentials excluded)
    """
    return _list_connections_impl()


def _get_pool_impl(pool_name: str) -> str:
    try:
        adapter = _get_adapter()
        data = adapter.get_pool(pool_name)
        return json.dumps(data, indent=2)
    except Exception as e:
        return str(e)


def _list_pools_impl(
    limit: int = DEFAULT_LIMIT,
    offset: int = DEFAULT_OFFSET,
) -> str:
    try:
        adapter = _get_adapter()
        data = adapter.list_pools(limit=limit, offset=offset)

        if "pools" in data:
            return _wrap_list_response(data["pools"], "pools", data)
        return f"No pools found. Response: {data}"
    except Exception as e:
        return str(e)


def _list_plugins_impl(
    limit: int = DEFAULT_LIMIT,
    offset: int = DEFAULT_OFFSET,
) -> str:
    try:
        adapter = _get_adapter()
        data = adapter.list_plugins(limit=limit, offset=offset)

        if "plugins" in data:
            return _wrap_list_response(data["plugins"], "plugins", data)
        return f"No plugins found. Response: {data}"
    except Exception as e:
        return str(e)


def _list_providers_impl() -> str:
    try:
        adapter = _get_adapter()
        data = adapter.list_providers()

        if "providers" in data:
            return _wrap_list_response(data["providers"], "providers", data)
        return f"No providers found. Response: {data}"
    except Exception as e:
        return str(e)


@mcp.tool()
def get_pool(pool_name: str) -> str:
    """Get detailed information about a specific resource pool.

    Use this tool when the user asks about:
    - "Show me details for pool X" or "What's the status of pool Y?"
    - "How many slots are available in pool Z?" or "Is pool X full?"
    - "What's using pool Y?" or "How many tasks are running in pool X?"
    - "Get information about the default_pool" or "Show me pool details"

    Pools are used to limit parallelism for specific sets of tasks. This returns
    detailed real-time information about a specific pool's capacity and utilization.

    Returns detailed pool information including:
    - name: Name of the pool
    - slots: Total number of available slots in the pool
    - occupied_slots: Number of currently occupied slots (running + queued)
    - running_slots: Number of slots with currently running tasks
    - queued_slots: Number of slots with queued tasks waiting to run
    - open_slots: Number of available slots (slots - occupied_slots)
    - description: Human-readable description of the pool's purpose

    Args:
        pool_name: The name of the pool to get details for (e.g., "default_pool")

    Returns:
        JSON with complete details about the specified pool
    """
    return _get_pool_impl(pool_name=pool_name)


@mcp.tool()
def list_pools() -> str:
    """Get resource pools for managing task concurrency and resource allocation.

    Use this tool when the user asks about:
    - "What pools are configured?" or "List all pools"
    - "Show me the resource pools" or "What pools exist?"
    - "How many slots does pool X have?" or "What's the pool capacity?"
    - "Which pools are available?" or "What's the pool configuration?"

    Pools are used to limit parallelism for specific sets of tasks. Each pool
    has a certain number of slots, and tasks assigned to a pool will only run
    if there are available slots. This is useful for limiting concurrent access
    to resources like databases or external APIs.

    Returns pool information including:
    - name: Name of the pool
    - slots: Total number of available slots in the pool
    - occupied_slots: Number of currently occupied slots
    - running_slots: Number of slots with running tasks
    - queued_slots: Number of slots with queued tasks
    - open_slots: Number of available slots (slots - occupied_slots)
    - description: Human-readable description of the pool's purpose

    Returns:
        JSON with list of all pools and their current utilization
    """
    return _list_pools_impl()


@mcp.tool()
def list_plugins() -> str:
    """Get information about installed Airflow plugins.

    Use this tool when the user asks about:
    - "What plugins are installed?" or "List all plugins"
    - "Show me the plugins" or "Which plugins are enabled?"
    - "Is plugin X installed?" or "Do we have any custom plugins?"
    - "What's in the plugins directory?"

    Plugins extend Airflow functionality by adding custom operators, hooks,
    views, menu items, or other components. This returns information about
    all plugins discovered by Airflow's plugin system.

    Returns information about installed plugins including:
    - name: Name of the plugin
    - hooks: Custom hooks provided by the plugin
    - executors: Custom executors provided by the plugin
    - macros: Custom macros provided by the plugin
    - flask_blueprints: Flask blueprints for custom UI pages
    - appbuilder_views: Flask-AppBuilder views for admin interface
    - appbuilder_menu_items: Custom menu items in the UI

    Returns:
        JSON with list of all installed plugins and their components
    """
    return _list_plugins_impl()


@mcp.tool()
def list_providers() -> str:
    """Get information about installed Airflow provider packages.

    Use this tool when the user asks about:
    - "What providers are installed?" or "List all providers"
    - "What integrations are available?" or "Show me installed packages"
    - "Do we have the AWS provider?" or "Is the Snowflake provider installed?"
    - "What version of provider X is installed?"

    Returns information about installed provider packages including:
    - package_name: Name of the provider package (e.g., "apache-airflow-providers-amazon")
    - version: Version of the provider package
    - description: What the provider does
    - provider_info: Details about operators, hooks, and sensors included

    Returns:
        JSON with list of all installed provider packages and their details
    """
    return _list_providers_impl()
