"""FastMCP server for Airflow integration."""

import json
import os
import time
from typing import Any
from urllib.parse import urlparse

import httpx
from fastmcp import FastMCP
from fastmcp.server.middleware.logging import LoggingMiddleware

from astro_airflow_mcp.adapters import AirflowAdapter, create_adapter
from astro_airflow_mcp.logging import get_logger

logger = get_logger(__name__)

# Default configuration values
DEFAULT_AIRFLOW_URL = "http://localhost:8080"
DEFAULT_LIMIT = 100
DEFAULT_OFFSET = 0
# Buffer time before token expiry to trigger refresh (5 minutes)
TOKEN_REFRESH_BUFFER_SECONDS = 300
# Terminal states for DAG runs (polling stops when reached)
TERMINAL_DAG_RUN_STATES = {"success", "failed", "upstream_failed"}
# Essential fields to keep when trimming task metadata (avoids context overflow).
# All available fields from Airflow API:
#   task_id, task_display_name, owner, start_date, end_date,
#   trigger_rule, depends_on_past, wait_for_downstream, retries,
#   queue, pool, pool_slots, execution_timeout, retry_delay,
#   retry_exponential_backoff, priority_weight, weight_rule,
#   ui_color, ui_fgcolor, template_fields, downstream_task_ids,
#   doc_md, operator_name, params, class_ref, is_mapped, extra_links
TASK_ESSENTIAL_FIELDS = {
    "task_id",
    "task_display_name",
    "operator_name",
    "owner",
    "pool",
    "trigger_rule",
    "retries",
    "downstream_task_ids",
    "is_mapped",
}


class AirflowTokenManager:
    """Manages JWT token lifecycle for Airflow API authentication.

    Handles fetching tokens from /auth/token endpoint (Airflow 3.x),
    automatic refresh when tokens expire, and supports both credential-based
    and credential-less (all_admins mode) authentication.

    For Airflow 2.x (which doesn't have /auth/token), this manager will detect
    the 404 and stop attempting token fetches, falling back to basic auth.
    """

    def __init__(
        self,
        airflow_url: str,
        username: str | None = None,
        password: str | None = None,
    ):
        """Initialize the token manager.

        Args:
            airflow_url: Base URL of the Airflow webserver
            username: Optional username for token authentication
            password: Optional password for token authentication
        """
        self.airflow_url = airflow_url
        self.username = username
        self.password = password
        self._token: str | None = None
        self._token_fetched_at: float | None = None
        self._token_lifetime_seconds: float = 1800
        self._token_endpoint_available: bool | None = None

    def get_token(self) -> str | None:
        """Get current token, fetching/refreshing if needed.

        Returns:
            JWT token string, or None if token fetch fails or endpoint unavailable
        """
        if self._token_endpoint_available is False:
            return None
        if self._should_refresh():
            self._fetch_token()
        return self._token

    def get_basic_auth(self) -> tuple[str, str] | None:
        """Get basic auth credentials for Airflow 2.x fallback.

        Returns:
            Tuple of (username, password) if available, None otherwise
        """
        if self.username and self.password:
            return (self.username, self.password)
        return None

    def is_token_endpoint_available(self) -> bool | None:
        """Check if the token endpoint is available.

        Returns:
            True if available (Airflow 3.x), False if not (Airflow 2.x),
            None if not yet determined.
        """
        return self._token_endpoint_available

    def _should_refresh(self) -> bool:
        """Check if token needs refresh (expired or not yet fetched)."""
        if self._token is None:
            return True
        if self._token_fetched_at is None:
            return True
        elapsed = time.time() - self._token_fetched_at
        return elapsed >= (self._token_lifetime_seconds - TOKEN_REFRESH_BUFFER_SECONDS)

    def _fetch_token(self) -> None:
        """Fetch new token from /auth/token endpoint.

        Tries credential-less GET first if no username/password provided,
        otherwise uses POST with credentials. For Airflow 2.x (404 response),
        marks the endpoint as unavailable and stops future attempts.
        """
        token_url = f"{self.airflow_url}/auth/token"

        try:
            with httpx.Client(timeout=30.0) as client:
                if self.username and self.password:
                    logger.debug("Fetching token with username/password credentials")
                    response = client.post(
                        token_url,
                        json={"username": self.username, "password": self.password},
                        headers={"Content-Type": "application/json"},
                    )
                else:
                    logger.debug("Attempting credential-less token fetch")
                    response = client.get(token_url)

            if response.status_code == 404:
                self._token_endpoint_available = False
                self._token = None
                if not self.username and not self.password:
                    logger.info(
                        "Token endpoint not available (Airflow 2.x). "
                        "Defaulting to admin:admin for basic auth."
                    )
                    self.username = "admin"  # nosec B105 - default for local dev
                    self.password = "admin"  # nosec B105 - default for local dev
                else:
                    logger.info(
                        "Token endpoint not available (Airflow 2.x). "
                        "Using provided credentials for basic auth."
                    )
                return

            response.raise_for_status()
            data = response.json()

            if "access_token" in data:
                self._token = data["access_token"]
                self._token_fetched_at = time.time()
                self._token_endpoint_available = True
                if "expires_in" in data:
                    self._token_lifetime_seconds = float(data["expires_in"])
                logger.info("Successfully fetched Airflow API token")
            else:
                logger.warning("Unexpected token response format: %s", data)
                self._token = None

        except httpx.RequestError as e:
            logger.warning("Failed to fetch token from %s: %s", token_url, e)
            self._token = None

    def invalidate(self) -> None:
        """Force token refresh on next request."""
        self._token = None
        self._token_fetched_at = None


# Create MCP server
mcp = FastMCP(
    "Airflow MCP Server",
    instructions="""
    This server provides access to Apache Airflow's REST API through MCP tools.

    Use these tools to:
    - List and inspect DAGs (Directed Acyclic Graphs / workflows)
    - View DAG runs and their execution status
    - Check task instances and their states
    - Inspect Airflow connections, variables, and pools
    - Monitor DAG statistics and warnings
    - View system configuration and version information

    When the user asks about Airflow workflows, pipelines, or data orchestration,
    use these tools to provide detailed, accurate information directly from the
    Airflow instance.
    """,
)

# Add logging middleware to log all MCP tool calls
mcp.add_middleware(LoggingMiddleware(include_payloads=True))


# Global configuration for Airflow API access
class AirflowConfig:
    """Global configuration for Airflow API access."""

    def __init__(self):
        self.url: str = DEFAULT_AIRFLOW_URL
        self.auth_token: str | None = None
        self.token_manager: AirflowTokenManager | None = None
        self.project_dir: str | None = None


_config = AirflowConfig()

# Global adapter instance (lazy-initialized)
_adapter: AirflowAdapter | None = None


def _get_adapter() -> AirflowAdapter:
    """Get or create the global adapter instance."""
    global _adapter
    if _adapter is None:
        logger.info("Initializing adapter for %s", _config.url)
        _adapter = create_adapter(
            airflow_url=_config.url,
            token_getter=_get_auth_token,
            basic_auth_getter=_get_basic_auth,
        )
        logger.info("Created adapter for Airflow %s", _adapter.version)
    return _adapter


def _reset_adapter() -> None:
    """Reset the global adapter (e.g., when config changes)."""
    global _adapter
    _adapter = None


def configure(
    url: str | None = None,
    auth_token: str | None = None,
    username: str | None = None,
    password: str | None = None,
    project_dir: str | None = None,
) -> None:
    """Configure global Airflow connection settings.

    Args:
        url: Base URL of Airflow webserver
        auth_token: Direct bearer token for authentication (takes precedence)
        username: Username for token-based authentication
        password: Password for token-based authentication
        project_dir: Project directory where Claude Code is running

    If auth_token is provided, it will be used directly.
    If username/password are provided (without auth_token), a token manager
    will be created to fetch and refresh tokens automatically.
    If neither is provided, credential-less token fetch will be attempted.
    """
    if project_dir:
        _config.project_dir = project_dir
    if url:
        _config.url = url
    if auth_token:
        _config.auth_token = auth_token
        _config.token_manager = None
    elif username or password:
        _config.auth_token = None
        _config.token_manager = AirflowTokenManager(
            airflow_url=_config.url,
            username=username,
            password=password,
        )
    else:
        _config.auth_token = None
        _config.token_manager = AirflowTokenManager(
            airflow_url=_config.url,
            username=None,
            password=None,
        )

    _reset_adapter()


def _get_auth_token() -> str | None:
    """Get the current authentication token."""
    if _config.auth_token:
        return _config.auth_token
    if _config.token_manager:
        return _config.token_manager.get_token()
    return None


def _get_basic_auth() -> tuple[str, str] | None:
    """Get basic auth credentials for Airflow 2.x fallback."""
    if _config.token_manager:
        return _config.token_manager.get_basic_auth()
    return None


def get_project_dir() -> str | None:
    """Get the configured project directory."""
    return _config.project_dir


def _invalidate_token() -> None:
    """Invalidate the current token to force refresh on next request."""
    if _config.token_manager:
        _config.token_manager.invalidate()


def _is_read_only() -> bool:
    return os.getenv("AF_READ_ONLY", "").lower() in ("true", "1", "yes")


def _check_write_allowed(operation_name: str) -> str | None:
    if _is_read_only():
        return json.dumps(
            {
                "error": "Write operation blocked",
                "operation": operation_name,
                "reason": "Server is in read-only mode (AF_READ_ONLY=true)",
            },
            indent=2,
        )
    return None


# Helper functions for response formatting
def _wrap_list_response(items: list[dict[str, Any]], key_name: str, data: dict[str, Any]) -> str:
    """Wrap API list response with pagination metadata."""
    total_entries = data.get("total_entries", len(items))
    result: dict[str, Any] = {
        f"total_{key_name}": total_entries,
        "returned_count": len(items),
        key_name: items,
    }
    return json.dumps(result, indent=2)


def _environment_label() -> str:
    """Return a short label identifying the connected Airflow environment."""
    parsed = urlparse(_config.url)
    host = parsed.hostname or "localhost"
    port = parsed.port
    if port and port not in (80, 443):
        return f"{host}:{port}"
    return host


# =============================================================================
# MCP RESOURCES (Static, read-only information)
# =============================================================================


@mcp.resource("airflow://version")
def resource_version() -> str:
    """Get Airflow version information as a resource."""
    from astro_airflow_mcp.tools.system import _get_version_impl
    return _get_version_impl()


@mcp.resource("airflow://providers")
def resource_providers() -> str:
    """Get installed Airflow providers as a resource."""
    from astro_airflow_mcp.tools.admin import _list_providers_impl
    return _list_providers_impl()


@mcp.resource("airflow://plugins")
def resource_plugins() -> str:
    """Get installed Airflow plugins as a resource."""
    from astro_airflow_mcp.tools.admin import _list_plugins_impl
    return _list_plugins_impl()


@mcp.resource("airflow://config")
def resource_config() -> str:
    """Get Airflow configuration as a resource."""
    from astro_airflow_mcp.tools.system import _get_config_impl
    return _get_config_impl()


# =============================================================================
# MCP PROMPTS (Guided workflows)
# =============================================================================


@mcp.prompt()
def troubleshoot_failed_dag(dag_id: str) -> str:
    """Step-by-step guide to troubleshoot a failed DAG.

    Args:
        dag_id: The DAG ID to troubleshoot
    """
    return f"""You are helping troubleshoot failures for DAG '{dag_id}'. Follow these steps:

1. First, use `explore_dag` to understand the DAG structure and check for any import errors.

2. Use `list_dag_runs` (filter by dag_id if possible) to find recent failed runs.

3. For each failed run, use `diagnose_dag_run` to get detailed information about:
   - Which tasks failed
   - The state of upstream tasks
   - Start/end times to understand duration

4. Based on the failed tasks, investigate:
   - Check task logs if available
   - Look at task dependencies (upstream_task_ids)
   - Check if any pools are at capacity using `list_pools`

5. Check system-wide issues using `get_system_health` to see if there are
   import errors or warnings that might be related.

6. Summarize your findings and provide recommendations for fixing the issues.

Start by running `explore_dag("{dag_id}")` to understand the DAG.
"""


@mcp.prompt()
def daily_health_check() -> str:
    """Morning health check workflow for Airflow."""
    return """You are performing a daily health check on the Airflow system. Follow these steps:

1. Start with `get_system_health` to get an overview of:
   - Import errors (broken DAG files)
   - DAG warnings
   - Overall system status

2. If there are import errors, prioritize investigating those first as they prevent DAGs from running.

3. Use `list_dag_runs` to see recent DAG run activity and identify any failures.

4. Check resource utilization with `list_pools` to see if any pools are at capacity.

5. Review `list_connections` to ensure all expected connections are configured.

6. Summarize the health status with:
   - Number of healthy vs problematic DAGs
   - Any blocking issues
   - Recommended actions

Start by running `get_system_health()` to assess the overall system state.
"""


@mcp.prompt()
def onboard_new_dag(dag_id: str) -> str:
    """Guide to understanding a new DAG.

    Args:
        dag_id: The DAG ID to learn about
    """
    return f"""You are helping someone understand the DAG '{dag_id}'. Provide a thorough overview:

1. Use `explore_dag` to get comprehensive DAG information including:
   - Schedule and timing
   - Owner and tags
   - All tasks and their relationships
   - Source code

2. Explain the DAG's purpose based on its description and task structure.

3. Walk through the task dependencies - what runs first, what runs in parallel,
   what are the critical path tasks.

4. Identify any external dependencies:
   - Check what connections the DAG might use with `list_connections`
   - Check for any assets/datasets it produces or consumes with `list_assets`

5. Show recent execution history with `list_dag_runs` filtered to this DAG.

6. Highlight any potential issues:
   - Is the DAG paused?
   - Are there any warnings?
   - What's the recent success/failure rate?

Start by running `explore_dag("{dag_id}")` to get the full picture.
"""


# =============================================================================
# REGISTER TOOLS (import subpackage to register all @mcp.tool() decorators)
# =============================================================================

import astro_airflow_mcp.tools  # noqa: E402, F401
from astro_airflow_mcp.tools.asset import (  # noqa: E402, F401
    _get_upstream_asset_events_impl,
    _list_asset_events_impl,
)

# =============================================================================
# BACKWARD COMPATIBILITY RE-EXPORTS (used by tests)
# =============================================================================
from astro_airflow_mcp.tools.dag import (  # noqa: E402, F401
    _get_dag_details_impl,
    _list_dags_impl,
)
from astro_airflow_mcp.tools.dag_run import (  # noqa: E402, F401
    _clear_dag_run_impl,
    _clear_task_instances_impl,
    _pause_dag_impl,
    _trigger_dag_impl,
    _unpause_dag_impl,
)
from astro_airflow_mcp.tools.diagnostic import (  # noqa: E402, F401
    diagnose_dag_run,
    explore_dag,
    get_system_health,
)
