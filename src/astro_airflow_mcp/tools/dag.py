"""DAG metadata and configuration tools."""

import json

from astro_airflow_mcp.logging import get_logger
from astro_airflow_mcp.server import mcp
from astro_airflow_mcp.tools._common import _get_adapter, _wrap_list_response

logger = get_logger(__name__)

DEFAULT_LIMIT = 100
DEFAULT_OFFSET = 0


def _get_dag_details_impl(dag_id: str) -> str:
    try:
        adapter = _get_adapter()
        data = adapter.get_dag(dag_id)
        return json.dumps(data, indent=2)
    except Exception as e:
        return str(e)


@mcp.tool()
def get_dag_details(dag_id: str) -> str:
    """Get detailed information about a specific Apache Airflow DAG.

    Use this tool when the user asks about:
    - "Show me details for DAG X" or "What are the details of DAG Y?"
    - "Tell me about DAG Z" or "Get information for this specific DAG"
    - "What's the schedule for DAG X?" or "When does this DAG run?"
    - "Is DAG Y paused?" or "Show me the configuration of DAG Z"
    - "Who owns this DAG?" or "What are the tags for this workflow?"

    Returns complete DAG information including:
    - dag_id: Unique identifier for the DAG
    - is_paused: Whether the DAG is currently paused
    - is_active: Whether the DAG is active
    - is_subdag: Whether this is a SubDAG
    - fileloc: File path where the DAG is defined
    - file_token: Unique token for the DAG file
    - owners: List of DAG owners
    - description: Human-readable description of what the DAG does
    - schedule_interval: Cron expression or timedelta for scheduling
    - tags: List of tags/labels for categorization
    - max_active_runs: Maximum number of concurrent runs
    - max_active_tasks: Maximum number of concurrent tasks
    - has_task_concurrency_limits: Whether task concurrency limits are set
    - has_import_errors: Whether the DAG has import errors
    - next_dagrun: When the next DAG run is scheduled
    - next_dagrun_create_after: Earliest time for next DAG run creation

    Args:
        dag_id: The ID of the DAG to get details for

    Returns:
        JSON with complete details about the specified DAG
    """
    return _get_dag_details_impl(dag_id=dag_id)


def _list_dags_impl(
    limit: int = DEFAULT_LIMIT,
    offset: int = DEFAULT_OFFSET,
) -> str:
    try:
        adapter = _get_adapter()
        data = adapter.list_dags(limit=limit, offset=offset)

        if "dags" not in data:
            return f"No DAGs found. Response: {data}"

        all_dags = list(data["dags"])
        total = data.get("total_entries") or data.get("total_dags")

        if total and offset == 0:
            if total > 500:
                logger.warning(
                    "list_dags: auto-paginating through %d DAGs. "
                    "This may produce a large response.",
                    total,
                )
            while len(all_dags) < total:
                page = adapter.list_dags(limit=limit, offset=len(all_dags))
                batch = page.get("dags", [])
                if not batch:
                    break
                all_dags.extend(batch)

        data["dags"] = all_dags
        return _wrap_list_response(all_dags, "dags", data)
    except Exception as e:
        return str(e)


@mcp.tool()
def list_dags() -> str:
    """Get information about all Apache Airflow DAGs (Directed Acyclic Graphs).

    Use this tool when the user asks about:
    - "What DAGs are available?" or "List all DAGs"
    - "Show me the workflows" or "What pipelines exist?"
    - "Which DAGs are paused/active?"
    - DAG schedules, descriptions, or tags
    - Finding a specific DAG by name

    Returns comprehensive DAG metadata including:
    - dag_id: Unique identifier for the DAG
    - is_paused: Whether the DAG is currently paused
    - is_active: Whether the DAG is active
    - schedule_interval: How often the DAG runs
    - description: Human-readable description
    - tags: Labels/categories for the DAG
    - owners: Who maintains the DAG
    - file_token: Location of the DAG file

    Returns:
        JSON with list of all DAGs and their complete metadata
    """
    return _list_dags_impl()


def _get_dag_source_impl(dag_id: str) -> str:
    try:
        adapter = _get_adapter()
        source_data = adapter.get_dag_source(dag_id)
        return json.dumps(source_data, indent=2)
    except Exception as e:
        return str(e)


@mcp.tool()
def get_dag_source(dag_id: str) -> str:
    """Get the source code for a specific Apache Airflow DAG.

    Use this tool when the user asks about:
    - "Show me the code for DAG X" or "What's the source of DAG Y?"
    - "How is DAG Z implemented?" or "What does the DAG file look like?"
    - "Can I see the Python code for this workflow?"
    - "What tasks are defined in the DAG code?"

    Returns the DAG source file contents including:
    - content: The actual Python source code of the DAG file
    - file_token: Unique identifier for the source file

    Args:
        dag_id: The ID of the DAG to get source code for

    Returns:
        JSON with DAG source code and metadata
    """
    return _get_dag_source_impl(dag_id=dag_id)


def _get_dag_stats_impl(dag_ids: list[str] | None = None) -> str:
    try:
        adapter = _get_adapter()
        stats_data = adapter.get_dag_stats(dag_ids=dag_ids)
        return json.dumps(stats_data, indent=2)
    except Exception as e:
        return str(e)


@mcp.tool()
def get_dag_stats(dag_ids: list[str] | None = None) -> str:
    """Get statistics about DAG runs (success/failure counts by state).

    Use this tool when the user asks about:
    - "What's the overall health of my DAGs?" or "Show me DAG statistics"
    - "How many DAG runs succeeded/failed?" or "What's the success rate?"
    - "Give me a summary of DAG run states"
    - "How many runs are currently running/queued?"
    - "Show me stats for specific DAGs"

    Returns statistics showing counts of DAG runs grouped by state:
    - success: Number of successful runs
    - failed: Number of failed runs
    - running: Number of currently running runs
    - queued: Number of queued runs
    - And other possible states

    Args:
        dag_ids: Optional list of DAG IDs to filter by. If not provided, returns stats for all DAGs.

    Returns:
        JSON with DAG run statistics organized by DAG and state
    """
    return _get_dag_stats_impl(dag_ids=dag_ids)


def _list_dag_warnings_impl(
    limit: int = DEFAULT_LIMIT,
    offset: int = DEFAULT_OFFSET,
) -> str:
    try:
        adapter = _get_adapter()
        data = adapter.list_dag_warnings(limit=limit, offset=offset)

        if "dag_warnings" in data:
            return _wrap_list_response(data["dag_warnings"], "dag_warnings", data)
        return f"No DAG warnings found. Response: {data}"
    except Exception as e:
        return str(e)


def _list_import_errors_impl(
    limit: int = DEFAULT_LIMIT,
    offset: int = DEFAULT_OFFSET,
) -> str:
    try:
        adapter = _get_adapter()
        data = adapter.list_import_errors(limit=limit, offset=offset)

        if "import_errors" in data:
            return _wrap_list_response(data["import_errors"], "import_errors", data)
        return f"No import errors found. Response: {data}"
    except Exception as e:
        return str(e)


@mcp.tool()
def list_dag_warnings() -> str:
    """Get warnings and issues detected in DAG definitions.

    Use this tool when the user asks about:
    - "Are there any DAG warnings?" or "Show me DAG issues"
    - "What problems exist with my DAGs?" or "Any DAG errors?"
    - "Check DAG health" or "Show me DAG validation warnings"
    - "What's wrong with my workflows?"

    Returns warnings about DAG configuration issues including:
    - dag_id: Which DAG has the warning
    - warning_type: Type of warning (e.g., deprecation, configuration issue)
    - message: Description of the warning
    - timestamp: When the warning was detected

    Returns:
        JSON with list of DAG warnings and their details
    """
    return _list_dag_warnings_impl()


@mcp.tool()
def list_import_errors() -> str:
    """Get import errors from DAG files that failed to parse or load.

    Use this tool when the user asks about:
    - "Are there any import errors?" or "Show me import errors"
    - "Why isn't my DAG showing up?" or "DAG not appearing in Airflow"
    - "What DAG files have errors?" or "Show me broken DAGs"
    - "Check for syntax errors" or "Are there any parsing errors?"
    - "Why is my DAG file failing to load?"

    Import errors occur when DAG files have problems that prevent Airflow
    from parsing them, such as:
    - Python syntax errors
    - Missing imports or dependencies
    - Module not found errors
    - Invalid DAG definitions
    - Runtime errors during file parsing

    Returns import error details including:
    - import_error_id: Unique identifier for the error
    - timestamp: When the error was detected
    - filename: Path to the DAG file with the error
    - stack_trace: Complete error message and traceback

    Returns:
        JSON with list of import errors and their stack traces
    """
    return _list_import_errors_impl()
