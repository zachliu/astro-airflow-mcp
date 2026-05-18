"""Consolidated diagnostic tools for complex investigations."""

import json
from typing import Any

from astro_airflow_mcp.server import TASK_ESSENTIAL_FIELDS, mcp
from astro_airflow_mcp.tools._common import _get_adapter


@mcp.tool()
def explore_dag(dag_id: str) -> str:
    """Comprehensive investigation of a DAG - get all relevant info in one call.

    USE THIS TOOL WHEN you need to understand a DAG completely. Instead of making
    multiple calls, this returns everything about a DAG in a single response.

    This is the preferred first tool when:
    - User asks "Tell me about DAG X" or "What is this DAG?"
    - You need to understand a DAG's structure before diagnosing issues
    - You want to know the schedule, tasks, and source code together

    Returns combined data:
    - DAG metadata (schedule, owners, tags, paused status)
    - All tasks with their operators and dependencies
    - DAG source code
    - Any import errors or warnings for this DAG

    Args:
        dag_id: The ID of the DAG to explore

    Returns:
        JSON with comprehensive DAG information
    """
    result: dict[str, Any] = {"dag_id": dag_id}
    adapter = _get_adapter()

    try:
        result["dag_info"] = adapter.get_dag(dag_id)
    except Exception as e:
        result["dag_info"] = {"error": str(e)}

    try:
        tasks_data = adapter.list_tasks(dag_id)
        result["tasks"] = [
            {k: v for k, v in t.items() if k in TASK_ESSENTIAL_FIELDS}
            for t in tasks_data.get("tasks", [])
        ]
    except Exception as e:
        result["tasks"] = {"error": str(e)}

    try:
        result["source"] = adapter.get_dag_source(dag_id)
    except Exception as e:
        result["source"] = {"error": str(e)}

    return json.dumps(result, indent=2)


@mcp.tool()
def diagnose_dag_run(dag_id: str, dag_run_id: str) -> str:
    """Diagnose issues with a specific DAG run - get run details and failed tasks.

    USE THIS TOOL WHEN troubleshooting a failed or problematic DAG run. Returns
    all the information you need to understand what went wrong.

    This is the preferred tool when:
    - User asks "Why did this DAG run fail?"
    - User asks "What's wrong with run X?"
    - You need to investigate task failures in a specific run

    Returns combined data:
    - DAG run metadata (state, start/end times, trigger type)
    - All task instances for this run with their states
    - Highlighted failed/upstream_failed tasks with details
    - Summary of task states

    Args:
        dag_id: The ID of the DAG
        dag_run_id: The ID of the DAG run (e.g., "manual__2024-01-01T00:00:00+00:00")

    Returns:
        JSON with diagnostic information about the DAG run
    """
    result: dict[str, Any] = {"dag_id": dag_id, "dag_run_id": dag_run_id}
    adapter = _get_adapter()

    try:
        result["run_info"] = adapter.get_dag_run(dag_id, dag_run_id)
    except Exception as e:
        result["run_info"] = {"error": str(e)}
        return json.dumps(result, indent=2)

    try:
        tasks_data = adapter.get_task_instances(dag_id, dag_run_id)
        task_instances = tasks_data.get("task_instances", [])

        keep_fields = {
            "task_id",
            "state",
            "start_date",
            "end_date",
            "duration",
            "try_number",
            "operator_name",
        }
        result["task_instances"] = [
            {k: v for k, v in ti.items() if k in keep_fields}
            for ti in task_instances
        ]

        state_counts: dict[str, int] = {}
        failed_tasks = []
        for ti in task_instances:
            state = ti.get("state", "unknown")
            state_counts[state] = state_counts.get(state, 0) + 1
            if state in ("failed", "upstream_failed"):
                failed_tasks.append(
                    {
                        "task_id": ti.get("task_id"),
                        "state": state,
                        "start_date": ti.get("start_date"),
                        "end_date": ti.get("end_date"),
                        "duration": ti.get("duration"),
                        "try_number": ti.get("try_number"),
                        "operator_name": ti.get("operator_name"),
                    }
                )

        result["summary"] = {
            "total_tasks": len(task_instances),
            "state_counts": state_counts,
            "failed_tasks": failed_tasks,
        }
    except Exception as e:
        result["task_instances"] = {"error": str(e)}

    return json.dumps(result, indent=2)


@mcp.tool()
def get_system_health() -> str:
    """Get overall Airflow system health - import errors, warnings, and DAG stats.

    USE THIS TOOL WHEN you need a quick health check of the Airflow system.
    Returns a consolidated view of potential issues across the entire system.

    This is the preferred tool when:
    - User asks "Are there any problems with Airflow?"
    - User asks "Show me the system health" or "Any errors?"
    - You want to do a morning health check
    - You're starting an investigation and want to see the big picture

    Returns combined data:
    - Import errors (DAG files that failed to parse)
    - DAG warnings (deprecations, configuration issues)
    - DAG statistics (run counts by state) if available
    - Version information

    Returns:
        JSON with system health overview
    """
    result: dict[str, Any] = {}
    adapter = _get_adapter()

    try:
        result["version"] = adapter.get_version()
    except Exception as e:
        result["version"] = {"error": str(e)}

    try:
        errors_data = adapter.list_import_errors(limit=100)
        import_errors = errors_data.get("import_errors", [])
        result["import_errors"] = {
            "count": len(import_errors),
            "errors": import_errors,
        }
    except Exception as e:
        result["import_errors"] = {"error": str(e)}

    try:
        warnings_data = adapter.list_dag_warnings(limit=100)
        dag_warnings = warnings_data.get("dag_warnings", [])
        result["dag_warnings"] = {
            "count": len(dag_warnings),
            "warnings": dag_warnings,
        }
    except Exception as e:
        result["dag_warnings"] = {"error": str(e)}

    try:
        result["dag_stats"] = adapter.get_dag_stats()
    except Exception:
        result["dag_stats"] = {"available": False, "note": "dagStats endpoint not available"}

    import_error_count = result.get("import_errors", {}).get("count", 0)
    warning_count = result.get("dag_warnings", {}).get("count", 0)

    if import_error_count > 0:
        result["overall_status"] = "unhealthy"
        result["status_reason"] = f"{import_error_count} import error(s) detected"
    elif warning_count > 0:
        result["overall_status"] = "warning"
        result["status_reason"] = f"{warning_count} DAG warning(s) detected"
    else:
        result["overall_status"] = "healthy"
        result["status_reason"] = "No import errors or warnings"

    return json.dumps(result, indent=2)
