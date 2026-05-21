"""Task definition and instance tools."""

import json
from pathlib import Path
from typing import Any

from astro_airflow_mcp.server import TASK_ESSENTIAL_FIELDS, mcp
from astro_airflow_mcp.tools._common import _get_adapter, _wrap_list_response

LOG_DIR = "/tmp/airflow-logs"


def _get_task_impl(dag_id: str, task_id: str) -> str:
    try:
        adapter = _get_adapter()
        data = adapter.get_task(dag_id, task_id)
        return json.dumps(data, indent=2)
    except Exception as e:
        return str(e)


def _list_tasks_impl(dag_id: str) -> str:
    try:
        adapter = _get_adapter()
        data = adapter.list_tasks(dag_id)

        if "tasks" in data:
            trimmed = [
                {k: v for k, v in t.items() if k in TASK_ESSENTIAL_FIELDS}
                for t in data["tasks"]
            ]
            return _wrap_list_response(trimmed, "tasks", data)
        return f"No tasks found. Response: {data}"
    except Exception as e:
        return str(e)


def _get_task_instance_impl(dag_id: str, dag_run_id: str, task_id: str) -> str:
    try:
        adapter = _get_adapter()
        data = adapter.get_task_instance(dag_id, dag_run_id, task_id)
        return json.dumps(data, indent=2)
    except Exception as e:
        return str(e)


def _get_task_logs_impl(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    try_number: int = 1,
    map_index: int = -1,
) -> str:
    try:
        adapter = _get_adapter()
        data = adapter.get_task_logs(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            try_number=try_number,
            map_index=map_index,
            full_content=True,
        )

        raw = data.get("content", "") if isinstance(data, dict) else data
        content = "\n".join(str(entry) for entry in raw) if isinstance(raw, list) else str(raw)
        total_chars = len(content)
        total_lines = content.count("\n")

        log_dir = Path(LOG_DIR)
        log_dir.mkdir(parents=True, exist_ok=True)

        safe_run_id = dag_run_id.replace("/", "_").replace(":", "-")
        filename = f"{dag_id}__{task_id}__try{try_number}__{safe_run_id}.log"
        filepath = log_dir / filename
        filepath.write_text(content)

        result = {
            "status": "saved",
            "file_path": str(filepath),
            "dag_id": dag_id,
            "task_id": task_id,
            "dag_run_id": dag_run_id,
            "try_number": try_number,
            "total_chars": total_chars,
            "total_lines": total_lines,
            "note": (
                f"Logs saved to {filepath} ({total_chars:,} chars, "
                f"{total_lines:,} lines). Use Read tool on this file to "
                "inspect content, or search for errors with grep."
            ),
        }
        return json.dumps(result, indent=2)
    except Exception as e:
        return str(e)


@mcp.tool()
def get_task(dag_id: str, task_id: str) -> str:
    """Get detailed information about a specific task definition in a DAG.

    Use this tool when the user asks about:
    - "Show me details for task X in DAG Y" or "What does task Z do?"
    - "What operator does task A use?" or "What's the configuration of task B?"
    - "Tell me about task C" or "Get task definition for D"
    - "What are the dependencies of task E?" or "Which tasks does F depend on?"

    Returns task definition information including:
    - task_id: Unique identifier for the task
    - task_display_name: Human-readable display name
    - owner: Who owns this task
    - start_date: When this task becomes active
    - end_date: When this task becomes inactive (if set)
    - trigger_rule: When this task should run (all_success, one_failed, etc.)
    - depends_on_past: Whether task depends on previous run's success
    - wait_for_downstream: Whether to wait for downstream tasks
    - retries: Number of retry attempts
    - retry_delay: Time between retries
    - execution_timeout: Maximum execution time
    - operator_name: Type of operator (PythonOperator, BashOperator, etc.)
    - pool: Resource pool assignment
    - queue: Queue for executor
    - downstream_task_ids: List of tasks that depend on this task
    - upstream_task_ids: List of tasks this task depends on

    Args:
        dag_id: The ID of the DAG containing the task
        task_id: The ID of the task to get details for

    Returns:
        JSON with complete task definition details
    """
    return _get_task_impl(dag_id=dag_id, task_id=task_id)


@mcp.tool()
def list_tasks(dag_id: str) -> str:
    """Get all tasks defined in a specific DAG.

    Use this tool when the user asks about:
    - "What tasks are in DAG X?" or "List all tasks for DAG Y"
    - "Show me the tasks in this workflow" or "What's in the DAG?"
    - "What are the steps in DAG Z?" or "Show me the task structure"
    - "What does this DAG do?" or "Explain the workflow steps"

    Returns information about all tasks in the DAG including:
    - task_id: Unique identifier for the task
    - task_display_name: Human-readable display name
    - owner: Who owns this task
    - operator_name: Type of operator (PythonOperator, BashOperator, etc.)
    - start_date: When this task becomes active
    - end_date: When this task becomes inactive (if set)
    - trigger_rule: When this task should run
    - retries: Number of retry attempts
    - pool: Resource pool assignment
    - downstream_task_ids: List of tasks that depend on this task
    - upstream_task_ids: List of tasks this task depends on

    Args:
        dag_id: The ID of the DAG to list tasks for

    Returns:
        JSON with list of all tasks in the DAG and their configurations
    """
    return _list_tasks_impl(dag_id=dag_id)


@mcp.tool()
def get_task_instance(dag_id: str, dag_run_id: str, task_id: str) -> str:
    """Get detailed information about a specific task instance execution.

    Use this tool when the user asks about:
    - "Show me details for task X in DAG run Y" or "What's the status of task Z?"
    - "Why did task A fail?" or "When did task B start/finish?"
    - "What's the duration of task C?" or "Show me task execution details"
    - "Get logs for task D" or "What operator does task E use?"

    Returns detailed task instance information including:
    - task_id: Name of the task
    - state: Current state (success, failed, running, queued, etc.)
    - start_date: When the task started
    - end_date: When the task finished
    - duration: How long the task ran
    - try_number: Which attempt this is
    - max_tries: Maximum retry attempts
    - operator: What operator type (PythonOperator, BashOperator, etc.)
    - executor_config: Executor configuration
    - pool: Resource pool assignment

    Args:
        dag_id: The ID of the DAG
        dag_run_id: The ID of the DAG run (e.g., "manual__2024-01-01T00:00:00+00:00")
        task_id: The ID of the task within the DAG

    Returns:
        JSON with complete task instance details
    """
    return _get_task_instance_impl(dag_id=dag_id, dag_run_id=dag_run_id, task_id=task_id)


@mcp.tool()
def get_task_logs(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    try_number: int = 1,
    map_index: int = -1,
) -> str:
    """Get logs for a specific task instance execution.

    Use this tool when the user asks about:
    - "Show me the logs for task X" or "Get logs for task Y"
    - "What did task Z output?" or "Show me task execution logs"
    - "Why did task A fail?" (to see error messages in logs)
    - "What happened during task B execution?"
    - "Show me the stdout/stderr for task C"
    - "Debug task D" or "Troubleshoot task E"

    Returns the actual log output from the task execution, which includes:
    - Task execution output (stdout/stderr)
    - Error messages and stack traces (if task failed)
    - Timing information
    - Any logged messages from the task code

    This is essential for debugging failed tasks or understanding what
    happened during task execution.

    Args:
        dag_id: The ID of the DAG (e.g., "example_dag")
        dag_run_id: The ID of the DAG run (e.g., "manual__2024-01-01T00:00:00+00:00")
        task_id: The ID of the task within the DAG (e.g., "extract_data")
        try_number: The task try/attempt number, 1-indexed (default: 1).
                    Use higher numbers to get logs from retry attempts.
        map_index: For mapped tasks, which map index to get logs for.
                   Use -1 for non-mapped tasks (default: -1).

    Returns:
        JSON with the task logs content
    """
    return _get_task_logs_impl(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        try_number=try_number,
        map_index=map_index,
    )


def _list_task_instances_batch_impl(
    pool: list[str] | None = None,
    state: list[str] | None = None,
    dag_ids: list[str] | None = None,
    logical_date_gte: str | None = None,
    logical_date_lte: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> str:
    try:
        adapter = _get_adapter()
        data = adapter.list_task_instances_batch(
            dag_ids=dag_ids,
            pool=pool,
            state=state,
            logical_date_gte=logical_date_gte,
            logical_date_lte=logical_date_lte,
            limit=limit,
            offset=offset,
        )

        task_instances = data.get("task_instances", [])
        total = data.get("total_entries", len(task_instances))

        trimmed: list[dict[str, Any]] = []
        for ti in task_instances:
            trimmed.append({
                "dag_id": ti.get("dag_id"),
                "task_id": ti.get("task_id"),
                "dag_run_id": ti.get("dag_run_id"),
                "state": ti.get("state"),
                "logical_date": ti.get("logical_date"),
                "start_date": ti.get("start_date"),
                "end_date": ti.get("end_date"),
                "duration": ti.get("duration"),
                "pool": ti.get("pool"),
                "operator": ti.get("operator_name"),
                "try_number": ti.get("try_number"),
            })

        result: dict[str, Any] = {
            "total_entries": total,
            "returned_count": len(trimmed),
            "offset": offset,
            "task_instances": trimmed,
        }
        return json.dumps(result, indent=2)
    except Exception as e:
        return str(e)


@mcp.tool()
def list_task_instances_batch(
    pool: list[str] | None = None,
    state: list[str] | None = None,
    dag_ids: list[str] | None = None,
    logical_date_gte: str | None = None,
    logical_date_lte: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> str:
    """Query task instances across multiple DAGs with filters.

    Use this tool when the user asks about:
    - "How many ETL tasks ran today?"
    - "Show me all failed tasks in pool X"
    - "What tasks are running right now across all DAGs?"
    - "List all task instances for these DAGs from this week"
    - "How many tasks use the heavy_compute pool?"
    - "Show me tasks that failed yesterday"

    This is a powerful batch query that searches across ALL DAGs without
    needing to specify a specific DAG or run. Supports filtering by pool,
    state, date range, and DAG IDs.

    Returns trimmed task instance data including:
    - dag_id: Which DAG this task belongs to
    - task_id: The task name
    - dag_run_id: Which run this instance is part of
    - state: Current state (success, failed, running, queued, etc.)
    - logical_date: The logical/execution date
    - start_date/end_date: When it ran
    - duration: How long it took (seconds)
    - pool: Resource pool assignment
    - operator: Operator type

    Args:
        pool: Filter by pool names. Example: ["etl_pool", "default_pool"]
        state: Filter by task states. Example: ["failed", "success"]
        dag_ids: Filter to specific DAG IDs.
        logical_date_gte: Only tasks from runs on or after this date (ISO 8601).
                          Example: "2026-05-21T00:00:00Z"
        logical_date_lte: Only tasks from runs on or before this date (ISO 8601).
        limit: Maximum instances to return (default: 100, max varies by server)
        offset: Pagination offset (default: 0)

    Returns:
        JSON with matching task instances, total count, and pagination info
    """
    return _list_task_instances_batch_impl(
        pool=pool,
        state=state,
        dag_ids=dag_ids,
        logical_date_gte=logical_date_gte,
        logical_date_lte=logical_date_lte,
        limit=limit,
        offset=offset,
    )
