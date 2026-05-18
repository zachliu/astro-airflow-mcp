"""DAG run execution and lifecycle tools."""

import json
import time
from typing import Any

from astro_airflow_mcp.server import TERMINAL_DAG_RUN_STATES, mcp
from astro_airflow_mcp.tools._common import (
    _check_write_allowed,
    _environment_label,
    _get_adapter,
    _wrap_list_response,
)

DEFAULT_LIMIT = 100
DEFAULT_OFFSET = 0


def _list_dag_runs_impl(
    dag_id: str | None = None,
    state: str | None = None,
    limit: int = DEFAULT_LIMIT,
    offset: int = DEFAULT_OFFSET,
    start_date_gte: str | None = None,
    start_date_lte: str | None = None,
    order_by: str | None = None,
) -> str:
    try:
        adapter = _get_adapter()
        kwargs: dict[str, Any] = {}
        if state:
            kwargs["state"] = state
        if start_date_gte:
            kwargs["start_date_gte"] = start_date_gte
        if start_date_lte:
            kwargs["start_date_lte"] = start_date_lte
        if order_by:
            kwargs["order_by"] = order_by

        data = adapter.list_dag_runs(
            dag_id=dag_id, limit=limit, offset=offset, **kwargs
        )

        if "dag_runs" in data:
            return _wrap_list_response(data["dag_runs"], "dag_runs", data)
        return f"No DAG runs found. Response: {data}"
    except Exception as e:
        return str(e)


@mcp.tool()
def list_dag_runs(
    dag_id: str | None = None,
    state: str | None = None,
    limit: int = 25,
    start_date_gte: str | None = None,
    start_date_lte: str | None = None,
    order_by: str | None = None,
) -> str:
    """Get execution history and status of DAG runs (workflow executions).

    IMPORTANT: Production Airflow instances can have 100K+ DAG runs.
    Always filter by dag_id or state to avoid enormous responses.
    If the user asks a broad question like "show me recent runs", ask them
    to specify a DAG name or state (failed, running, etc.) first.

    Use this tool when the user asks about:
    - "What DAG runs have executed?" or "Show me recent runs"
    - "Which runs failed/succeeded?"
    - "What's the status of my workflows?"
    - "When did DAG X last run?"
    - Execution times, durations, or states
    - Finding runs by date or status

    Returns execution metadata including:
    - dag_run_id: Unique identifier for this execution
    - dag_id: Which DAG this run belongs to
    - state: Current state (running, success, failed, queued)
    - logical_date: Logical/execution date for this run
    - start_date: When execution actually started
    - end_date: When execution completed (if finished)
    - run_type: manual, scheduled, or backfill
    - conf: Configuration passed to this run

    Args:
        dag_id: Filter by DAG ID (omit for all DAGs - use with caution)
        state: Filter by state: 'failed', 'success', 'running', or 'queued'
        limit: Maximum number of runs to return (default: 25)
        start_date_gte: Filter runs starting on or after this date (ISO 8601, e.g. '2025-05-01T00:00:00Z')
        start_date_lte: Filter runs starting on or before this date (ISO 8601)
        order_by: Sort field with optional '-' prefix for descending (e.g. '-start_date')

    Returns:
        JSON with list of DAG runs matching the filters
    """
    return _list_dag_runs_impl(
        dag_id=dag_id,
        state=state,
        limit=limit,
        start_date_gte=start_date_gte,
        start_date_lte=start_date_lte,
        order_by=order_by,
    )


def _get_dag_run_impl(
    dag_id: str,
    dag_run_id: str,
) -> str:
    try:
        adapter = _get_adapter()
        data = adapter.get_dag_run(dag_id, dag_run_id)
        return json.dumps(data, indent=2)
    except Exception as e:
        return str(e)


@mcp.tool()
def get_dag_run(dag_id: str, dag_run_id: str) -> str:
    """Get detailed information about a specific DAG run execution.

    Use this tool when the user asks about:
    - "Show me details for DAG run X" or "What's the status of run Y?"
    - "When did this run start/finish?" or "How long did run Z take?"
    - "Why did this run fail?" or "Get execution details for run X"
    - "What was the configuration for this run?" or "Show me run metadata"
    - "What's the state of DAG run X?" or "Did run Y succeed?"

    Returns detailed information about a specific DAG run execution including:
    - dag_run_id: Unique identifier for this execution
    - dag_id: Which DAG this run belongs to
    - state: Current state (running, success, failed, queued, etc.)
    - execution_date: When this run was scheduled to execute
    - start_date: When execution actually started
    - end_date: When execution completed (if finished)
    - duration: How long the run took (in seconds)
    - run_type: Type of run (manual, scheduled, backfill, etc.)
    - conf: Configuration parameters passed to this run
    - external_trigger: Whether this was triggered externally
    - data_interval_start: Start of the data interval
    - data_interval_end: End of the data interval
    - last_scheduling_decision: Last scheduling decision timestamp
    - note: Optional note attached to the run

    Args:
        dag_id: The ID of the DAG (e.g., "example_dag")
        dag_run_id: The ID of the DAG run (e.g., "manual__2024-01-01T00:00:00+00:00")

    Returns:
        JSON with complete details about the specified DAG run
    """
    return _get_dag_run_impl(dag_id=dag_id, dag_run_id=dag_run_id)


def _trigger_dag_impl(
    dag_id: str,
    conf: dict | None = None,
    logical_date: str | None = None,
) -> str:
    blocked = _check_write_allowed("trigger_dag")
    if blocked:
        return blocked
    try:
        adapter = _get_adapter()
        auto_unpaused = False
        try:
            dag_info = adapter.get_dag(dag_id)
            if dag_info.get("is_paused"):
                adapter.unpause_dag(dag_id)
                auto_unpaused = True
        except Exception:
            pass
        data = adapter.trigger_dag_run(
            dag_id=dag_id, logical_date=logical_date, conf=conf
        )
        env = _environment_label()
        data["_environment"] = env
        if auto_unpaused:
            data["_auto_unpaused"] = True
            data["_warning"] = f"DAG '{dag_id}' was paused and has been automatically unpaused to allow this trigger."
        return json.dumps(data, indent=2)
    except Exception as e:
        return str(e)


def _get_failed_task_instances(
    dag_id: str,
    dag_run_id: str,
) -> list[dict[str, Any]]:
    try:
        adapter = _get_adapter()
        data = adapter.get_task_instances(dag_id, dag_run_id)

        failed_states = {"failed", "upstream_failed"}
        failed_tasks = []

        if "task_instances" in data:
            for task in data["task_instances"]:
                if task.get("state") in failed_states:
                    failed_tasks.append(
                        {
                            "task_id": task.get("task_id"),
                            "state": task.get("state"),
                            "try_number": task.get("try_number"),
                            "start_date": task.get("start_date"),
                            "end_date": task.get("end_date"),
                        }
                    )

        return failed_tasks
    except Exception:
        return []


def _trigger_dag_and_wait_impl(
    dag_id: str,
    conf: dict | None = None,
    logical_date: str | None = None,
    poll_interval: float = 5.0,
    timeout: float = 3600.0,
) -> str:
    blocked = _check_write_allowed("trigger_dag_and_wait")
    if blocked:
        return blocked
    trigger_response = _trigger_dag_impl(
        dag_id=dag_id,
        conf=conf,
        logical_date=logical_date,
    )

    try:
        trigger_data = json.loads(trigger_response)
    except json.JSONDecodeError:
        return json.dumps(
            {
                "error": f"Failed to trigger DAG: {trigger_response}",
                "timed_out": False,
            },
            indent=2,
        )

    dag_run_id = trigger_data.get("dag_run_id")
    if not dag_run_id:
        return json.dumps(
            {
                "error": f"No dag_run_id in trigger response: {trigger_response}",
                "timed_out": False,
            },
            indent=2,
        )

    start_time = time.time()
    current_state = trigger_data.get("state", "queued")

    while True:
        elapsed = time.time() - start_time

        if elapsed >= timeout:
            result: dict[str, Any] = {
                "dag_id": dag_id,
                "dag_run_id": dag_run_id,
                "state": current_state,
                "timed_out": True,
                "elapsed_seconds": round(elapsed, 2),
                "message": f"Timed out after {timeout} seconds. DAG run is still {current_state}.",
            }
            return json.dumps(result, indent=2)

        time.sleep(poll_interval)

        status_response = _get_dag_run_impl(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
        )

        try:
            status_data = json.loads(status_response)
        except json.JSONDecodeError:
            continue

        current_state = status_data.get("state", current_state)

        if current_state in TERMINAL_DAG_RUN_STATES:
            result = {
                "dag_run": status_data,
                "timed_out": False,
                "elapsed_seconds": round(time.time() - start_time, 2),
            }

            if current_state != "success":
                failed_tasks = _get_failed_task_instances(
                    dag_id=dag_id,
                    dag_run_id=dag_run_id,
                )
                if failed_tasks:
                    result["failed_tasks"] = failed_tasks

            return json.dumps(result, indent=2)


@mcp.tool()
def trigger_dag(
    dag_id: str,
    conf: dict | None = None,
    logical_date: str | None = None,
) -> str:
    """Trigger a new DAG run (start a workflow execution manually).

    Use this tool when the user asks to:
    - "Run DAG X" or "Start DAG Y" or "Execute DAG Z"
    - "Trigger a run of DAG X" or "Kick off DAG Y"
    - "Run this workflow" or "Start this pipeline"
    - "Execute DAG X with config Y" or "Trigger DAG with parameters"
    - "Start a manual run" or "Manually execute this DAG"
    - "Backfill DAG X for date Y" or "Run DAG for yesterday"

    This creates a new DAG run that will be picked up by the scheduler and executed.
    You can optionally pass configuration parameters that will be available to the
    DAG during execution via the `conf` context variable.

    IMPORTANT: This is a write operation that modifies Airflow state by creating
    a new DAG run. Use with caution.

    Returns information about the newly triggered DAG run including:
    - dag_run_id: Unique identifier for the new execution
    - dag_id: Which DAG was triggered
    - state: Initial state (typically 'queued')
    - logical_date: The logical/execution date for this run
    - start_date: When execution started (may be null if queued)
    - run_type: Type of run (will be 'manual')
    - conf: Configuration passed to the run
    - external_trigger: Set to true for manual triggers

    Args:
        dag_id: The ID of the DAG to trigger (e.g., "example_dag")
        conf: Optional configuration dictionary to pass to the DAG run.
              This will be available in the DAG via context['dag_run'].conf
        logical_date: Optional logical date for the run (ISO 8601, e.g.
                      '2025-05-01T00:00:00Z'). Used for backfills to run
                      a DAG as if it were a specific date. If omitted,
                      Airflow assigns the current time.

    Returns:
        JSON with details about the newly triggered DAG run
    """
    return _trigger_dag_impl(
        dag_id=dag_id,
        conf=conf,
        logical_date=logical_date,
    )


@mcp.tool()
def trigger_dag_and_wait(
    dag_id: str,
    conf: dict | None = None,
    logical_date: str | None = None,
    timeout: float = 3600.0,
) -> str:
    """Trigger a DAG run and wait for it to complete before returning.

    Use this tool when the user asks to:
    - "Run DAG X and wait for it to finish" or "Execute DAG Y and tell me when it's done"
    - "Trigger DAG Z and wait for completion" or "Run this pipeline synchronously"
    - "Start DAG X and let me know the result" or "Execute and monitor DAG Y"
    - "Run DAG X and show me if it succeeds or fails"
    - "Backfill DAG X for date Y and tell me when it's done"

    This is a BLOCKING operation that will:
    1. Trigger the specified DAG
    2. Poll for status automatically (interval scales with timeout)
    3. Return once the DAG run reaches a terminal state (success, failed, upstream_failed)
    4. Include details about any failed tasks if the run was not successful

    IMPORTANT: This tool blocks until the DAG completes or times out. For long-running
    DAGs, consider using `trigger_dag` instead and checking status separately with
    `get_dag_run`.

    Default timeout is 60 minutes. Adjust the `timeout` parameter for longer DAGs.

    Returns information about the completed DAG run including:
    - dag_id: Which DAG was run
    - dag_run_id: Unique identifier for this execution
    - state: Final state (success, failed, upstream_failed)
    - start_date: When execution started
    - end_date: When execution completed
    - elapsed_seconds: How long we waited
    - timed_out: Whether we hit the timeout before completion
    - failed_tasks: List of failed task details (only if state != success)

    Args:
        dag_id: The ID of the DAG to trigger (e.g., "example_dag")
        conf: Optional configuration dictionary to pass to the DAG run.
              This will be available in the DAG via context['dag_run'].conf
        logical_date: Optional logical date for the run (ISO 8601, e.g.
                      '2025-05-01T00:00:00Z'). Used for backfills to run
                      a DAG as if it were a specific date. If omitted,
                      Airflow assigns the current time.
        timeout: Maximum time to wait in seconds (default: 3600.0 / 60 minutes)

    Returns:
        JSON with final DAG run status and any failed task details
    """
    poll_interval = max(2.0, min(10.0, timeout / 120))

    return _trigger_dag_and_wait_impl(
        dag_id=dag_id,
        conf=conf,
        logical_date=logical_date,
        poll_interval=poll_interval,
        timeout=timeout,
    )


def _pause_dag_impl(dag_id: str) -> str:
    blocked = _check_write_allowed("pause_dag")
    if blocked:
        return blocked
    try:
        adapter = _get_adapter()
        data = adapter.pause_dag(dag_id)
        env = _environment_label()
        data["_environment"] = env
        return json.dumps(data, indent=2)
    except Exception as e:
        return str(e)


@mcp.tool()
def pause_dag(dag_id: str) -> str:
    """Pause a DAG to prevent new scheduled runs from starting.

    Use this tool when the user asks to:
    - "Pause DAG X" or "Stop DAG Y from running"
    - "Disable DAG Z" or "Prevent new runs of DAG X"
    - "Turn off DAG scheduling" or "Suspend DAG execution"

    When a DAG is paused:
    - No new scheduled runs will be created
    - Currently running tasks will complete
    - Manual triggers are still possible
    - The DAG remains visible in the UI with a paused indicator

    IMPORTANT: This is a write operation that modifies Airflow state.
    The DAG will remain paused until explicitly unpaused.

    Args:
        dag_id: The ID of the DAG to pause (e.g., "example_dag")

    Returns:
        JSON with updated DAG details showing is_paused=True
    """
    return _pause_dag_impl(dag_id=dag_id)


def _unpause_dag_impl(dag_id: str) -> str:
    blocked = _check_write_allowed("unpause_dag")
    if blocked:
        return blocked
    try:
        adapter = _get_adapter()
        data = adapter.unpause_dag(dag_id)
        env = _environment_label()
        data["_environment"] = env
        return json.dumps(data, indent=2)
    except Exception as e:
        return str(e)


@mcp.tool()
def unpause_dag(dag_id: str) -> str:
    """Unpause a DAG to allow scheduled runs to resume.

    Use this tool when the user asks to:
    - "Unpause DAG X" or "Resume DAG Y"
    - "Enable DAG Z" or "Start DAG scheduling again"
    - "Turn on DAG X" or "Activate DAG Y"

    When a DAG is unpaused:
    - The scheduler will create new runs based on the schedule
    - Any missed runs (depending on catchup setting) may be created
    - The DAG will appear active in the UI

    IMPORTANT: This is a write operation that modifies Airflow state.
    New DAG runs will be scheduled according to the DAG's schedule_interval.

    Args:
        dag_id: The ID of the DAG to unpause (e.g., "example_dag")

    Returns:
        JSON with updated DAG details showing is_paused=False
    """
    return _unpause_dag_impl(dag_id=dag_id)


def _clear_dag_run_impl(dag_id: str, dag_run_id: str, dry_run: bool = False) -> str:
    blocked = _check_write_allowed("clear_dag_run")
    if blocked:
        return blocked
    try:
        adapter = _get_adapter()
        data = adapter.clear_dag_run(dag_id=dag_id, dag_run_id=dag_run_id, dry_run=dry_run)
        env = _environment_label()
        data["_environment"] = env
        data["_dry_run"] = dry_run
        return json.dumps(data, indent=2)
    except Exception as e:
        return str(e)


@mcp.tool()
def clear_dag_run(dag_id: str, dag_run_id: str, dry_run: bool = False) -> str:
    """Clear all task instances in a DAG run, allowing them to re-run.

    Use this tool when the user asks to:
    - "Clear DAG run X" or "Retry all tasks in this run"
    - "Re-run DAG run Y" or "Reset this DAG run"
    - "Clear failed DAG run" or "Restart the run"

    Clearing a DAG run resets all task instances to a schedulable state,
    so the scheduler will re-execute them. Use dry_run=True to preview
    which tasks would be affected.

    IMPORTANT: This is a write operation that modifies Airflow state.

    Args:
        dag_id: The ID of the DAG (e.g., "example_dag")
        dag_run_id: The ID of the DAG run to clear
        dry_run: If True, show what would be cleared without actually clearing (default: False)

    Returns:
        JSON with list of task instances that were cleared (or would be cleared if dry_run)
    """
    return _clear_dag_run_impl(dag_id=dag_id, dag_run_id=dag_run_id, dry_run=dry_run)


def _clear_task_instances_impl(
    dag_id: str,
    dag_run_id: str,
    task_ids: list[str],
    only_failed: bool = False,
    include_downstream: bool = False,
    dry_run: bool = False,
) -> str:
    blocked = _check_write_allowed("clear_task_instances")
    if blocked:
        return blocked
    try:
        adapter = _get_adapter()
        data = adapter.clear_task_instances(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_ids=task_ids,
            only_failed=only_failed,
            include_downstream=include_downstream,
            dry_run=dry_run,
        )
        env = _environment_label()
        data["_environment"] = env
        data["_dry_run"] = dry_run
        return json.dumps(data, indent=2)
    except Exception as e:
        return str(e)


@mcp.tool()
def clear_task_instances(
    dag_id: str,
    dag_run_id: str,
    task_ids: list[str],
    only_failed: bool = False,
    include_downstream: bool = False,
    dry_run: bool = False,
) -> str:
    """Clear specific task instances in a DAG run, allowing them to re-run.

    Use this tool when the user asks to:
    - "Clear task X in this run" or "Retry just the failed tasks"
    - "Re-run task Y and its downstream" or "Reset specific tasks"
    - "Clear only the failed tasks" or "Retry task Z"

    More targeted than clear_dag_run - use this when only specific tasks
    need to be re-run rather than the entire DAG run.

    IMPORTANT: This is a write operation that modifies Airflow state.

    Args:
        dag_id: The ID of the DAG (e.g., "example_dag")
        dag_run_id: The ID of the DAG run
        task_ids: List of task IDs to clear (e.g., ["extract", "transform"])
        only_failed: Only clear tasks that are in a failed state (default: False)
        include_downstream: Also clear tasks downstream of the specified tasks (default: False)
        dry_run: If True, show what would be cleared without actually clearing (default: False)

    Returns:
        JSON with list of task instances that were cleared (or would be cleared if dry_run)
    """
    return _clear_task_instances_impl(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_ids=task_ids,
        only_failed=only_failed,
        include_downstream=include_downstream,
        dry_run=dry_run,
    )
