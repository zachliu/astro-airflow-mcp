"""Tests for write tools: clear operations, read-only mode, auto-unpause."""

import json

from astro_airflow_mcp.server import (
    _clear_dag_run_impl,
    _clear_task_instances_impl,
    _pause_dag_impl,
    _trigger_dag_impl,
    _unpause_dag_impl,
)


class TestClearDagRun:
    def test_dry_run(self, mocker):
        mock_adapter = mocker.Mock()
        mock_adapter.clear_dag_run.return_value = {
            "task_instances": [
                {"task_id": "extract", "state": "failed"},
                {"task_id": "transform", "state": "success"},
            ]
        }
        mocker.patch("astro_airflow_mcp.server._get_adapter", return_value=mock_adapter)

        result = json.loads(_clear_dag_run_impl("my_dag", "run_1", dry_run=True))

        assert result["_dry_run"] is True
        assert "_environment" in result
        mock_adapter.clear_dag_run.assert_called_once_with(
            dag_id="my_dag", dag_run_id="run_1", dry_run=True
        )

    def test_actual_clear(self, mocker):
        mock_adapter = mocker.Mock()
        mock_adapter.clear_dag_run.return_value = {
            "task_instances": [{"task_id": "extract", "state": "cleared"}]
        }
        mocker.patch("astro_airflow_mcp.server._get_adapter", return_value=mock_adapter)

        result = json.loads(_clear_dag_run_impl("my_dag", "run_1", dry_run=False))

        assert result["_dry_run"] is False
        mock_adapter.clear_dag_run.assert_called_once_with(
            dag_id="my_dag", dag_run_id="run_1", dry_run=False
        )

    def test_error_handling(self, mocker):
        mock_adapter = mocker.Mock()
        mock_adapter.clear_dag_run.side_effect = Exception("DAG run not found")
        mocker.patch("astro_airflow_mcp.server._get_adapter", return_value=mock_adapter)

        result = _clear_dag_run_impl("my_dag", "nonexistent_run")
        assert "DAG run not found" in result


class TestClearTaskInstances:
    def test_clear_specific_tasks(self, mocker):
        mock_adapter = mocker.Mock()
        mock_adapter.clear_task_instances.return_value = {
            "task_instances": [{"task_id": "extract", "state": "cleared"}]
        }
        mocker.patch("astro_airflow_mcp.server._get_adapter", return_value=mock_adapter)

        result = json.loads(
            _clear_task_instances_impl("my_dag", "run_1", ["extract", "transform"])
        )

        assert result["_dry_run"] is False
        assert "_environment" in result
        mock_adapter.clear_task_instances.assert_called_once_with(
            dag_id="my_dag",
            dag_run_id="run_1",
            task_ids=["extract", "transform"],
            only_failed=False,
            include_downstream=False,
            dry_run=False,
        )

    def test_only_failed(self, mocker):
        mock_adapter = mocker.Mock()
        mock_adapter.clear_task_instances.return_value = {"task_instances": []}
        mocker.patch("astro_airflow_mcp.server._get_adapter", return_value=mock_adapter)

        _clear_task_instances_impl(
            "my_dag", "run_1", ["extract"], only_failed=True
        )

        mock_adapter.clear_task_instances.assert_called_once_with(
            dag_id="my_dag",
            dag_run_id="run_1",
            task_ids=["extract"],
            only_failed=True,
            include_downstream=False,
            dry_run=False,
        )

    def test_include_downstream(self, mocker):
        mock_adapter = mocker.Mock()
        mock_adapter.clear_task_instances.return_value = {"task_instances": []}
        mocker.patch("astro_airflow_mcp.server._get_adapter", return_value=mock_adapter)

        _clear_task_instances_impl(
            "my_dag", "run_1", ["extract"], include_downstream=True
        )

        mock_adapter.clear_task_instances.assert_called_once_with(
            dag_id="my_dag",
            dag_run_id="run_1",
            task_ids=["extract"],
            only_failed=False,
            include_downstream=True,
            dry_run=False,
        )

    def test_dry_run(self, mocker):
        mock_adapter = mocker.Mock()
        mock_adapter.clear_task_instances.return_value = {
            "task_instances": [{"task_id": "extract", "state": "failed"}]
        }
        mocker.patch("astro_airflow_mcp.server._get_adapter", return_value=mock_adapter)

        result = json.loads(
            _clear_task_instances_impl("my_dag", "run_1", ["extract"], dry_run=True)
        )

        assert result["_dry_run"] is True


class TestReadOnlyMode:
    def test_trigger_blocked(self, mocker, monkeypatch):
        monkeypatch.setenv("AF_READ_ONLY", "true")
        result = json.loads(_trigger_dag_impl("my_dag"))
        assert result["error"] == "Write operation blocked"
        assert result["operation"] == "trigger_dag"

    def test_pause_blocked(self, mocker, monkeypatch):
        monkeypatch.setenv("AF_READ_ONLY", "1")
        result = json.loads(_pause_dag_impl("my_dag"))
        assert result["error"] == "Write operation blocked"
        assert result["operation"] == "pause_dag"

    def test_unpause_blocked(self, mocker, monkeypatch):
        monkeypatch.setenv("AF_READ_ONLY", "yes")
        result = json.loads(_unpause_dag_impl("my_dag"))
        assert result["error"] == "Write operation blocked"
        assert result["operation"] == "unpause_dag"

    def test_clear_dag_run_blocked(self, mocker, monkeypatch):
        monkeypatch.setenv("AF_READ_ONLY", "TRUE")
        result = json.loads(_clear_dag_run_impl("my_dag", "run_1"))
        assert result["error"] == "Write operation blocked"
        assert result["operation"] == "clear_dag_run"

    def test_clear_task_instances_blocked(self, mocker, monkeypatch):
        monkeypatch.setenv("AF_READ_ONLY", "true")
        result = json.loads(
            _clear_task_instances_impl("my_dag", "run_1", ["extract"])
        )
        assert result["error"] == "Write operation blocked"
        assert result["operation"] == "clear_task_instances"

    def test_not_blocked_when_disabled(self, mocker, monkeypatch):
        monkeypatch.setenv("AF_READ_ONLY", "false")
        mock_adapter = mocker.Mock()
        mock_adapter.get_dag.return_value = {"is_paused": False}
        mock_adapter.trigger_dag_run.return_value = {"dag_run_id": "new_run"}
        mocker.patch("astro_airflow_mcp.server._get_adapter", return_value=mock_adapter)

        result = json.loads(_trigger_dag_impl("my_dag"))
        assert "error" not in result
        assert result["dag_run_id"] == "new_run"

    def test_not_blocked_when_unset(self, mocker, monkeypatch):
        monkeypatch.delenv("AF_READ_ONLY", raising=False)
        mock_adapter = mocker.Mock()
        mock_adapter.pause_dag.return_value = {"dag_id": "my_dag", "is_paused": True}
        mocker.patch("astro_airflow_mcp.server._get_adapter", return_value=mock_adapter)

        result = json.loads(_pause_dag_impl("my_dag"))
        assert "error" not in result


class TestAutoUnpause:
    def test_auto_unpause_when_paused(self, mocker):
        mock_adapter = mocker.Mock()
        mock_adapter.get_dag.return_value = {"dag_id": "my_dag", "is_paused": True}
        mock_adapter.unpause_dag.return_value = {"dag_id": "my_dag", "is_paused": False}
        mock_adapter.trigger_dag_run.return_value = {"dag_run_id": "new_run"}
        mocker.patch("astro_airflow_mcp.server._get_adapter", return_value=mock_adapter)

        result = json.loads(_trigger_dag_impl("my_dag"))

        assert result["_auto_unpaused"] is True
        assert "_warning" in result
        mock_adapter.unpause_dag.assert_called_once_with("my_dag")
        mock_adapter.trigger_dag_run.assert_called_once()

    def test_no_unpause_when_already_active(self, mocker):
        mock_adapter = mocker.Mock()
        mock_adapter.get_dag.return_value = {"dag_id": "my_dag", "is_paused": False}
        mock_adapter.trigger_dag_run.return_value = {"dag_run_id": "new_run"}
        mocker.patch("astro_airflow_mcp.server._get_adapter", return_value=mock_adapter)

        result = json.loads(_trigger_dag_impl("my_dag"))

        assert "_auto_unpaused" not in result
        assert "_warning" not in result
        mock_adapter.unpause_dag.assert_not_called()

    def test_trigger_proceeds_if_dag_check_fails(self, mocker):
        mock_adapter = mocker.Mock()
        mock_adapter.get_dag.side_effect = Exception("Network error")
        mock_adapter.trigger_dag_run.return_value = {"dag_run_id": "new_run"}
        mocker.patch("astro_airflow_mcp.server._get_adapter", return_value=mock_adapter)

        result = json.loads(_trigger_dag_impl("my_dag"))

        assert result["dag_run_id"] == "new_run"
        assert "_auto_unpaused" not in result
