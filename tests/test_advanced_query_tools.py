"""Tests for advanced query tools: list_dags filtering, search_dag_source, list_task_instances_batch."""

import json
from unittest.mock import MagicMock

import pytest


@pytest.fixture()
def mock_adapter(mocker):
    adapter = MagicMock()
    mocker.patch("astro_airflow_mcp.server._get_adapter", return_value=adapter)
    return adapter


class TestListDagsFiltering:
    """Tests for list_dags with filtering parameters."""

    def test_list_dags_no_filters(self, mock_adapter):
        from astro_airflow_mcp.tools.dag import _list_dags_impl

        mock_adapter.list_dags.return_value = {
            "dags": [
                {"dag_id": "dag_a", "is_paused": False, "tags": [{"name": "clean"}]},
                {"dag_id": "dag_b", "is_paused": True, "tags": [{"name": "backfill"}]},
            ],
            "total_entries": 2,
        }

        result = json.loads(_list_dags_impl())
        assert result["returned_count"] == 2

    def test_list_dags_with_tags_filter(self, mock_adapter):
        from astro_airflow_mcp.tools.dag import _list_dags_impl

        mock_adapter.list_dags.return_value = {
            "dags": [{"dag_id": "dag_a", "tags": [{"name": "clean"}]}],
            "total_entries": 1,
        }

        _list_dags_impl(tags=["clean"])
        call_kwargs = mock_adapter.list_dags.call_args
        assert call_kwargs.kwargs["tags"] == ["clean"]

    def test_list_dags_with_paused_filter(self, mock_adapter):
        from astro_airflow_mcp.tools.dag import _list_dags_impl

        mock_adapter.list_dags.return_value = {
            "dags": [{"dag_id": "dag_a", "is_paused": False}],
            "total_entries": 1,
        }

        _list_dags_impl(paused=False)
        call_kwargs = mock_adapter.list_dags.call_args
        assert call_kwargs.kwargs["paused"] is False

    def test_list_dags_with_dag_id_pattern(self, mock_adapter):
        from astro_airflow_mcp.tools.dag import _list_dags_impl

        mock_adapter.list_dags.return_value = {
            "dags": [
                {"dag_id": "client_a_clean_fb", "is_paused": False},
                {"dag_id": "client_b_clean_fb", "is_paused": False},
                {"dag_id": "other_dag", "is_paused": False},
            ],
            "total_entries": 3,
        }

        result = json.loads(_list_dags_impl(dag_id_pattern="^client_a"))
        assert result["returned_count"] == 1
        assert result["dags"][0]["dag_id"] == "client_a_clean_fb"

    def test_list_dags_combined_filters(self, mock_adapter):
        from astro_airflow_mcp.tools.dag import _list_dags_impl

        mock_adapter.list_dags.return_value = {
            "dags": [
                {"dag_id": "client_a_daily_clean_fb", "is_paused": False},
                {"dag_id": "client_a_daily_clean_google", "is_paused": False},
                {"dag_id": "other_daily_clean", "is_paused": False},
            ],
            "total_entries": 3,
        }

        result = json.loads(_list_dags_impl(tags=["clean"], paused=False, dag_id_pattern="client_a"))
        assert result["returned_count"] == 2

    def test_list_dags_pagination(self, mock_adapter):
        from astro_airflow_mcp.tools.dag import _list_dags_impl

        page1 = [{"dag_id": f"dag_{i}"} for i in range(100)]
        page2 = [{"dag_id": f"dag_{i}"} for i in range(100, 150)]

        mock_adapter.list_dags.side_effect = [
            {"dags": page1, "total_entries": 150},
            {"dags": page2, "total_entries": 150},
        ]

        result = json.loads(_list_dags_impl())
        assert result["returned_count"] == 150


class TestSearchDagSource:
    """Tests for search_dag_source tool."""

    def test_search_finds_matches(self, mock_adapter):
        from astro_airflow_mcp.tools.dag import _search_dag_source_impl

        mock_adapter.list_dags.return_value = {
            "dags": [
                {"dag_id": "dag_a", "is_paused": False, "timetable_summary": "0 5 * * *"},
                {"dag_id": "dag_b", "is_paused": True, "timetable_summary": "0 6 * * *"},
            ],
            "total_entries": 2,
        }
        mock_adapter.get_dag_source.side_effect = [
            {"content": 'extra_args=["-c", "table1", "table2"]'},
            {"content": 'extra_args=["-p", "facebook"]'},
        ]

        result = json.loads(_search_dag_source_impl('"-c"'))
        assert result["dags_searched"] == 2
        assert result["dags_matched"] == 1
        assert result["matches"][0]["dag_id"] == "dag_a"

    def test_search_no_matches(self, mock_adapter):
        from astro_airflow_mcp.tools.dag import _search_dag_source_impl

        mock_adapter.list_dags.return_value = {
            "dags": [{"dag_id": "dag_a", "is_paused": False}],
            "total_entries": 1,
        }
        mock_adapter.get_dag_source.return_value = {"content": "nothing here"}

        result = json.loads(_search_dag_source_impl("nonexistent_pattern"))
        assert result["dags_matched"] == 0

    def test_search_with_tag_filter(self, mock_adapter):
        from astro_airflow_mcp.tools.dag import _search_dag_source_impl

        mock_adapter.list_dags.return_value = {
            "dags": [{"dag_id": "clean_dag", "is_paused": False}],
            "total_entries": 1,
        }
        mock_adapter.get_dag_source.return_value = {
            "content": "data-processor\nsome code"
        }

        result = json.loads(_search_dag_source_impl("data-processor", tags=["clean"]))
        assert result["dags_matched"] == 1
        mock_adapter.list_dags.assert_called_with(limit=100, offset=0, tags=["clean"])

    def test_search_with_dag_id_pattern(self, mock_adapter):
        from astro_airflow_mcp.tools.dag import _search_dag_source_impl

        mock_adapter.list_dags.return_value = {
            "dags": [
                {"dag_id": "clientA_clean_fb", "is_paused": False},
                {"dag_id": "clientB_clean_fb", "is_paused": False},
            ],
            "total_entries": 2,
        }
        mock_adapter.get_dag_source.return_value = {"content": "data-processor task"}

        result = json.loads(
            _search_dag_source_impl("data-processor", dag_id_pattern="^clientA")
        )
        assert result["dags_searched"] == 1
        assert result["dags_matched"] == 1

    def test_search_handles_source_errors(self, mock_adapter):
        from astro_airflow_mcp.tools.dag import _search_dag_source_impl

        mock_adapter.list_dags.return_value = {
            "dags": [
                {"dag_id": "dag_ok", "is_paused": False},
                {"dag_id": "dag_err", "is_paused": False},
            ],
            "total_entries": 2,
        }
        mock_adapter.get_dag_source.side_effect = [
            {"content": "data-processor here"},
            Exception("404 Not Found"),
        ]

        result = json.loads(_search_dag_source_impl("data-processor"))
        assert result["dags_matched"] == 1
        assert result["errors"] == 1

    def test_search_respects_limit(self, mock_adapter):
        from astro_airflow_mcp.tools.dag import _search_dag_source_impl

        mock_adapter.list_dags.return_value = {
            "dags": [{"dag_id": f"dag_{i}", "is_paused": False} for i in range(10)],
            "total_entries": 10,
        }
        mock_adapter.get_dag_source.return_value = {"content": "matching_pattern here"}

        result = json.loads(_search_dag_source_impl("matching_pattern", limit=3))
        assert result["dags_matched"] == 3
        assert result["truncated"] is True

    def test_search_context_lines(self, mock_adapter):
        from astro_airflow_mcp.tools.dag import _search_dag_source_impl

        source = "line1\nline2\ntarget_line\nline4\nline5"
        mock_adapter.list_dags.return_value = {
            "dags": [{"dag_id": "dag_a", "is_paused": False}],
            "total_entries": 1,
        }
        mock_adapter.get_dag_source.return_value = {"content": source}

        result = json.loads(_search_dag_source_impl("target_line", context_lines=1))
        snippet = result["matches"][0]["snippets"][0]
        assert "line2" in snippet
        assert "target_line" in snippet
        assert "line4" in snippet


class TestListTaskInstancesBatch:
    """Tests for list_task_instances_batch tool."""

    def test_batch_with_pool_filter(self, mock_adapter):
        from astro_airflow_mcp.tools.task import _list_task_instances_batch_impl

        mock_adapter.list_task_instances_batch.return_value = {
            "task_instances": [
                {
                    "dag_id": "dag_a",
                    "task_id": "classify",
                    "dag_run_id": "scheduled__2026-05-21",
                    "state": "success",
                    "logical_date": "2026-05-21T05:00:00Z",
                    "start_date": "2026-05-21T05:01:00Z",
                    "end_date": "2026-05-21T05:03:00Z",
                    "duration": 120.0,
                    "pool": "etl_pool",
                    "operator_name": "EcsOperator",
                    "try_number": 1,
                },
            ],
            "total_entries": 1,
        }

        result = json.loads(_list_task_instances_batch_impl(pool=["etl_pool"]))
        assert result["total_entries"] == 1
        assert result["task_instances"][0]["dag_id"] == "dag_a"
        assert result["task_instances"][0]["state"] == "success"

        mock_adapter.list_task_instances_batch.assert_called_once_with(
            dag_ids=None,
            pool=["etl_pool"],
            state=None,
            logical_date_gte=None,
            logical_date_lte=None,
            limit=100,
            offset=0,
        )

    def test_batch_with_state_filter(self, mock_adapter):
        from astro_airflow_mcp.tools.task import _list_task_instances_batch_impl

        mock_adapter.list_task_instances_batch.return_value = {
            "task_instances": [
                {
                    "dag_id": "dag_a",
                    "task_id": "task1",
                    "dag_run_id": "run1",
                    "state": "failed",
                    "logical_date": "2026-05-21T05:00:00Z",
                    "start_date": None,
                    "end_date": None,
                    "duration": None,
                    "pool": "default",
                    "operator_name": "PythonOperator",
                    "try_number": 1,
                },
            ],
            "total_entries": 1,
        }

        result = json.loads(_list_task_instances_batch_impl(state=["failed"]))
        assert result["task_instances"][0]["state"] == "failed"

    def test_batch_with_date_range(self, mock_adapter):
        from astro_airflow_mcp.tools.task import _list_task_instances_batch_impl

        mock_adapter.list_task_instances_batch.return_value = {
            "task_instances": [],
            "total_entries": 0,
        }

        _list_task_instances_batch_impl(
            logical_date_gte="2026-05-21T00:00:00Z",
            logical_date_lte="2026-05-21T23:59:59Z",
        )

        mock_adapter.list_task_instances_batch.assert_called_once_with(
            dag_ids=None,
            pool=None,
            state=None,
            logical_date_gte="2026-05-21T00:00:00Z",
            logical_date_lte="2026-05-21T23:59:59Z",
            limit=100,
            offset=0,
        )

    def test_batch_with_pagination(self, mock_adapter):
        from astro_airflow_mcp.tools.task import _list_task_instances_batch_impl

        mock_adapter.list_task_instances_batch.return_value = {
            "task_instances": [{"dag_id": "d", "task_id": "t", "dag_run_id": "r",
                               "state": "success", "logical_date": "2026-05-21T00:00:00Z",
                               "start_date": None, "end_date": None, "duration": None,
                               "pool": "default", "operator_name": "Op", "try_number": 1}],
            "total_entries": 500,
        }

        result = json.loads(_list_task_instances_batch_impl(limit=100, offset=200))
        assert result["offset"] == 200
        assert result["total_entries"] == 500

    def test_batch_trims_fields(self, mock_adapter):
        from astro_airflow_mcp.tools.task import _list_task_instances_batch_impl

        mock_adapter.list_task_instances_batch.return_value = {
            "task_instances": [
                {
                    "id": "some-uuid",
                    "dag_id": "dag_a",
                    "task_id": "task1",
                    "dag_run_id": "run1",
                    "state": "success",
                    "logical_date": "2026-05-21T00:00:00Z",
                    "start_date": "2026-05-21T05:01:00Z",
                    "end_date": "2026-05-21T05:03:00Z",
                    "duration": 120.0,
                    "pool": "default",
                    "operator_name": "EcsOperator",
                    "try_number": 1,
                    "hostname": "worker-1",
                    "unixname": "airflow",
                    "pid": 12345,
                    "executor_config": "{}",
                    "queued_when": "2026-05-21T05:00:50Z",
                },
            ],
            "total_entries": 1,
        }

        result = json.loads(_list_task_instances_batch_impl())
        ti = result["task_instances"][0]
        assert "id" not in ti
        assert "hostname" not in ti
        assert "pid" not in ti
        assert "executor_config" not in ti
        assert ti["dag_id"] == "dag_a"
        assert ti["operator"] == "EcsOperator"

    def test_batch_error_handling(self, mock_adapter):
        from astro_airflow_mcp.tools.task import _list_task_instances_batch_impl

        mock_adapter.list_task_instances_batch.side_effect = Exception("401 Unauthorized")

        result = _list_task_instances_batch_impl()
        assert "401 Unauthorized" in result
