"""Shared utilities for tool modules."""

import json
import os
from typing import Any

from astro_airflow_mcp import server as _server


def _get_adapter():
    return _server._get_adapter()


def _environment_label() -> str:
    return _server._environment_label()


def _wrap_list_response(items: list[dict[str, Any]], key_name: str, data: dict[str, Any]) -> str:
    return _server._wrap_list_response(items, key_name, data)


def _get_config_url() -> str:
    return _server._config.url


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
