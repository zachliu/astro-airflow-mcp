"""Shared utility functions."""

from urllib.parse import urlsplit, urlunsplit


def normalize_airflow_url(url: str) -> str:
    """Strip query string, fragment, and trailing slash from an Airflow base URL.

    API URLs are built by concatenation (eg ``f"{airflow_url}/api/v2/version"``),
    so a stored URL like ``https://host/dep?orgId=foo`` produces the malformed
    ``https://host/dep?orgId=foo/api/v2/version`` — the path stays at ``/dep``
    and ``/api/...`` ends up inside the query string. Normalizing once at the
    boundary keeps every downstream call safe.

    Any userinfo (``user:password@``) is also dropped: credentials are supplied
    separately via the token/basic-auth getters, and the normalized URL can end
    up in logs and in structured tool errors surfaced to the model.
    """
    if not url:
        return url
    parts = urlsplit(url)
    path = parts.path.rstrip("/")
    # Strip any ``user:password@`` userinfo while preserving host[:port] (and
    # IPv6 brackets) verbatim.
    netloc = parts.netloc.rsplit("@", 1)[-1]
    return urlunsplit((parts.scheme, netloc, path, "", ""))
