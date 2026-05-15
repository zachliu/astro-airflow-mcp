"""Auth0 PKCE authentication for the Airflow MCP server.

Flow:
1. `login()` opens browser to Auth0 authorize URL with redirect to the
   Airflow plugin callback (`/oauth/mcp-callback`)
2. The plugin exchanges the Auth0 code server-side, looks up the FAB user,
   and mints an Airflow API JWT - displayed on a page for the user to copy
3. User pastes the Airflow JWT back into the CLI
4. Token is stored locally for the MCP server to use

Two entry points:
- `login()`: interactive CLI command (run by user in terminal)
- `get_access_token()`: non-interactive, used by MCP server on startup
"""

import json
import secrets
import sys
import time
import webbrowser
from pathlib import Path
from urllib.parse import urlencode

from astro_airflow_mcp.logging import get_logger

logger = get_logger(__name__)

TOKEN_FILE = Path.home() / ".config" / "astro-airflow-mcp" / "token.json"
# Airflow JWT expiration is 86400s (24h) by default; warn 30min before expiry
EXPIRY_BUFFER_SECONDS = 1800


def _load_token() -> dict | None:
    if not TOKEN_FILE.exists():
        return None
    try:
        return json.loads(TOKEN_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def _save_token(token: str) -> None:
    TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    token_data = {
        "access_token": token,
        "fetched_at": time.time(),
        "expires_in": 86400,
    }
    TOKEN_FILE.write_text(json.dumps(token_data, indent=2))
    TOKEN_FILE.chmod(0o600)


def _token_is_expired(token_data: dict) -> bool:
    fetched_at = token_data.get("fetched_at", 0)
    expires_in = token_data.get("expires_in", 86400)
    return (time.time() - fetched_at) >= (expires_in - EXPIRY_BUFFER_SECONDS)


def login(
    auth0_domain: str,
    client_id: str,
    audience: str,
    callback_url: str,
    scopes: str = "openid profile email",
) -> bool:
    """Interactive login. Opens browser to Auth0, user pastes back the Airflow JWT.

    The callback_url should point to the Airflow plugin endpoint
    (e.g., https://<your-domain>/oauth/mcp-callback).
    """
    authorize_params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": callback_url,
        "scope": scopes,
        "state": secrets.token_urlsafe(32),
    }
    if audience:
        authorize_params["audience"] = audience

    authorize_url = f"https://{auth0_domain}/authorize?{urlencode(authorize_params)}"

    print(
        f"\nOpening browser for Auth0 login...\n"
        f"\nIf the browser doesn't open, visit:\n{authorize_url}\n"
    )
    webbrowser.open(authorize_url)

    print(
        "\nAfter logging in, you'll see a page with an Airflow API token.\n"
        "Copy that token and paste it below.\n"
    )
    token = input("Paste the Airflow token here: ").strip()
    if not token:
        print("No token provided. Login aborted.")
        return False

    _save_token(token)
    print(f"\nLogin successful. Token stored at {TOKEN_FILE}")
    return True


def get_access_token(
    auth0_domain: str,
    client_id: str,
) -> str | None:
    """Non-interactive. Returns a valid Airflow JWT or None.

    Used by the MCP server on startup. If the token is expired,
    returns None (user must re-run `astro-airflow-mcp-login`).
    """
    token_data = _load_token()

    if token_data and not _token_is_expired(token_data):
        return token_data.get("access_token")

    if token_data:
        logger.warning("Stored token has expired. Run `astro-airflow-mcp-login` to re-authenticate.")

    return None
