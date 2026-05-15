"""Auth0 authorization code authentication for the Airflow MCP server.

Flow:
1. `login()` opens browser to Auth0 authorize URL with redirect to the
   Airflow plugin callback (`/oauth/mcp-callback`)
2. The plugin exchanges the Auth0 code server-side (using client_secret),
   looks up the FAB user, and mints an Airflow API JWT - displayed on a
   page for the user to copy
3. User pastes the Airflow JWT back into the CLI
4. Token is stored locally for the MCP server to use

Two entry points:
- `login()`: interactive CLI command (run by user in terminal)
- `get_access_token()`: non-interactive, used by MCP server on startup

Tokens are stored per-environment, keyed by Auth0 domain:
  ~/.config/astro-airflow-mcp/tokens/<auth0-domain>.json
"""

import base64
import json
import re
import secrets
import time
import webbrowser
from pathlib import Path
from urllib.parse import urlencode

from astro_airflow_mcp.logging import get_logger

logger = get_logger(__name__)

TOKEN_DIR = Path.home() / ".config" / "astro-airflow-mcp" / "tokens"
# Legacy single-file path (for migration)
LEGACY_TOKEN_FILE = Path.home() / ".config" / "astro-airflow-mcp" / "token.json"
DEFAULT_EXPIRY_SECONDS = 86400
EXPIRY_BUFFER_SECONDS = 1800


def _token_file_for_domain(auth0_domain: str | None) -> Path:
    """Get the token file path for a given Auth0 domain.

    Examples:
        mycompany-dev.us.auth0.com -> tokens/mycompany-dev.us.auth0.com.json
        mycompany.auth0.com        -> tokens/mycompany.auth0.com.json
    """
    if not auth0_domain:
        return LEGACY_TOKEN_FILE
    key = re.sub(r"[^a-zA-Z0-9._-]", "_", auth0_domain)
    return TOKEN_DIR / f"{key}.json"


def _parse_jwt_exp(token: str) -> int | None:
    """Extract exp claim from a JWT without verifying signature."""
    try:
        payload_b64 = token.split(".")[1]
        # Add padding for base64url
        padding = 4 - len(payload_b64) % 4
        if padding != 4:
            payload_b64 += "=" * padding
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
        return payload.get("exp")
    except (IndexError, ValueError, json.JSONDecodeError):
        return None


def _load_token(auth0_domain: str | None = None) -> dict | None:
    token_file = _token_file_for_domain(auth0_domain)
    if not token_file.exists():
        # Fall back to legacy single-file if no per-env token exists
        if auth0_domain and LEGACY_TOKEN_FILE.exists():
            logger.info(
                "No per-environment token found for %s; checking legacy token file.",
                auth0_domain,
            )
            try:
                return json.loads(LEGACY_TOKEN_FILE.read_text())
            except (json.JSONDecodeError, OSError):
                return None
        return None
    try:
        return json.loads(token_file.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def _save_token(token: str, auth0_domain: str | None = None) -> None:
    token_file = _token_file_for_domain(auth0_domain)
    token_file.parent.mkdir(parents=True, exist_ok=True)
    now = time.time()
    exp = _parse_jwt_exp(token)
    expires_in = int(exp - now) if exp else DEFAULT_EXPIRY_SECONDS
    token_data = {
        "access_token": token,
        "fetched_at": now,
        "expires_in": expires_in,
        "auth0_domain": auth0_domain,
    }
    token_file.write_text(json.dumps(token_data, indent=2))
    token_file.chmod(0o600)


def _token_is_expired(token_data: dict) -> bool:
    fetched_at = token_data.get("fetched_at", 0)
    expires_in = token_data.get("expires_in", DEFAULT_EXPIRY_SECONDS)
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

    Token is stored keyed by auth0_domain, so each Auth0 tenant
    (integration vs production) gets its own token file automatically.
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
        f"\nAuthenticating via: {auth0_domain}\n"
        f"Opening browser for Auth0 login...\n"
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

    token_file = _token_file_for_domain(auth0_domain)
    _save_token(token, auth0_domain=auth0_domain)
    print(f"\nLogin successful ({auth0_domain}).")
    print(f"Token stored at {token_file}")
    return True


def get_access_token(
    auth0_domain: str,
    client_id: str,
) -> str | None:
    """Non-interactive. Returns a valid Airflow JWT or None.

    Used by the MCP server on startup. If the token is expired,
    returns None (user must re-run `astro-airflow-mcp-login`).

    Looks up the token file keyed by auth0_domain.
    """
    token_data = _load_token(auth0_domain=auth0_domain)

    if token_data and not _token_is_expired(token_data):
        return token_data.get("access_token")

    if token_data:
        logger.warning(
            "Stored token for %s has expired. Run `astro-airflow-mcp-login` to re-authenticate.",
            auth0_domain,
        )

    return None


def list_stored_environments() -> list[dict]:
    """List all environments that have stored tokens.

    Returns:
        List of dicts with 'auth0_domain', 'token_file', and 'expired' keys.
    """
    results = []
    if not TOKEN_DIR.exists():
        return results
    for token_file in sorted(TOKEN_DIR.glob("*.json")):
        try:
            data = json.loads(token_file.read_text())
            expired = _token_is_expired(data)
            results.append({
                "auth0_domain": data.get("auth0_domain", token_file.stem),
                "token_file": str(token_file),
                "expired": expired,
            })
        except (json.JSONDecodeError, OSError):
            continue
    return results
