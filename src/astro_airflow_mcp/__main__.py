"""Main entry point for running the Airflow MCP server."""

import argparse
import logging
import os
from pathlib import Path

import yaml

from astro_airflow_mcp.auth import get_access_token
from astro_airflow_mcp.logging import configure_logging, get_logger
from astro_airflow_mcp.server import configure, mcp

logger = get_logger("main")

# Default Airflow URL if no config is found
DEFAULT_AIRFLOW_URL = "http://localhost:8080"


def discover_airflow_url(project_dir: str | None) -> str | None:
    """Discover Airflow URL from .astro/config.yaml in the project directory.

    Looks for the Astro CLI config file and extracts the webserver/api-server port.
    Prefers api-server.port (Airflow 3.x) over webserver.port (Airflow 2.x).

    Args:
        project_dir: The project directory to search in

    Returns:
        The discovered Airflow URL (e.g., "http://localhost:8081"), or None if not found
    """
    if not project_dir:
        return None

    config_path = Path(project_dir) / ".astro" / "config.yaml"
    if not config_path.exists():
        return None

    try:
        with open(config_path) as f:
            config = yaml.safe_load(f)

        if not config:
            return None

        # Try api-server.port first (Airflow 3.x), then webserver.port (Airflow 2.x)
        port = None
        if "api-server" in config and isinstance(config["api-server"], dict):
            port = config["api-server"].get("port")
        if port is None and "webserver" in config and isinstance(config["webserver"], dict):
            port = config["webserver"].get("port")

        if port:
            return f"http://localhost:{port}"

    except Exception as e:
        # Log but don't fail - we'll fall back to default
        logger.debug("Failed to read .astro/config.yaml: %s", e)

    return None


def main():
    """Main entry point for the Airflow MCP server."""
    # Parse command line arguments first to determine transport mode
    parser = argparse.ArgumentParser(description="Airflow MCP Server")
    parser.add_argument(
        "--transport",
        type=str,
        default=os.getenv("MCP_TRANSPORT", "stdio"),
        choices=["stdio", "http"],
        help="Transport mode: stdio (default) or http",
    )
    parser.add_argument(
        "--host",
        type=str,
        default=os.getenv("MCP_HOST", "localhost"),
        help="Host to bind to (only for http transport, default: localhost)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("MCP_PORT", "8000")),
        help="Port to bind to (only for http transport, default: 8000)",
    )
    parser.add_argument(
        "--airflow-url",
        type=str,
        default=os.getenv("AIRFLOW_API_URL"),  # None if not set
        help="Base URL of Airflow webserver (auto-discovered from .astro/config.yaml if not provided)",
    )
    parser.add_argument(
        "--auth-token",
        type=str,
        default=os.getenv("AIRFLOW_AUTH_TOKEN"),
        help="Bearer token for Airflow API authentication (takes precedence over username/password)",
    )
    parser.add_argument(
        "--username",
        type=str,
        default=os.getenv("AIRFLOW_USERNAME"),
        help="Username for Airflow API token authentication",
    )
    parser.add_argument(
        "--password",
        type=str,
        default=os.getenv("AIRFLOW_PASSWORD"),
        help="Password for Airflow API token authentication",
    )
    parser.add_argument(
        "--airflow-project-dir",
        type=str,
        default=os.getenv("AIRFLOW_PROJECT_DIR") or os.getenv("PWD") or os.getcwd(),
        help="Astro project directory for auto-discovering Airflow URL from .astro/config.yaml (default: $PWD)",
    )
    parser.add_argument(
        "--auth0-domain",
        type=str,
        default=os.getenv("AUTH0_DOMAIN"),
        help="Auth0 tenant domain (e.g., '<your-domain>.us.auth0.com')",
    )
    parser.add_argument(
        "--auth0-client-id",
        type=str,
        default=os.getenv("AUTH0_CLIENT_ID"),
        help="Auth0 application client ID",
    )
    parser.add_argument(
        "--auth0-audience",
        type=str,
        default=os.getenv("AUTH0_AUDIENCE"),
        help="Auth0 API audience identifier",
    )
    parser.add_argument(
        "--auth0-callback-url",
        type=str,
        default=os.getenv("AUTH0_CALLBACK_URL"),
        help="OAuth callback URL (e.g., 'https://<your-domain>/oauth/mcp-callback')",
    )

    args = parser.parse_args()

    # Configure logging - use stderr in stdio mode to avoid corrupting JSON-RPC
    stdio_mode = args.transport == "stdio"
    configure_logging(level=logging.INFO, stdio_mode=stdio_mode)

    # Determine Airflow URL: explicit > auto-discover > default
    airflow_url = args.airflow_url
    url_source = "explicit"
    if not airflow_url:
        # Try auto-discovery from .astro/config.yaml
        airflow_url = discover_airflow_url(args.airflow_project_dir)
        if airflow_url:
            url_source = "auto-discovered"
        else:
            airflow_url = DEFAULT_AIRFLOW_URL
            url_source = "default"

    # Resolve auth token: explicit > stored Airflow JWT > username/password > credential-less
    auth_token = args.auth_token
    if not auth_token and args.auth0_domain and args.auth0_client_id:
        auth_token = get_access_token(
            auth0_domain=args.auth0_domain,
            client_id=args.auth0_client_id,
        )
        if not auth_token:
            logger.error(
                "No valid token found for Auth0 domain '%s'. "
                "Run `astro-airflow-mcp-login --auth0-domain %s` first to authenticate.",
                args.auth0_domain,
                args.auth0_domain,
            )
            raise SystemExit(1)

    # Configure Airflow connection settings
    configure(
        url=airflow_url,
        auth_token=auth_token,
        username=args.username,
        password=args.password,
        project_dir=args.airflow_project_dir,
    )

    # Log configuration
    logger.info("Project directory: %s", args.airflow_project_dir)
    logger.info("Airflow URL: %s (%s)", airflow_url, url_source)
    if auth_token and args.auth0_domain:
        logger.info("Authentication: Airflow JWT via Auth0 (domain: %s)", args.auth0_domain)
    elif auth_token:
        logger.info("Authentication: Direct bearer token")
    elif args.username:
        logger.info("Authentication: Token manager (username: %s)", args.username)
    else:
        logger.info("Authentication: Token manager (credential-less mode)")

    # Run the server with specified transport
    if args.transport == "http":
        mcp.run(transport="http", host=args.host, port=args.port, show_banner=False)
    else:
        mcp.run(show_banner=False)


if __name__ == "__main__":
    main()
