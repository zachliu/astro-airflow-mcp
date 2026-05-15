"""CLI login command: `astro-airflow-mcp-login`."""

import argparse
import os
import sys

from astro_airflow_mcp.auth import get_access_token, login, TOKEN_FILE


def main():
    parser = argparse.ArgumentParser(
        description="Log in to Airflow via Auth0 (authorization code flow through Airflow plugin)",
    )
    parser.add_argument(
        "--auth0-domain",
        default=os.getenv("AUTH0_DOMAIN"),
        required="AUTH0_DOMAIN" not in os.environ,
        help="Auth0 tenant domain (e.g., '<your-domain>.us.auth0.com')",
    )
    parser.add_argument(
        "--auth0-client-id",
        default=os.getenv("AUTH0_CLIENT_ID"),
        required="AUTH0_CLIENT_ID" not in os.environ,
        help="Auth0 application client ID",
    )
    parser.add_argument(
        "--auth0-audience",
        default=os.getenv("AUTH0_AUDIENCE", ""),
        help="Auth0 API audience identifier (optional)",
    )
    parser.add_argument(
        "--auth0-callback-url",
        default=os.getenv("AUTH0_CALLBACK_URL"),
        required="AUTH0_CALLBACK_URL" not in os.environ,
        help="OAuth callback URL on your Airflow server (e.g., 'https://<your-domain>/oauth/mcp-callback')",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Check current token status without logging in",
    )

    args = parser.parse_args()

    if args.status:
        token = get_access_token(
            auth0_domain=args.auth0_domain or "",
            client_id=args.auth0_client_id or "",
        )
        if token:
            print(f"Valid token found at {TOKEN_FILE}")
        else:
            print("No valid token. Run `astro-airflow-mcp-login` to authenticate.")
            sys.exit(1)
        return

    ok = login(
        auth0_domain=args.auth0_domain,
        client_id=args.auth0_client_id,
        audience=args.auth0_audience,
        callback_url=args.auth0_callback_url,
    )
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
