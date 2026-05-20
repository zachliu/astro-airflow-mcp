"""CLI login command: `astro-airflow-mcp-login`."""

import argparse
import os
import sys

from astro_airflow_mcp.auth import (
    _token_file_for_domain,
    get_access_token,
    list_stored_environments,
    login,
)


def main():
    parser = argparse.ArgumentParser(
        description="Log in to Airflow via Auth0 (authorization code flow through Airflow plugin)",
    )
    parser.add_argument(
        "--auth0-domain",
        default=os.getenv("AUTH0_DOMAIN"),
        help="Auth0 tenant domain (e.g., 'mycompany-dev.us.auth0.com')",
    )
    parser.add_argument(
        "--auth0-client-id",
        default=os.getenv("AUTH0_CLIENT_ID"),
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
        help="OAuth callback URL on your Airflow server (e.g., 'https://<your-domain>/oauth/mcp-callback')",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Check current token status without logging in",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        dest="list_envs",
        help="List all stored environment tokens and their status",
    )

    args = parser.parse_args()

    if args.list_envs:
        envs = list_stored_environments()
        if not envs:
            print("No stored tokens found.")
            print("Run `astro-airflow-mcp-login` to authenticate.")
            sys.exit(0)
        print("Stored environment tokens:\n")
        for env in envs:
            status = "EXPIRED" if env["expired"] else "valid"
            print(f"  [{status:>7}] {env['auth0_domain']}")
            print(f"           {env['token_file']}")
        sys.exit(0)

    if args.status:
        if not args.auth0_domain:
            parser.error("--auth0-domain is required for --status")
        token = get_access_token(
            auth0_domain=args.auth0_domain,
            client_id=args.auth0_client_id or "",
        )
        if token:
            token_file = _token_file_for_domain(args.auth0_domain)
            print(f"Auth0 domain: {args.auth0_domain}")
            print(f"Valid token found at {token_file}")
        else:
            print("No valid token. Run `astro-airflow-mcp-login` to authenticate.")
            sys.exit(1)
        return

    # Validate required args for login
    missing = []
    if not args.auth0_domain:
        missing.append("--auth0-domain")
    if not args.auth0_client_id:
        missing.append("--auth0-client-id")
    if not args.auth0_callback_url:
        missing.append("--auth0-callback-url")
    if missing:
        parser.error(f"the following arguments are required for login: {', '.join(missing)}")

    ok = login(
        auth0_domain=args.auth0_domain,
        client_id=args.auth0_client_id,
        audience=args.auth0_audience,
        callback_url=args.auth0_callback_url,
    )
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
