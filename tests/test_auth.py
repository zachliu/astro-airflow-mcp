"""Tests for Auth0 authentication module."""

import json
import time

import pytest

from astro_airflow_mcp.auth import (
    LEGACY_TOKEN_FILE,
    TOKEN_DIR,
    _load_token,
    _parse_jwt_exp,
    _save_token,
    _token_file_for_domain,
    _token_is_expired,
    get_access_token,
    list_stored_environments,
)


class TestTokenFileForDomain:
    def test_none_returns_legacy_path(self):
        result = _token_file_for_domain(None)
        assert result == LEGACY_TOKEN_FILE

    def test_simple_domain(self):
        result = _token_file_for_domain("mycompany.auth0.com")
        assert result == TOKEN_DIR / "mycompany.auth0.com.json"

    def test_domain_with_subdomain(self):
        result = _token_file_for_domain("mycompany-dev.us.auth0.com")
        assert result == TOKEN_DIR / "mycompany-dev.us.auth0.com.json"

    def test_special_characters_sanitized(self):
        result = _token_file_for_domain("weird/domain:with@chars")
        assert "/" not in result.name
        assert ":" not in result.name
        assert "@" not in result.name


class TestParseJwtExp:
    def test_valid_jwt(self):
        # JWT with exp=1778966691
        import base64

        payload = base64.urlsafe_b64encode(
            json.dumps({"exp": 1778966691, "sub": "test"}).encode()
        ).rstrip(b"=")
        token = f"header.{payload.decode()}.signature"
        assert _parse_jwt_exp(token) == 1778966691

    def test_missing_exp_claim(self):
        import base64

        payload = base64.urlsafe_b64encode(
            json.dumps({"sub": "test"}).encode()
        ).rstrip(b"=")
        token = f"header.{payload.decode()}.signature"
        assert _parse_jwt_exp(token) is None

    def test_invalid_token_format(self):
        assert _parse_jwt_exp("not-a-jwt") is None

    def test_invalid_base64(self):
        assert _parse_jwt_exp("header.!!!invalid!!!.signature") is None


class TestTokenIsExpired:
    def test_fresh_token(self):
        token_data = {
            "fetched_at": time.time(),
            "expires_in": 86400,
        }
        assert _token_is_expired(token_data) is False

    def test_expired_token(self):
        token_data = {
            "fetched_at": time.time() - 90000,
            "expires_in": 86400,
        }
        assert _token_is_expired(token_data) is True

    def test_within_buffer_is_expired(self):
        # Token expires in 1800s (the buffer), should be treated as expired
        token_data = {
            "fetched_at": time.time() - 84600,
            "expires_in": 86400,
        }
        assert _token_is_expired(token_data) is True

    def test_missing_fields_defaults(self):
        token_data = {}
        # fetched_at=0 means it's ancient, should be expired
        assert _token_is_expired(token_data) is True


class TestSaveAndLoadToken:
    def test_save_and_load(self, tmp_path, monkeypatch):
        monkeypatch.setattr("astro_airflow_mcp.auth.TOKEN_DIR", tmp_path / "tokens")

        import base64

        exp = int(time.time()) + 3600
        payload = base64.urlsafe_b64encode(
            json.dumps({"exp": exp}).encode()
        ).rstrip(b"=")
        token = f"h.{payload.decode()}.s"

        _save_token(token, auth0_domain="test.auth0.com")

        token_file = tmp_path / "tokens" / "test.auth0.com.json"
        assert token_file.exists()

        data = json.loads(token_file.read_text())
        assert data["access_token"] == token
        assert data["auth0_domain"] == "test.auth0.com"
        assert data["expires_in"] > 0

    def test_load_nonexistent(self, tmp_path, monkeypatch):
        monkeypatch.setattr("astro_airflow_mcp.auth.TOKEN_DIR", tmp_path / "tokens")
        monkeypatch.setattr(
            "astro_airflow_mcp.auth.LEGACY_TOKEN_FILE", tmp_path / "nope.json"
        )
        assert _load_token(auth0_domain="missing.auth0.com") is None

    def test_load_falls_back_to_legacy(self, tmp_path, monkeypatch):
        monkeypatch.setattr("astro_airflow_mcp.auth.TOKEN_DIR", tmp_path / "tokens")
        legacy_file = tmp_path / "token.json"
        legacy_data = {"access_token": "legacy", "fetched_at": time.time(), "expires_in": 86400}
        legacy_file.write_text(json.dumps(legacy_data))
        monkeypatch.setattr("astro_airflow_mcp.auth.LEGACY_TOKEN_FILE", legacy_file)

        result = _load_token(auth0_domain="any.auth0.com")
        assert result["access_token"] == "legacy"

    def test_file_permissions(self, tmp_path, monkeypatch):
        monkeypatch.setattr("astro_airflow_mcp.auth.TOKEN_DIR", tmp_path / "tokens")

        import base64

        payload = base64.urlsafe_b64encode(
            json.dumps({"exp": int(time.time()) + 3600}).encode()
        ).rstrip(b"=")
        token = f"h.{payload.decode()}.s"

        _save_token(token, auth0_domain="secure.auth0.com")

        token_file = tmp_path / "tokens" / "secure.auth0.com.json"
        assert oct(token_file.stat().st_mode & 0o777) == "0o600"


class TestGetAccessToken:
    def test_returns_valid_token(self, tmp_path, monkeypatch):
        monkeypatch.setattr("astro_airflow_mcp.auth.TOKEN_DIR", tmp_path / "tokens")
        (tmp_path / "tokens").mkdir()

        token_data = {
            "access_token": "valid-jwt",
            "fetched_at": time.time(),
            "expires_in": 86400,
        }
        (tmp_path / "tokens" / "test.auth0.com.json").write_text(
            json.dumps(token_data)
        )

        result = get_access_token(auth0_domain="test.auth0.com", client_id="unused")
        assert result == "valid-jwt"

    def test_returns_none_for_expired(self, tmp_path, monkeypatch):
        monkeypatch.setattr("astro_airflow_mcp.auth.TOKEN_DIR", tmp_path / "tokens")
        (tmp_path / "tokens").mkdir()

        token_data = {
            "access_token": "old-jwt",
            "fetched_at": time.time() - 90000,
            "expires_in": 86400,
        }
        (tmp_path / "tokens" / "expired.auth0.com.json").write_text(
            json.dumps(token_data)
        )

        result = get_access_token(auth0_domain="expired.auth0.com", client_id="unused")
        assert result is None

    def test_returns_none_when_no_token(self, tmp_path, monkeypatch):
        monkeypatch.setattr("astro_airflow_mcp.auth.TOKEN_DIR", tmp_path / "tokens")
        monkeypatch.setattr(
            "astro_airflow_mcp.auth.LEGACY_TOKEN_FILE", tmp_path / "nope.json"
        )

        result = get_access_token(auth0_domain="missing.auth0.com", client_id="unused")
        assert result is None


class TestListStoredEnvironments:
    def test_empty_dir(self, tmp_path, monkeypatch):
        monkeypatch.setattr("astro_airflow_mcp.auth.TOKEN_DIR", tmp_path / "tokens")
        assert list_stored_environments() == []

    def test_lists_environments(self, tmp_path, monkeypatch):
        tokens_dir = tmp_path / "tokens"
        tokens_dir.mkdir()
        monkeypatch.setattr("astro_airflow_mcp.auth.TOKEN_DIR", tokens_dir)

        for domain, expired_offset in [("dev.auth0.com", 0), ("prod.auth0.com", 90000)]:
            data = {
                "access_token": "jwt",
                "fetched_at": time.time() - expired_offset,
                "expires_in": 86400,
                "auth0_domain": domain,
            }
            (tokens_dir / f"{domain}.json").write_text(json.dumps(data))

        envs = list_stored_environments()
        assert len(envs) == 2
        assert envs[0]["auth0_domain"] == "dev.auth0.com"
        assert envs[0]["expired"] is False
        assert envs[1]["auth0_domain"] == "prod.auth0.com"
        assert envs[1]["expired"] is True

    def test_skips_corrupt_files(self, tmp_path, monkeypatch):
        tokens_dir = tmp_path / "tokens"
        tokens_dir.mkdir()
        monkeypatch.setattr("astro_airflow_mcp.auth.TOKEN_DIR", tokens_dir)

        (tokens_dir / "bad.auth0.com.json").write_text("not json{{{")

        assert list_stored_environments() == []
