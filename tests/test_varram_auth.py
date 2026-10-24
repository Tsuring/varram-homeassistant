"""Standard-library tests for app-token renewal and auth response handling."""

from __future__ import annotations

import base64
import importlib.util
import json
import sys
import unittest
from pathlib import Path
from types import ModuleType
from urllib.parse import parse_qs


ROOT = Path(__file__).resolve().parents[1]
PACKAGE_DIR = ROOT / "custom_components" / "varram"
TEST_PACKAGE = "_varram_auth_test_package"
package = ModuleType(TEST_PACKAGE)
package.__path__ = [str(PACKAGE_DIR)]
sys.modules.setdefault(TEST_PACKAGE, package)


def load_module(name: str):
    qualified_name = f"{TEST_PACKAGE}.{name}"
    if qualified_name in sys.modules:
        return sys.modules[qualified_name]
    spec = importlib.util.spec_from_file_location(qualified_name, PACKAGE_DIR / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[qualified_name] = module
    spec.loader.exec_module(module)
    return module


load_module("const")
load_module("auth")
load_module("models")
api = load_module("api")


def token(user_id: str, expires_at: int) -> str:
    claims = json.dumps({"userid": user_id, "ptype": "app", "exp": expires_at}).encode()
    payload = base64.urlsafe_b64encode(claims).decode().rstrip("=")
    return f"header.{payload}.signature"


class FakeHeaders(dict):
    def getall(self, name: str, default: list[str] | None = None) -> list[str]:
        value = self.get(name)
        if value is None:
            return default or []
        return value if isinstance(value, list) else [value]


class FakeResponse:
    def __init__(self, payload: object, *, status: int = 200, headers=None) -> None:
        self.payload = payload
        self.status = status
        self.headers = FakeHeaders(headers or {"Content-Type": "application/json"})

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return None

    async def json(self, **_):
        return self.payload


class FakeSession:
    def __init__(self, login_response: FakeResponse, request_response: FakeResponse) -> None:
        self.login_response = login_response
        self.request_response = request_response
        self.posts: list[dict[str, object]] = []
        self.requests: list[dict[str, object]] = []

    def post(self, url: str, **kwargs):
        self.posts.append({"url": url, **kwargs})
        return self.login_response

    def request(self, method: str, url: str, **kwargs):
        self.requests.append({"method": method, "url": url, **kwargs})
        return self.request_response


class AuthRenewalTests(unittest.IsolatedAsyncioTestCase):
    async def test_blank_seed_token_uses_native_login_and_validates_new_token(self) -> None:
        now = 1_800_000_000
        fresh_token = token("account-1", now + 30 * 24 * 60 * 60)
        session = FakeSession(
            FakeResponse({"data": {"access_token": fresh_token}}),
            FakeResponse({"type": "success", "data": []}),
        )
        client = api.VarramApiClient(session, None, clock=lambda: now)

        validated_token = await client.async_validate_login(
            "owner@example.test", "password-value"
        )

        self.assertEqual(validated_token, fresh_token)
        self.assertEqual(client.user_id, "account-1")
        self.assertEqual(len(session.posts), 1)
        self.assertEqual(len(session.requests), 1)
        self.assertEqual(session.requests[0]["headers"]["auth"], fresh_token)

    async def test_expired_token_renews_before_initial_pets_request(self) -> None:
        now = 1_800_000_000
        fresh_token = token("account-1", now + 30 * 24 * 60 * 60)
        pet_response = FakeResponse(
            {"type": "success", "data": [{"pet_id": "pet-1", "name": "Mandu"}]}
        )
        session = FakeSession(
            FakeResponse({"data": {"access_token": fresh_token}}), pet_response
        )
        persisted: list[str] = []
        client = api.VarramApiClient(
            session,
            token("account-1", now - 1),
            email="owner@example.test",
            password="password-value",
            expected_user_id="account-1",
            on_token_update=persisted.append,
            clock=lambda: now,
        )

        pets = await client.async_get_pets()

        self.assertEqual(pets, [{"pet_id": "pet-1", "name": "Mandu"}])
        self.assertEqual(len(session.posts), 1)
        self.assertEqual(
            session.posts[0]["url"], "https://auth.varram-robot.com/v2/login"
        )
        self.assertEqual(
            parse_qs(session.posts[0]["data"]),
            {"email": ["owner@example.test"], "password": ["password-value"]},
        )
        self.assertFalse(session.posts[0]["allow_redirects"])
        self.assertEqual(
            session.posts[0]["headers"]["User-Agent"],
            "(iPhone; CPU iPhone OS 18_7 like Mac OS X) pcss/app/vapp/2.3.7/KR",
        )
        self.assertEqual(len(session.requests), 2)
        self.assertTrue(all(call["headers"]["auth"] == fresh_token for call in session.requests))
        self.assertEqual(persisted, [fresh_token])

    async def test_mismatched_login_account_is_never_persisted(self) -> None:
        now = 1_800_000_000
        wrong_account_token = token("account-2", now + 3600)
        session = FakeSession(
            FakeResponse({"access_token": wrong_account_token}),
            FakeResponse({"type": "success", "data": []}),
        )
        persisted: list[str] = []
        client = api.VarramApiClient(
            session,
            token("account-1", now - 1),
            email="owner@example.test",
            password="password-value",
            expected_user_id="account-1",
            on_token_update=persisted.append,
            clock=lambda: now,
        )

        with self.assertRaises(api.VarramAuthError):
            await client.async_get_pets()

        self.assertEqual(persisted, [])
        self.assertEqual(session.requests, [])

    async def test_json_jwt_failure_is_auth_error_not_empty_pets(self) -> None:
        now = 1_800_000_000
        session = FakeSession(
            FakeResponse({}),
            FakeResponse({"type": "fail", "msg": "JsonWebTokenError"}),
        )
        client = api.VarramApiClient(session, token("account-1", now + 3600), clock=lambda: now)

        with self.assertRaises(api.VarramAuthError):
            await client.async_get_pets()

    async def test_expired_login_token_is_rejected_before_persist(self) -> None:
        now = 1_800_000_000
        expired_token = token("account-1", now - 10)
        session = FakeSession(
            FakeResponse({"access_token": expired_token}),
            FakeResponse({"type": "success", "data": []}),
        )
        persisted: list[str] = []
        client = api.VarramApiClient(
            session,
            token("account-1", now - 1),
            email="owner@example.test",
            password="password-value",
            expected_user_id="account-1",
            on_token_update=persisted.append,
            clock=lambda: now,
        )

        with self.assertRaises(api.VarramAuthError):
            await client.async_get_pets()

        self.assertEqual(persisted, [])


if __name__ == "__main__":
    unittest.main()
