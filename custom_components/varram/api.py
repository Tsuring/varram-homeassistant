"""Async client for VARRAM's read-only pet and feed-history endpoints."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Callable, Mapping
from typing import Any
from urllib.parse import urlencode
from urllib.parse import quote

import aiohttp

from .auth import InvalidToken, TokenClaims, decode_token_claims
from .const import (
    APP_USER_AGENT,
    AUTH_BASE_URL,
    BASE_URL,
    MAX_FEED_ROWS,
    REQUEST_TIMEOUT_SECONDS,
    TOKEN_RENEWAL_WINDOW_SECONDS,
)
from .models import FeedRecord, InvalidFeedPayload, parse_feed_payload

_LOGGER = logging.getLogger(__name__)
_AUTH_FAILURES = {
    "JsonWebTokenError",
    "TokenExpiredError",
    "NotBeforeError",
    "UnauthorizedError",
}


class VarramError(Exception):
    """Base API error with no response body or credential data attached."""


class VarramAuthError(VarramError):
    """The app token or account credentials are no longer accepted."""


class VarramConnectionError(VarramError):
    """VARRAM could not be reached or returned a transient server failure."""


class VarramInvalidResponse(VarramError):
    """VARRAM returned an invalid or unsuccessful response."""


class VarramApiClient:
    """VARRAM client using one Home Assistant-managed aiohttp session."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        token: str | None,
        *,
        email: str | None = None,
        password: str | None = None,
        expected_user_id: str | None = None,
        on_token_update: Callable[[str], None] | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._session = session
        self._token = token.strip() if token else None
        self._email = email.strip() if email else None
        self._password = password if password else None
        self._on_token_update = on_token_update
        self._clock = clock
        self._claims = self._decode_claims(self._token) if self._token else None
        self._expected_user_id = (
            expected_user_id
            or (self._claims.user_id if self._claims is not None else None)
        )
        self._auth_lock = asyncio.Lock()

    @property
    def token_expires_at(self) -> int:
        """Return the current app-token expiry timestamp."""
        if self._claims is None:
            raise VarramAuthError("No VARRAM app token is available")
        return self._claims.expires_at

    @property
    def user_id(self) -> str:
        """Return the validated account identifier carried by the app JWT."""
        if self._claims is not None:
            return self._claims.user_id
        if self._expected_user_id is not None:
            return self._expected_user_id
        raise VarramAuthError("No VARRAM account identifier is available")

    async def async_get_pets(self, token: str | None = None) -> list[dict[str, str]]:
        """Read pet identifiers and names only."""
        payload = await self._async_request_authenticated("GET", "/res/pet/info", token=token)
        data = payload.get("data")
        if not isinstance(data, list):
            raise VarramInvalidResponse("Pet information is missing")
        pets: list[dict[str, str]] = []
        for item in data:
            if not isinstance(item, Mapping):
                raise VarramInvalidResponse("Pet information is invalid")
            pet_id = item.get("pet_id")
            name = item.get("name")
            if not isinstance(pet_id, str) or not pet_id:
                raise VarramInvalidResponse("Pet identifier is missing")
            if not isinstance(name, str):
                name = pet_id
            pets.append({"pet_id": pet_id, "name": name})
        return pets

    async def async_get_connected_feeders(self) -> list[dict[str, str]]:
        """Read connected devices and retain feeder bindings only."""
        payload = await self._async_request_authenticated("GET", "/get/connected_pet")
        data = payload.get("data")
        if not isinstance(data, Mapping) or not isinstance(data.get("connect_list"), list):
            raise VarramInvalidResponse("Connected device list is missing")

        feeders: list[dict[str, str]] = []
        for item in data["connect_list"]:
            if not isinstance(item, Mapping):
                continue
            model_name = item.get("model_name")
            pet_id = item.get("pet_id")
            device_id = item.get("device_id")
            if (
                isinstance(model_name, str)
                and model_name.casefold() == "feeder"
                and isinstance(pet_id, str)
                and pet_id
                and isinstance(device_id, str)
                and device_id
            ):
                feeders.append(
                    {"pet_id": pet_id, "device_id": device_id, "model_name": "feeder"}
                )
        return feeders

    async def async_get_feed_records(
        self, pet_id: str, start_time: str, end_time: str
    ) -> tuple[FeedRecord, ...]:
        """Read a bounded date range of feed records for one pet."""
        path = f"/get/device/info/feed/v2/{quote(pet_id, safe='')}"
        body = {"start_time": start_time, "end_time": end_time}
        payload = await self._async_request_authenticated("POST", path, json_body=body)
        try:
            records = parse_feed_payload(payload)
        except InvalidFeedPayload as err:
            raise VarramInvalidResponse("Feed history response is invalid") from err
        if len(records) > MAX_FEED_ROWS:
            raise VarramInvalidResponse("Feed history exceeds the supported limit")
        return records

    async def async_login(self, email: str, password: str) -> tuple[str, TokenClaims]:
        """Submit the observed native app login form without following redirects."""
        form = urlencode({"email": email, "password": password})
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
        try:
            async with self._session.post(
                f"{AUTH_BASE_URL}/v2/login",
                data=form,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "User-Agent": APP_USER_AGENT,
                },
                timeout=timeout,
                allow_redirects=False,
            ) as response:
                status = response.status
                payload: Any = None
                if "json" in response.headers.get("Content-Type", "").casefold():
                    try:
                        payload = await response.json(content_type=None)
                    except (aiohttp.ContentTypeError, json.JSONDecodeError):
                        payload = None
                token = _extract_login_token(payload)
                if status >= 500 or status == 429:
                    raise VarramConnectionError("VARRAM login service is temporarily unavailable")
                if status >= 400:
                    raise VarramAuthError("VARRAM rejected the account credentials")
        except (asyncio.TimeoutError, aiohttp.ClientError) as err:
            raise VarramConnectionError("Could not reach the VARRAM login service") from err

        if not token:
            raise VarramAuthError("VARRAM login did not return an app access token")
        try:
            claims = decode_token_claims(token)
        except InvalidToken as err:
            raise VarramAuthError("VARRAM login returned an invalid app token") from err
        if claims.expires_at <= self._clock():
            raise VarramAuthError("VARRAM login returned an expired app token")
        return token, claims

    async def async_validate_login(
        self, email: str, password: str, expected_user_id: str | None = None
    ) -> str:
        """Login, enforce account continuity, and verify the fresh app token."""
        token, claims = await self.async_login(email, password)
        if expected_user_id is not None and claims.user_id != expected_user_id:
            raise VarramAuthError("VARRAM credentials belong to a different account")
        await self.async_get_pets(token=token)
        self._token = token
        self._claims = claims
        self._expected_user_id = claims.user_id
        return token

    async def async_get_socket_auth(self) -> dict[str, str]:
        """Return a current token/account pair for the status-only WebSocket."""
        await self._async_ensure_valid_token()
        assert self._claims is not None
        return {"access_token": self._token, "user_id": self._claims.user_id}

    async def _async_ensure_valid_token(self) -> None:
        if self._claims is None or self._token is None:
            raise VarramAuthError("No VARRAM app token is available")
        renew_before = self._clock() + TOKEN_RENEWAL_WINDOW_SECONDS
        if self._claims.expires_at <= renew_before and self._has_credentials:
            await self._async_refresh_token(force=False)
        elif self._claims.expires_at <= self._clock():
            raise VarramAuthError("VARRAM app token has expired")

    async def _async_request_authenticated(
        self,
        method: str,
        path: str,
        *,
        token: str | None = None,
        json_body: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Make an authenticated request with at most one safe login retry.

        An explicit token is used only for validating newly issued credentials;
        that path bypasses proactive refresh and retry to avoid recursion.
        """
        if token is not None:
            return await self._request_json(method, path, token=token, json_body=json_body)

        await self._async_ensure_valid_token()
        used_token = self._token
        try:
            return await self._request_json(method, path, json_body=json_body)
        except VarramAuthError:
            if not self._has_credentials:
                raise
            await self._async_refresh_token(force=True, stale_token=used_token)
            return await self._request_json(method, path, json_body=json_body)

    async def _async_refresh_token(
        self, *, force: bool, stale_token: str | None = None
    ) -> None:
        async with self._auth_lock:
            if stale_token is not None and self._token != stale_token:
                return
            renew_before = self._clock() + TOKEN_RENEWAL_WINDOW_SECONDS
            if not force and self._claims is not None and self._claims.expires_at > renew_before:
                return
            if not self._has_credentials or not self._expected_user_id:
                if self._claims is None or self._claims.expires_at <= self._clock():
                    raise VarramAuthError("VARRAM app token has expired")
                return

            assert self._email is not None and self._password is not None
            token, claims = await self.async_login(self._email, self._password)
            if claims.user_id != self._expected_user_id:
                raise VarramAuthError("VARRAM credentials belong to a different account")
            await self.async_get_pets(token=token)

            # Replace in-memory state and persist only after both account and API
            # validation have succeeded. The caller's callback stores the token.
            self._token = token
            self._claims = claims
            if self._on_token_update is not None:
                self._on_token_update(token)
            _LOGGER.debug("Renewed VARRAM app token for the configured account")

    async def async_refresh_token(self, stale_token: str | None = None) -> None:
        """Refresh credentials after a status socket reports an auth rejection."""
        await self._async_refresh_token(force=True, stale_token=stale_token)

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        token: str | None = None,
        json_body: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        headers = {"auth": token or self._token}
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
        try:
            async with self._session.request(
                method,
                f"{BASE_URL}{path}",
                headers=headers,
                json=json_body,
                timeout=timeout,
                allow_redirects=False,
            ) as response:
                status = response.status
                if status in (401, 403):
                    raise VarramAuthError("VARRAM rejected the app token")
                if status == 429 or status >= 500:
                    raise VarramConnectionError("VARRAM service is temporarily unavailable")
                if status >= 400:
                    raise VarramInvalidResponse("VARRAM request was rejected")
                try:
                    payload = await response.json(content_type=None)
                except (aiohttp.ContentTypeError, json.JSONDecodeError) as err:
                    raise VarramInvalidResponse("VARRAM response was not valid JSON") from err
        except VarramError:
            raise
        except (asyncio.TimeoutError, aiohttp.ClientError) as err:
            raise VarramConnectionError("Could not reach VARRAM") from err

        if not isinstance(payload, dict):
            raise VarramInvalidResponse("VARRAM response was not an object")
        if payload.get("type") != "success":
            message = payload.get("msg")
            if isinstance(message, str) and message in _AUTH_FAILURES:
                raise VarramAuthError("VARRAM rejected the app token")
            raise VarramInvalidResponse("VARRAM returned an unsuccessful response")
        return payload

    @property
    def _has_credentials(self) -> bool:
        return bool(self._email and self._password)

    @staticmethod
    def _decode_claims(token: str) -> TokenClaims:
        try:
            return decode_token_claims(token)
        except InvalidToken as err:
            raise VarramAuthError("Configured app token is invalid") from err


def _extract_login_token(payload: Any) -> str | None:
    """Read only the access token returned in the confirmed app-login JSON."""
    if isinstance(payload, Mapping):
        token = payload.get("access_token")
        if isinstance(token, str) and token:
            return token.strip()
        data = payload.get("data")
        if isinstance(data, Mapping):
            token = data.get("access_token")
            if isinstance(token, str) and token:
                return token.strip()

    return None
