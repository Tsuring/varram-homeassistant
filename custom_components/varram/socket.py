"""Sequential read-only feeder status polling over VARRAM WebSockets."""

from __future__ import annotations

import asyncio
import json
import logging
import math
from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import datetime
from typing import Any

import aiohttp
from yarl import URL

AUTH_FAILURES = {"JsonWebTokenError", "TokenExpiredError", "NotBeforeError", "UnauthorizedError"}
AUTH_RETRY_INTERVAL = 300
SUPPORTED_STATUSES = {
    "waiting",
    "feed_finish",
    "eat",
    "feed_start",
    "none_bowl",
    "empty_feed",
}


class SocketAuthError(Exception):
    """The status socket rejected its app token."""


def parse_status_packet(raw: str | bytes, expected_device_id: str) -> dict[str, Any] | None:
    """Parse only a read-only device status response for the requested feeder."""
    try:
        packet = json.loads(raw)
    except (TypeError, ValueError, UnicodeDecodeError):
        return None
    if not isinstance(packet, Mapping):
        return None
    message = packet.get("msg")
    if packet.get("type") == "fail" and isinstance(message, str) and message in AUTH_FAILURES:
        raise SocketAuthError
    data = packet.get("data")
    if packet.get("type") != "nt_device_status" or not isinstance(data, Mapping):
        return None
    if data.get("device_id") != expected_device_id:
        return None
    feeding = data.get("feeding")
    if not isinstance(feeding, Mapping):
        return None
    status = feeding.get("type")
    if not isinstance(status, str) or status not in SUPPORTED_STATUSES:
        return None
    remaining_grams: float | None = None
    if status in {"waiting", "feed_finish"}:
        value = feeding.get("left_feed")
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value < 0
        ):
            return None
        remaining_grams = float(value)
    power = data.get("power")
    power_type = power.get("type") if isinstance(power, Mapping) else None
    feed_lack = data.get("feed_lack")
    return {
        "status": status,
        "remaining_grams": remaining_grams,
        "power_type": power_type if isinstance(power_type, str) else None,
        "feed_lack": feed_lack if isinstance(feed_lack, bool) else None,
    }


class VarramSocketMonitor:
    """Query each feeder one at a time and expire current status after 180s."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        *,
        get_auth: Callable[[], Awaitable[Mapping[str, str]]],
        on_status: Callable[[dict[str, Any]], None],
        on_unavailable: Callable[[dict[str, str]], None],
        on_auth_error: Callable[[str], Awaitable[None]] | None = None,
        logger: logging.Logger | None = None,
        status_interval: float = 60,
        query_timeout: float = 10,
        stale_after: float = 180,
        inter_device_delay: float = 1,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if status_interval <= 0 or query_timeout <= 0 or stale_after <= 0:
            raise ValueError("Socket timing values must be positive")
        self.session = session
        self.get_auth = get_auth
        self.on_status = on_status
        self.on_unavailable = on_unavailable
        self.on_auth_error = on_auth_error
        self.logger = logger or logging.getLogger(__name__)
        self.status_interval = status_interval
        self.query_timeout = query_timeout
        self.stale_after = stale_after
        self.inter_device_delay = max(0, inter_device_delay)
        self.now = now or (lambda: datetime.now().astimezone())
        self.connections: tuple[dict[str, str], ...] = ()
        self._task: asyncio.Task[None] | None = None
        self._stopped = True
        self._wakeup = asyncio.Event()
        self._last_valid_at: dict[str, float] = {}
        self._stale_handles: dict[str, asyncio.TimerHandle] = {}
        self._unavailable: set[str] = set()
        self._auth_retry_after = 0.0

    async def async_start(self, connections: Sequence[Mapping[str, str]]) -> None:
        """Start the sequential status cycle for connected feeder devices."""
        await self.async_stop()
        normalized: list[dict[str, str]] = []
        for connection in connections:
            pet_id = connection.get("pet_id")
            device_id = connection.get("device_id")
            if not isinstance(pet_id, str) or not pet_id or not isinstance(device_id, str) or not device_id:
                raise ValueError("Feeder connections need string pet_id and device_id")
            normalized.append({"pet_id": pet_id, "device_id": device_id})
        self.connections = tuple(normalized)
        self._stopped = False
        self._wakeup = asyncio.Event()
        for connection in self.connections:
            self._mark_unavailable(connection)
        if self.connections:
            self._task = asyncio.create_task(self._run())

    async def async_stop(self) -> None:
        """Cancel the active request, stale timers, and future cycles."""
        self._stopped = True
        self._wakeup.set()
        for handle in self._stale_handles.values():
            handle.cancel()
        self._stale_handles.clear()
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
        self._last_valid_at.clear()
        self._unavailable.clear()
        self.connections = ()

    def async_refresh_auth(self) -> None:
        """Wake the next sequential query so it reads the newest token."""
        if not self._stopped:
            self._wakeup.set()

    async def _run(self) -> None:
        while not self._stopped:
            self._wakeup.clear()
            for index, connection in enumerate(self.connections):
                if self._stopped:
                    return
                if index and self.inter_device_delay:
                    await asyncio.sleep(self.inter_device_delay)
                    if self._stopped:
                        return
                token: str | None = None
                try:
                    auth = await self.get_auth()
                    token = auth.get("access_token")
                    user_id = auth.get("user_id")
                    if not isinstance(token, str) or not token or not isinstance(user_id, str) or not user_id:
                        raise SocketAuthError
                    status = await self._query_status(connection, token, user_id)
                except asyncio.CancelledError:
                    raise
                except SocketAuthError:
                    self.logger.debug("VARRAM status socket authentication failed")
                    continue
                except aiohttp.WSServerHandshakeError as err:
                    if (
                        err.status in (401, 403)
                        and token is not None
                        and self.on_auth_error is not None
                    ):
                        await self._async_refresh_auth(token)
                    continue
                except (asyncio.TimeoutError, aiohttp.ClientError, ValueError, TypeError):
                    self.logger.debug("VARRAM status socket query unavailable")
                    continue
                except Exception:  # noqa: BLE001
                    self.logger.debug("VARRAM status socket query unavailable")
                    continue

                if status is not None:
                    self._record_status(connection, status)

            if self._stopped:
                return
            try:
                await asyncio.wait_for(self._wakeup.wait(), timeout=self.status_interval)
            except TimeoutError:
                pass

    async def _query_status(
        self, connection: Mapping[str, str], token: str, user_id: str
    ) -> dict[str, Any] | None:
        url = URL("wss://smb.varram-robot.com/").with_query(
            {"auth": token, "target": connection["device_id"]}
        )
        async with asyncio.timeout(self.query_timeout):
            async with self.session.ws_connect(url, heartbeat=None, autoping=True) as websocket:
                await websocket.send_json({
                    "type": "req_device_status",
                    "data": {"device_id": connection["device_id"], "msg_id": user_id},
                })
                while True:
                    message = await websocket.receive()
                    if message.type == aiohttp.WSMsgType.TEXT:
                        try:
                            status = parse_status_packet(
                                message.data, connection["device_id"]
                            )
                        except SocketAuthError:
                            if self.on_auth_error is not None:
                                await self._async_refresh_auth(token)
                            raise
                        if status is not None:
                            return status
                    if message.type in {
                        aiohttp.WSMsgType.CLOSED,
                        aiohttp.WSMsgType.CLOSE,
                        aiohttp.WSMsgType.CLOSING,
                        aiohttp.WSMsgType.ERROR,
                    }:
                        return None

    def _record_status(self, connection: Mapping[str, str], status: Mapping[str, Any]) -> None:
        pet_id = connection["pet_id"]
        now = self.now()
        loop = asyncio.get_running_loop()
        self._last_valid_at[pet_id] = loop.time()
        self._unavailable.discard(pet_id)
        old_timer = self._stale_handles.pop(pet_id, None)
        if old_timer is not None:
            old_timer.cancel()
        self._stale_handles[pet_id] = loop.call_later(
            self.stale_after, self._expire_status, dict(connection), self._last_valid_at[pet_id]
        )
        try:
            self.on_status({
                "pet_id": pet_id,
                "device_id": connection["device_id"],
                **status,
                "observed_at": now,
            })
        except Exception:  # noqa: BLE001
            self.logger.debug("VARRAM status callback failed")

    def _expire_status(self, connection: dict[str, str], last_valid_at: float) -> None:
        pet_id = connection["pet_id"]
        if self._stopped or self._last_valid_at.get(pet_id) != last_valid_at:
            return
        self._mark_unavailable(connection)

    def _mark_unavailable(self, connection: Mapping[str, str]) -> None:
        pet_id = connection["pet_id"]
        if pet_id in self._unavailable:
            return
        self._unavailable.add(pet_id)
        try:
            self.on_unavailable({"pet_id": pet_id, "device_id": connection["device_id"]})
        except Exception:  # noqa: BLE001
            self.logger.debug("VARRAM status availability callback failed")

    async def _async_refresh_auth(self, stale_token: str) -> None:
        """Attempt one socket-triggered login per five-minute retry window."""
        if self.on_auth_error is None:
            return
        loop = asyncio.get_running_loop()
        now = loop.time()
        if now < self._auth_retry_after:
            return
        self._auth_retry_after = now + AUTH_RETRY_INTERVAL
        try:
            await self.on_auth_error(stale_token)
        except Exception:  # noqa: BLE001
            self.logger.debug("VARRAM status socket authentication failed")
            return
        self._auth_retry_after = 0.0
