"""Sequential status polling and guarded one-shot feeder control via WebSockets."""

from __future__ import annotations

import asyncio
import json
import logging
import math
from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextlib import AsyncExitStack
from datetime import datetime
from typing import Any

import aiohttp
from yarl import URL

from .const import (
    ADDITIONAL_FEEDING_COOLDOWN_SECONDS,
    FEEDING_ACK_TIMEOUT_SECONDS,
    FEEDING_PROGRESS_TIMEOUT_SECONDS,
    FEEDING_REQUEST_GRAM_FIELD,
    MAX_SAFE_FEEDER_REMAINING_GRAMS,
)

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


class SocketClosedError(Exception):
    """The feeder closed a WebSocket during an operation."""


class VarramFeedingError(Exception):
    """An additional-feeding request was blocked or could not be confirmed."""

    def __init__(
        self,
        translation_key: str,
        *,
        send_attempted: bool = False,
    ) -> None:
        super().__init__(translation_key)
        self.translation_key = translation_key
        self.send_attempted = send_attempted


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
    value = feeding.get("left_feed")
    reported_remaining_grams: float | None = None
    if not isinstance(value, bool) and isinstance(value, (int, float)):
        try:
            candidate = float(value)
        except OverflowError:
            candidate = math.nan
        if math.isfinite(candidate) and candidate >= 0:
            reported_remaining_grams = candidate
    if status in {"waiting", "feed_finish"} and reported_remaining_grams is None:
        return None
    remaining_grams = (
        reported_remaining_grams if status in {"waiting", "feed_finish"} else None
    )
    power = data.get("power")
    power_type = power.get("type") if isinstance(power, Mapping) else None
    feed_lack = data.get("feed_lack")
    return {
        "status": status,
        "remaining_grams": remaining_grams,
        "reported_remaining_grams": reported_remaining_grams,
        "power_type": power_type if isinstance(power_type, str) else None,
        "feed_lack": feed_lack if isinstance(feed_lack, bool) else None,
    }


def _is_matching_feeding_ack(raw: str, expected_device_id: str, user_id: str) -> bool:
    """Accept a response ACK, rejecting conflicting identity fields if present."""
    try:
        packet = json.loads(raw)
    except (TypeError, ValueError, UnicodeDecodeError):
        return False
    if not isinstance(packet, Mapping) or packet.get("type") != "rsp_feeding":
        return False
    data = packet.get("data")
    if data is not None and not isinstance(data, Mapping):
        return False
    if isinstance(data, Mapping):
        response_device_id = data.get("device_id")
        if response_device_id is not None and response_device_id != expected_device_id:
            return False
        response_user_id = data.get("msg_id")
        if response_user_id is not None and response_user_id != user_id:
            return False
    return True


class VarramSocketMonitor:
    """Poll feeders one at a time and serialize control through the same socket."""

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
        feeding_cooldown: float = ADDITIONAL_FEEDING_COOLDOWN_SECONDS,
        feeding_ack_timeout: float = FEEDING_ACK_TIMEOUT_SECONDS,
        feeding_progress_timeout: float = FEEDING_PROGRESS_TIMEOUT_SECONDS,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if (
            status_interval <= 0
            or query_timeout <= 0
            or stale_after <= 0
            or feeding_cooldown <= 0
            or feeding_ack_timeout <= 0
            or feeding_progress_timeout <= 0
        ):
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
        self.feeding_cooldown = feeding_cooldown
        self.feeding_ack_timeout = feeding_ack_timeout
        self.feeding_progress_timeout = feeding_progress_timeout
        self.now = now or (lambda: datetime.now().astimezone())
        self.connections: tuple[dict[str, str], ...] = ()
        self._task: asyncio.Task[None] | None = None
        self._stopped = True
        self._wakeup = asyncio.Event()
        self._resume_status = asyncio.Event()
        self._resume_status.set()
        self._socket_operation_lock = asyncio.Lock()
        self._additional_feed_lock = asyncio.Lock()
        self._last_additional_feed_at: dict[str, float] = {}
        self._command_tasks: set[asyncio.Task[Any]] = set()
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
        self._resume_status.set()
        for connection in self.connections:
            self._mark_unavailable(connection)
        if self.connections:
            self._task = asyncio.create_task(self._run())

    async def async_stop(self) -> None:
        """Cancel the active request, stale timers, and future cycles."""
        self._stopped = True
        self._resume_status.set()
        self._wakeup.set()
        for handle in self._stale_handles.values():
            handle.cancel()
        self._stale_handles.clear()
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
        current = asyncio.current_task()
        command_tasks = [task for task in self._command_tasks if task is not current]
        for task in command_tasks:
            task.cancel()
        if command_tasks:
            await asyncio.gather(*command_tasks, return_exceptions=True)
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
                await self._resume_status.wait()
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
        async with self._socket_operation_lock:
            if not self._resume_status.is_set():
                return None
            return await self._query_status_with_socket_lock(
                connection, token, user_id
            )

    async def _query_status_with_socket_lock(
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

    async def async_request_additional_feed(
        self,
        connection: Mapping[str, str],
        *,
        minimum_feed_amount: float,
    ) -> None:
        """Send one app-compatible additional-feed request and verify progress.

        The request's fixed gram field value has unknown unit and semantics. Once
        send_json is attempted this method never resends the command, even if the
        socket closes or the execution transition cannot be confirmed.
        """
        if self._stopped or not any(
            item["pet_id"] == connection.get("pet_id")
            and item["device_id"] == connection.get("device_id")
            for item in self.connections
        ):
            raise VarramFeedingError("feeding_not_ready")
        if self._additional_feed_lock.locked():
            raise VarramFeedingError("feeding_busy")

        await self._additional_feed_lock.acquire()
        task = asyncio.current_task()
        if task is not None:
            self._command_tasks.add(task)
        self._resume_status.clear()
        self._wakeup.set()
        send_attempted = False
        device_id = connection["device_id"]
        loop = asyncio.get_running_loop()
        try:
            if (
                isinstance(minimum_feed_amount, bool)
                or not isinstance(minimum_feed_amount, (int, float))
                or not math.isfinite(minimum_feed_amount)
                or minimum_feed_amount <= 0
            ):
                raise VarramFeedingError("feeding_calibration_missing")

            last_attempt = self._last_additional_feed_at.get(device_id)
            if (
                last_attempt is not None
                and loop.time() - last_attempt < self.feeding_cooldown
            ):
                raise VarramFeedingError("feeding_cooldown")

            try:
                auth = await self.get_auth()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                raise VarramFeedingError("feeding_auth") from None
            token = auth.get("access_token")
            user_id = auth.get("user_id")
            if not isinstance(token, str) or not token or not isinstance(user_id, str) or not user_id:
                raise VarramFeedingError("feeding_auth")

            url = URL("wss://smb.varram-robot.com/").with_query(
                {"auth": token, "target": device_id}
            )
            async with self._socket_operation_lock:
                if self._stopped:
                    raise VarramFeedingError("feeding_not_ready")
                try:
                    async with AsyncExitStack() as stack:
                        try:
                            async with asyncio.timeout(self.query_timeout):
                                websocket = await stack.enter_async_context(
                                    self.session.ws_connect(
                                        url, heartbeat=None, autoping=True
                                    )
                                )
                                preflight = await self._request_fresh_status(
                                    websocket, connection, user_id
                                )
                        except SocketAuthError:
                            raise VarramFeedingError("feeding_auth") from None
                        except (asyncio.TimeoutError, SocketClosedError):
                            raise VarramFeedingError(
                                "feeding_status_unavailable"
                            ) from None

                        self._record_status(connection, preflight)
                        self._validate_preflight_status(preflight)

                        # Mark before awaiting send_json: a transport exception can
                        # happen after the peer received the complete frame.
                        send_attempted = True
                        self._last_additional_feed_at[device_id] = loop.time()
                        try:
                            async with asyncio.timeout(self.feeding_ack_timeout):
                                await websocket.send_json(
                                    {
                                        "type": "req_feeding",
                                        "data": {
                                            "device_id": device_id,
                                            "msg_id": user_id,
                                            "gram": FEEDING_REQUEST_GRAM_FIELD,
                                        },
                                    }
                                )
                                saw_feed_start = await self._wait_for_feeding_ack(
                                    websocket, connection, user_id
                                )
                        except SocketAuthError:
                            raise VarramFeedingError(
                                "feeding_outcome_unknown", send_attempted=True
                            ) from None
                        except (
                            asyncio.TimeoutError,
                            SocketClosedError,
                            aiohttp.ClientError,
                        ):
                            raise VarramFeedingError(
                                "feeding_outcome_unknown", send_attempted=True
                            ) from None

                        try:
                            async with asyncio.timeout(
                                self.feeding_progress_timeout
                            ):
                                completed = await self._wait_for_feeding_completion(
                                    websocket,
                                    connection,
                                    user_id,
                                    saw_feed_start=saw_feed_start,
                                )
                        except VarramFeedingError:
                            raise
                        except asyncio.TimeoutError:
                            raise VarramFeedingError(
                                "feeding_execution_unknown", send_attempted=True
                            ) from None
                        except (SocketClosedError, aiohttp.ClientError):
                            raise VarramFeedingError(
                                "feeding_outcome_unknown", send_attempted=True
                            ) from None
                        if not completed:
                            raise VarramFeedingError(
                                "feeding_execution_unknown", send_attempted=True
                            )
                except VarramFeedingError:
                    raise
                except asyncio.CancelledError:
                    raise
                except (asyncio.TimeoutError, SocketClosedError, aiohttp.ClientError):
                    key = (
                        "feeding_outcome_unknown"
                        if send_attempted
                        else "feeding_status_unavailable"
                    )
                    raise VarramFeedingError(
                        key, send_attempted=send_attempted
                    ) from None
                except Exception:  # noqa: BLE001
                    key = (
                        "feeding_outcome_unknown"
                        if send_attempted
                        else "feeding_status_unavailable"
                    )
                    raise VarramFeedingError(
                        key, send_attempted=send_attempted
                    ) from None
        finally:
            self._resume_status.set()
            self._wakeup.set()
            self._additional_feed_lock.release()
            if task is not None:
                self._command_tasks.discard(task)

    def _validate_preflight_status(self, status: Mapping[str, Any]) -> None:
        if status.get("status") not in {"waiting", "feed_finish", "eat"}:
            raise VarramFeedingError("feeding_state_blocked")
        remaining = status.get("reported_remaining_grams")
        if (
            isinstance(remaining, bool)
            or not isinstance(remaining, (int, float))
            or not math.isfinite(remaining)
            or remaining < 0
        ):
            raise VarramFeedingError("feeding_status_unavailable")
        if remaining > MAX_SAFE_FEEDER_REMAINING_GRAMS:
            raise VarramFeedingError("feeding_overfull")

    async def _request_fresh_status(
        self,
        websocket: aiohttp.ClientWebSocketResponse,
        connection: Mapping[str, str],
        user_id: str,
    ) -> dict[str, Any]:
        await websocket.send_json(
            {
                "type": "req_device_status",
                "data": {"device_id": connection["device_id"], "msg_id": user_id},
            }
        )
        while True:
            raw = await self._receive_text(websocket)
            if raw is None:
                continue
            status = parse_status_packet(raw, connection["device_id"])
            if status is not None:
                return status

    async def _wait_for_feeding_ack(
        self,
        websocket: aiohttp.ClientWebSocketResponse,
        connection: Mapping[str, str],
        user_id: str,
    ) -> bool:
        """Wait for a matching ACK; payload contents do not prove execution."""
        saw_feed_start = False
        while True:
            raw = await self._receive_text(websocket)
            if raw is None:
                continue
            try:
                status = parse_status_packet(raw, connection["device_id"])
            except SocketAuthError:
                raise
            if status is not None:
                self._record_status(connection, status)
                if status["status"] in {"none_bowl", "empty_feed"}:
                    raise VarramFeedingError(
                        "feeding_failed", send_attempted=True
                    )
                saw_feed_start = saw_feed_start or status["status"] == "feed_start"
                continue
            if _is_matching_feeding_ack(raw, connection["device_id"], user_id):
                return saw_feed_start

    async def _wait_for_feeding_completion(
        self,
        websocket: aiohttp.ClientWebSocketResponse,
        connection: Mapping[str, str],
        user_id: str,
        *,
        saw_feed_start: bool,
    ) -> bool:
        """Require a fresh feed_start followed by feed_finish or eat."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.feeding_progress_timeout
        while loop.time() < deadline:
            await websocket.send_json(
                {
                    "type": "req_device_status",
                    "data": {
                        "device_id": connection["device_id"],
                        "msg_id": user_id,
                    },
                }
            )
            poll_deadline = min(deadline, loop.time() + 1)
            while loop.time() < poll_deadline:
                try:
                    async with asyncio.timeout(poll_deadline - loop.time()):
                        raw = await self._receive_text(websocket)
                except TimeoutError:
                    break
                if raw is None:
                    continue
                status = parse_status_packet(raw, connection["device_id"])
                if status is None:
                    continue
                self._record_status(connection, status)
                current = status["status"]
                if current in {"none_bowl", "empty_feed"}:
                    raise VarramFeedingError(
                        "feeding_failed", send_attempted=True
                    )
                if current == "feed_start":
                    saw_feed_start = True
                elif saw_feed_start and current in {"feed_finish", "eat"}:
                    return True
        return False

    @staticmethod
    async def _receive_text(
        websocket: aiohttp.ClientWebSocketResponse,
    ) -> str | None:
        message = await websocket.receive()
        if message.type == aiohttp.WSMsgType.TEXT:
            return message.data
        if message.type in {
            aiohttp.WSMsgType.CLOSED,
            aiohttp.WSMsgType.CLOSE,
            aiohttp.WSMsgType.CLOSING,
            aiohttp.WSMsgType.ERROR,
        }:
            raise SocketClosedError
        return None

    def _record_status(self, connection: Mapping[str, str], status: Mapping[str, Any]) -> None:
        if self._stopped:
            return
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
