"""Mocked tests for the one-shot additional-feeding WebSocket operation."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
import unittest
from collections import deque
from pathlib import Path
from types import ModuleType, SimpleNamespace

import aiohttp


ROOT = Path(__file__).resolve().parents[1]
PACKAGE_DIR = ROOT / "custom_components" / "varram"
TEST_PACKAGE = "_varram_feeding_test_package"
package = ModuleType(TEST_PACKAGE)
package.__path__ = [str(PACKAGE_DIR)]
sys.modules.setdefault(TEST_PACKAGE, package)

spec = importlib.util.spec_from_file_location(
    f"{TEST_PACKAGE}.socket", PACKAGE_DIR / "socket.py"
)
assert spec is not None and spec.loader is not None
socket = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = socket
spec.loader.exec_module(socket)

CONNECTION = {"pet_id": "pet-1", "device_id": "device-1"}


def status_packet(
    status: str,
    *,
    device_id: str = "device-1",
    remaining: float | None = 12.5,
) -> dict[str, object]:
    feeding: dict[str, object] = {"type": status}
    if remaining is not None:
        feeding["left_feed"] = remaining
    return {
        "type": "nt_device_status",
        "data": {
            "device_id": device_id,
            "feeding": feeding,
            "power": {"type": "main"},
            "feed_lack": False,
        },
    }


def ack_packet(
    *,
    device_id: str | None = "device-1",
    msg_id: str | None = "account-1",
) -> dict[str, object]:
    data: dict[str, str] = {}
    if device_id is not None:
        data["device_id"] = device_id
    if msg_id is not None:
        data["msg_id"] = msg_id
    return {"type": "rsp_feeding", "data": data}


class _FakeWebSocket:
    def __init__(self, session, connection_number: int, device_id: str) -> None:
        self.session = session
        self.connection_number = connection_number
        self.device_id = device_id
        self.messages: asyncio.Queue[SimpleNamespace] = asyncio.Queue()
        self.status_request_count = 0
        self.progress_queued = False
        self.ack_queued = False

    async def __aenter__(self):
        self.session.active += 1
        self.session.max_active = max(self.session.max_active, self.session.active)
        self.session.opened.append(self)
        return self

    async def __aexit__(self, *_):
        self.session.active -= 1

    async def send_json(self, payload) -> None:
        self.session.sent.append(payload)
        if payload["type"] == "req_device_status":
            self.status_request_count += 1
            if self.session.hang_progress_send and self.status_request_count > 1:
                await asyncio.Future()
            if self.session.hold_first_poll and self.connection_number == 1:
                self.session.poll_started.set()
                await self.session.release_poll.wait()
                self._push(status_packet("waiting"))
            elif self.status_request_count == 1:
                self._push(self.session.preflight_packet)
            elif not self.progress_queued:
                self.progress_queued = True
                for status in self.session.progress_statuses:
                    self._push(status)
            else:
                self._push(status_packet("waiting"))
        elif payload["type"] == "req_feeding":
            self.session.feeding_payloads.append(payload)
            self.session.command_ws = self
            self.session.command_sent.set()
            if self.session.ack_mode == "success":
                self._push(self.session.ack_payload)
            elif self.session.ack_mode == "disconnect":
                self._push_closed()
            elif self.session.ack_mode == "wrong_device":
                self._push(ack_packet(device_id="other-device"))
            elif self.session.ack_mode == "wrong_user":
                self._push(ack_packet(msg_id="other-account"))
            elif self.session.ack_mode == "empty_ack":
                self._push({"type": "rsp_feeding"})
            elif self.session.ack_mode == "hold":
                self.session.ack_waiting.set()
                await self.session.release_ack.wait()
                if not self.ack_queued:
                    self._push(ack_packet())
                    self.ack_queued = True
            # no_ack deliberately leaves the receive queue empty.

    async def receive(self):
        item = await self.messages.get()
        return item

    def _push(self, payload: dict[str, object]) -> None:
        self.messages.put_nowait(
            SimpleNamespace(type=aiohttp.WSMsgType.TEXT, data=json.dumps(payload))
        )

    def _push_closed(self) -> None:
        self.messages.put_nowait(
            SimpleNamespace(type=aiohttp.WSMsgType.CLOSED, data="")
        )


class _FakeSession:
    def __init__(
        self,
        *,
        preflight: dict[str, object] | None = None,
        progress_statuses: list[dict[str, object]] | None = None,
        ack_mode: str = "success",
        ack_payload: dict[str, object] | None = None,
        hold_first_poll: bool = False,
        hang_progress_send: bool = False,
    ) -> None:
        self.preflight_packet = preflight or status_packet("waiting")
        self.progress_statuses = progress_statuses or [
            status_packet("feed_start"),
            status_packet("feed_finish"),
        ]
        self.ack_mode = ack_mode
        self.ack_payload = ack_payload or ack_packet()
        self.hold_first_poll = hold_first_poll
        self.hang_progress_send = hang_progress_send
        self.release_poll = asyncio.Event()
        self.poll_started = asyncio.Event()
        self.release_ack = asyncio.Event()
        self.ack_waiting = asyncio.Event()
        self.command_sent = asyncio.Event()
        self.active = 0
        self.max_active = 0
        self.opened: list[_FakeWebSocket] = []
        self.sent: list[dict[str, object]] = []
        self.feeding_payloads: list[dict[str, object]] = []
        self.command_ws: _FakeWebSocket | None = None
        self.connection_count = 0

    def ws_connect(self, url, **_kwargs):
        self.connection_count += 1
        return _FakeWebSocket(
            self, self.connection_count, url.query["target"]
        )


class AdditionalFeedingTests(unittest.IsolatedAsyncioTestCase):
    async def _make_monitor(
        self,
        session: _FakeSession,
        *,
        ack_timeout: float = 0.03,
        progress_timeout: float = 0.03,
    ):
        monitor = socket.VarramSocketMonitor(
            session,
            get_auth=self._get_auth,
            on_status=lambda _status: None,
            on_unavailable=lambda _connection: None,
            status_interval=3600,
            query_timeout=0.04,
            stale_after=3600,
            inter_device_delay=0,
            feeding_cooldown=60,
            feeding_ack_timeout=ack_timeout,
            feeding_progress_timeout=progress_timeout,
        )
        await monitor.async_start([CONNECTION])
        # Keep the background poll at its next operation boundary for these
        # focused command tests; the serialization test exercises it actively.
        monitor._resume_status.clear()
        return monitor

    async def _request(self, monitor, *, minimum: float | None = 6.69) -> None:
        await monitor.async_request_additional_feed(
            CONNECTION, minimum_feed_amount=minimum
        )

    @staticmethod
    async def _get_auth() -> dict[str, str]:
        return {"access_token": "fake-token", "user_id": "account-1"}

    async def test_success_sends_exact_app_payload_once_and_requires_transition(self) -> None:
        session = _FakeSession()
        monitor = await self._make_monitor(session)
        try:
            await self._request(monitor)

            self.assertEqual(
                session.feeding_payloads,
                [
                    {
                        "type": "req_feeding",
                        "data": {
                            "device_id": "device-1",
                            "msg_id": "account-1",
                            "gram": 1,
                        },
                    }
                ],
            )
            self.assertEqual(
                [payload["type"] for payload in session.sent],
                [
                    "req_device_status",
                    "req_feeding",
                    "req_device_status",
                ],
            )
        finally:
            await monitor.async_stop()

    async def test_empty_ack_is_only_ack_and_still_requires_fresh_execution_transition(self) -> None:
        session = _FakeSession(ack_mode="empty_ack")
        monitor = await self._make_monitor(session)
        try:
            await self._request(monitor)
            self.assertEqual(len(session.feeding_payloads), 1)
        finally:
            await monitor.async_stop()

    async def test_old_eat_then_feed_finish_without_feed_start_is_unknown(self) -> None:
        session = _FakeSession(
            preflight=status_packet("eat", remaining=12.5),
            progress_statuses=[
                status_packet("eat", remaining=12.5),
                status_packet("feed_finish", remaining=11.5),
            ],
        )
        monitor = await self._make_monitor(session)
        try:
            with self.assertRaises(socket.VarramFeedingError) as raised:
                await self._request(monitor)
            self.assertEqual(raised.exception.translation_key, "feeding_execution_unknown")
            self.assertEqual(len(session.feeding_payloads), 1)
        finally:
            await monitor.async_stop()

    async def test_failure_status_after_start_is_reported_without_retry(self) -> None:
        session = _FakeSession(
            progress_statuses=[
                status_packet("feed_start"),
                status_packet("empty_feed"),
            ]
        )
        monitor = await self._make_monitor(session)
        try:
            with self.assertRaises(socket.VarramFeedingError) as raised:
                await self._request(monitor)
            self.assertEqual(raised.exception.translation_key, "feeding_failed")
            self.assertTrue(raised.exception.send_attempted)
            self.assertEqual(len(session.feeding_payloads), 1)
        finally:
            await monitor.async_stop()

    async def test_none_bowl_after_command_is_failure_even_without_feed_start(self) -> None:
        session = _FakeSession(progress_statuses=[status_packet("none_bowl")])
        monitor = await self._make_monitor(session)
        try:
            with self.assertRaises(socket.VarramFeedingError) as raised:
                await self._request(monitor)
            self.assertEqual(raised.exception.translation_key, "feeding_failed")
            self.assertEqual(len(session.feeding_payloads), 1)
        finally:
            await monitor.async_stop()

    async def test_preflight_disallows_unsafe_or_unknown_states_without_sending(self) -> None:
        for status in ("feed_start", "none_bowl", "empty_feed", "unknown"):
            with self.subTest(status=status):
                session = _FakeSession(
                    preflight=status_packet(status),
                    progress_statuses=[],
                )
                monitor = await self._make_monitor(session)
                try:
                    with self.assertRaises(socket.VarramFeedingError):
                        await self._request(monitor)
                    self.assertEqual(session.feeding_payloads, [])
                finally:
                    await monitor.async_stop()

    async def test_eat_preflight_requires_finite_raw_left_and_enforces_60g_limit(self) -> None:
        for preflight, expected in (
            (status_packet("eat", remaining=None), "feeding_status_unavailable"),
            (status_packet("eat", remaining=60.01), "feeding_overfull"),
            (status_packet("waiting", remaining=60.01), "feeding_overfull"),
        ):
            session = _FakeSession(preflight=preflight)
            monitor = await self._make_monitor(session)
            try:
                with self.assertRaises(socket.VarramFeedingError) as raised:
                    await self._request(monitor)
                self.assertEqual(raised.exception.translation_key, expected)
                self.assertEqual(session.feeding_payloads, [])
            finally:
                await monitor.async_stop()

    async def test_missing_calibration_blocks_before_connecting(self) -> None:
        session = _FakeSession()
        monitor = await self._make_monitor(session)
        try:
            with self.assertRaises(socket.VarramFeedingError) as raised:
                await self._request(monitor, minimum=None)
            self.assertEqual(raised.exception.translation_key, "feeding_calibration_missing")
            self.assertEqual(session.connection_count, 0)
            self.assertEqual(session.feeding_payloads, [])
        finally:
            await monitor.async_stop()

    async def test_wrong_device_or_user_ack_is_not_accepted(self) -> None:
        for mode in ("wrong_device", "wrong_user"):
            with self.subTest(mode=mode):
                session = _FakeSession(ack_mode=mode)
                monitor = await self._make_monitor(session)
                try:
                    with self.assertRaises(socket.VarramFeedingError) as raised:
                        await self._request(monitor)
                    self.assertEqual(raised.exception.translation_key, "feeding_outcome_unknown")
                    self.assertEqual(session.command_ws.status_request_count, 1)
                    self.assertEqual(len(session.feeding_payloads), 1)
                finally:
                    await monitor.async_stop()

    async def test_ack_timeout_and_disconnect_never_retry(self) -> None:
        for mode in ("no_ack", "disconnect"):
            with self.subTest(mode=mode):
                session = _FakeSession(ack_mode=mode)
                monitor = await self._make_monitor(session)
                try:
                    with self.assertRaises(socket.VarramFeedingError) as raised:
                        await self._request(monitor)
                    self.assertEqual(raised.exception.translation_key, "feeding_outcome_unknown")
                    self.assertTrue(raised.exception.send_attempted)
                    self.assertIsNone(raised.exception.__cause__)
                    self.assertTrue(raised.exception.__suppress_context__)
                    self.assertEqual(len(session.feeding_payloads), 1)
                finally:
                    await monitor.async_stop()

    async def test_progress_send_timeout_is_bounded_and_never_retries(self) -> None:
        session = _FakeSession(hang_progress_send=True)
        monitor = await self._make_monitor(session, progress_timeout=0.02)
        try:
            with self.assertRaises(socket.VarramFeedingError) as raised:
                await self._request(monitor)
            self.assertEqual(raised.exception.translation_key, "feeding_execution_unknown")
            self.assertEqual(len(session.feeding_payloads), 1)
        finally:
            await monitor.async_stop()

    async def test_concurrent_press_is_busy_and_does_not_queue_second_command(self) -> None:
        session = _FakeSession(ack_mode="hold")
        monitor = await self._make_monitor(session, ack_timeout=1)
        first = asyncio.create_task(self._request(monitor))
        try:
            await asyncio.wait_for(session.ack_waiting.wait(), timeout=0.5)
            with self.assertRaises(socket.VarramFeedingError) as raised:
                await self._request(monitor)
            self.assertEqual(raised.exception.translation_key, "feeding_busy")
            self.assertEqual(len(session.feeding_payloads), 1)
            session.release_ack.set()
            await first
        finally:
            session.release_ack.set()
            await monitor.async_stop()

    async def test_device_cooldown_blocks_second_manual_press(self) -> None:
        session = _FakeSession()
        monitor = await self._make_monitor(session)
        try:
            await self._request(monitor)
            with self.assertRaises(socket.VarramFeedingError) as raised:
                await self._request(monitor)
            self.assertEqual(raised.exception.translation_key, "feeding_cooldown")
            self.assertEqual(len(session.feeding_payloads), 1)
        finally:
            await monitor.async_stop()

    async def test_poll_and_control_never_hold_two_sockets_at_once(self) -> None:
        session = _FakeSession(hold_first_poll=True)
        monitor = socket.VarramSocketMonitor(
            session,
            get_auth=self._get_auth,
            on_status=lambda _status: None,
            on_unavailable=lambda _connection: None,
            status_interval=3600,
            query_timeout=0.1,
            stale_after=3600,
            inter_device_delay=0,
            feeding_ack_timeout=0.1,
            feeding_progress_timeout=0.1,
        )
        await monitor.async_start([CONNECTION])
        command = None
        try:
            await asyncio.wait_for(session.poll_started.wait(), timeout=0.5)
            command = asyncio.create_task(self._request(monitor))
            await asyncio.sleep(0)
            self.assertFalse(session.command_sent.is_set())
            self.assertEqual(session.active, 1)

            session.release_poll.set()
            await command
            self.assertEqual(session.max_active, 1)
            self.assertEqual(len(session.feeding_payloads), 1)
        finally:
            session.release_poll.set()
            if command is not None and not command.done():
                command.cancel()
            await monitor.async_stop()

    async def test_unload_cancels_inflight_command_without_retry(self) -> None:
        session = _FakeSession(ack_mode="hold")
        monitor = await self._make_monitor(session, ack_timeout=1)
        command = asyncio.create_task(self._request(monitor))
        try:
            await asyncio.wait_for(session.ack_waiting.wait(), timeout=0.5)
            await monitor.async_stop()
            self.assertTrue(command.cancelled())
            self.assertEqual(len(session.feeding_payloads), 1)
            self.assertEqual(session.active, 0)
            self.assertEqual(monitor._stale_handles, {})
        finally:
            session.release_ack.set()
            if not command.done():
                command.cancel()
                await asyncio.gather(command, return_exceptions=True)
            await monitor.async_stop()


if __name__ == "__main__":
    unittest.main()
