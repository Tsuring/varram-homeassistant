"""Standard-library tests for VARRAM status parsing and sequential polling."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
PACKAGE_DIR = ROOT / "custom_components" / "varram"
TEST_PACKAGE = "_varram_socket_test_package"
package = ModuleType(TEST_PACKAGE)
package.__path__ = [str(PACKAGE_DIR)]
sys.modules.setdefault(TEST_PACKAGE, package)


def load_module(name: str):
    qualified_name = f"{TEST_PACKAGE}.{name}"
    if qualified_name in sys.modules:
        return sys.modules[qualified_name]
    spec = importlib.util.spec_from_file_location(
        qualified_name, PACKAGE_DIR / f"{name}.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[qualified_name] = module
    spec.loader.exec_module(module)
    return module


socket = load_module("socket")


def packet(
    device_id: str,
    status: str = "waiting",
    remaining: float | None = 12.25,
) -> str:
    feeding = {"type": status}
    if remaining is not None:
        feeding["left_feed"] = remaining
    return json.dumps(
        {
            "type": "nt_device_status",
            "data": {
                "device_id": device_id,
                "feeding": feeding,
                "power": {"type": "main"},
                "feed_lack": False,
            },
        }
    )


class StatusPacketTests(unittest.TestCase):
    def test_waiting_includes_finite_weight_and_status_fields(self) -> None:
        parsed = socket.parse_status_packet(packet("device-1"), "device-1")

        self.assertEqual(parsed["status"], "waiting")
        self.assertEqual(parsed["remaining_grams"], 12.25)
        self.assertEqual(parsed["power_type"], "main")
        self.assertFalse(parsed["feed_lack"])

    def test_feed_finish_can_report_finite_remaining_weight(self) -> None:
        parsed = socket.parse_status_packet(
            packet("device-1", "feed_finish", remaining=18.13), "device-1"
        )

        self.assertEqual(parsed["status"], "feed_finish")
        self.assertEqual(parsed["remaining_grams"], 18.13)

    def test_eat_keeps_sensor_weight_unavailable_but_retains_preflight_measurement(self) -> None:
        parsed = socket.parse_status_packet(
            packet("device-1", "eat", remaining=22.77), "device-1"
        )

        self.assertIsNone(parsed["remaining_grams"])
        self.assertEqual(parsed["reported_remaining_grams"], 22.77)

    def test_non_waiting_status_has_no_weight_and_invalid_packets_are_ignored(self) -> None:
        for status in ("eat", "feed_start", "none_bowl", "empty_feed"):
            with self.subTest(status=status):
                parsed = socket.parse_status_packet(
                    packet("device-1", status, remaining=3), "device-1"
                )
                self.assertIsNone(parsed["remaining_grams"])
        self.assertIsNone(socket.parse_status_packet(packet("other"), "device-1"))
        self.assertIsNone(socket.parse_status_packet("not json", "device-1"))
        self.assertIsNone(
            socket.parse_status_packet(
                json.dumps({"type": "nt_device_status", "data": {"feeding": {"type": []}}}),
                "device-1",
            )
        )
        self.assertIsNone(
            socket.parse_status_packet(
                json.dumps({"type": "fail", "msg": []}), "device-1"
            )
        )
        self.assertIsNone(
            socket.parse_status_packet(packet("device-1", remaining=float("inf")), "device-1")
        )

    def test_json_jwt_error_is_distinguished(self) -> None:
        with self.assertRaises(socket.SocketAuthError):
            socket.parse_status_packet(
                json.dumps({"type": "fail", "msg": "TokenExpiredError"}),
                "device-1",
            )


class _FakeWebSocket:
    def __init__(self, session, device_id: str) -> None:
        self.session = session
        self.device_id = device_id
        self.receive_count = 0

    async def __aenter__(self):
        self.session.active += 1
        self.session.max_active = max(self.session.max_active, self.session.active)
        self.session.opened.append(self.device_id)
        return self

    async def __aexit__(self, *_):
        self.session.active -= 1

    async def send_json(self, payload) -> None:
        self.session.requests.append((self.device_id, payload))

    async def receive(self):
        self.receive_count += 1
        if self.device_id == "device-timeout":
            await asyncio.Future()
        if self.device_id.startswith("device-auth"):
            self.session.auth_responses += 1
            if self.session.auth_responses >= 2:
                self.session.two_auth_responses.set()
            return SimpleNamespace(
                type=1,
                data=json.dumps({"type": "fail", "msg": "JsonWebTokenError"}),
            )
        if self.device_id == "device-interleaved" and self.receive_count == 1:
            return SimpleNamespace(
                type=1,
                data=json.dumps({"type": "nt_device_status", "data": {"message": "unrelated"}}),
            )
        return SimpleNamespace(
            type=1,
            data=packet(self.device_id, remaining=18.13),
        )


class _FakeSession:
    def __init__(self) -> None:
        self.active = 0
        self.max_active = 0
        self.opened: list[str] = []
        self.requests: list[tuple[str, dict[str, object]]] = []
        self.two_auth_responses = asyncio.Event()
        self.auth_responses = 0

    def ws_connect(self, url, **_kwargs):
        return _FakeWebSocket(self, url.query["target"])


class SocketMonitorTests(unittest.IsolatedAsyncioTestCase):
    async def test_query_skips_unrelated_text_until_status_arrives(self) -> None:
        session = _FakeSession()
        monitor = socket.VarramSocketMonitor(
            session,
            get_auth=self._get_auth,
            on_status=lambda _status: None,
            on_unavailable=lambda _connection: None,
            query_timeout=0.1,
        )

        status = await monitor._query_status(
            {"pet_id": "pet-1", "device_id": "device-interleaved"},
            "test-token",
            "account-1",
        )

        self.assertEqual(status["status"], "waiting")
        self.assertEqual(status["remaining_grams"], 18.13)

    async def test_feeders_are_queried_sequentially_and_later_feeder_recovers_after_timeout(self) -> None:
        session = _FakeSession()
        statuses: list[dict[str, object]] = []
        unavailable: list[dict[str, str]] = []
        status_received = asyncio.Event()

        def on_status(status) -> None:
            statuses.append(status)
            if len(statuses) == 1:
                status_received.set()

        monitor = socket.VarramSocketMonitor(
            session,
            get_auth=self._get_auth,
            on_status=on_status,
            on_unavailable=unavailable.append,
            status_interval=3600,
            query_timeout=0.02,
            inter_device_delay=0,
        )
        await monitor.async_start(
            [
                {"pet_id": "pet-1", "device_id": "device-timeout"},
                {"pet_id": "pet-2", "device_id": "device-2"},
            ]
        )
        try:
            await asyncio.wait_for(status_received.wait(), timeout=1)
        finally:
            await monitor.async_stop()

        self.assertEqual(session.opened, ["device-timeout", "device-2"])
        self.assertEqual(session.max_active, 1)
        self.assertEqual([status["pet_id"] for status in statuses], ["pet-2"])
        self.assertEqual(unavailable[0]["pet_id"], "pet-1")
        self.assertEqual(session.requests[0][1]["type"], "req_device_status")
        self.assertEqual(session.requests[0][1]["data"]["msg_id"], "account-1")

    async def test_auth_failure_triggers_at_most_one_login_for_both_feeders(self) -> None:
        session = _FakeSession()
        auth_attempts: list[str] = []

        async def reject_login(stale_token: str) -> None:
            auth_attempts.append(stale_token)
            raise RuntimeError("credentials rejected")

        monitor = socket.VarramSocketMonitor(
            session,
            get_auth=self._get_auth,
            on_status=lambda _status: None,
            on_unavailable=lambda _connection: None,
            on_auth_error=reject_login,
            status_interval=3600,
            query_timeout=0.02,
            inter_device_delay=0,
        )
        await monitor.async_start(
            [
                {"pet_id": "pet-1", "device_id": "device-auth-1"},
                {"pet_id": "pet-2", "device_id": "device-auth-2"},
            ]
        )
        try:
            await asyncio.wait_for(session.two_auth_responses.wait(), timeout=1)
        finally:
            await monitor.async_stop()

        self.assertEqual(auth_attempts, ["test-token"])

    @staticmethod
    async def _get_auth() -> dict[str, str]:
        return {"access_token": "test-token", "user_id": "account-1"}


if __name__ == "__main__":
    unittest.main()
