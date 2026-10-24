"""Timezone tests for VARRAM's Seoul-local API timestamps and date buckets."""

from __future__ import annotations

import importlib.util
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
PACKAGE_DIR = ROOT / "custom_components" / "varram"
TEST_PACKAGE = "_varram_time_test_package"
package = ModuleType(TEST_PACKAGE)
package.__path__ = [str(PACKAGE_DIR)]
sys.modules.setdefault(TEST_PACKAGE, package)

spec = importlib.util.spec_from_file_location(
    f"{TEST_PACKAGE}.time_utils", PACKAGE_DIR / "time_utils.py"
)
assert spec is not None and spec.loader is not None
time_utils = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = time_utils
spec.loader.exec_module(time_utils)


class VarramTimezoneTests(unittest.TestCase):
    def test_feed_window_uses_seoul_calendar_date_not_host_utc_date(self) -> None:
        # 15:30 UTC is 00:30 on the next day in Seoul.
        now = datetime(2026, 10, 24, 15, 30, tzinfo=timezone.utc)

        today, start_time, end_time = time_utils.feed_history_window(now)

        self.assertEqual(today.isoformat(), "2026-10-25")
        self.assertEqual(start_time, "2026-10-19 00:00:00")
        self.assertEqual(end_time, "2026-10-25 23:59:59")

    def test_naive_api_timestamp_is_seoul_time_before_display_conversion(self) -> None:
        naive_korea_time = datetime(2026, 10, 25, 0, 30)

        displayed = time_utils.to_display_timezone(
            naive_korea_time, ZoneInfo("America/Los_Angeles")
        )

        self.assertEqual(displayed.isoformat(), "2026-10-24T08:30:00-07:00")

    def test_aware_socket_timestamp_preserves_instant_when_converted(self) -> None:
        socket_time = datetime(2026, 10, 24, 15, 30, tzinfo=timezone.utc)

        displayed = time_utils.to_display_timezone(
            socket_time, ZoneInfo("America/Los_Angeles")
        )

        self.assertEqual(displayed.isoformat(), "2026-10-24T08:30:00-07:00")

    def test_feed_window_requires_positive_days_and_aware_now(self) -> None:
        with self.assertRaises(ValueError):
            time_utils.feed_history_window(days=0)
        with self.assertRaises(ValueError):
            time_utils.feed_history_window(datetime(2026, 10, 25, 0, 30))


if __name__ == "__main__":
    unittest.main()
