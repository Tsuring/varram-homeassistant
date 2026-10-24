"""Local standard-library tests for the Home Assistant-independent reducer."""

from __future__ import annotations

import importlib.util
import json
import sys
import unittest
from datetime import date
from pathlib import Path
from types import ModuleType


ROOT = Path(__file__).resolve().parents[1]
PACKAGE_DIR = ROOT / "custom_components" / "varram"
TEST_PACKAGE = "_varram_test_package"
package = ModuleType(TEST_PACKAGE)
package.__path__ = [str(PACKAGE_DIR)]
sys.modules.setdefault(TEST_PACKAGE, package)


def load_module(name: str):
    """Load a package module without importing Home Assistant's integration setup."""
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
models = load_module("models")


def row(
    status: str,
    give_time: str,
    eat_time: str,
    *,
    gram: float = 10.0,
    left: float = 10.0,
    immediate: float = 0.0,
    eat_count: int = 100,
) -> dict[str, object]:
    return {
        "status": status,
        "immediate": immediate,
        "left": left,
        "gram": gram,
        "give_time": give_time,
        "spend": 0,
        "eatCount": eat_count,
        "eat_time": eat_time,
    }


def parse_rows(rows: list[dict[str, object]]):
    return models.parse_feed_payload({"type": "success", "data": {"data_list": rows}})


class FeedRecordAggregationTests(unittest.TestCase):
    def test_duplicate_and_out_of_order_snapshots_count_one_meal(self) -> None:
        first = row(
            "waiting", "2026-10-25 08:00:00", "2026-10-25 08:01:00",
            gram=10.2, left=9.4,
        )
        latest = row(
            "waiting", "2026-10-25 08:00:00", "2026-10-25 08:02:00",
            gram=10.2, left=8.2,
        )
        records = parse_rows([latest, first, latest, row(
            "feed_finish", "2026-10-25 08:00:00", "2026-10-25 08:00:00",
            gram=10.2,
        )])

        metrics = models.aggregate_feed_records(records, today=date(2026, 10, 25))

        self.assertEqual(metrics.today_observed_intake_grams, 2.0)
        self.assertEqual(metrics.today_app_intake_grams, 2)
        self.assertEqual(metrics.today_app_feed_amount_grams, 11)
        self.assertEqual(metrics.today_feeding_count, 1)
        self.assertEqual(metrics.latest_intake_at.isoformat(sep=" "), "2026-10-25 08:02:00")
        self.assertEqual(len(metrics.meals), 1)

    def test_immediate_manual_group_is_one_feed_event(self) -> None:
        automatic = row(
            "feed_finish", "2026-10-25 08:00:00", "2026-10-25 08:00:00",
            gram=10.2,
        )
        manual = row(
            "feed_finish", "2026-10-25 12:00:00", "2026-10-25 12:00:00",
            gram=7.25, immediate=7.25,
        )
        metrics = models.aggregate_feed_records(
            parse_rows([automatic, manual]), today=date(2026, 10, 25)
        )

        self.assertEqual(metrics.today_feeding_count, 2)
        self.assertEqual(metrics.today_app_feed_amount_grams, 11)
        self.assertEqual(metrics.today_record_feed_amount_grams, 10.2)
        self.assertEqual(metrics.today_observed_intake_grams, 0)
        self.assertEqual(metrics.latest_feeding_at.isoformat(sep=" "), "2026-10-25 12:00:00")

    def test_empty_feed_and_bowl_removal_are_not_intake(self) -> None:
        records = parse_rows(
            [
                row("empty_feed", "2026-10-25 08:00:00", "2026-10-25 08:00:01"),
                row("none_bowl", "2026-10-25 09:00:00", "2026-10-25 09:00:01", left=0),
            ]
        )
        metrics = models.aggregate_feed_records(records, today=date(2026, 10, 25))

        self.assertEqual(metrics.today_observed_intake_grams, 0)
        self.assertEqual(metrics.today_app_intake_grams, 0)
        self.assertEqual(metrics.today_feeding_count, 0)

    def test_bowl_removal_marks_remaining_unavailable_and_feed_failure_keeps_last_measure(self) -> None:
        removed = models.aggregate_feed_records(
            parse_rows(
                [
                    row("feed_finish", "2026-10-25 08:00:00", "2026-10-25 08:00:00", left=4),
                    row("none_bowl", "2026-10-25 08:00:00", "2026-10-25 08:05:00", left=0),
                ]
            ),
            today=date(2026, 10, 25),
        )
        failed = models.aggregate_feed_records(
            parse_rows(
                [
                    row("feed_finish", "2026-10-25 08:00:00", "2026-10-25 08:00:00", left=4),
                    row("empty_feed", "2026-10-25 08:00:00", "2026-10-25 08:05:00", left=0),
                ]
            ),
            today=date(2026, 10, 25),
        )

        self.assertIsNone(removed.latest_record_remaining_grams)
        self.assertEqual(removed.latest_event_status, "none_bowl")
        self.assertEqual(failed.latest_record_remaining_grams, 4)
        self.assertEqual(failed.latest_event_status, "empty_feed")

    def test_calendar_cutoff_uses_give_time_and_exposes_record_day_estimate(self) -> None:
        records = parse_rows(
            [
                row("waiting", "2026-10-24 23:30:00", "2026-10-24 23:45:00", left=8),
                row("waiting", "2026-10-24 23:30:00", "2026-10-25 00:15:00", left=7),
                row("waiting", "2026-10-25 00:30:00", "2026-10-25 00:35:00", left=9),
            ]
        )
        metrics = models.aggregate_feed_records(records, today=date(2026, 10, 25))

        self.assertEqual(metrics.today_observed_intake_grams, 1)
        self.assertEqual(metrics.yesterday_observed_intake_grams, 3)
        self.assertEqual(metrics.today_record_day_intake_grams, 2)

    def test_history_is_grouped_and_capped_at_ten_meals(self) -> None:
        rows = [
            row(
                "feed_finish",
                f"2026-10-{day:02d} 08:00:00",
                f"2026-10-{day:02d} 08:00:00",
                gram=5,
            )
            for day in range(1, 13)
        ]
        metrics = models.aggregate_feed_records(parse_rows(rows), today=date(2026, 10, 25))
        history = models.meal_history_attributes(metrics.meals)

        self.assertEqual(len(metrics.meals), 10)
        self.assertEqual(len(history), 10)
        self.assertEqual(history[0]["give_time"], "2026-10-12 08:00:00")
        self.assertEqual(history[0]["gram"], 5)

    def test_invalid_or_unsuccessful_payload_is_not_treated_as_empty(self) -> None:
        with self.assertRaises(models.InvalidFeedPayload):
            models.parse_feed_payload({"type": "fail", "msg": "JsonWebTokenError"})
        with self.assertRaises(models.InvalidFeedPayload):
            models.parse_feed_payload({"type": "success", "data": {}})
        with self.assertRaises(models.InvalidFeedPayload):
            parse_rows([{"status": "waiting"}])

    def test_sanitized_fixture_today_values(self) -> None:
        fixture_path = ROOT / "tests" / "fixtures" / "feed-records.json"
        fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
        metrics = {}
        for pet in fixture["pets"]:
            records = parse_rows(pet["rows"])
            metrics[pet["id"]] = models.aggregate_feed_records(
                records, today=date(2026, 10, 25)
            )

        self.assertEqual(metrics["pet_1"].today_observed_intake_grams, 0)
        self.assertEqual(metrics["pet_1"].today_app_intake_grams, 1)
        self.assertEqual(metrics["pet_1"].today_app_feed_amount_grams, 21)
        self.assertEqual(metrics["pet_1"].today_record_feed_amount_grams, 20.07)
        self.assertEqual(metrics["pet_1"].today_record_day_intake_grams, 3.09)
        self.assertEqual(metrics["pet_2"].today_observed_intake_grams, 3.33)
        self.assertEqual(metrics["pet_2"].today_app_intake_grams, 4)
        self.assertEqual(metrics["pet_2"].today_app_feed_amount_grams, 27)
        self.assertEqual(metrics["pet_2"].today_record_feed_amount_grams, 26.1)
        self.assertEqual(metrics["pet_2"].today_record_day_intake_grams, 3.33)


if __name__ == "__main__":
    unittest.main()
