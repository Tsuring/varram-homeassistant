"""Validated read-only feed record processing, independent of Home Assistant."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from .const import MAX_FEED_ROWS, MAX_HISTORY_MEALS

TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
_STATUS_ORDER = {
    "feed_finish": 0,
    "none_bowl": 1,
    "empty_feed": 2,
    "waiting": 3,
}


class InvalidFeedPayload(ValueError):
    """The server returned a malformed feed-history payload."""


@dataclass(frozen=True, slots=True)
class FeedRecord:
    """The small subset of a VARRAM feed record needed for sensors."""

    status: str
    immediate: float
    left: float
    gram: float
    give_time: datetime
    spend: float
    eat_count: int
    eat_time: datetime


@dataclass(frozen=True, slots=True)
class MealSummary:
    """One give_time cohort, collapsed from its cumulative event snapshots."""

    give_time: datetime
    record_feed_amount_grams: float
    app_feed_amount_grams: int
    observed_intake_grams: float
    app_intake_grams: int
    latest_record_gram: float
    latest_record_left: float
    last_reported_at: datetime
    has_dispense_event: bool
    has_intake_event: bool
    intake_at: datetime | None


@dataclass(frozen=True, slots=True)
class PetMetrics:
    """Current daily totals and bounded recent-history values for one feeder."""

    today_app_feed_amount_grams: int
    today_record_feed_amount_grams: float
    today_observed_intake_grams: float
    today_app_intake_grams: int
    today_record_day_intake_grams: float
    today_feeding_count: int
    yesterday_observed_intake_grams: float
    yesterday_app_intake_grams: int
    latest_feeding_at: datetime | None
    latest_intake_at: datetime | None
    latest_record_remaining_grams: float | None
    latest_record_reported_at: datetime | None
    latest_event_status: str | None
    latest_event_at: datetime | None
    meals: tuple[MealSummary, ...]


def parse_feed_payload(payload: Any) -> tuple[FeedRecord, ...]:
    """Validate and parse an API feed-history response.

    A successful empty ``data_list`` is a valid empty history. Missing fields or
    a non-success response are errors so the integration never turns API errors
    into zero-valued sensors.
    """
    if not isinstance(payload, Mapping) or payload.get("type") != "success":
        raise InvalidFeedPayload("Feed API did not return a success payload")

    data = payload.get("data")
    if not isinstance(data, Mapping) or not isinstance(data.get("data_list"), list):
        raise InvalidFeedPayload("Feed API data_list is missing")
    rows = data["data_list"]
    if len(rows) > MAX_FEED_ROWS:
        raise InvalidFeedPayload("Feed history exceeds the supported response limit")

    records: list[FeedRecord] = []
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise InvalidFeedPayload(f"Feed record {index} is not an object")
        try:
            status = row["status"]
            give_time = datetime.strptime(row["give_time"], TIME_FORMAT)
            eat_time = datetime.strptime(row["eat_time"], TIME_FORMAT)
            gram = _finite_number(row["gram"])
            immediate = _finite_number(row["immediate"])
            left = _finite_number(row["left"])
            spend = _finite_number(row["spend"])
            eat_count = row["eatCount"]
        except (KeyError, TypeError, ValueError, OverflowError) as err:
            raise InvalidFeedPayload(f"Feed record {index} is incomplete or invalid") from err

        if not isinstance(status, str) or not status:
            raise InvalidFeedPayload(f"Feed record {index} has an invalid status")
        if min(gram, immediate, left, spend) < 0:
            raise InvalidFeedPayload(f"Feed record {index} has a negative measurement")
        if isinstance(eat_count, bool) or not isinstance(eat_count, int) or eat_count < 0:
            raise InvalidFeedPayload(f"Feed record {index} has an invalid eatCount")

        records.append(
            FeedRecord(
                status=status,
                immediate=immediate,
                left=left,
                gram=gram,
                give_time=give_time,
                spend=spend,
                eat_count=eat_count,
                eat_time=eat_time,
            )
        )
    return tuple(records)


def aggregate_feed_records(
    records: Sequence[FeedRecord], *, today: date
) -> PetMetrics:
    """Aggregate feed snapshots using VARRAM's app-compatible gram rules.

    Rows are deduplicated by their full record values and grouped by ``give_time``.
    Each cohort contributes at most its final waiting snapshot to intake; its
    cumulative updates are not added together. Calendar attribution follows the
    feeder's ``give_time``, matching the app when eating crosses midnight.
    """
    yesterday = date.fromordinal(today.toordinal() - 1)
    unique_records = tuple(dict.fromkeys(records))
    grouped: dict[datetime, list[FeedRecord]] = defaultdict(list)
    for record in unique_records:
        grouped[record.give_time].append(record)

    all_meals = tuple(
        _summarize_meal(give_time, event_rows)
        for give_time, event_rows in sorted(grouped.items())
    )
    # Standalone bowl-removal events are not meals. Keep them in the record
    # status below, but omit them from the bounded feeding history.
    meals = tuple(
        meal
        for meal in all_meals
        if any(record.status != "none_bowl" for record in grouped[meal.give_time])
    )
    today_meals = [meal for meal in meals if meal.give_time.date() == today]
    yesterday_meals = [meal for meal in meals if meal.give_time.date() == yesterday]

    latest_event = max(
        unique_records,
        key=lambda record: (
            record.eat_time,
            _STATUS_ORDER.get(record.status, 4),
            record.left,
            record.gram,
            record.immediate,
        ),
        default=None,
    )
    measured_records = [
        record for record in unique_records if record.status in {"feed_finish", "waiting"}
    ]
    latest_measurement = max(
        measured_records,
        key=lambda record: (
            record.eat_time,
            _STATUS_ORDER.get(record.status, 4),
            record.left,
            record.gram,
            record.immediate,
        ),
        default=None,
    )
    bowl_removed = bool(
        latest_event is not None
        and latest_event.status == "none_bowl"
        and (latest_measurement is None or latest_event.eat_time >= latest_measurement.eat_time)
    )
    feed_meals = [meal for meal in meals if meal.has_dispense_event]
    intake_meals = [meal for meal in meals if meal.has_intake_event]

    return PetMetrics(
        today_app_feed_amount_grams=sum(meal.app_feed_amount_grams for meal in today_meals),
        today_record_feed_amount_grams=round(
            sum(meal.record_feed_amount_grams for meal in today_meals), 6
        ),
        today_observed_intake_grams=round(
            sum(meal.observed_intake_grams for meal in today_meals), 6
        ),
        today_app_intake_grams=sum(meal.app_intake_grams for meal in today_meals),
        today_record_day_intake_grams=_record_day_intake(unique_records, today),
        today_feeding_count=sum(meal.has_dispense_event for meal in today_meals),
        yesterday_observed_intake_grams=round(
            sum(meal.observed_intake_grams for meal in yesterday_meals), 6
        ),
        yesterday_app_intake_grams=sum(meal.app_intake_grams for meal in yesterday_meals),
        latest_feeding_at=max((meal.give_time for meal in feed_meals), default=None),
        latest_intake_at=max(
            (meal.intake_at for meal in intake_meals if meal.intake_at is not None),
            default=None,
        ),
        latest_record_remaining_grams=(
            latest_measurement.left
            if latest_measurement is not None and not bowl_removed
            else None
        ),
        latest_record_reported_at=(
            latest_measurement.eat_time
            if latest_measurement is not None and not bowl_removed
            else None
        ),
        latest_event_status=latest_event.status if latest_event is not None else None,
        latest_event_at=latest_event.eat_time if latest_event is not None else None,
        meals=tuple(sorted(meals, key=lambda meal: meal.give_time, reverse=True)[:MAX_HISTORY_MEALS]),
    )


def meal_history_attributes(meals: Sequence[MealSummary]) -> list[dict[str, Any]]:
    """Return bounded, grouped meal summaries suitable for entity attributes."""
    return [
        {
            "give_time": meal.give_time.isoformat(sep=" "),
            "record_feed_amount_grams": meal.record_feed_amount_grams,
            "app_feed_amount_grams": meal.app_feed_amount_grams,
            "observed_intake_grams": meal.observed_intake_grams,
            "app_intake_grams": meal.app_intake_grams,
            "gram": meal.latest_record_gram,
            "left": meal.latest_record_left,
            "last_reported_at": meal.last_reported_at.isoformat(sep=" "),
        }
        for meal in meals[:MAX_HISTORY_MEALS]
    ]


def _summarize_meal(give_time: datetime, rows: Sequence[FeedRecord]) -> MealSummary:
    ordered = sorted(
        rows,
        key=lambda record: (
            record.eat_time,
            _STATUS_ORDER.get(record.status, 4),
            record.left,
            record.gram,
            record.immediate,
            record.eat_count,
            record.spend,
        ),
    )

    record_feed_amount = 0.0
    app_feed_amount = 0
    has_dispense_event = False
    previous_immediate: int | None = None
    for index, record in enumerate(ordered):
        gram = _ceil(record.gram)
        immediate = _ceil(record.immediate)
        if record.status == "feed_finish":
            has_dispense_event = True
            if index == 0:
                record_feed_amount = max(0.0, record.gram - record.immediate)
                app_feed_amount = max(0, gram - immediate)
            elif previous_immediate != immediate:
                record_feed_amount = max(0.0, record.gram - record.immediate)
                app_feed_amount = max(0, gram - immediate)
        elif record.status == "waiting":
            record_feed_amount = max(0.0, record.gram - record.immediate)
            app_feed_amount = max(0, gram - immediate)
        previous_immediate = immediate

    waiting_rows = [record for record in ordered if record.status == "waiting"]
    latest_waiting = max(
        waiting_rows,
        key=lambda record: (
            record.eat_time,
            record.left,
            record.gram,
            record.immediate,
            record.eat_count,
        ),
        default=None,
    )

    observed_intake = 0.0
    app_intake = 0
    has_intake_event = False
    if latest_waiting is not None:
        observed_intake = max(0.0, latest_waiting.gram - latest_waiting.left)
        app_intake = max(0, _ceil(latest_waiting.gram) - _ceil(latest_waiting.left))
        # Match the vendor app's documented zero-gram correction for its low
        # eatCount readings. The history still records whole grams only.
        if app_intake == 0 and latest_waiting.eat_count < 70000:
            app_intake = 1
        has_intake_event = observed_intake > 0

    latest_record = max(
        ordered,
        key=lambda record: (
            record.eat_time,
            _STATUS_ORDER.get(record.status, 4),
            record.left,
            record.gram,
            record.immediate,
        ),
    )
    return MealSummary(
        give_time=give_time,
        record_feed_amount_grams=record_feed_amount,
        app_feed_amount_grams=app_feed_amount,
        observed_intake_grams=observed_intake,
        app_intake_grams=app_intake,
        latest_record_gram=latest_record.gram,
        latest_record_left=latest_record.left,
        last_reported_at=latest_record.eat_time,
        has_dispense_event=has_dispense_event,
        has_intake_event=has_intake_event,
        intake_at=(latest_waiting.eat_time if has_intake_event else None),
    )


def _record_day_intake(records: Sequence[FeedRecord], record_day: date) -> float:
    """Sum positive cumulative intake changes assigned to each eat_time day."""
    grouped: dict[datetime, list[FeedRecord]] = defaultdict(list)
    for record in records:
        if record.status == "waiting":
            grouped[record.give_time].append(record)

    total = 0.0
    for rows in grouped.values():
        ordered = sorted(
            rows,
            key=lambda record: (
                record.eat_time,
                record.left,
                record.gram,
                record.immediate,
                record.eat_count,
            ),
        )
        previous = 0.0
        for record in ordered:
            current = max(0.0, record.gram - record.left)
            delta = max(0.0, current - previous)
            if record.eat_time.date() == record_day:
                total += delta
            previous = max(previous, current)
    return round(total, 6)


def _finite_number(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("Expected a number")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("Expected a finite number")
    return result


def _ceil(value: float) -> int:
    return math.ceil(value)
