"""Read-only VARRAM feeder sensors."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.const import UnitOfMass
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.typing import StateType
from homeassistant.helpers.update_coordinator import CoordinatorEntity
from homeassistant.util import dt as dt_util

from . import VarramConfigEntry
from .const import DOMAIN
from .coordinator import VarramCoordinator, VarramSocketCoordinator
from .models import PetMetrics, meal_history_attributes
from .time_utils import to_display_timezone


@dataclass(frozen=True, kw_only=True)
class VarramSensorDescription(SensorEntityDescription):
    """Description of a supported VARRAM metric."""

    value_key: str


GRAMS = UnitOfMass.GRAMS
MEASUREMENT = SensorStateClass.MEASUREMENT

SENSOR_DESCRIPTIONS: tuple[VarramSensorDescription, ...] = (
    VarramSensorDescription(
        key="today_app_feed_amount",
        translation_key="today_app_feed_amount",
        value_key="today_app_feed_amount_grams",
        device_class=SensorDeviceClass.WEIGHT,
        native_unit_of_measurement=GRAMS,
        state_class=MEASUREMENT,
        suggested_display_precision=0,
    ),
    VarramSensorDescription(
        key="today_record_feed_amount",
        translation_key="today_record_feed_amount",
        value_key="today_record_feed_amount_grams",
        device_class=SensorDeviceClass.WEIGHT,
        native_unit_of_measurement=GRAMS,
        state_class=MEASUREMENT,
        suggested_display_precision=2,
    ),
    VarramSensorDescription(
        key="today_observed_intake",
        translation_key="today_observed_intake",
        value_key="today_observed_intake_grams",
        device_class=SensorDeviceClass.WEIGHT,
        native_unit_of_measurement=GRAMS,
        state_class=MEASUREMENT,
        suggested_display_precision=2,
    ),
    VarramSensorDescription(
        key="today_app_intake",
        translation_key="today_app_intake",
        value_key="today_app_intake_grams",
        device_class=SensorDeviceClass.WEIGHT,
        native_unit_of_measurement=GRAMS,
        state_class=MEASUREMENT,
        suggested_display_precision=0,
    ),
    VarramSensorDescription(
        key="today_record_day_intake",
        translation_key="today_record_day_intake",
        value_key="today_record_day_intake_grams",
        device_class=SensorDeviceClass.WEIGHT,
        native_unit_of_measurement=GRAMS,
        state_class=MEASUREMENT,
        suggested_display_precision=2,
    ),
    VarramSensorDescription(
        key="today_feeding_count",
        translation_key="today_feeding_count",
        value_key="today_feeding_count",
        state_class=MEASUREMENT,
    ),
    VarramSensorDescription(
        key="yesterday_observed_intake",
        translation_key="yesterday_observed_intake",
        value_key="yesterday_observed_intake_grams",
        device_class=SensorDeviceClass.WEIGHT,
        native_unit_of_measurement=GRAMS,
        state_class=MEASUREMENT,
        suggested_display_precision=2,
    ),
    VarramSensorDescription(
        key="yesterday_app_intake",
        translation_key="yesterday_app_intake",
        value_key="yesterday_app_intake_grams",
        device_class=SensorDeviceClass.WEIGHT,
        native_unit_of_measurement=GRAMS,
        state_class=MEASUREMENT,
        suggested_display_precision=0,
    ),
    VarramSensorDescription(
        key="latest_feeding",
        translation_key="latest_feeding",
        value_key="latest_feeding_at",
        device_class=SensorDeviceClass.TIMESTAMP,
    ),
    VarramSensorDescription(
        key="latest_intake",
        translation_key="latest_intake",
        value_key="latest_intake_at",
        device_class=SensorDeviceClass.TIMESTAMP,
    ),
    VarramSensorDescription(
        key="latest_record_remaining",
        translation_key="latest_record_remaining",
        value_key="latest_record_remaining_grams",
        device_class=SensorDeviceClass.WEIGHT,
        native_unit_of_measurement=GRAMS,
        state_class=MEASUREMENT,
        suggested_display_precision=2,
    ),
    VarramSensorDescription(
        key="feed_history",
        translation_key="feed_history",
        value_key="history_count",
    ),
)

SOCKET_SENSOR_DESCRIPTIONS: tuple[VarramSensorDescription, ...] = (
    VarramSensorDescription(
        key="current_remaining_g",
        translation_key="current_remaining_g",
        value_key="remaining_grams",
        device_class=SensorDeviceClass.WEIGHT,
        native_unit_of_measurement=GRAMS,
        state_class=MEASUREMENT,
        suggested_display_precision=2,
    ),
    VarramSensorDescription(
        key="current_feeder_status",
        translation_key="current_feeder_status",
        value_key="status",
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: VarramConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up sensors for feeder pets."""
    coordinator = entry.runtime_data
    entities: list[SensorEntity] = []
    for pet_id, pet in coordinator.pets.items():
        if pet_id not in coordinator.feeder_pet_ids:
            continue
        entities.extend(
            VarramSensor(coordinator, pet_id, pet["name"], description)
            for description in SENSOR_DESCRIPTIONS
        )
        entities.extend(
            VarramSocketSensor(
                coordinator.socket_coordinator, hass, pet_id, pet["name"], description
            )
            for description in SOCKET_SENSOR_DESCRIPTIONS
        )
    async_add_entities(entities)


class VarramSensor(CoordinatorEntity[VarramCoordinator], SensorEntity):
    """A single value derived from the coordinator's read-only feed records."""

    entity_description: VarramSensorDescription
    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: VarramCoordinator,
        pet_id: str,
        pet_name: str,
        description: VarramSensorDescription,
    ) -> None:
        super().__init__(coordinator)
        self.coordinator = coordinator
        self.pet_id = pet_id
        self.pet_name = pet_name
        self.entity_description = description
        self._attr_unique_id = f"{pet_id}_{description.key}"
        self._attr_device_info = {
            "identifiers": {(DOMAIN, pet_id)},
            "name": pet_name,
            "manufacturer": "VARRAM",
            "model": "Feeder",
        }

    @property
    def available(self) -> bool:
        """Keep entities unavailable when a poll fails."""
        if not super().available or self.pet_id not in self.coordinator.data:
            return False
        metrics = self.coordinator.data[self.pet_id]
        if self.entity_description.key == "latest_feeding":
            return metrics.latest_feeding_at is not None
        if self.entity_description.key == "latest_intake":
            return metrics.latest_intake_at is not None
        if self.entity_description.key == "latest_record_remaining":
            return metrics.latest_record_remaining_grams is not None
        return True

    @property
    def native_value(self) -> StateType | datetime:
        """Return the current metric, with missing history as unavailable."""
        metrics = self._metrics
        if metrics is None:
            return None
        key = self.entity_description.value_key
        if key == "history_count":
            return len(metrics.meals)
        value = getattr(metrics, key)
        if isinstance(value, datetime):
            return _as_home_assistant_datetime(self.coordinator.hass, value)
        return value

    @property
    def extra_state_attributes(self) -> dict[str, Any] | None:
        """Expose only a small history or its source-record timestamp."""
        metrics = self._metrics
        if metrics is None:
            return None
        if self.entity_description.key == "feed_history":
            return {"meals": meal_history_attributes(metrics.meals)}
        if self.entity_description.key == "latest_record_remaining":
            reported_at = metrics.latest_record_reported_at
            return {
                "last_reported_at": (
                    _as_home_assistant_datetime(self.coordinator.hass, reported_at)
                    if reported_at is not None
                    else None
                ),
                "latest_event_status": metrics.latest_event_status,
                "latest_event_at": (
                    _as_home_assistant_datetime(self.coordinator.hass, metrics.latest_event_at)
                    if metrics.latest_event_at is not None
                    else None
                ),
            }
        return None

    @property
    def _metrics(self) -> PetMetrics | None:
        return self.coordinator.data.get(self.pet_id)


def _as_home_assistant_datetime(hass: HomeAssistant, value: datetime) -> datetime:
    """Interpret VARRAM's timezone-naive local timestamps in HA's timezone."""
    time_zone = dt_util.get_time_zone(hass.config.time_zone)
    return to_display_timezone(value, time_zone)


class VarramSocketSensor(CoordinatorEntity[VarramSocketCoordinator], SensorEntity):
    """A live status value with availability independent from feed history."""

    entity_description: VarramSensorDescription
    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: VarramSocketCoordinator,
        hass: HomeAssistant,
        pet_id: str,
        pet_name: str,
        description: VarramSensorDescription,
    ) -> None:
        super().__init__(coordinator)
        self.coordinator = coordinator
        self.hass = hass
        self.pet_id = pet_id
        self.entity_description = description
        self._attr_unique_id = f"{pet_id}_{description.key}"
        self._attr_device_info = {
            "identifiers": {(DOMAIN, pet_id)},
            "name": pet_name,
            "manufacturer": "VARRAM",
            "model": "Feeder",
        }

    @property
    def available(self) -> bool:
        """Expose socket data only while that feeder's status is fresh."""
        status = self.coordinator.data.get(self.pet_id)
        if not super().available or not status or not status.get("available"):
            return False
        if self.entity_description.key == "current_remaining_g":
            return status.get("status") in {"waiting", "feed_finish"} and status.get("remaining_grams") is not None
        return True

    @property
    def native_value(self) -> StateType:
        """Return the latest socket status or its waiting-state bowl weight."""
        status = self.coordinator.data.get(self.pet_id, {})
        return status.get(self.entity_description.value_key)

    @property
    def extra_state_attributes(self) -> dict[str, Any] | None:
        """Include observation time and non-sensitive feeder flags."""
        status = self.coordinator.data.get(self.pet_id)
        if not status:
            return None
        observed_at = status.get("observed_at")
        return {
            "last_received_at": (
                _as_home_assistant_datetime(self.hass, observed_at)
                if isinstance(observed_at, datetime)
                else None
            ),
            "power_type": status.get("power_type"),
            "feed_lack": status.get("feed_lack"),
        }
