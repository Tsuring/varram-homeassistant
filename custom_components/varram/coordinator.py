"""Poll connected VARRAM feeders and coordinate guarded button requests."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api import (
    VarramApiClient,
    VarramAuthError,
    VarramConnectionError,
    VarramInvalidResponse,
)
from .const import CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL, DOMAIN
from .models import PetMetrics, aggregate_feed_records
from .socket import VarramFeedingError
from .time_utils import feed_history_window

_LOGGER = logging.getLogger(__name__)


class VarramCoordinator(DataUpdateCoordinator[dict[str, PetMetrics]]):
    """Fetch feeder records and reduce cumulative snapshots into metrics."""

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        client: VarramApiClient,
    ) -> None:
        super().__init__(
            hass,
            _LOGGER,
            config_entry=entry,
            name=DOMAIN,
            update_interval=timedelta(
                seconds=int(entry.options.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL))
            ),
        )
        self.client = client
        self.entry = entry
        self.pets: dict[str, dict[str, str]] = {}
        self.feeder_pet_ids: tuple[str, ...] = ()
        self.feeder_connections: tuple[dict[str, str], ...] = ()
        self.minimum_feed_amounts: dict[str, float | None] = {}
        self.socket_coordinator = VarramSocketCoordinator(hass, entry)
        self.socket_monitor: Any | None = None
        self._socket_fingerprints: dict[str, tuple[str, float | None]] = {}
        self._socket_refresh_handle: asyncio.TimerHandle | None = None
        self._feeding_request_lock = asyncio.Lock()
        self._feeding_request_tasks: set[asyncio.Task[Any]] = set()

    async def _async_setup(self) -> None:
        """Load the account's minimal pet list and feeder associations."""
        try:
            pet_rows = await self.client.async_get_pets()
            connected = await self.client.async_get_connected_feeders()
        except VarramAuthError as err:
            raise ConfigEntryAuthFailed(
                translation_domain=DOMAIN, translation_key="invalid_auth"
            ) from err
        except VarramConnectionError as err:
            raise ConfigEntryNotReady(
                translation_domain=DOMAIN, translation_key="cannot_connect"
            ) from err
        except VarramInvalidResponse as err:
            raise ConfigEntryNotReady(
                translation_domain=DOMAIN, translation_key="invalid_response"
            ) from err

        self.pets = {pet["pet_id"]: pet for pet in pet_rows}
        self.feeder_connections = tuple(
            {"pet_id": item["pet_id"], "device_id": item["device_id"]}
            for item in connected
            if item["pet_id"] in self.pets
        )
        self.feeder_pet_ids = tuple(dict.fromkeys(
            item["pet_id"] for item in self.feeder_connections
        ))
        for connection in self.feeder_connections:
            self.socket_coordinator.async_set_unavailable(connection)
            try:
                self.minimum_feed_amounts[connection["device_id"]] = (
                    await self.client.async_get_feeder_optimization(
                        connection["device_id"]
                    )
                )
            except VarramAuthError as err:
                raise ConfigEntryAuthFailed(
                    translation_domain=DOMAIN, translation_key="invalid_auth"
                ) from err
            except (VarramConnectionError, VarramInvalidResponse):
                self.minimum_feed_amounts[connection["device_id"]] = None
                _LOGGER.debug("VARRAM feeder calibration metadata is unavailable")

    async def _async_update_data(self) -> dict[str, PetMetrics]:
        """Fetch the recent week, leaving unavailable data unavailable on errors."""
        today, start_time, end_time = feed_history_window(datetime.now(timezone.utc))

        data: dict[str, PetMetrics] = {}
        try:
            for pet_id in self.feeder_pet_ids:
                records = await self.client.async_get_feed_records(
                    pet_id, start_time, end_time
                )
                data[pet_id] = aggregate_feed_records(records, today=today)
        except VarramAuthError as err:
            raise ConfigEntryAuthFailed(
                translation_domain=DOMAIN, translation_key="invalid_auth"
            ) from err
        except VarramConnectionError as err:
            raise UpdateFailed("Could not reach VARRAM feed history") from err
        except VarramInvalidResponse as err:
            raise UpdateFailed("VARRAM returned invalid feed history") from err
        return data

    def async_handle_socket_status(self, status: dict[str, Any]) -> None:
        """Update live-only state and debounce an HTTP history refresh on changes."""
        self.socket_coordinator.async_set_status(status)
        pet_id = status["pet_id"]
        fingerprint = (status["status"], status["remaining_grams"])
        previous = self._socket_fingerprints.get(pet_id)
        self._socket_fingerprints[pet_id] = fingerprint
        if previous is not None and previous != fingerprint:
            self._schedule_socket_history_refresh()

    def async_handle_socket_unavailable(self, connection: dict[str, str]) -> None:
        """Mark one socket device unavailable without affecting HTTP sensors."""
        self.socket_coordinator.async_set_unavailable(connection)

    async def async_request_additional_feed(
        self, connection: dict[str, str]
    ) -> None:
        """Validate calibration and send one guarded feed request."""
        if self._feeding_request_lock.locked():
            raise VarramFeedingError("feeding_busy")
        await self._feeding_request_lock.acquire()
        task = asyncio.current_task()
        if task is not None:
            self._feeding_request_tasks.add(task)
        try:
            if self.socket_monitor is None:
                raise VarramFeedingError("feeding_not_ready")
            try:
                minimum_feed_amount = await self.client.async_get_feeder_optimization(
                    connection["device_id"]
                )
            except VarramAuthError:
                raise VarramFeedingError("feeding_auth") from None
            except (VarramConnectionError, VarramInvalidResponse):
                self.minimum_feed_amounts[connection["device_id"]] = None
                raise VarramFeedingError(
                    "feeding_calibration_unavailable"
                ) from None

            self.minimum_feed_amounts[connection["device_id"]] = minimum_feed_amount
            if minimum_feed_amount is None:
                raise VarramFeedingError("feeding_calibration_missing")

            try:
                await self.socket_monitor.async_request_additional_feed(
                    connection,
                    minimum_feed_amount=minimum_feed_amount,
                )
            except VarramFeedingError as err:
                if err.send_attempted:
                    self._schedule_socket_history_refresh()
                raise
            self._schedule_socket_history_refresh()
        finally:
            if task is not None:
                self._feeding_request_tasks.discard(task)
            self._feeding_request_lock.release()

    def _schedule_socket_history_refresh(self) -> None:
        if self._socket_refresh_handle is not None:
            self._socket_refresh_handle.cancel()
        loop = asyncio.get_running_loop()
        self._socket_refresh_handle = loop.call_later(
            10, self._async_socket_history_refresh
        )

    def _async_socket_history_refresh(self) -> None:
        self._socket_refresh_handle = None
        if self.hass.is_running:
            self.hass.async_create_task(self.async_request_refresh())

    async def async_shutdown(self) -> None:
        """Cancel debounce work and all active status socket operations."""
        current = asyncio.current_task()
        feeding_tasks = [
            task for task in self._feeding_request_tasks if task is not current
        ]
        for task in feeding_tasks:
            task.cancel()
        if feeding_tasks:
            await asyncio.gather(*feeding_tasks, return_exceptions=True)
        self._feeding_request_tasks.clear()
        if self._socket_refresh_handle is not None:
            self._socket_refresh_handle.cancel()
            self._socket_refresh_handle = None
        if self.socket_monitor is not None:
            await self.socket_monitor.async_stop()


class VarramSocketCoordinator(DataUpdateCoordinator[dict[str, dict[str, Any]]]):
    """Live socket data with availability independent from HTTP history polls."""

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        super().__init__(
            hass,
            _LOGGER,
            config_entry=entry,
            name=f"{DOMAIN}_socket",
            update_interval=None,
        )
        self.async_set_updated_data({})

    def async_set_status(self, status: dict[str, Any]) -> None:
        updated = dict(self.data or {})
        updated[status["pet_id"]] = {**status, "available": True}
        self.async_set_updated_data(updated)

    def async_set_unavailable(self, connection: dict[str, str]) -> None:
        updated = dict(self.data or {})
        previous = updated.get(connection["pet_id"], {})
        updated[connection["pet_id"]] = {
            **previous,
            **connection,
            "available": False,
        }
        self.async_set_updated_data(updated)
