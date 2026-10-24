"""One-shot VARRAM additional-feeding buttons."""

from __future__ import annotations

import asyncio
from typing import Any

from homeassistant.components.button import ButtonEntity, ButtonEntityDescription
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import VarramConfigEntry
from .const import DOMAIN
from .coordinator import VarramCoordinator
from .socket import VarramFeedingError


ADDITIONAL_FEED_BUTTON = ButtonEntityDescription(
    key="additional_feed",
    translation_key="additional_feed",
    icon="mdi:food",
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: VarramConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Create one additional-feed button for each connected feeder."""
    coordinator = entry.runtime_data
    async_add_entities(
        VarramAdditionalFeedButton(
            coordinator,
            connection,
            coordinator.pets[connection["pet_id"]]["name"],
        )
        for connection in coordinator.feeder_connections
    )


class VarramAdditionalFeedButton(ButtonEntity):
    """Request the vendor app's single additional-feed operation once."""

    entity_description = ADDITIONAL_FEED_BUTTON
    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: VarramCoordinator,
        connection: dict[str, str],
        pet_name: str,
    ) -> None:
        self.coordinator = coordinator
        self.connection = dict(connection)
        self.device_id = connection["device_id"]
        self.entity_description = ADDITIONAL_FEED_BUTTON
        self._attr_unique_id = (
            f"{connection['pet_id']}_{connection['device_id']}_additional_feed"
        )
        self._attr_device_info = {
            "identifiers": {(DOMAIN, connection["pet_id"])},
            "name": pet_name,
            "manufacturer": "VARRAM",
            "model": "Feeder",
        }

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Show the vendor-calculated minimum without treating it as dose size."""
        return {
            "minimum_feed_amount": self.coordinator.minimum_feed_amounts.get(
                self.device_id
            ),
            "minimum_feed_amount_unit": "g",
        }

    async def async_press(self) -> None:
        """Refresh calibration, then make exactly one guarded request."""
        try:
            await self.coordinator.async_request_additional_feed(self.connection)
        except VarramFeedingError as err:
            raise HomeAssistantError(
                translation_domain=DOMAIN,
                translation_key=err.translation_key,
            ) from None
        finally:
            task = asyncio.current_task()
            if task is None or not task.cancelling():
                self.async_write_ha_state()
