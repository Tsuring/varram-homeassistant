"""VARRAM read-only pet feeder integration."""

from __future__ import annotations

from typing import TypeAlias

import aiohttp

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady
from homeassistant.helpers.aiohttp_client import async_create_clientsession

from .api import VarramApiClient, VarramAuthError, VarramConnectionError, VarramInvalidResponse
from .const import (
    CONF_ACCESS_TOKEN,
    CONF_EMAIL,
    CONF_PASSWORD,
    CONF_SCAN_INTERVAL,
    CONF_USER_ID,
    DEFAULT_SCAN_INTERVAL,
    DOMAIN,
    MAX_FIELD_SIZE,
    PLATFORMS,
)
from .coordinator import VarramCoordinator
from .socket import VarramSocketMonitor

VarramConfigEntry: TypeAlias = ConfigEntry[VarramCoordinator]


async def async_setup_entry(hass: HomeAssistant, entry: VarramConfigEntry) -> bool:
    """Set up the VARRAM account and feeder sensors."""

    coordinator: VarramCoordinator | None = None

    def persist_refreshed_token(token: str) -> None:
        updated = dict(entry.data)
        updated[CONF_ACCESS_TOKEN] = token
        hass.config_entries.async_update_entry(entry, data=updated)
        if coordinator is not None and coordinator.socket_monitor is not None:
            coordinator.socket_monitor.async_refresh_auth()

    session = async_create_clientsession(
        hass,
        cookie_jar=aiohttp.DummyCookieJar(),
        connector_owner=False,
        max_field_size=MAX_FIELD_SIZE,
    )
    try:
        client = VarramApiClient(
            session,
            entry.data[CONF_ACCESS_TOKEN],
            email=entry.data.get(CONF_EMAIL),
            password=entry.data.get(CONF_PASSWORD),
            expected_user_id=entry.data[CONF_USER_ID],
            on_token_update=persist_refreshed_token,
        )
        coordinator = VarramCoordinator(hass, entry, client)
        entry.runtime_data = coordinator
        await coordinator.async_config_entry_first_refresh()
        coordinator.socket_monitor = VarramSocketMonitor(
            session,
            get_auth=client.async_get_socket_auth,
            on_status=coordinator.async_handle_socket_status,
            on_unavailable=coordinator.async_handle_socket_unavailable,
            on_auth_error=client.async_refresh_token,
        )
        await coordinator.socket_monitor.async_start(coordinator.feeder_connections)
    except VarramAuthError as err:
        raise ConfigEntryAuthFailed(
            translation_domain=DOMAIN, translation_key="invalid_auth"
        ) from err
    except (VarramConnectionError, VarramInvalidResponse) as err:
        raise ConfigEntryNotReady(
            translation_domain=DOMAIN, translation_key="cannot_connect"
        ) from err

    entry.async_on_unload(entry.add_update_listener(_async_update_listener))
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: VarramConfigEntry) -> bool:
    """Unload VARRAM platforms."""
    if not await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        return False
    await entry.runtime_data.async_shutdown()
    return True


async def _async_update_listener(hass: HomeAssistant, entry: VarramConfigEntry) -> None:
    """Reload when the user changes the polling interval."""
    desired_seconds = int(entry.options.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL))
    coordinator = entry.runtime_data
    if coordinator.update_interval.total_seconds() != desired_seconds:
        await hass.config_entries.async_reload(entry.entry_id)
