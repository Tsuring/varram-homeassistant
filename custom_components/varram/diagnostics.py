"""Non-sensitive VARRAM integration diagnostics."""

from datetime import datetime, timezone
from typing import Any

from homeassistant.core import HomeAssistant

from .const import (
    CONF_ACCESS_TOKEN,
    CONF_EMAIL,
    CONF_PASSWORD,
    CONF_SCAN_INTERVAL,
    DEFAULT_SCAN_INTERVAL,
    DOMAIN,
)
from .auth import InvalidToken, decode_token_claims


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant, entry: Any
) -> dict[str, Any]:
    """Return token expiry and configuration health without secrets."""
    token = entry.data.get(CONF_ACCESS_TOKEN, "")
    try:
        expires_at = datetime.fromtimestamp(
            decode_token_claims(token).expires_at, tz=timezone.utc
        ).isoformat()
    except (InvalidToken, OverflowError, OSError, ValueError):
        expires_at = None
    return {
        "domain": DOMAIN,
        "access_token_expires_at": expires_at,
        "automatic_relogin_enabled": bool(
            entry.data.get(CONF_EMAIL) and entry.data.get(CONF_PASSWORD)
        ),
        "poll_interval_seconds": int(
            entry.options.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL)
        ),
    }
