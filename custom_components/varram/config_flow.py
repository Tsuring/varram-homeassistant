"""Config, reauth, reconfigure, and options flows for VARRAM."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import aiohttp
import voluptuous as vol

from homeassistant.config_entries import ConfigFlow, ConfigFlowResult, OptionsFlow
from homeassistant.const import CONF_EMAIL, CONF_PASSWORD
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_create_clientsession
from homeassistant.helpers.selector import TextSelector, TextSelectorConfig, TextSelectorType

from .api import (
    VarramApiClient,
    VarramAuthError,
    VarramConnectionError,
    VarramInvalidResponse,
)
from .auth import InvalidToken, decode_token_claims
from .const import (
    CONF_ACCESS_TOKEN,
    CONF_SCAN_INTERVAL,
    CONF_USER_ID,
    DEFAULT_SCAN_INTERVAL,
    DOMAIN,
    MAX_FIELD_SIZE,
    MAX_SCAN_INTERVAL,
    MIN_SCAN_INTERVAL,
)


class VarramConfigFlow(ConfigFlow, domain=DOMAIN):
    """Configure and recover one VARRAM account."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Set up an account using its native email/password login."""
        errors: dict[str, str] = {}
        if user_input is not None:
            token: str | None = None
            email, password, pair_error = _get_credential_pair(user_input)
            if pair_error:
                errors["base"] = pair_error
            elif not (email and password):
                errors["base"] = "credentials_required"
            else:
                async with _temporary_session(self.hass) as session:
                    try:
                        seed_token = str(user_input.get(CONF_ACCESS_TOKEN, "")).strip() or None
                        client = VarramApiClient(session, seed_token)
                        expected_user_id = client.user_id if seed_token else None
                        token = await client.async_validate_login(
                            email, password, expected_user_id
                        )
                    except VarramAuthError:
                        errors["base"] = "invalid_auth"
                    except VarramConnectionError:
                        errors["base"] = "cannot_connect"
                    except VarramInvalidResponse:
                        errors["base"] = "invalid_response"
                    except (InvalidToken, KeyError, TypeError):
                        errors["base"] = "invalid_auth"

            if not errors:
                try:
                    assert token is not None
                    claims = decode_token_claims(token)
                except InvalidToken:
                    errors["base"] = "invalid_auth"
                else:
                    await self.async_set_unique_id(claims.user_id)
                    self._abort_if_unique_id_configured()
                    data: dict[str, Any] = {
                        CONF_ACCESS_TOKEN: token,
                        CONF_USER_ID: claims.user_id,
                    }
                    data[CONF_EMAIL] = email
                    data[CONF_PASSWORD] = password
                    return self.async_create_entry(title="VARRAM", data=data)

        return self.async_show_form(
            step_id="user", data_schema=_user_schema(), errors=errors
        )

    async def async_step_reauth(self, entry_data: Mapping[str, Any]) -> ConfigFlowResult:
        """Start reauthentication when an app token has stopped working."""
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Validate a new token or log in with the stored account identity."""
        entry = self._get_reauth_entry()
        errors: dict[str, str] = {}
        if user_input is not None:
            email, password, pair_error = _get_credential_pair(user_input)
            candidate_token = str(user_input.get(CONF_ACCESS_TOKEN, "")).strip()
            if pair_error:
                errors["base"] = pair_error
            elif not candidate_token and not (email and password):
                errors["base"] = "credentials_required"
            else:
                try:
                    token = await _validate_entry_auth(
                        self.hass,
                        entry.data,
                        candidate_token=candidate_token or None,
                        email=email,
                        password=password,
                    )
                except VarramAuthError:
                    errors["base"] = "invalid_auth"
                except VarramConnectionError:
                    errors["base"] = "cannot_connect"
                except VarramInvalidResponse:
                    errors["base"] = "invalid_response"
                else:
                    claims = decode_token_claims(token)
                    await self.async_set_unique_id(claims.user_id)
                    self._abort_if_unique_id_mismatch()
                    updates: dict[str, Any] = {
                        CONF_ACCESS_TOKEN: token,
                        CONF_USER_ID: claims.user_id,
                    }
                    if email and password:
                        updates[CONF_EMAIL] = email
                        updates[CONF_PASSWORD] = password
                    return self.async_update_reload_and_abort(
                        entry, data_updates=updates
                    )

        return self.async_show_form(
            step_id="reauth_confirm",
            data_schema=_auth_schema(include_token=True),
            errors=errors,
        )

    async def async_step_reconfigure(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Change credentials or replace the token without removing the entry."""
        entry = self._get_reconfigure_entry()
        errors: dict[str, str] = {}
        if user_input is not None:
            email, password, pair_error = _get_credential_pair(user_input)
            candidate_token = str(user_input.get(CONF_ACCESS_TOKEN, "")).strip()
            if pair_error:
                errors["base"] = pair_error
            elif not candidate_token and not (email and password):
                errors["base"] = "credentials_required"
            else:
                try:
                    token = await _validate_entry_auth(
                        self.hass,
                        entry.data,
                        candidate_token=candidate_token or None,
                        email=email,
                        password=password,
                    )
                except VarramAuthError:
                    errors["base"] = "invalid_auth"
                except VarramConnectionError:
                    errors["base"] = "cannot_connect"
                except VarramInvalidResponse:
                    errors["base"] = "invalid_response"
                else:
                    claims = decode_token_claims(token)
                    await self.async_set_unique_id(claims.user_id)
                    self._abort_if_unique_id_mismatch()
                    updates: dict[str, Any] = {
                        CONF_ACCESS_TOKEN: token,
                        CONF_USER_ID: claims.user_id,
                    }
                    if email and password:
                        updates[CONF_EMAIL] = email
                        updates[CONF_PASSWORD] = password
                    return self.async_update_reload_and_abort(
                        entry, data_updates=updates
                    )

        return self.async_show_form(
            step_id="reconfigure",
            data_schema=_auth_schema(include_token=True),
            errors=errors,
        )

    @staticmethod
    def async_get_options_flow(config_entry: Any) -> OptionsFlow:
        """Return the polling interval options form."""
        return VarramOptionsFlow()


class VarramOptionsFlow(OptionsFlow):
    """User settings that change integration polling behavior."""

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        if user_input is not None:
            return self.async_create_entry(title="", data=user_input)
        schema = vol.Schema(
            {
                vol.Required(
                    CONF_SCAN_INTERVAL,
                    default=int(
                        self.config_entry.options.get(
                            CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL
                        )
                    ),
                ): vol.All(
                    vol.Coerce(int),
                    vol.Range(min=MIN_SCAN_INTERVAL, max=MAX_SCAN_INTERVAL),
                )
            }
        )
        return self.async_show_form(step_id="init", data_schema=schema)


async def _validate_entry_auth(
    hass: HomeAssistant,
    entry_data: Mapping[str, Any],
    *,
    candidate_token: str | None,
    email: str | None,
    password: str | None,
) -> str:
    """Validate credentials and persistable token before any config mutation."""
    expected_user_id = entry_data[CONF_USER_ID]
    old_token = entry_data[CONF_ACCESS_TOKEN]
    initial_token = candidate_token or old_token
    try:
        initial_claims = decode_token_claims(initial_token)
    except InvalidToken as err:
        raise VarramAuthError("Configured app token is invalid") from err
    if initial_claims.user_id != expected_user_id:
        raise VarramAuthError("Token belongs to a different VARRAM account")

    async with _temporary_session(hass) as session:
        client = VarramApiClient(
            session,
            initial_token,
            expected_user_id=expected_user_id,
        )
        if email and password:
            return await client.async_validate_login(email, password, expected_user_id)
        await client.async_get_pets()
        return initial_token


def _get_credential_pair(
    user_input: Mapping[str, Any],
) -> tuple[str | None, str | None, str | None]:
    """Return normalized optional email/password or a form error key."""
    email_value = user_input.get(CONF_EMAIL)
    password_value = user_input.get(CONF_PASSWORD)
    email = email_value.strip() if isinstance(email_value, str) else ""
    password = password_value if isinstance(password_value, str) else ""
    if bool(email) != bool(password):
        return None, None, "credentials_pair_required"
    return email or None, password or None, None


def _user_schema() -> vol.Schema:
    """Require the native email/password login for the initial setup."""
    return vol.Schema(
        {
            vol.Required(CONF_EMAIL): TextSelector(
                TextSelectorConfig(type=TextSelectorType.EMAIL)
            ),
            vol.Required(CONF_PASSWORD): TextSelector(
                TextSelectorConfig(type=TextSelectorType.PASSWORD)
            ),
        }
    )


def _auth_schema(*, include_token: bool) -> vol.Schema:
    """Secret fields for reauth and reconfiguration; never prefill them."""
    schema: dict[Any, Any] = {
        vol.Optional(CONF_EMAIL): TextSelector(
            TextSelectorConfig(type=TextSelectorType.EMAIL)
        ),
        vol.Optional(CONF_PASSWORD): TextSelector(
            TextSelectorConfig(type=TextSelectorType.PASSWORD)
        ),
    }
    if include_token:
        schema[vol.Optional(CONF_ACCESS_TOKEN)] = TextSelector(
            TextSelectorConfig(type=TextSelectorType.PASSWORD)
        )
    return vol.Schema(schema)


class _temporary_session:
    """Create and close an isolated, cookie-free HA-managed flow session."""

    def __init__(self, hass: HomeAssistant) -> None:
        self.hass = hass
        self.session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> aiohttp.ClientSession:
        self.session = async_create_clientsession(
            self.hass,
            auto_cleanup=False,
            cookie_jar=aiohttp.DummyCookieJar(),
            connector_owner=False,
            max_field_size=MAX_FIELD_SIZE,
        )
        return self.session

    async def __aexit__(self, *_: object) -> None:
        if self.session is not None:
            self.session.detach()
