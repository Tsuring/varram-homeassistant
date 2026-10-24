"""VARRAM app-token helpers.

The token is an app JWT. Its payload is decoded only to read the stable account
identifier and expiration; the service remains the authority that validates it.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Mapping
from dataclasses import dataclass


class InvalidToken(ValueError):
    """The token does not contain the expected VARRAM app claims."""


@dataclass(frozen=True, slots=True)
class TokenClaims:
    """Non-secret identity and expiry claims read from an app JWT."""

    user_id: str
    expires_at: int


def decode_token_claims(token: str) -> TokenClaims:
    """Decode the JWT payload without treating it as signature validation."""
    parts = token.split(".")
    if len(parts) != 3:
        raise InvalidToken("Token is not a JWT")

    try:
        payload = parts[1]
        payload += "=" * (-len(payload) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload).decode("utf-8"))
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as err:
        raise InvalidToken("Token claims are not readable") from err

    if not isinstance(claims, Mapping):
        raise InvalidToken("Token claims are not an object")
    user_id = claims.get("userid")
    expires_at = claims.get("exp")
    if not isinstance(user_id, str) or not user_id:
        raise InvalidToken("Token has no app account identifier")
    if isinstance(expires_at, bool) or not isinstance(expires_at, int) or expires_at <= 0:
        raise InvalidToken("Token has no expiration")

    return TokenClaims(user_id=user_id, expires_at=expires_at)
