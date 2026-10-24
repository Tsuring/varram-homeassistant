"""Constants for the VARRAM integration."""

from datetime import timedelta

DOMAIN = "varram"
BASE_URL = "https://web.varram-robot.com"
AUTH_BASE_URL = "https://auth.varram-robot.com"
APP_USER_AGENT = "(iPhone; CPU iPhone OS 18_7 like Mac OS X) pcss/app/vapp/2.3.7/KR"

CONF_ACCESS_TOKEN = "access_token"
CONF_EMAIL = "email"
CONF_PASSWORD = "password"
CONF_SCAN_INTERVAL = "scan_interval"
CONF_USER_ID = "user_id"

DEFAULT_SCAN_INTERVAL = 300
MIN_SCAN_INTERVAL = 60
MAX_SCAN_INTERVAL = 3600
SCAN_INTERVAL = timedelta(seconds=DEFAULT_SCAN_INTERVAL)

REQUEST_TIMEOUT_SECONDS = 25
MAX_FIELD_SIZE = 64 * 1024
MAX_FEED_ROWS = 5000
MAX_HISTORY_MEALS = 10
TOKEN_RENEWAL_WINDOW_SECONDS = 24 * 60 * 60

PLATFORMS = ["sensor"]
