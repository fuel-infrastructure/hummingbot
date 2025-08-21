from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

DEFAULT_DOMAIN = "bitget_main"

HBOT_ORDER_ID_PREFIX = "BITGET-"

# This value is based on other exchange implementations
MAX_ORDER_ID_LEN = 32
HBOT_BROKER_ID = "Hummingbot"

SIDE_BUY = "buy"
SIDE_SELL = "sell"

TIME_IN_FORCE_GTC = "gtc"
TIME_IN_FORCE_IOC = "ioc"
TIME_IN_FORCE_FOK = "fok"
TIME_IN_FORCE_PO = "post_only"

# Base URLs - Bitget v2.1 API
REST_URLS = {
    "bitget_main": "https://api.bitget.com",
}

WSS_PUBLIC_URL = {
    "bitget_main": "wss://ws.bitget.com/v2/ws/public",
}

WSS_PRIVATE_URL = {
    "bitget_main": "wss://ws.bitget.com/v2/ws/private",
}

# API request timeout settings
REQUEST_TIMEOUT = 10.0
WS_HEARTBEAT_TIME_INTERVAL = 30.0

# Bitget product type
INST_TYPE = "SPOT"

# WebSocket heartbeat requests
WS_PING_REQUEST = "ping"
WS_PONG_RESPONSE = "pong"

# Public WebSocket channels
WS_DEPTH15_CHANNEL_NAME = "books15"
WS_DEPTH_DIFF_CHANNEL_NAME = "books"

# Private WebSocket channels
WS_ORDERS_CHANNEL_NAME = "orders"
WS_ACCOUNT_CHANNEL_NAME = "account"

# Public API endpoints - Bitget v2.1
SYMBOL_INFO_PATH_URL = "/api/v2/spot/public/symbols"
TICKER_INFO_PATH_URL = "/api/v2/spot/market/tickers"
ORDER_BOOK_PATH_URL = "/api/v2/spot/market/orderbook"
SERVER_TIME_PATH_URL = "/api/v2/public/time"
RECENT_TRADES_PATH_URL = "/api/v2/spot/market/fills"

# Private API endpoints - Bitget v2.1
ACCOUNT_ASSETS_PATH_URL = "/api/v2/spot/account/assets"
ORDER_INFO_PATH_URL = "/api/v2/spot/trade/orderInfo"
ORDER_HISTORY_PATH_URL = "/api/v2/spot/trade/history-orders"
FILLS_HISTORY_PATH_URL = "/api/v2/spot/trade/fills"
UNFILLED_ORDERS_PATH_URL = "/api/v2/spot/trade/unfilled-orders"
PLACE_ORDER_PATH_URL = "/api/v2/spot/trade/place-order"
CANCEL_ORDER_PATH_URL = "/api/v2/spot/trade/cancel-order"

# Order Types
ORDER_TYPE_LIMIT = "limit"
ORDER_TYPE_MARKET = "market"

# Order States - Bitget v2.1 status mapping (based on official API documentation)
ORDER_STATE = {
    "live": OrderState.OPEN,
    "partially_filled": OrderState.PARTIALLY_FILLED,
    "filled": OrderState.FILLED,
    "cancelled": OrderState.CANCELED,
}

# API Response codes - Most important response codes
RET_CODE_OK = "00000"

### TODO: Need to revize from this point onwards because we need to understand how to setup the limits. Do they need to be grouped and linked? What about maximum connections? etc.
### Websocket endpoints need to be added here as well most probably

# Rate limiting defaults
# Helps
RATE_LIMIT_GET_REQUEST = "GET_REQUEST"
RATE_LIMIT_POST_REQUEST = "POST_REQUEST"
RATE_LIMIT_WS_REQUEST = "WS_REQUEST"

ONE_SECOND = 1
ONE_MINUTE = 60

# Generic rate limits (to be updated with actual Bitget limits)
MAX_REQUEST_LIMIT_DEFAULT = 600  # placeholder - needs verification
MAX_WS_CONNECTIONS = 5

# Define rate limits based on Bitget API documentation
# Note: These are estimated values and should be verified against official documentation
RATE_LIMITS = [
    # Public API endpoints
    RateLimit(
        limit_id=SYMBOL_INFO_PATH_URL,
        limit=20,
        time_interval=ONE_SECOND,
    ),
    RateLimit(
        limit_id=TICKER_INFO_PATH_URL,
        limit=20,
        time_interval=ONE_SECOND,
    ),
    RateLimit(
        limit_id=ORDER_BOOK_PATH_URL,
        limit=20,
        time_interval=ONE_SECOND,
    ),
    RateLimit(
        limit_id=SERVER_TIME_PATH_URL,
        limit=20,
        time_interval=ONE_SECOND,
    ),
    RateLimit(
        limit_id=RECENT_TRADES_PATH_URL,
        limit=20,
        time_interval=ONE_SECOND,
    ),

    # Private API endpoints
    RateLimit(
        limit_id=ACCOUNT_ASSETS_PATH_URL,
        limit=20,
        time_interval=ONE_SECOND,
    ),
    RateLimit(
        limit_id=PLACE_ORDER_PATH_URL,
        limit=100,
        time_interval=ONE_SECOND,
    ),
    RateLimit(
        limit_id=CANCEL_ORDER_PATH_URL,
        limit=100,
        time_interval=ONE_SECOND,
    ),
    RateLimit(
        limit_id=ORDER_INFO_PATH_URL,
        limit=20,
        time_interval=ONE_SECOND,
    ),
    RateLimit(
        limit_id=UNFILLED_ORDERS_PATH_URL,
        limit=20,
        time_interval=ONE_SECOND,
    ),
    RateLimit(
        limit_id=ORDER_HISTORY_PATH_URL,
        limit=20,
        time_interval=ONE_SECOND,
    ),
    RateLimit(
        limit_id=FILLS_HISTORY_PATH_URL,
        limit=20,
        time_interval=ONE_SECOND,
    ),

    # Generic rate limits
    RateLimit(
        limit_id=RATE_LIMIT_GET_REQUEST,
        limit=600,
        time_interval=ONE_MINUTE,
    ),
    RateLimit(
        limit_id=RATE_LIMIT_POST_REQUEST,
        limit=600,
        time_interval=ONE_MINUTE,
    ),
    RateLimit(
        limit_id=RATE_LIMIT_WS_REQUEST,
        limit=5,
        time_interval=ONE_SECOND,
    ),
]
