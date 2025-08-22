from tkinter import ON
from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

DEFAULT_DOMAIN = "bitget_main"

HBOT_ORDER_ID_PREFIX = "BITGET-"

# This value is based on other exchange implementations
MAX_ORDER_ID_LEN = 32
HBOT_BROKER_ID = "Hummingbot"

# Bitget specific constants

SIDE_BUY = "buy"
SIDE_SELL = "sell"

TIME_IN_FORCE_GTC = "gtc"
TIME_IN_FORCE_IOC = "ioc"
TIME_IN_FORCE_FOK = "fok"
TIME_IN_FORCE_PO = "post_only"

PAIR_ACTIVE_STR = "online"

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

# Request timeout and interval settings
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

# Public API endpoints
SYMBOL_INFO_PATH_URL = "/api/v2/spot/public/symbols"
TICKER_INFO_PATH_URL = "/api/v2/spot/market/tickers"
ORDER_BOOK_PATH_URL = "/api/v2/spot/market/orderbook"
SERVER_TIME_PATH_URL = "/api/v2/public/time"

# Private API endpoints
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

# Order States
ORDER_STATE = {
    "live": OrderState.OPEN,
    "partially_filled": OrderState.PARTIALLY_FILLED,
    "filled": OrderState.FILLED,
    "cancelled": OrderState.CANCELED,
}

# API Response codes - Most important response codes
RET_CODE_OK = "00000"

# Rate limiting constants - Based on official Bitget WebSocket documentation
# Official Connection Limits:
# - 300 connection requests/IP/5min AND Max 100 connections/IP (taking the more restrictive limit)
WS_CONNECTION_LIMIT_ID = "WSConnection"
WS_CONNECTION_LIMIT = 100

# Official Message Limits:
# - Up to 10 messages per second (includes ping and JSON messages)
WS_REQUEST_LIMIT_ID = "WSRequest"
WS_REQUEST_LIMIT = 10

# Global REST API Rate Limit
# - 6000 requests per minute globally across all REST endpoints
ALL_ENDPOINTS_LIMIT_ID = "ALL_ENDPOINTS_LIMIT"
ALL_ENDPOINTS_LIMIT = 6000

# Utility constants
ONE_SECOND = 1
ONE_MINUTE = 60
FIVE_MINUTES = 5 * ONE_MINUTE

# Define rate limits based on official Bitget API documentation
RATE_LIMITS = [
    # Global rate limit for all REST API endpoints
    RateLimit(
        limit_id=ALL_ENDPOINTS_LIMIT_ID,
        limit=ALL_ENDPOINTS_LIMIT,
        time_interval=ONE_MINUTE
    ),

    # WebSocket connection and request limits
    RateLimit(WS_CONNECTION_LIMIT_ID, limit=WS_CONNECTION_LIMIT, time_interval=FIVE_MINUTES),
    RateLimit(WS_REQUEST_LIMIT_ID, limit=WS_REQUEST_LIMIT, time_interval=ONE_SECOND),

    # Public API endpoints - all linked to global limit
    RateLimit(
        limit_id=SYMBOL_INFO_PATH_URL,
        limit=20,
        time_interval=ONE_SECOND,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT_ID)]
    ),
    RateLimit(
        limit_id=TICKER_INFO_PATH_URL,
        limit=20,
        time_interval=ONE_SECOND,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT_ID)]
    ),
    RateLimit(
        limit_id=ORDER_BOOK_PATH_URL,
        limit=20,
        time_interval=ONE_SECOND,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT_ID)]
    ),
    RateLimit(
        limit_id=SERVER_TIME_PATH_URL,
        limit=20,
        time_interval=ONE_SECOND,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT_ID)]
    ),

    # Private API endpoints - all linked to global limit
    RateLimit(
        limit_id=ACCOUNT_ASSETS_PATH_URL,
        limit=10,
        time_interval=ONE_SECOND,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT_ID)]
    ),
    RateLimit(
        limit_id=ORDER_INFO_PATH_URL,
        limit=20,
        time_interval=ONE_SECOND,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT_ID)]
    ),
    RateLimit(
        limit_id=ORDER_HISTORY_PATH_URL,
        limit=20,
        time_interval=ONE_SECOND,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT_ID)]
    ),
    RateLimit(
        limit_id=FILLS_HISTORY_PATH_URL,
        limit=10,
        time_interval=ONE_SECOND,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT_ID)]
    ),
    RateLimit(
        limit_id=UNFILLED_ORDERS_PATH_URL,
        limit=20,
        time_interval=ONE_SECOND,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT_ID)]
    ),
    RateLimit(
        limit_id=PLACE_ORDER_PATH_URL,
        limit=10,
        time_interval=ONE_SECOND,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT_ID)]
    ),
    RateLimit(
        limit_id=CANCEL_ORDER_PATH_URL,
        limit=10,
        time_interval=ONE_SECOND,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT_ID)]
    ),
]
