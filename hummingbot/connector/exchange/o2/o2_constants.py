import sys

from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit

EXCHANGE_NAME = "o2"

DEFAULT_DOMAIN = "testnet"

HBOT_ORDER_ID_PREFIX = "O2HB"
HBOT_BROKER_ID = "HBOT"
MAX_ORDER_ID_LEN = 32
MARKET_ID_PREFIX = "0x"
TRADING_ACCOUNT_PREFIX = "0x"

REST_URLS = {
    "testnet": "https://api.testnet.o2.app",
    "local": "http://localhost:3001",
    "devnet": "https://api.devnet.o2.app"
}

ORDER_REST_URL = "http://localhost:4567"

WSS_URLS = {
    "testnet": "wss://api.testnet.o2.app/ws",
    "local": "ws://localhost:3001/ws",
    "devnet": "wss://api.devnet.o2.app/ws"
}

# Rest Endpoints
MARKETS_PATH_URL = "/markets"
TICKER_PATH_URL = "/markets/ticker"
SUMMARY_PATH_URL = "/markets/summary"
DEPTH_PATH_URL = "/depth"
TRADES_PATH_URL = "/trades"
BALANCE_PATH_URL = "/balance"
ORDERS_PATH_URL = "/orders"
ORDER_PATH_URL = "/order"
HEALTH_PATH_URL = "/health"
ACCOUNTS_PATH_URL = "/accounts"

# Websocket Endpoints
WS_SUBSCRIBE_DEPTH = "subscribe_depth"
WS_SUBSCRIBE_DEPTH_UPDATE = "subscribe_depth_update"
WS_SUBSCRIBE_PERIODIC_DEPTH = "subscribe_depth_view"
WS_SUBSCRIBE_TRADES = "subscribe_trades"
WS_SUBSCRIBE_ORDERS = "subscribe_orders"
WS_SUBSCRIBE_BALANCES = "subscribe_balances"

WS_DEFAULT_PRECISION = "10"
WS_HEARTBEAT_TIME_INTERVAL = 30

ONE_MINUTE = 60
ONE_SECOND = 1

ASSETS_DECIMALS_MAP = {
    "FUEL": 9,
    "USDC": 6,
    "ETH": 9,
}

DEFAULT_ASSET_DECIMALS = 9

# Global REST API Rate Limit
# - 6000 requests per minute globally across all REST endpoints
ALL_ENDPOINTS_LIMIT_ID = "ALL_ENDPOINTS_LIMIT"
ALL_ENDPOINTS_LIMIT = 6000

# Per-endpoint max request limits
MAX_REQUEST = 100

NO_LIMIT = sys.maxsize

RATE_LIMITS = [

    # Global rate limit for all REST API endpoints
    RateLimit(
        limit_id=ALL_ENDPOINTS_LIMIT_ID,
        limit=ALL_ENDPOINTS_LIMIT,
        time_interval=ONE_MINUTE
    ),

    RateLimit(limit_id=MARKETS_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_SECOND, linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT_ID)]),
    RateLimit(limit_id=TICKER_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_SECOND, linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT_ID)]),
    RateLimit(limit_id=SUMMARY_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_SECOND, linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT_ID)]),
    RateLimit(limit_id=DEPTH_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_SECOND, linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT_ID)]),
    RateLimit(limit_id=TRADES_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_SECOND, linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT_ID)]),
    RateLimit(limit_id=BALANCE_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_SECOND, linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT_ID)]),
    RateLimit(limit_id=ORDERS_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_SECOND, linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT_ID)]),
    RateLimit(limit_id=ORDER_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_SECOND, linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT_ID)]),
    RateLimit(limit_id=HEALTH_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_SECOND, linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT_ID)]),
    RateLimit(limit_id=ACCOUNTS_PATH_URL, limit=MAX_REQUEST, time_interval=ONE_SECOND, linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT_ID)]),
]
