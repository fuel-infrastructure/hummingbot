import sys

from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState, OrderType

EXCHANGE_NAME = "o2"

DEFAULT_DOMAIN = "testnet"

# O2 uses integer prices scaled by 10^6
O2_PRICE_SCALE_FACTOR = 6

HBOT_ORDER_ID_PREFIX = "O2HB"
HBOT_BROKER_ID = "HBOT"
MAX_ORDER_ID_LEN = 32

REST_URLS = {
    "testnet": "https://api.testnet.o2.app",
    "local": "http://localhost:3001",
}

ORDER_REST_URL = "http://localhost:4567"

WSS_URLS = {
    "testnet": "wss://api.testnet.o2.app/ws",
    "local": "ws://localhost:3001/ws",
}

MARKETS_PATH_URL = "/markets"
TICKER_PATH_URL = "/markets/ticker"
DEPTH_PATH_URL = "/depth"
TRADES_PATH_URL = "/trades"
BALANCE_PATH_URL = "/balance"
BALANCES_PATH_URL = "/balances"
ORDERS_PATH_URL = "/orders"
ORDER_PATH_URL = "/order"
HEALTH_PATH_URL = "/health"
ACCOUNTS_PATH_URL = "/accounts"

WS_HEARTBEAT_TIME_INTERVAL = 30

WS_SUBSCRIBE_DEPTH = "subscribe_depth"
WS_SUBSCRIBE_DEPTH_UPDATE = "subscribe_depth_update"
WS_SUBSCRIBE_TRADES = "subscribe_trades"
WS_SUBSCRIBE_ORDERS = "subscribe_orders"
WS_SUBSCRIBE_BALANCES = "subscribe_balances"

WS_DEFAULT_PRECISION = "10"
WS_MARKET_ID_PREFIX = "0x"

MARKETS_LM_ID = "markets_rate_limit"
TICKER_LM_ID = "ticker_rate_limit"
DEPTH_LM_ID = "depth_rate_limit"
TRADES_LM_ID = "trades_rate_limit"
BALANCE_LM_ID = "balance_rate_limit"
ORDERS_LM_ID = "orders_rate_limit"
CANCEL_ORDER_LM_ID = "cancel_order_rate_limit"
USER_STREAM_LM_ID = "user_stream_rate_limit"
ACCOUNTS_LM_ID = "accounts_rate_limit"

REQUEST_WEIGHT = "REQUEST_WEIGHT"
ORDERS = "ORDERS"
RAW_REQUESTS = "RAW_REQUESTS"

ONE_MINUTE = 60
ONE_SECOND = 1

MAX_REQUEST = 1000

# O2 order state mappings
ORDER_STATE = {
    "open": OrderState.OPEN,
    "filled": OrderState.FILLED,
    "partially_filled": OrderState.PARTIALLY_FILLED,
    "canceled": OrderState.CANCELED,
    "rejected": OrderState.FAILED,
}

SIDE_BUY = "buy"
SIDE_SELL = "sell"

MAX_SLIPPAGE_PERCENTAGE = 5

ASSETS_DECIMALS_MAP = {
    "FUEL": 9,
    "USDC": 6,
}

DEFAULT_ASSET_DECIMALS = 9

NO_LIMIT = sys.maxsize
RATE_LIMITS = [
    RateLimit(limit_id=REQUEST_WEIGHT, limit=NO_LIMIT, time_interval=1),
    RateLimit(limit_id=ORDERS, limit=NO_LIMIT, time_interval=1),
    RateLimit(limit_id=RAW_REQUESTS, limit=NO_LIMIT, time_interval=1),
    RateLimit(limit_id=MARKETS_LM_ID, limit=100, time_interval=1),
    RateLimit(limit_id=TICKER_LM_ID, limit=100, time_interval=1),
    RateLimit(limit_id=DEPTH_LM_ID, limit=100, time_interval=1),
    RateLimit(limit_id=TRADES_LM_ID, limit=100, time_interval=1),
    RateLimit(limit_id=BALANCE_LM_ID, limit=100, time_interval=1),
    RateLimit(limit_id=ORDERS_LM_ID, limit=100, time_interval=1),
    RateLimit(limit_id=CANCEL_ORDER_LM_ID, limit=100, time_interval=1),
    RateLimit(limit_id=USER_STREAM_LM_ID, limit=100, time_interval=1),
    RateLimit(limit_id=ACCOUNTS_LM_ID, limit=100, time_interval=1),
    RateLimit(limit_id=MARKETS_PATH_URL, limit=100, time_interval=1),
    RateLimit(limit_id=TICKER_PATH_URL, limit=100, time_interval=1),
    RateLimit(limit_id=DEPTH_PATH_URL, limit=100, time_interval=1),
    RateLimit(limit_id=TRADES_PATH_URL, limit=100, time_interval=1),
    RateLimit(limit_id=BALANCE_PATH_URL, limit=100, time_interval=1),
    RateLimit(limit_id=ORDERS_PATH_URL, limit=100, time_interval=1),
    RateLimit(limit_id=ORDER_PATH_URL, limit=100, time_interval=1),
    RateLimit(limit_id=HEALTH_PATH_URL, limit=100, time_interval=1),
    RateLimit(limit_id=ACCOUNTS_PATH_URL, limit=100, time_interval=1),
]
