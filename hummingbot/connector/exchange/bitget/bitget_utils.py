# TODO: Just a template, needs modifying

from typing import Dict, Any, Optional
from decimal import Decimal

import hummingbot.connector.exchange.bitget.bitget_constants as CONSTANTS
from hummingbot.core.utils.tracking_nonce import get_tracking_nonce


def convert_from_exchange_symbol(symbol: str) -> str:
    """
    Converts exchange symbol to Hummingbot trading pair format
    Example: BTCUSDT -> BTC-USDT
    """
    # Bitget uses symbols like BTCUSDT, ETHUSDT etc.
    # We need to find where to split - usually before USDT, USDC, BTC, ETH
    common_quote_assets = ["USDT", "USDC", "BTC", "ETH", "BUSD", "DAI"]

    for quote in common_quote_assets:
        if symbol.endswith(quote):
            base = symbol[:-len(quote)]
            return f"{base}-{quote}"

    # If no common quote asset found, assume last 3-4 characters are quote
    if len(symbol) >= 6:
        base = symbol[:-4]
        quote = symbol[-4:]
        return f"{base}-{quote}"

    return symbol


def convert_to_exchange_symbol(trading_pair: str) -> str:
    """
    Converts Hummingbot trading pair to exchange symbol format
    Example: BTC-USDT -> BTCUSDT
    """
    return trading_pair.replace("-", "")


def get_new_client_order_id(is_buy: bool, trading_pair: str) -> str:
    """
    Creates a client order ID for a new order
    """
    side = "B" if is_buy else "S"
    symbols = trading_pair.split("-")
    base = symbols[0] if len(symbols) > 0 else ""
    quote = symbols[1] if len(symbols) > 1 else ""
    base_str = base[:2] if len(base) >= 2 else base
    quote_str = quote[:2] if len(quote) >= 2 else quote

    return f"{CONSTANTS.HBOT_ORDER_ID_PREFIX}{side}-{base_str}{quote_str}-{get_tracking_nonce()}"


def build_api_factory() -> str:
    """
    Returns the Bitget API factory class
    """
    return "BitgetAPIFactory"


def convert_order_type_to_exchange(order_type: str) -> str:
    """
    Convert Hummingbot order type to Bitget order type
    """
    if order_type.lower() == "limit":
        return CONSTANTS.ORDER_TYPE_LIMIT
    elif order_type.lower() == "market":
        return CONSTANTS.ORDER_TYPE_MARKET
    else:
        return CONSTANTS.ORDER_TYPE_LIMIT


def convert_order_type_from_exchange(order_type: str) -> str:
    """
    Convert Bitget order type to Hummingbot order type
    """
    return order_type.lower()


def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    """
    Verifies if a trading pair is enabled to trade by checking the exchange info
    """
    return (
        exchange_info.get("status") == "online" and
        exchange_info.get("quoteCoin") is not None and
        exchange_info.get("baseCoin") is not None
    )


def get_rest_url_for_endpoint(endpoint: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Returns the complete URL for a REST API endpoint
    """
    return CONSTANTS.REST_URLS[domain] + endpoint


def get_wss_url(domain: str = CONSTANTS.DEFAULT_DOMAIN, private: bool = False) -> str:
    """
    Returns the WebSocket URL for the given domain
    """
    if private:
        return CONSTANTS.WSS_PRIVATE_URL[domain]
    else:
        return CONSTANTS.WSS_PUBLIC_URL[domain]


def decimal_val_or_none(value: Any) -> Optional[Decimal]:
    """
    Safely converts a value to Decimal or returns None if conversion fails
    """
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def decimal_val_or_zero(value: Any) -> Decimal:
    """
    Safely converts a value to Decimal or returns zero if conversion fails
    """
    result = decimal_val_or_none(value)
    return result if result is not None else Decimal("0")


def validate_trading_pair(trading_pair: str) -> bool:
    """
    Validates if the trading pair format is correct
    """
    if not trading_pair or "-" not in trading_pair:
        return False

    parts = trading_pair.split("-")
    return len(parts) == 2 and all(len(part) > 0 for part in parts)


def format_trading_pair(base: str, quote: str) -> str:
    """
    Formats base and quote assets into a trading pair
    """
    return f"{base.upper()}-{quote.upper()}"
