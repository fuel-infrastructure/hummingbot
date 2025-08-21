# TODO: Just a template, needs modifying

import time
from typing import Dict, Any, Optional

import hummingbot.connector.exchange.bitget.bitget_constants as CONSTANTS
from hummingbot.connector.exchange.bitget.bitget_auth import BitgetAuth
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates a full URL for private REST API endpoints.
    """
    return CONSTANTS.REST_URLS[domain] + path_url


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates a full URL for public REST API endpoints.
    """
    return CONSTANTS.REST_URLS[domain] + path_url


def wss_url(domain: str = CONSTANTS.DEFAULT_DOMAIN, private: bool = False) -> str:
    """
    Creates a full URL for WebSocket connections.
    """
    if private:
        return CONSTANTS.WSS_PRIVATE_URL[domain]
    else:
        return CONSTANTS.WSS_PUBLIC_URL[domain]


def build_api_factory(
    throttler: Optional[AsyncThrottler] = None,
    time_synchronizer: Optional[TimeSynchronizer] = None,
    domain: str = CONSTANTS.DEFAULT_DOMAIN,
    auth: Optional[BitgetAuth] = None,
) -> WebAssistantsFactory:
    """
    Builds a WebAssistantsFactory instance for Bitget API
    """
    time_synchronizer = time_synchronizer or TimeSynchronizer()
    throttler = throttler or create_throttler()
    api_factory = WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
        rest_pre_processors=[],
        ws_pre_processors=[],
    )
    return api_factory


def get_current_server_time() -> int:
    """
    Returns the current server time in milliseconds
    """
    return int(time.time() * 1000)


def format_symbol_for_exchange(trading_pair: str) -> str:
    """
    Converts trading pair format to exchange symbol format
    Example: BTC-USDT -> BTCUSDT
    """
    return trading_pair.replace("-", "")


def format_symbol_from_exchange(symbol: str) -> str:
    """
    Converts exchange symbol format to trading pair format
    """
    # This is a simple implementation - may need refinement based on actual symbol patterns
    common_quote_assets = ["USDT", "USDC", "BTC", "ETH", "BUSD", "DAI"]

    for quote in common_quote_assets:
        if symbol.endswith(quote):
            base = symbol[:-len(quote)]
            return f"{base}-{quote}"

    # Default fallback
    if len(symbol) >= 6:
        return f"{symbol[:-4]}-{symbol[-4:]}"

    return symbol


def create_throttler() -> AsyncThrottler:
    """
    Creates an AsyncThrottler instance with Bitget rate limits
    """
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


def rest_response_with_errors(response: Dict[str, Any]) -> bool:
    """
    Checks if a REST API response contains errors
    """
    return (
        response.get("code") != CONSTANTS.RET_CODE_OK or
        response.get("msg") != "success"
    )


def is_rest_response_success(response: Dict[str, Any]) -> bool:
    """
    Checks if a REST API response is successful
    """
    return response.get("code") == CONSTANTS.RET_CODE_OK


def get_rest_response_error_message(response: Dict[str, Any]) -> str:
    """
    Extracts error message from REST API response
    """
    return response.get("msg", "Unknown error")


def get_rest_response_data(response: Dict[str, Any]) -> Any:
    """
    Extracts data from REST API response
    """
    return response.get("data")


def validate_ws_message(message: Dict[str, Any]) -> bool:
    """
    Validates WebSocket message format
    """
    return isinstance(message, dict) and "action" in message


def extract_ws_message_data(message: Dict[str, Any]) -> Any:
    """
    Extracts data from WebSocket message
    """
    return message.get("data", [])


def get_ws_message_action(message: Dict[str, Any]) -> Optional[str]:
    """
    Gets the action type from WebSocket message
    """
    return message.get("action")


def get_ws_message_arg(message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Gets the arg object from WebSocket message
    """
    return message.get("arg", {})


def format_timestamp(timestamp: float) -> int:
    """
    Formats timestamp for API requests
    """
    return int(timestamp * 1000)


def parse_timestamp(timestamp: Any) -> float:
    """
    Parses timestamp from API responses
    """
    if isinstance(timestamp, str):
        return float(timestamp) / 1000
    elif isinstance(timestamp, (int, float)):
        # Check if timestamp is in milliseconds or seconds
        if timestamp > 1e12:  # Milliseconds
            return timestamp / 1000
        else:  # Seconds
            return float(timestamp)
    else:
        return time.time()


def get_api_reason_for_rejection(code: str) -> str:
    """
    Maps Bitget error codes to human-readable messages
    """
    error_codes = {
        CONSTANTS.RET_CODE_AUTH_FAILED: "Authentication failed",
        CONSTANTS.RET_CODE_INVALID_PARAMS: "Invalid parameters",
        CONSTANTS.RET_CODE_INSUFFICIENT_BALANCE: "Insufficient balance",
        CONSTANTS.RET_CODE_ORDER_NOT_FOUND: "Order not found",
        CONSTANTS.RET_CODE_RATE_LIMIT: "Rate limit exceeded",
    }
    return error_codes.get(code, f"Unknown error code: {code}")


def next_order_id() -> str:
    """
    Generates a unique order ID
    """
    return f"{CONSTANTS.HBOT_ORDER_ID_PREFIX}{int(time.time() * 1000)}"
