import time
from typing import Optional

import hummingbot.connector.exchange.o2.o2_constants as CONSTANTS
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    url = CONSTANTS.REST_URLS.get(domain, CONSTANTS.REST_URLS.get(CONSTANTS.DEFAULT_DOMAIN))
    if url is None:
        raise ValueError(f"No REST URL found for domain '{domain}'. Available domains: {list(CONSTANTS.REST_URLS.keys())}")
    return url + path_url


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    url = CONSTANTS.REST_URLS.get(domain, CONSTANTS.REST_URLS.get(CONSTANTS.DEFAULT_DOMAIN))
    if url is None:
        raise ValueError(f"No REST URL found for domain '{domain}'. Available domains: {list(CONSTANTS.REST_URLS.keys())}")
    return url + path_url


def order_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    return CONSTANTS.ORDER_REST_URL + path_url


def build_api_factory(
    throttler: Optional[AsyncThrottler] = None,
    auth: Optional[AuthBase] = None,
) -> WebAssistantsFactory:
    throttler = throttler or create_throttler()
    api_factory = WebAssistantsFactory(throttler=throttler, auth=auth)
    return api_factory


def create_throttler() -> AsyncThrottler:
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)

# TODO: get server time
async def get_current_server_time(
    throttler: Optional[AsyncThrottler] = None,
    domain: str = CONSTANTS.DEFAULT_DOMAIN,
) -> int:
    return int(time.time() * 1e3)