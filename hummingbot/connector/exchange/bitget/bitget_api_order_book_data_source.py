# TODO: Just a template, needs modifying

import asyncio
import logging
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional

import hummingbot.connector.exchange.bitget.bitget_constants as CONSTANTS
import hummingbot.connector.exchange.bitget.bitget_web_utils as web_utils
from hummingbot.connector.exchange.bitget.bitget_utils import convert_from_exchange_symbol
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger


class BitgetAPIOrderBookDataSource(OrderBookTrackerDataSource):
    HEARTBEAT_TIME_INTERVAL = 30.0

    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        trading_pairs: List[str],
        connector: Optional[Any] = None,
        api_factory: Optional[WebAssistantsFactory] = None,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
        throttler: Optional[AsyncThrottler] = None,
        time_synchronizer: Optional[TimeSynchronizer] = None,
    ):
        super().__init__(trading_pairs)
        self._connector = connector
        self._domain = domain
        self._time_synchronizer = time_synchronizer
        self._throttler = throttler
        self._api_factory = api_factory or web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=domain,
        )
        self._message_queue: Dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)
        self._last_ws_message_sent_timestamp = 0
        self._trading_pairs = trading_pairs

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = HummingbotLogger(__name__)
        return cls._logger

    @classmethod
    async def get_last_traded_prices(
        cls,
        trading_pairs: List[str],
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
        throttler: Optional[AsyncThrottler] = None,
    ) -> Dict[str, float]:
        """
        Fetches the last traded price for each trading pair
        """
        if throttler is None:
            throttler = web_utils.create_throttler()

        api_factory = web_utils.build_api_factory(
            throttler=throttler,
            time_synchronizer=None,
            domain=domain,
        )
        rest_assistant = await api_factory.get_rest_assistant()

        url = web_utils.public_rest_url(CONSTANTS.LAST_TRADED_PRICE_PATH, domain)

        async with throttler.execute_task(limit_id=CONSTANTS.LAST_TRADED_PRICE_PATH):
            response = await rest_assistant.execute_request(
                url=url,
                method=RESTMethod.GET,
                throttler_limit_id=CONSTANTS.LAST_TRADED_PRICE_PATH,
            )

        result = {}
        if isinstance(response, dict) and web_utils.is_rest_response_success(response):
            data = web_utils.get_rest_response_data(response)
            for ticker_data in data:
                try:
                    trading_pair = convert_from_exchange_symbol(ticker_data["symbol"])
                    if trading_pair in trading_pairs:
                        result[trading_pair] = float(ticker_data["lastPrice"])
                except (KeyError, ValueError):
                    continue

        return result

    @classmethod
    async def fetch_trading_pairs(
        cls,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
        throttler: Optional[AsyncThrottler] = None,
    ) -> List[str]:
        """
        Fetches all available trading pairs from the exchange
        """
        if throttler is None:
            throttler = web_utils.create_throttler()

        api_factory = web_utils.build_api_factory(
            throttler=throttler,
            time_synchronizer=None,
            domain=domain,
        )
        rest_assistant = await api_factory.get_rest_assistant()

        url = web_utils.public_rest_url(CONSTANTS.EXCHANGE_INFO_PATH_URL, domain)

        async with throttler.execute_task(limit_id=CONSTANTS.EXCHANGE_INFO_PATH_URL):
            response = await rest_assistant.execute_request(
                url=url,
                method=RESTMethod.GET,
                throttler_limit_id=CONSTANTS.EXCHANGE_INFO_PATH_URL,
            )

        trading_pairs = []
        if isinstance(response, dict) and web_utils.is_rest_response_success(response):
            data = web_utils.get_rest_response_data(response)
            for symbol_data in data:
                try:
                    if symbol_data.get("status") == "online":
                        trading_pair = convert_from_exchange_symbol(symbol_data["symbol"])
                        trading_pairs.append(trading_pair)
                except KeyError:
                    continue

        return trading_pairs

    async def get_new_order_book(self, trading_pair: str) -> OrderBookMessage:
        """
        Gets a full order book snapshot for a trading pair
        """
        rest_assistant = await self._api_factory.get_rest_assistant()

        symbol = web_utils.format_symbol_for_exchange(trading_pair)
        params = {"symbol": symbol, "limit": 100}

        url = web_utils.public_rest_url(CONSTANTS.ORDER_BOOK_PATH_URL, self._domain)

        async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.ORDER_BOOK_PATH_URL):
            response = await rest_assistant.execute_request(
                url=url,
                params=params,
                method=RESTMethod.GET,
                throttler_limit_id=CONSTANTS.ORDER_BOOK_PATH_URL,
            )

        if not isinstance(response, dict) or not web_utils.is_rest_response_success(response):
            raise IOError(f"Failed to fetch order book for {trading_pair}")

        data = web_utils.get_rest_response_data(response)
        timestamp = int(data.get("ts", time.time() * 1000))

        return OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content={
                "trading_pair": trading_pair,
                "update_id": timestamp,
                "bids": data.get("bids", []),
                "asks": data.get("asks", []),
            },
            timestamp=timestamp / 1000,
        )

    async def listen_for_subscriptions(self):
        """
        Connects to the WebSocket and subscribes to channels - basic implementation
        """
        # TODO: Implement WebSocket subscription logic
        pass

    async def listen_for_trades(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Listens for trade events and pushes them to the output queue - basic implementation
        """
        # TODO: Implement trade listening logic
        pass

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Listens for order book diff events and pushes them to the output queue - basic implementation
        """
        # TODO: Implement order book diff listening logic
        pass

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Listens for order book snapshot events and pushes them to the output queue - basic implementation
        """
        # TODO: Implement order book snapshot listening logic
        pass
