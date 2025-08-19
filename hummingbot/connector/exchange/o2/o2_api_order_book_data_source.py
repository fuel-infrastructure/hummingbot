import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.o2 import o2_constants as CONSTANTS, o2_utils, o2_web_utils as web_utils
from hummingbot.connector.exchange.o2.o2_order_book import O2OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest, WSPlainTextRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.o2.o2_exchange import O2Exchange


class O2APIOrderBookDataSource(OrderBookTrackerDataSource):
    HEARTBEAT_TIME_INTERVAL = 30.0

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'O2Exchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__(trading_pairs)
        self._connector = connector
        self._domain = domain
        self._api_factory = api_factory

    @property
    def trading_pairs(self) -> List[str]:
        return self._trading_pairs

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        try:
            self.logger().info(f"Requesting order book snapshot for trading pair: {trading_pair}")
            market_id = await self._connector.exchange_market_id_associated_to_pair(trading_pair=trading_pair)
            self.logger().info(f"Got market_id {market_id} for trading pair {trading_pair}")
            rest_assistant = await self._api_factory.get_rest_assistant()
            params = {
                "market_id": market_id,
                "precision": "10"  # Default 10
            }

            data = await rest_assistant.execute_request(
                url=web_utils.public_rest_url(path_url=CONSTANTS.DEPTH_PATH_URL, domain=self._domain),
                params=params,
                method=RESTMethod.GET,
                throttler_limit_id=CONSTANTS.DEPTH_LM_ID,
            )

            self.logger().info(f"Order book snapshot response for {trading_pair}: {data}")
            return data

        except Exception as e:
            self.logger().error(f"Error fetching order book snapshot for {trading_pair}: {e}", exc_info=True)
            raise

    async def _subscribe_channels(self, ws: WSAssistant):
        try:
            self.logger().info(f"Starting channel subscriptions for {len(self._trading_pairs)} trading pairs")
            subscription_tasks = []

            for trading_pair in self._trading_pairs:
                market_id = await self._connector.exchange_market_id_associated_to_pair(trading_pair=trading_pair)

                depth_subscription = {
                    "action": CONSTANTS.WS_SUBSCRIBE_DEPTH,
                    "market_id": market_id,
                    "precision": CONSTANTS.WS_DEFAULT_PRECISION
                }

                trade_subscription = {
                    "action": CONSTANTS.WS_SUBSCRIBE_TRADES,
                    "market_id": market_id
                }

                depth_request = WSJSONRequest(payload=depth_subscription)
                trade_request = WSJSONRequest(payload=trade_subscription)

                subscription_tasks.extend([
                    ws.send(depth_request),
                    ws.send(trade_request)
                ])

                self.logger().info(
                    f"Initiating O2 WebSocket subscriptions for {trading_pair} "
                    f"(market_id: {market_id})"
                )

            await asyncio.gather(*subscription_tasks)

            self.logger().info(
                f"Successfully subscribed to O2 WebSocket channels for {len(self._trading_pairs)} trading pairs: "
                f"{', '.join(self._trading_pairs)}"
            )

        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().error(
                f"Failed to subscribe to O2 WebSocket channels. Error: {e}",
                exc_info=True
            )
            raise

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws_url = CONSTANTS.WSS_URLS.get(self._domain)
        self.logger().info(f"Connecting to O2 WebSocket at {ws_url}")
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(
            ws_url=ws_url,
            ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL
        )
        self.logger().info(f"Successfully connected to O2 WebSocket")
        return ws

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        try:
            self.logger().info(f"Fetching order book snapshot for {trading_pair}")
            snapshot_response: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)

            snapshot_timestamp: float = time.time()

            snapshot_msg: OrderBookMessage = O2OrderBook.snapshot_message_from_exchange(
                snapshot_response,
                snapshot_timestamp,
                metadata={"trading_pair": trading_pair, "connector": self._connector}
            )

            self.logger().info(f"Order book snapshot created for {trading_pair}")
            return snapshot_msg
        except Exception as e:
            self.logger().error(f"Failed to create order book snapshot for {trading_pair}: {e}", exc_info=True)
            raise

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        try:
            action = raw_message.get("action")
            if action not in [CONSTANTS.WS_SUBSCRIBE_DEPTH, CONSTANTS.WS_SUBSCRIBE_DEPTH_UPDATE]:
                return

            raw_market_id = raw_message.get("market_id", "")
            market_id = o2_utils.normalize_market_id(raw_market_id)
            if not market_id:
                self.logger().warning("No market_id in O2 diff message")
                return

            market_id_map = await self._connector.trading_pair_market_id_map()
            if market_id not in market_id_map.inverse:
                self.logger().warning(f"Unknown market_id in O2 diff message: {market_id}")
                return

            trading_pair = market_id_map.inverse[market_id]
            timestamp = time.time()

            orders = raw_message.get("orders", {})
            if not orders.get("buys") and not orders.get("sells"):
                return

            if action == CONSTANTS.WS_SUBSCRIBE_DEPTH:
                snapshot_message = O2OrderBook.snapshot_message_from_exchange(
                    raw_message,
                    timestamp,
                    metadata={"trading_pair": trading_pair, "connector": self._connector}
                )
                self._message_queue[self._snapshot_messages_queue_key].put_nowait(snapshot_message)
            else:
                diff_message = O2OrderBook.diff_message_from_exchange(
                    raw_message,
                    timestamp,
                    metadata={"trading_pair": trading_pair, "connector": self._connector}
                )
                message_queue.put_nowait(diff_message)

        except Exception as e:
            self.logger().error(f"Error parsing O2 order book diff message: {e}", exc_info=True)

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        try:
            if raw_message.get("action") != CONSTANTS.WS_SUBSCRIBE_TRADES:
                return

            raw_market_id = raw_message.get("market_id", "")
            market_id = o2_utils.normalize_market_id(raw_market_id)
            if not market_id:
                self.logger().warning("No market_id in O2 trade message")
                return

            market_id_map = await self._connector.trading_pair_market_id_map()
            if market_id not in market_id_map.inverse:
                self.logger().warning(f"Unknown market_id in O2 trade message: {market_id}")
                return

            trading_pair = market_id_map.inverse[market_id]

            trade_data = raw_message.get("trade", raw_message.get("trades"))
            if not trade_data:
                self.logger().debug(f"No trade data in O2 message for {trading_pair}")
                return

            trade_message = O2OrderBook.trade_message_from_exchange(
                raw_message,
                metadata={"trading_pair": trading_pair, "connector": self._connector}
            )

            message_queue.put_nowait(trade_message)
            self.logger().debug(f"Processed O2 trade message for {trading_pair}")

        except Exception as e:
            self.logger().error(f"Error parsing O2 trade message: {e}", exc_info=True)

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        action = event_message.get("action", "")

        if action in [CONSTANTS.WS_SUBSCRIBE_DEPTH, CONSTANTS.WS_SUBSCRIBE_DEPTH_UPDATE]:
            return self._diff_messages_queue_key
        elif action == CONSTANTS.WS_SUBSCRIBE_TRADES:
            return self._trade_messages_queue_key
        elif action == CONSTANTS.WS_SUBSCRIBE_ORDERS:
            return "orders"
        else:
            self.logger().debug(f"Unknown O2 WebSocket action '{action}', routing to depth queue")
            return self._diff_messages_queue_key

    def _get_messages_queue_keys(self) -> List[str]:
        return [self._diff_messages_queue_key, self._trade_messages_queue_key, self._snapshot_messages_queue_key]

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant):
        ping_interval = 30.0
        last_ping_time = 0

        while True:
            try:
                current_time = time.time()
                if current_time - last_ping_time >= ping_interval:
                    ping_request = WSPlainTextRequest(payload="PING")
                    await websocket_assistant.send(request=ping_request)
                    last_ping_time = current_time
                    self.logger().debug("Sent PING to O2 WebSocket")

                try:
                    receive_task = asyncio.create_task(self._receive_and_queue_message(websocket_assistant))

                    await asyncio.wait_for(receive_task, timeout=5.0)

                except asyncio.TimeoutError:
                    if not receive_task.done():
                        receive_task.cancel()
                        try:
                            await receive_task
                        except asyncio.CancelledError:
                            pass
                    continue

            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Error in O2 WebSocket message processing: {e}")
                raise

    async def _receive_and_queue_message(self, websocket_assistant: WSAssistant):
        async for ws_response in websocket_assistant.iter_messages():
            data = ws_response.data

            if data == "PONG" or (isinstance(data, dict) and data.get("data") == "PONG"):
                self.logger().debug("Received PONG from O2 WebSocket")
                continue

            if data is None:
                break

            channel = self._channel_originating_message(event_message=data)
            valid_channels = self._get_messages_queue_keys()

            if channel in valid_channels:
                self._message_queue[channel].put_nowait(data)
            else:
                self.logger().warning(f"Unknown channel '{channel}' for message: {data}")

            return

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        message_queue = self._message_queue[self._snapshot_messages_queue_key]
        while True:
            try:
                try:
                    snapshot_msg = await asyncio.wait_for(
                        message_queue.get(),
                        timeout=self.FULL_ORDER_BOOK_RESET_DELTA_SECONDS
                    )

                    if isinstance(snapshot_msg, OrderBookMessage):
                        output.put_nowait(snapshot_msg)
                        self.logger().debug(f"Forwarded O2 snapshot for {snapshot_msg.trading_pair}")
                    else:
                        self.logger().warning(f"Unexpected snapshot message type: {type(snapshot_msg)}")

                except asyncio.TimeoutError:
                    await self._request_order_book_snapshots(output=output)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "Unexpected error occurred fetching orderbook snapshots. Retrying in 3 seconds...", exc_info=True
                )
                await asyncio.sleep(3.0)

    async def _request_order_book_snapshots(self, output: asyncio.Queue):
        for trading_pair in self._trading_pairs:
            try:
                snapshot = await self._order_book_snapshot(trading_pair=trading_pair)
                output.put_nowait(snapshot)
            except Exception:
                self.logger().exception(f"Unexpected error fetching order book snapshot for {trading_pair}.")
                raise
