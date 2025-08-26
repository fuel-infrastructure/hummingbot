import asyncio
import logging
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.o2 import o2_constants as CONSTANTS, o2_web_utils as web_utils
from hummingbot.connector.exchange.o2.o2_auth import O2Auth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest, WSPlainTextRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.o2.o2_exchange import O2Exchange


class O2APIUserStreamDataSource(UserStreamTrackerDataSource):
    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        auth: O2Auth,
        trading_pairs: List[str],
        connector: "O2Exchange",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        super().__init__()
        self._auth = auth
        self._trading_pairs = trading_pairs
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    @property
    def order_book_class(self):
        from hummingbot.core.data_type.order_book import OrderBook
        return OrderBook

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        ws_url = CONSTANTS.WSS_URLS.get(self._domain, CONSTANTS.WSS_URLS.get(CONSTANTS.DEFAULT_DOMAIN))

        await ws.connect(
            ws_url=ws_url,
            ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL
        )

        self.logger().info(f"Connected to O2 WebSocket at {ws_url}")
        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        try:
            account_address = self._connector._trade_account

            if not account_address:
                self.logger().error("Trade account not available for WebSocket subscription")
                raise ValueError("Trade account must be fetched before subscribing to user streams")

            orders_payload = {
                "action": CONSTANTS.WS_SUBSCRIBE_ORDERS,
                "identities": [
                    {
                        "ContractId": account_address
                    }
                ]
            }
            await websocket_assistant.send(WSJSONRequest(payload=orders_payload))
            self.logger().info(f"Subscribed to order updates for account: {account_address}")

            for trading_pair in self._trading_pairs:
                market_id = await self._connector.exchange_market_id_associated_to_pair(trading_pair)
                trades_payload = {
                    "action": CONSTANTS.WS_SUBSCRIBE_TRADES,
                    "market_id": market_id
                }
                await websocket_assistant.send(WSJSONRequest(payload=trades_payload))
                self.logger().info(f"Subscribed to trade updates for {trading_pair}")

            balances_payload = {
                "action": CONSTANTS.WS_SUBSCRIBE_BALANCES,
                "identities": [
                    {
                        "ContractId": account_address
                    }
                ]
            }
            await websocket_assistant.send(WSJSONRequest(payload=balances_payload))
            self.logger().info(f"Subscribed to balance updates for account: {account_address}")

        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error occurred subscribing to O2 user streams...")
            raise

    async def _process_event_message(self, event_message: Dict[str, Any], queue: asyncio.Queue):
        try:
            action = event_message.get("action", "")

            if action == "subscribe_orders":
                event_message["channel"] = "orders"
                queue.put_nowait(event_message)

            elif action == "subscribe_trades":
                event_message["channel"] = "trades"
                queue.put_nowait(event_message)

            elif action == "subscribe_balances":
                self.logger().info(f"Received O2 WebSocket balance update")
                event_message["channel"] = "balances"
                queue.put_nowait(event_message)

            elif "error" in event_message:
                error_msg = event_message.get("error", "Unknown error")
                self.logger().error(f"O2 WebSocket error: {error_msg}")
                raise IOError(f"O2 WebSocket error: {error_msg}")

            else:
                self.logger().info(f"Unhandled O2 WebSocket message with action '{action}': {event_message}")

        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception(f"Unexpected error processing O2 WebSocket message: {event_message}")

    async def _on_user_stream_interruption(self, websocket_assistant: Optional[WSAssistant]):
        if websocket_assistant:
            self.logger().info("Closing O2 user stream WebSocket connection...")
            try:
                await websocket_assistant.disconnect()
                self.logger().info("O2 user stream WebSocket closed successfully")
            except Exception as e:
                self.logger().warning(f"Error closing O2 WebSocket: {e}")

        self.logger().warning("O2 user stream interrupted. Will attempt to reconnect...")

    async def stop(self):
        self.logger().info("Stopping O2 user stream data source...")

        if self._ws_assistant:
            await self._on_user_stream_interruption(self._ws_assistant)
            self._ws_assistant = None

        await super().stop()

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue):
        ping_interval = 30.0
        last_ping_time = 0

        while True:
            try:
                current_time = time.time()
                if current_time - last_ping_time >= ping_interval:
                    ping_request = WSPlainTextRequest(payload="PING")
                    await websocket_assistant.send(request=ping_request)
                    last_ping_time = current_time
                    self.logger().debug("Sent PING to O2 user stream WebSocket")

                try:
                    message_task = asyncio.create_task(self._get_next_message(websocket_assistant))
                    message = await asyncio.wait_for(message_task, timeout=5.0)

                    if message is not None:
                        await self._process_event_message(message, queue)

                except asyncio.TimeoutError:
                    continue

            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Error in O2 user stream WebSocket message processing: {e}")
                raise

    async def _get_next_message(self, websocket_assistant: WSAssistant) -> Optional[Dict[str, Any]]:
        async for ws_response in websocket_assistant.iter_messages():
            data = ws_response.data

            if isinstance(data, str) and data == "PONG":
                continue
            return data
        return None
