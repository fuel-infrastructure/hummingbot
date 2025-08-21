# TODO: Just a template, needs modifying

import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import hummingbot.connector.exchange.bitget.bitget_constants as CONSTANTS
import hummingbot.connector.exchange.bitget.bitget_web_utils as web_utils
from hummingbot.connector.exchange.bitget.bitget_auth import BitgetAuth
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.bitget.bitget_exchange import BitgetExchange


class BitgetAPIUserStreamDataSource(UserStreamTrackerDataSource):
    HEARTBEAT_TIME_INTERVAL = 30.0

    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        auth: BitgetAuth,
        trading_pairs: List[str],
        connector: 'BitgetExchange',
        api_factory: Optional[WebAssistantsFactory] = None,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        super().__init__()
        self._auth: BitgetAuth = auth
        self._trading_pairs = trading_pairs
        self._connector = connector
        self._domain = domain
        self._api_factory = api_factory or web_utils.build_api_factory(
            auth=auth,
            throttler=None,
            time_synchronizer=None,
            domain=domain,
        )
        self._last_ws_message_sent_timestamp = 0

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = HummingbotLogger(__name__)
        return cls._logger

    @property
    def last_recv_time(self) -> float:
        """
        Returns the timestamp of the last received WebSocket message
        """
        return self._last_ws_message_sent_timestamp

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Creates and connects a WebSocket assistant for private streams
        """
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        url = web_utils.wss_url(domain=self._domain, private=True)
        await ws.connect(ws_url=url, ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
        return ws

    async def _authenticate(self, ws: WSAssistant):
        """
        Authenticates the WebSocket connection
        """
        try:
            auth_payload = self._auth.get_ws_auth_payload()
            auth_request = WSJSONRequest(payload=auth_payload)
            await ws.send(auth_request)
            self.logger().info("Sent authentication request")
        except Exception:
            self.logger().error("Error during WebSocket authentication", exc_info=True)
            raise

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to private channels
        """
        try:
            # Subscribe to order updates
            order_payload = {
                "op": "subscribe",
                "args": [{
                    "instType": "SPOT",
                    "channel": CONSTANTS.WS_SUBSCRIPTION_ORDERS_ENDPOINT_NAME,
                    "instId": "default"
                }]
            }
            subscribe_order_request = WSJSONRequest(payload=order_payload)
            await ws.send(subscribe_order_request)

            # Subscribe to trade fills
            fill_payload = {
                "op": "subscribe",
                "args": [{
                    "instType": "SPOT",
                    "channel": CONSTANTS.WS_SUBSCRIPTION_EXECUTIONS_ENDPOINT_NAME,
                    "instId": "default"
                }]
            }
            subscribe_fill_request = WSJSONRequest(payload=fill_payload)
            await ws.send(subscribe_fill_request)

            # Subscribe to balance updates
            balance_payload = {
                "op": "subscribe",
                "args": [{
                    "instType": "SPOT",
                    "channel": CONSTANTS.WS_SUBSCRIPTION_WALLET_ENDPOINT_NAME,
                    "instId": "default"
                }]
            }
            subscribe_balance_request = WSJSONRequest(payload=balance_payload)
            await ws.send(subscribe_balance_request)

            self.logger().info("Subscribed to private channels")

        except Exception:
            self.logger().error("Error subscribing to private channels", exc_info=True)
            raise

    async def _process_websocket_message(self, websocket_assistant: WSAssistant, message_queue: asyncio.Queue):
        """
        Processes incoming WebSocket messages
        """
        try:
            message = await asyncio.wait_for(
                websocket_assistant.receive(),
                timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL
            )

            if message:
                self._last_ws_message_sent_timestamp = time.time()
                message_queue.put_nowait(message)

        except asyncio.TimeoutError:
            await self._send_ping(websocket_assistant)
        except Exception:
            self.logger().error("Error processing WebSocket message", exc_info=True)

    async def _send_ping(self, websocket_assistant: WSAssistant):
        """
        Sends ping message to keep WebSocket connection alive
        """
        try:
            ping_request = WSJSONRequest(payload={"op": "ping"})
            await websocket_assistant.send(ping_request)
            self._last_ws_message_sent_timestamp = time.time()
        except Exception:
            self.logger().error("Error sending ping message", exc_info=True)

    async def listen_for_user_stream(self, output: asyncio.Queue):
        """
        Connects to the user WebSocket stream and puts the messages in the output queue
        """
        ws = None
        while True:
            try:
                ws = await self._connected_websocket_assistant()
                await self._authenticate(ws)
                await self._subscribe_channels(ws)

                while True:
                    await self._process_websocket_message(ws, output)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "Unexpected error while listening to user stream. Retrying after 30 seconds...",
                    exc_info=True
                )
                await self._sleep(30.0)
            finally:
                if ws is not None:
                    await ws.disconnect()
