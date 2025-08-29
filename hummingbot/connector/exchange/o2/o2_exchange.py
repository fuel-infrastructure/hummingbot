import asyncio
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.o2 import o2_constants as CONSTANTS, o2_utils, o2_web_utils as web_utils
from hummingbot.connector.exchange.o2.o2_api_order_book_data_source import O2APIOrderBookDataSource
from hummingbot.connector.exchange.o2.o2_auth import O2Auth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.utils.tracking_nonce import NonceCreator
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter


class O2Exchange(ExchangePyBase):
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0

    web_utils = web_utils

    def __init__(
            self,
            client_config_map: "ClientConfigAdapter",
            trading_account: str,
            trading_pairs: Optional[List[str]] = None,
            trading_required: bool = True,
            domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs or []

        self._auth: O2Auth = O2Auth()  # No auth required for O2 yet
        self._trading_pair_symbol_map: Optional[Mapping[str, str]] = None
        self._trading_pair_market_id_map: Optional[bidict] = None
        self._asset_decimals: Dict[str, int] = {}
        self._asset_id_to_symbol: Dict[str, str] = {}
        self._mapping_initialization_lock = asyncio.Lock()
        self._trade_account = trading_account
        self._nonce_creator = NonceCreator.for_milliseconds()

        super().__init__(client_config_map)

        self.logger().info("=====================================")
        self.logger().info("O2EXCHANGE CONSTRUCTOR CALLED!")
        self.logger().info(f"Trading account: {trading_account}")
        self.logger().info(f"Trading pairs: {trading_pairs}")
        self.logger().info(f"Trading required: {trading_required}")
        self.logger().info(f"Domain: {domain}")
        self.logger().info(f"REST URLs available: {CONSTANTS.REST_URLS}")
        self.logger().info(f"WSS URLs available: {CONSTANTS.WSS_URLS}")
        self.logger().info(f"Using REST URL: {CONSTANTS.REST_URLS.get(domain, 'NOT FOUND')}")
        self.logger().info(f"Using WSS URL: {CONSTANTS.WSS_URLS.get(domain, 'NOT FOUND')}")
        self.logger().info("O2Exchange __init__ completed!")
        self.logger().info("=====================================")

    @property
    def authenticator(self) -> O2Auth:
        return self._auth

    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return self._domain

    @property
    def trade_account(self) -> str:
        """The trade account provided during initialization"""
        return self._trade_account

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.MARKETS_PATH_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.MARKETS_PATH_URL

    @property
    def check_network_request_path(self):
        return CONSTANTS.HEALTH_PATH_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def ready(self) -> bool:
        """
        Returns True when the connector is ready to trade.
        """
        return all(self.status_dict.values())

    @property
    def status_dict(self) -> Dict[str, bool]:
        """
        Returns the status of various connector components.
        This is checked by Hummingbot to determine if the connector is ready.
        """
        # During initialization, return not ready
        if not hasattr(self, '_trading_rules'):
            return {
                "symbols_mapping_initialized": False,
                "order_books_initialized": False,
                "account_balance": False,
                "trading_rule_initialized": False,
                "user_stream_initialized": False,
            }

        # Use parent's status check
        return super().status_dict

    async def start_network(self):
        instance_info = f"pairs={self._trading_pairs}, req={self._trading_required}"
        self.logger().info(f"=== O2Exchange start_network() called ({instance_info}) ===")

        try:
            if not self._trade_account:
                self.logger().error("Trading account not found.")
                raise ValueError("Trading account not found")

            self.logger().info(f"Using trading account: {self._trade_account}")

            self.logger().info("Calling parent start_network...")
            await super().start_network()
            self.logger().info("O2Exchange network started successfully!")

            # Add multiple checks with delays to understand timing
            for i in range(5):
                await asyncio.sleep(1)
                self.logger().info(f"\n=== Order book status check {i + 1}/5 after {i + 1}s ===")

                if self.order_book_tracker:
                    ready = self.order_book_tracker.ready
                    self.logger().info(f"Order book tracker ready: {ready}")

                    if hasattr(self.order_book_tracker, '_order_books_initialized'):
                        event_set = self.order_book_tracker._order_books_initialized.is_set()
                        self.logger().info(f"Order book initialized event set: {event_set}")
                    else:
                        self.logger().info("Order book initialized event NOT FOUND")

                    self.logger().info(f"Order books count: {len(self.order_book_tracker.order_books)}")
                    self.logger().info(f"Order books keys: {list(self.order_book_tracker.order_books.keys())}")

                    # Check each order book
                    for pair, book in self.order_book_tracker.order_books.items():
                        try:
                            # bid_entries() and ask_entries() return iterators
                            bids_count = len(list(book.bid_entries()))
                            asks_count = len(list(book.ask_entries()))
                            # snapshot_uid might exist - let's check
                            snapshot_uid = getattr(book, 'snapshot_uid', 'N/A')
                            self.logger().info(f"  {pair}: snapshot_uid={snapshot_uid}, "
                                               f"bids={bids_count}, asks={asks_count}")
                        except Exception as e:
                            self.logger().error(f"  {pair}: Error checking book state - {e}")

                    # Check task states
                    if hasattr(self.order_book_tracker, '_init_order_books_task'):
                        task = self.order_book_tracker._init_order_books_task
                        if task:
                            self.logger().info(f"Init task: done={task.done()}, cancelled={task.cancelled()}")
                            if task.done() and not task.cancelled():
                                try:
                                    exc = task.exception()
                                    if exc:
                                        self.logger().error(f"Init task exception: {exc}")
                                except Exception:
                                    pass
                        else:
                            self.logger().info("Init task is None")
                    else:
                        self.logger().info("No _init_order_books_task attribute")

                    # If ready, break early
                    if ready:
                        self.logger().info("Order book is ready! Breaking check loop.")
                        break
                else:
                    self.logger().info("Order book tracker is None")

            self.logger().info("\n=== Final order book status after checks ===")
            self.logger().info(f"Order book ready: {self.order_book_tracker.ready if self.order_book_tracker else 'None'}")
            self.logger().info(f"Exchange ready: {self.ready}")
            self.logger().info(f"Status dict: {self.status_dict}")
        except Exception as e:
            self.logger().error(f"O2Exchange start_network failed: {e}", exc_info=True)
            raise

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def get_new_numeric_client_order_id(self) -> int:
        return self._nonce_creator.get_tracking_nonce()

    async def stop_network(self):
        if self.order_book_tracker is not None:
            self.order_book_tracker.stop()

        # Stop user stream tracker if it exists
        if hasattr(self, '_user_stream_tracker') and self._user_stream_tracker is not None:
            await self._user_stream_tracker.stop()

    def supported_order_types(self):
        return [OrderType.LIMIT]

    def buy(self,
            trading_pair: str,
            amount: Decimal,
            order_type: OrderType = OrderType.LIMIT,
            price: Decimal = s_decimal_NaN,
            **kwargs) -> str:
        self.logger().info(f"BUY order requested - Pair: {trading_pair}, Amount: {amount}, Price: {price}, Type: {order_type}")

        order_id = str(self.get_new_numeric_client_order_id())

        safe_ensure_future(self._create_order(
            order_id=order_id,
            trading_pair=trading_pair,
            amount=amount,
            trade_type=TradeType.BUY,
            order_type=order_type,
            price=price,
            **kwargs
        ))

        return order_id

    def sell(self,
             trading_pair: str,
             amount: Decimal,
             order_type: OrderType = OrderType.LIMIT,
             price: Decimal = s_decimal_NaN,
             **kwargs) -> str:
        self.logger().info(f"SELL order requested - Pair: {trading_pair}, Amount: {amount}, Price: {price}, Type: {order_type}")

        order_id = str(self.get_new_numeric_client_order_id())

        safe_ensure_future(self._create_order(
            order_id=order_id,
            trading_pair=trading_pair,
            amount=amount,
            trade_type=TradeType.SELL,
            order_type=order_type,
            price=price,
            **kwargs
        ))

        return order_id

    async def cancel_all(self, timeout_seconds: float) -> List[CancellationResult]:
        """
        Cancels all currently active orders.

        :param timeout_seconds: the maximum time (in seconds) the cancel logic should run
        :return: a list of CancellationResult instances, one for each of the orders to be cancelled
        """
        incomplete_orders = [o for o in self.in_flight_orders.values() if not o.is_done]

        if not incomplete_orders:
            return []

        cancellation_results = []

        try:
            async with asyncio.timeout(timeout_seconds):
                # Cancel each order individually
                for order in incomplete_orders:
                    try:
                        success = await self._place_cancel(order.client_order_id, order)
                        cancellation_results.append(CancellationResult(order.client_order_id, success))
                    except Exception as e:
                        self.logger().error(f"Failed to cancel order {order.client_order_id}: {e}")
                        cancellation_results.append(CancellationResult(order.client_order_id, False))

        except asyncio.TimeoutError:
            self.logger().warning(f"Cancel all orders timed out after {timeout_seconds} seconds")
            # Mark remaining orders as failed to cancel
            for order in incomplete_orders:
                if not any(r.order_id == order.client_order_id for r in cancellation_results):
                    cancellation_results.append(CancellationResult(order.client_order_id, False))

        return cancellation_results

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        # O2 doesn't have strict time synchronization requirements yet
        return False

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        error_str = str(status_update_exception)
        return "not found" in error_str.lower() and "order" in error_str.lower()

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        error_str = str(cancelation_exception)
        return "not found" in error_str.lower() and "order" in error_str.lower()

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(throttler=self._throttler, auth=self._auth)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return O2APIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            domain=self.domain,
            api_factory=self._web_assistants_factory,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        from hummingbot.connector.exchange.o2.o2_api_user_stream_data_source import O2APIUserStreamDataSource
        return O2APIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain
        )

    def _get_fee(
            self,
            base_currency: str,
            quote_currency: str,
            order_type: OrderType,
            order_side: TradeType,
            amount: Decimal,
            price: Decimal = s_decimal_NaN,
            is_maker: Optional[bool] = None,
    ) -> TradeFeeBase:
        """
        Get the trading fee for a specific order.
        Uses dynamic fees loaded from the markets endpoint.
        """
        # Determine if this is a maker or taker order
        is_maker = is_maker or (order_type is OrderType.LIMIT_MAKER)

        # Get the trading pair
        trading_pair = combine_to_hb_trading_pair(base_currency, quote_currency)

        # Check if we have fee data for this trading pair
        if trading_pair in self._trading_fees:
            fees_data = self._trading_fees[trading_pair]
            fee_percent = fees_data["makerFeeRate"] if is_maker else fees_data["takerFeeRate"]

            self.logger().debug(f"Using {'maker' if is_maker else 'taker'} fee {fee_percent} for {trading_pair}")

            # O2 deducts fees from the received amount (like most exchanges)
            return DeductedFromReturnsTradeFee(percent=fee_percent)
        else:
            # Fallback to zero fees if not found (O2 currently has zero fees in testnet)
            self.logger().debug(f"No fee data for {trading_pair}, using zero fees")
            return DeductedFromReturnsTradeFee(percent=Decimal("0"))

    async def _make_trading_rules_request(self) -> Any:
        """
        Make request to O2 markets endpoint for trading rules and pair symbols
        """
        try:
            from hummingbot.core.web_assistant.connections.data_types import RESTMethod
            self.logger().info("Making trading rules request to O2...")
            response = await self._api_request(
                method=RESTMethod.GET,
                path_url=CONSTANTS.MARKETS_PATH_URL,
                is_auth_required=False
            )
            self.logger().info(f"Trading rules response received: {response}")
            return response
        except Exception as e:
            self.logger().error(f"Failed to fetch trading rules from O2: {e}", exc_info=True)
            # Return empty structure to prevent initialization failures
            return {"markets": []}

    async def _make_trading_pairs_request(self) -> Any:
        """
        Make request to O2 markets endpoint for trading pairs
        Same as trading rules request for O2
        """
        return await self._make_trading_rules_request()

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        mapping = bidict()
        market_id_mapping = bidict()

        # Parse the new O2 API format
        markets_list = exchange_info.get("markets", [])
        for market_info in markets_list:
            try:
                # Extract market IDs
                raw_market_id = market_info.get("market_id")
                market_id = o2_utils.normalize_market_id(raw_market_id) if raw_market_id else None

                # Extract base and quote information from new format
                base_info = market_info.get("base", {})
                quote_info = market_info.get("quote", {})

                base_symbol = base_info.get("symbol")
                quote_symbol = quote_info.get("symbol")

                if not all([market_id, base_symbol, quote_symbol]):
                    self.logger().warning(f"Skipping incomplete market info: {market_info}")
                    continue

                # Store asset decimals and IDs from new format
                base_asset_id = base_info.get("asset")
                quote_asset_id = quote_info.get("asset")

                if base_asset_id:
                    self._asset_decimals[base_symbol] = base_info.get("decimals", CONSTANTS.DEFAULT_ASSET_DECIMALS)
                    self._asset_id_to_symbol[base_asset_id] = base_symbol

                if quote_asset_id:
                    self._asset_decimals[quote_symbol] = quote_info.get("decimals", CONSTANTS.DEFAULT_ASSET_DECIMALS)
                    self._asset_id_to_symbol[quote_asset_id] = quote_symbol

                # Create Hummingbot trading pair notation
                trading_pair = combine_to_hb_trading_pair(base_symbol, quote_symbol)
                exchange_symbol = f"{base_symbol}-{quote_symbol}"

                mapping[exchange_symbol] = trading_pair
                market_id_mapping[trading_pair] = market_id

            except Exception as e:
                self.logger().error(f"Error parsing market info {market_info}: {e}")
                continue

        # Use parent class method to set symbol mapping
        self._set_trading_pair_symbol_map(mapping)
        self._trading_pair_market_id_map = market_id_mapping

        self.logger().info(f"O2Exchange initialized {len(mapping)} trading pair symbols: {dict(mapping)}")
        self.logger().info(f"Asset decimals: {self._asset_decimals}")
        self.logger().info(f"Asset ID mappings loaded: {len(self._asset_id_to_symbol)} assets - {dict(self._asset_id_to_symbol)}")

    async def exchange_symbol_associated_to_pair(self, trading_pair: str) -> str:
        symbol_map = await self.trading_pair_symbol_map()
        return symbol_map.inverse[trading_pair]

    async def exchange_market_id_associated_to_pair(self, trading_pair: str) -> str:
        market_id_map = await self.trading_pair_market_id_map()
        if trading_pair not in market_id_map:
            # Log detailed info for debugging
            self.logger().error(f"Trading pair {trading_pair} not found in market ID map. Available pairs: {list(market_id_map.keys())}")
            raise ValueError(f"Trading pair {trading_pair} not found in market ID map")

        return market_id_map[trading_pair]

    async def trading_pair_market_id_map(self) -> bidict:
        if self._trading_pair_market_id_map is None:
            await self._update_trading_rules()
        return self._trading_pair_market_id_map or bidict()

    async def all_trading_pairs(self) -> List[str]:
        all_pairs: bidict = await self.trading_pair_symbol_map()
        return list(all_pairs.inverse.keys())

    def get_asset_decimals(self, asset_symbol: str) -> int:
        """Get the decimal places for an asset from dynamic config"""
        # First check dynamic decimals from API
        if asset_symbol in self._asset_decimals:
            return self._asset_decimals[asset_symbol]
        # Fallback to constants if not found (for backward compatibility)
        return CONSTANTS.ASSETS_DECIMALS_MAP.get(asset_symbol, CONSTANTS.DEFAULT_ASSET_DECIMALS)

    async def get_last_traded_prices(self, trading_pairs: List[str]) -> Dict[str, float]:
        """
        Get last traded prices for multiple trading pairs from O2 ticker endpoint
        """
        last_prices = {}

        for trading_pair in trading_pairs:
            try:
                price = await self._get_last_traded_price(trading_pair)
                last_prices[trading_pair] = price
            except Exception as e:
                self.logger().error(f"Error getting last price for {trading_pair}: {e}")
                last_prices[trading_pair] = 0.0

        return last_prices

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        """
        Get last traded price for a single trading pair from O2 ticker endpoint
        """
        try:
            market_id = await self.exchange_market_id_associated_to_pair(trading_pair=trading_pair)

            resp_json = await self._api_request(
                method=RESTMethod.GET,
                path_url=CONSTANTS.TICKER_PATH_URL,
                params={"market_id": market_id}
            )

            # O2 ticker returns an array with a single ticker object
            if resp_json and len(resp_json) > 0:
                ticker_data = resp_json[0]
                # Get the 'last' price from ticker data
                last_price_str = ticker_data.get("last", "0")

                # Get quote asset decimals for price conversion
                base_asset, quote_asset = trading_pair.split("-")
                price_decimals = self.get_asset_decimals(quote_asset)

                # Validate and convert from raw price to decimal
                if not last_price_str or last_price_str == "":
                    self.logger().warning(f"Empty last price for {trading_pair}, using 0")
                    return 0.0

                try:
                    # Ensure we have a valid string and handle None
                    price_str = str(last_price_str).strip()
                    last_price = Decimal(price_str) / Decimal(10 ** price_decimals)
                    return float(last_price)
                except (ValueError, TypeError) as e:
                    self.logger().error(f"Invalid price format '{last_price_str}' for {trading_pair}: {e}")
                    return 0.0

            return 0.0

        except Exception as e:
            self.logger().error(f"Error fetching last traded price for {trading_pair}: {e}")
            return 0.0

    # Placeholder methods for order management
    async def _place_order(
            self,
            order_id: str,
            trading_pair: str,
            amount: Decimal,
            trade_type: TradeType,
            order_type: OrderType,
            price: Decimal,
            **kwargs,
    ) -> Tuple[str, float]:
        # Get base and quote assets to determine decimals
        base_asset, quote_asset = trading_pair.split("-")
        amount_decimals = self.get_asset_decimals(base_asset)
        price_decimals = self.get_asset_decimals(quote_asset)

        # Convert amount and price to scaled integers based on asset decimals
        scaled_amount = int(amount * Decimal(10 ** amount_decimals))
        scaled_price = int(price * Decimal(10 ** price_decimals))

        data = {
            "side": "buy" if trade_type == TradeType.BUY else "sell",
            "market_id": await self.exchange_market_id_associated_to_pair(trading_pair),
            "quantity": str(scaled_amount),  # Send as scaled integer
            "price": str(scaled_price),  # Send as scaled integer
        }

        # Use ORDER_REST_URL specifically for order placement
        order_url = web_utils.order_rest_url(CONSTANTS.ORDERS_PATH_URL, domain=self.domain)

        rest_assistant = await self._web_assistants_factory.get_rest_assistant()
        resp = await rest_assistant.execute_request(
            url=order_url,
            method=RESTMethod.POST,
            data=data,
            is_auth_required=True,
            headers={"referer": CONSTANTS.HBOT_BROKER_ID},
            throttler_limit_id=CONSTANTS.ORDERS_PATH_URL,
        )

        self.logger().info(f"Order placement response: {resp}")

        if isinstance(resp, dict) and resp.get("error"):
            raise Exception(f"O2 API error: {resp.get('error')}")

        exchange_order_id = resp.get("order_id") if isinstance(resp, dict) else str(order_id)

        self.logger().info(f"Order placed - Client ID: {order_id}, Exchange ID: {exchange_order_id}")

        return str(exchange_order_id), self.current_timestamp

    async def _api_request_url(self, path_url: str, is_auth_required: bool = False) -> str:
        if is_auth_required:
            return web_utils.private_rest_url(path_url, domain=self.domain)
        else:
            return web_utils.public_rest_url(path_url, domain=self.domain)

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        try:
            # Get market ID for the trading pair
            market_id = await self.exchange_market_id_associated_to_pair(
                trading_pair=tracked_order.trading_pair
            )

            # Check if we have a valid exchange order ID for cancellation
            if not tracked_order.exchange_order_id:
                self.logger().warning(f"Cannot cancel order {order_id}: No exchange order ID available yet")
                return False

            cancel_data = {
                "order_id": tracked_order.exchange_order_id,
                "market_id": market_id
            }

            self.logger().info(f"Cancelling order - Client ID: {order_id}, Exchange ID: {tracked_order.exchange_order_id}, Market: {market_id}")

            cancel_url = web_utils.order_rest_url("/orders", domain=self.domain)

            cancel_response = await self._api_delete(
                path_url="/orders",
                overwrite_url=cancel_url,
                data=cancel_data,
                is_auth_required=True,
                limit_id=CONSTANTS.ORDERS_PATH_URL,
                headers={"referer": CONSTANTS.HBOT_BROKER_ID}
            )

            self.logger().info(f"Cancel response type: {type(cancel_response)}, value: {cancel_response}")

            if isinstance(cancel_response, dict):
                # Check for success response
                if cancel_response.get("success") is True or cancel_response.get("status") == "cancelled":
                    self.logger().info(f"Successfully cancelled order {order_id} on O2. Response: {cancel_response}")
                    return True

                # Check for error response
                elif cancel_response.get("error") or cancel_response.get("message"):
                    error_message = cancel_response.get("message", cancel_response.get("error", "Unknown error"))

                    if "not found" in str(error_message).lower():
                        self.logger().info(f"Order {order_id} not found on O2 (possibly already cancelled or filled): {error_message}")
                        return True
                    else:
                        # Other error messages
                        self.logger().error(f"Failed to cancel order {order_id}: {error_message}")
                        return False

                # Check for successful response - contains market_id and order_id
                elif "market_id" in cancel_response and "order_id" in cancel_response:
                    self.logger().info(f"Successfully cancelled order {order_id} on O2. Response: {cancel_response}")
                    return True

                # Handle any other dict response format
                else:
                    self.logger().warning(f"Unexpected response format for order {order_id} cancellation: {cancel_response}")
                    # If we don't have an error message, assume success
                    return True

            else:
                # Non-dict response (shouldn't happen based on the API specs provided)
                self.logger().warning(f"Unexpected non-dict response for order {order_id}: {cancel_response}")
                return False

        except Exception as e:
            # Handle specific error cases in exception message
            error_msg = str(e).lower()

            # If order is already cancelled or doesn't exist, consider it successful
            if any(phrase in error_msg for phrase in [
                "order not found",
                "already cancelled",
                "already filled",
                "order does not exist",
                "not found"
            ]):
                self.logger().info(f"Order {order_id} already cancelled or doesn't exist: {e}")
                return True

            # Log other cancellation errors
            self.logger().error(f"Failed to cancel order {order_id} on O2: {e}")
            return False

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        trade_updates = []

        if order.exchange_order_id is not None:
            try:
                # Get order details from O2 API
                market_id = await self.exchange_market_id_associated_to_pair(trading_pair=order.trading_pair)
                order_response = await self._api_get(
                    path_url=CONSTANTS.ORDER_PATH_URL,  # "/order"
                    params={
                        "market_id": market_id,
                        "order_id": order.exchange_order_id
                    },
                    is_auth_required=True
                )

                order_info = order_response.get("order", {})
                history = order_info.get("history", [])

                trade_events = [event for event in history if event.get("type") == "trade"]

                if trade_events:
                    # Get base and quote assets to determine decimals
                    base_asset, quote_asset = order.trading_pair.split("-")
                    amount_decimals = self.get_asset_decimals(base_asset)
                    price_decimals = self.get_asset_decimals(quote_asset)

                    total_fill_quantity = Decimal(str(order_info.get("quantity_fill", "0"))) / Decimal(10 ** amount_decimals)
                    fill_price = Decimal(str(order_info.get("price_fill", order_info.get("price", "0")))) / Decimal(10 ** price_decimals)

                    if total_fill_quantity > 0 and len(trade_events) > 0:
                        fill_per_trade = total_fill_quantity / len(trade_events)

                        for i, trade_event in enumerate(trade_events):
                            trade_id = trade_event.get("tx_id", f"trade_{i}")

                            fill_base_amount = fill_per_trade
                            fill_quote_amount = fill_base_amount * fill_price

                            fee = self._get_fee(
                                base_currency=order.base_asset,
                                quote_currency=order.quote_asset,
                                order_type=order.order_type,
                                order_side=order.trade_type,
                                amount=fill_base_amount,
                                price=fill_price
                            )

                            trade_timestamp = int(order_info.get("timestamp", 0)) * 1e-3

                            trade_update = TradeUpdate(
                                trade_id=trade_id,
                                client_order_id=order.client_order_id,
                                exchange_order_id=str(order_info.get("order_id", "")),
                                trading_pair=order.trading_pair,
                                fill_timestamp=trade_timestamp,
                                fill_price=fill_price,
                                fill_base_amount=fill_base_amount,
                                fill_quote_amount=fill_quote_amount,
                                fee=fee,
                                is_taker=True
                            )
                            trade_updates.append(trade_update)

            except Exception as e:
                self.logger().error(f"Error fetching trade updates for order {order.client_order_id}: {e}")

        return trade_updates

    async def _format_trading_rules(self, instrument_info_dict: Dict[str, Any]) -> List[TradingRule]:
        trading_rules = []

        if not instrument_info_dict.get("markets"):
            self.logger().warning("No markets data available for trading rules")
            return trading_rules

        # Extract markets from O2 response
        markets_list = instrument_info_dict.get("markets", [])

        for market_info in markets_list:
            try:
                if not o2_utils.is_exchange_information_valid(market_info):
                    continue

                # Get trading pair from new market info format
                base_info = market_info.get("base", {})
                quote_info = market_info.get("quote", {})

                base_symbol = base_info.get("symbol", "")
                quote_symbol = quote_info.get("symbol", "")

                if not base_symbol or not quote_symbol:
                    continue

                trading_pair = combine_to_hb_trading_pair(base_symbol, quote_symbol)

                # Use dynamic precision values from new API format
                base_max_precision = base_info.get("max_precision", 8)
                quote_max_precision = quote_info.get("max_precision", 8)
                base_decimals = base_info.get("decimals", 9)
                quote_decimals = quote_info.get("decimals", 6)

                # Calculate increments based on precision
                min_base_increment = Decimal(f"1e-{base_max_precision}")
                min_quote_increment = Decimal(f"1e-{quote_max_precision}")
                min_price_increment = Decimal(f"1e-{quote_max_precision}")

                # Use min_order from API or calculate based on precision
                min_order_str = base_info.get("min_order")
                if min_order_str:
                    min_order_size = Decimal(min_order_str) / Decimal(10 ** base_decimals)
                else:
                    min_order_size = Decimal(f"1e-{base_max_precision}")

                # Calculate minimum notional from quote min_order if available
                min_notional = Decimal("0.1")  # Default
                quote_min_order_str = quote_info.get("min_order")
                if quote_min_order_str:
                    min_notional = Decimal(quote_min_order_str) / Decimal(10 ** quote_decimals)

                trading_rule = TradingRule(
                    trading_pair=trading_pair,
                    min_order_size=min_order_size,
                    max_order_size=Decimal("1000000"),  # Large maximum
                    min_price_increment=min_price_increment,
                    min_base_amount_increment=min_base_increment,
                    min_quote_amount_increment=min_quote_increment,
                    min_notional_size=min_notional,
                    supports_limit_orders=True,  # O2 supports limit orders
                    supports_market_orders=False  # O2 only supports limit orders currently
                )

                trading_rules.append(trading_rule)
                self.logger().info(f"Created trading rule for {trading_pair} with conservative defaults")

            except Exception:
                self.logger().exception(f"Error parsing trading rule for {market_info}. Skipping.")

        return trading_rules

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        market_id = await self.exchange_market_id_associated_to_pair(trading_pair=tracked_order.trading_pair)
        updated_order_data = await self._api_get(
            path_url=CONSTANTS.ORDER_PATH_URL,  # "/order"
            params={
                "market_id": market_id,
                "order_id": tracked_order.exchange_order_id
            },
            is_auth_required=True
        )

        # Extract order info from O2 API response
        order_info = updated_order_data.get("order", {})

        # Determine order status from the history and order fields
        new_state = self._parse_o2_order_status(order_info)

        return OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(order_info.get("order_id", "")),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=int(order_info.get("timestamp", 0)) * 1e-3,
            new_state=new_state,
        )

    def _parse_o2_order_status(self, order_info: Dict[str, Any]) -> OrderState:
        is_closed = order_info.get("close", False)
        history = order_info.get("history", [])
        quantity = Decimal(str(order_info.get("quantity", "0")))
        quantity_fill = Decimal(str(order_info.get("quantity_fill", "0")))
        cancel_flag = order_info.get("cancel", False)

        if cancel_flag:
            return OrderState.CANCELED

        if is_closed:
            if quantity_fill > 0:
                if quantity_fill >= quantity:
                    return OrderState.FILLED
                else:
                    return OrderState.PARTIALLY_FILLED
            else:
                return OrderState.CANCELED

        if quantity_fill > 0:
            return OrderState.PARTIALLY_FILLED

        for event in history:
            if event.get("type") == "created" and event.get("status") == "confirmed":
                return OrderState.OPEN

        return OrderState.OPEN

    async def _update_balances(self):
        try:
            self.logger().info(f"_update_balances called. Current balances: {self._account_balances}")
            self.logger().info("Starting balance update...")

            # Ensure we have a trade account before querying balances
            if not self._trade_account:
                self.logger().warning("Cannot update balances: trade account not yet available")
                return

            if not self._trading_pairs:
                self.logger().warning(f"No trading pairs configured for balance updates. _trading_pairs={self._trading_pairs}")
                return

            self.logger().info(f"Trading pairs for balance update: {self._trading_pairs}")

            tracking_assets = set()
            for trading_pair in self._trading_pairs:
                try:
                    # Parse trading pair to get base and quote assets
                    base, quote = trading_pair.split("-")
                    tracking_assets.add(base)
                    tracking_assets.add(quote)
                except ValueError:
                    self.logger().warning(f"Invalid trading pair format: {trading_pair}")
                    continue

            # Use new O2 balance endpoint format
            # Fetch balance for each asset using the new endpoint
            rest_assistant = await self._web_assistants_factory.get_rest_assistant()

            for asset in tracking_assets:
                try:
                    # Get asset ID from symbol
                    asset_id = None
                    # Check if we have it from dynamic mapping
                    for aid, symbol in self._asset_id_to_symbol.items():
                        if symbol == asset:
                            asset_id = aid
                            break

                    if not asset_id:
                        self.logger().warning(f"No asset ID found for {asset} in dynamic mappings. "
                                              f"Available assets: {list(self._asset_id_to_symbol.values())}")
                        continue

                    # Use the owner account address as the contract parameter
                    params = {
                        "asset_id": asset_id,
                        "contract": self._trade_account
                    }

                    response = await rest_assistant.execute_request(
                        url=web_utils.public_rest_url(
                            path_url=CONSTANTS.BALANCE_PATH_URL,
                            domain=self._domain
                        ),
                        method=RESTMethod.GET,
                        params=params,
                        throttler_limit_id=CONSTANTS.BALANCE_PATH_URL,
                    )

                    self.logger().info(f"Balance response for {asset}: {response}")

                    # Parse the new response format
                    # Response: {"order_books": {...}, "total": "99600000000000", "trading_account_balance": "98600000000000"}
                    if isinstance(response, dict):
                        # Use trading_account_balance for available balance
                        trading_balance_str = response.get("trading_account_balance", "0")
                        total_balance_str = response.get("total", "0")

                        decimals = self.get_asset_decimals(asset)

                        # Convert from raw amount to decimal
                        trading_balance = Decimal(trading_balance_str) / Decimal(10 ** decimals)
                        total_balance = Decimal(total_balance_str) / Decimal(10 ** decimals)

                        self._account_balances[asset] = total_balance
                        self._account_available_balances[asset] = trading_balance

                        self.logger().info(f"Updated {asset} balance - Total: {total_balance}, Available: {trading_balance}")

                except Exception as e:
                    self.logger().error(f"Error fetching balance for {asset}: {e}")

            self.logger().info(f"Balance update completed for assets: {list(tracking_assets)}")
            self.logger().info(f"Final account balances: {self._account_balances}")
            self.logger().info(f"Final available balances: {self._account_available_balances}")

        except Exception as e:
            self.logger().error(f"Error updating balances: {e}", exc_info=True)

    async def _make_network_check_request(self):
        """
        Make a network check request to O2 by testing the health endpoint.
        Also fetches trading account if not already present.
        """
        try:
            # O2 health endpoint returns plain text "OK", not JSON
            # We need to make a raw request and get the text response
            rest_assistant = await self._web_assistants_factory.get_rest_assistant()
            url = web_utils.public_rest_url(
                path_url=CONSTANTS.HEALTH_PATH_URL,
                domain=self._domain
            )

            response = await rest_assistant.execute_request_and_get_response(
                url=url,
                method=RESTMethod.GET,
                throttler_limit_id=CONSTANTS.HEALTH_PATH_URL
            )

            # Get the text response
            text_response = await response.text()

            # Check if we got "OK" response
            if text_response.strip() != "OK":
                raise Exception(f"Unexpected health response: {text_response}")

            self.logger().debug("Health check passed")

        except Exception as e:
            self.logger().error(f"Network check failed: {e}")
            raise

    async def _update_trading_fees(self):
        """
        Update trading fees from the markets endpoint since O2 includes fees in market data
        """
        try:
            markets_response = await self._make_trading_rules_request()  # GET /markets

            for market_info in markets_response.get("markets", []):
                base_info = market_info.get("base", {})
                quote_info = market_info.get("quote", {})

                base_symbol = base_info.get("symbol")
                quote_symbol = quote_info.get("symbol")

                if base_symbol and quote_symbol:
                    trading_pair = combine_to_hb_trading_pair(base_symbol, quote_symbol)

                    # Store fees from market data
                    # O2 provides fees as strings, convert to Decimal
                    self._trading_fees[trading_pair] = {
                        "makerFeeRate": Decimal(market_info.get("maker_fee", "0")) / Decimal("100000"),
                        "takerFeeRate": Decimal(market_info.get("taker_fee", "0")) / Decimal("100000")
                    }

                    self.logger().debug(f"Updated fees for {trading_pair}: "
                                        f"maker={self._trading_fees[trading_pair]['makerFeeRate']}, "
                                        f"taker={self._trading_fees[trading_pair]['takerFeeRate']}")

            self.logger().info(f"Successfully updated trading fees for {len(self._trading_fees)} pairs")
        except Exception as e:
            self.logger().error(f"Failed to update trading fees: {e}", exc_info=True)
            # Don't raise - let the connector continue with default fees

    async def _user_stream_event_listener(self):
        """
        Listen to user stream events from O2 WebSocket and process order/trade updates
        """
        async for stream_message in self._iter_user_event_queue():
            try:
                await self._process_user_stream_event(stream_message)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error processing O2 user stream event")

    async def _process_user_stream_event(self, event_message: Dict[str, Any]):
        """
        Process individual user stream events from O2 WebSocket
        """
        try:
            channel = event_message.get("channel", "")
            action = event_message.get("action", "")

            if channel == "orders" and action == "subscribe_orders":
                # Process order updates
                self.logger().info(f"Processing order update event with {len(event_message.get('orders', []))} orders")
                await self._process_order_update_event(event_message)
            elif channel == "trades" and action == "subscribe_trades":
                # Process trade updates
                await self._process_trade_update_event(event_message)
            elif channel == "balances" and action == "subscribe_balances":
                # Process balance updates
                await self._process_balance_update_event(event_message)
            else:
                self.logger().debug(f"Unhandled O2 user stream event: {event_message}")

        except Exception:
            self.logger().exception(f"Error processing O2 user stream event: {event_message}")

    async def _process_order_update_event(self, event_message: Dict[str, Any]):
        try:
            # Check if the update is for our account
            identity = event_message.get("identity", {})
            contract_id = identity.get("ContractId", "")

            if not self._trade_account or contract_id.lower() != self._trade_account.lower():
                return

            orders = event_message.get("orders", [])
            is_first_batch = event_message.get("first", False)

            if is_first_batch:
                self.logger().info(f"Received initial order batch with {len(orders)} orders")

            for order_data in orders:
                # Extract O2 order information (using order_id as order identifier)
                order_id = str(order_data.get("order_id", ""))
                raw_market_id = order_data.get("market_id", "")
                market_id = o2_utils.normalize_market_id(raw_market_id)

                # Get trading pair to determine asset decimals
                trading_pair = await self._get_trading_pair_from_market_id(market_id)
                if trading_pair:
                    base_asset, quote_asset = trading_pair.split("-")
                    amount_decimals = self.get_asset_decimals(base_asset)
                    price_decimals = self.get_asset_decimals(quote_asset)
                else:
                    # Fallback if trading pair not found
                    amount_decimals = CONSTANTS.DEFAULT_ASSET_DECIMALS
                    price_decimals = CONSTANTS.DEFAULT_ASSET_DECIMALS

                # Convert quantities and prices from scaled integers
                quantity = Decimal(str(order_data.get("quantity", "0"))) / Decimal(10 ** amount_decimals)
                quantity_fill = Decimal(str(order_data.get("quantity_fill", "0"))) / Decimal(10 ** amount_decimals)
                price = Decimal(str(order_data.get("price", "0"))) / Decimal(10 ** price_decimals)
                price_fill = Decimal(str(order_data.get("price_fill", "0"))) / Decimal(10 ** price_decimals)
                is_closed = order_data.get("close", False)
                is_canceled = order_data.get("cancel", False)
                timestamp = int(order_data.get("timestamp", 0)) * 1e-3

                # Find the corresponding tracked order by exchange_order_id
                tracked_order = None
                for order in self._order_tracker.active_orders.values():
                    if order.exchange_order_id == order_id:
                        tracked_order = order
                        break

                if not tracked_order:
                    # Order not being tracked (could be from before bot started)
                    self.logger().debug(f"Untracked order {order_id}")
                    continue

                # Determine order state based on O2 data
                if is_canceled:
                    new_state = OrderState.CANCELED
                elif is_closed:
                    if quantity_fill >= quantity:
                        new_state = OrderState.FILLED
                    else:
                        new_state = OrderState.CANCELED
                else:
                    if quantity_fill > 0:
                        new_state = OrderState.PARTIALLY_FILLED
                    else:
                        new_state = OrderState.OPEN

                # Create order update
                order_update = OrderUpdate(
                    client_order_id=tracked_order.client_order_id,
                    exchange_order_id=order_id,
                    trading_pair=await self._get_trading_pair_from_market_id(market_id),
                    update_timestamp=timestamp,
                    new_state=new_state,
                )

                # Process the order update
                self._order_tracker.process_order_update(order_update)
                self.logger().info(f"✓ PRIMARY: Order update via subscribe_orders: {tracked_order.client_order_id} -> {new_state} (filled: {quantity_fill}/{quantity})")

                # Create trade update if there's a fill
                if quantity_fill > tracked_order.executed_amount_base:
                    fill_amount = quantity_fill - tracked_order.executed_amount_base
                    trade_update = TradeUpdate(
                        trade_id=f"{order_id}_{int(timestamp)}",
                        client_order_id=tracked_order.client_order_id,
                        exchange_order_id=order_id,
                        trading_pair=tracked_order.trading_pair,
                        fill_timestamp=timestamp,
                        fill_price=price_fill if price_fill > 0 else price,
                        fill_base_amount=fill_amount,
                        fill_quote_amount=fill_amount * (price_fill if price_fill > 0 else price),
                        fee=self._get_fee(
                            base_currency=tracked_order.base_asset,
                            quote_currency=tracked_order.quote_asset,
                            order_type=tracked_order.order_type,
                            order_side=tracked_order.trade_type,
                            amount=fill_amount,
                            price=price_fill if price_fill > 0 else price
                        ),
                        is_taker=True  # Assuming taker for O2
                    )

                    self._order_tracker.process_trade_update(trade_update)
                    self.logger().info(f"✓ PRIMARY: Trade update from order fill: {fill_amount} @ {price_fill if price_fill > 0 else price}")

        except Exception:
            self.logger().exception(f"Error processing O2 order update: {event_message}")

    async def _process_trade_update_event(self, event_message: Dict[str, Any]):
        """
        Process O2 trade update events as FALLBACK for order matching
        (Primary mechanism is now subscribe_orders in _process_order_update_event)
        """
        try:
            trade_data = event_message.get("trade", {})
            raw_market_id = event_message.get("market_id", "")
            market_id = o2_utils.normalize_market_id(raw_market_id)

            if not trade_data:
                return

            # Extract trade information
            trade_id = str(trade_data.get("trade_id", ""))
            trading_pair = await self._get_trading_pair_from_market_id(market_id)
            trade_side = trade_data.get("side", "")

            # Get asset decimals
            if trading_pair:
                base_asset, quote_asset = trading_pair.split("-")
                amount_decimals = self.get_asset_decimals(base_asset)
                price_decimals = self.get_asset_decimals(quote_asset)
            else:
                amount_decimals = CONSTANTS.DEFAULT_ASSET_DECIMALS
                price_decimals = CONSTANTS.DEFAULT_ASSET_DECIMALS

            # Convert from scaled integers
            quantity = Decimal(str(trade_data.get("quantity", "0"))) / Decimal(10 ** amount_decimals)
            price = Decimal(str(trade_data.get("price", "0"))) / Decimal(10 ** price_decimals)
            timestamp = int(trade_data.get("timestamp", 0)) * 1e-3

            self.logger().debug(f"FALLBACK: Processing O2 trade: {trade_id} {trade_side} {quantity} @ {price}")

            matching_order = self._find_matching_order(trading_pair, trade_side, price, quantity, timestamp)

            if matching_order:
                expected_executed = matching_order.executed_amount_base + quantity
                if abs(matching_order.executed_amount_base - expected_executed) < Decimal("0.0001"):
                    self.logger().debug(f"FALLBACK: Trade {trade_id} already processed by subscribe_orders, skipping")
                    return

                trade_update = TradeUpdate(
                    trade_id=f"fallback_{trade_id}",
                    client_order_id=matching_order.client_order_id,
                    exchange_order_id=matching_order.exchange_order_id or "",
                    trading_pair=trading_pair,
                    fill_timestamp=timestamp,
                    fill_price=price,
                    fill_base_amount=quantity,
                    fill_quote_amount=quantity * price,
                    fee=self._get_fee(
                        base_currency=matching_order.base_asset,
                        quote_currency=matching_order.quote_asset,
                        order_type=matching_order.order_type,
                        order_side=matching_order.trade_type,
                        amount=quantity,
                        price=price
                    ),
                    is_taker=True  # Assuming taker for O2
                )

                # Process the trade update
                self._order_tracker.process_trade_update(trade_update)
                self.logger().info(f"⚠️ FALLBACK: Matched trade {trade_id} to order {matching_order.client_order_id}")
            else:
                # Trade doesn't match any of our orders (from another user or market data)
                self.logger().debug(f"FALLBACK: Trade {trade_id} doesn't match any bot orders - market trade")

        except Exception:
            self.logger().exception(f"Error processing O2 trade update: {event_message}")

    def _find_matching_order(self, trading_pair: str, trade_side: str, trade_price: Decimal,
                             trade_quantity: Decimal, trade_timestamp: float) -> Optional[InFlightOrder]:
        """
        Find bot order that matches this trade using heuristics
        """
        PRICE_TOLERANCE = Decimal("0.01")  # 1 cent tolerance
        TIME_WINDOW = 300  # 5 minutes in seconds
        current_time = self.current_timestamp

        for order in self._order_tracker.active_orders.values():
            if order.trading_pair != trading_pair:
                continue

            expected_side = "Buy" if order.trade_type.name == "BUY" else "Sell"
            if trade_side != expected_side:
                continue

            if order.price is None:
                continue

            price_diff = abs(trade_price - order.price)
            if price_diff > PRICE_TOLERANCE:
                continue

            remaining_quantity = order.amount - order.executed_amount_base
            if trade_quantity > remaining_quantity + Decimal("0.00000001"):  # Small tolerance for rounding
                continue

            time_since_order = current_time - order.creation_timestamp
            if time_since_order > TIME_WINDOW:
                continue

            if order.current_state not in [OrderState.OPEN, OrderState.PARTIALLY_FILLED]:
                continue

            return order

        return None

    async def _process_balance_update_event(self, event_message: Dict[str, Any]):
        try:
            # Log when balance update arrives to detect race conditions
            self.logger().info(f"Balance WebSocket update received. Asset mappings loaded: {len(self._asset_id_to_symbol)} assets")

            # Check if the update is for our account
            identity = event_message.get("identity", {})
            contract_id = identity.get("ContractId", "")

            if not self._trade_account or contract_id.lower() != self._trade_account.lower():
                return

            # Extract balance information
            asset_id = event_message.get("asset_id")
            total_str = event_message.get("total", "0")
            trading_balance_str = event_message.get("trading_account_balance", "0")

            if not asset_id:
                self.logger().warning(f"Balance update missing asset_id: {event_message}")
                return

            # Get asset symbol from asset ID
            asset_symbol = self._asset_id_to_symbol.get(asset_id)
            if not asset_symbol:
                self.logger().warning(f"Unknown asset ID in balance update: {asset_id}. "
                                      f"Asset not found in dynamic mappings (_asset_id_to_symbol). "
                                      f"This may indicate: 1) Race condition where balance arrived before markets loaded, "
                                      f"or 2) Asset not available in /markets endpoint. "
                                      f"Current mappings: {list(self._asset_id_to_symbol.keys())}")
                return

            # Get decimals for conversion
            decimals = self.get_asset_decimals(asset_symbol)

            # Convert raw amounts to decimal
            total_balance = Decimal(total_str) / (Decimal("10") ** decimals)
            available_balance = Decimal(trading_balance_str) / (Decimal("10") ** decimals)

            # Update balances
            self._account_balances[asset_symbol] = total_balance
            self._account_available_balances[asset_symbol] = available_balance

            self.logger().debug(
                f"Updated {asset_symbol} balance from WebSocket: "
                f"total={total_balance}, available={available_balance}"
            )

        except Exception:
            self.logger().exception(f"Error processing O2 balance update: {event_message}")

    async def _get_trading_pair_from_market_id(self, market_id: str) -> str:
        """
        Helper method to get trading pair from market ID
        """
        try:
            normalized_market_id = o2_utils.normalize_market_id(market_id)
            market_id_map = await self.trading_pair_market_id_map()
            return market_id_map.get(normalized_market_id, "UNKNOWN-PAIR")
        except Exception:
            self.logger().exception(f"Error getting trading pair for market_id: {market_id}")
            return "UNKNOWN-PAIR"

    async def _create_order(self,
                            order_id: str,
                            trading_pair: str,
                            amount: Decimal,
                            trade_type: TradeType,
                            order_type: OrderType,
                            price: Decimal,
                            **kwargs):
        self.start_tracking_order(
            order_id=order_id,
            exchange_order_id=None,
            trading_pair=trading_pair,
            trade_type=trade_type,
            price=price,
            amount=amount,
            order_type=order_type
        )

        try:
            exchange_order_id, timestamp = await self._place_order(
                order_id=order_id,
                trading_pair=trading_pair,
                amount=amount,
                trade_type=trade_type,
                order_type=order_type,
                price=price,
                **kwargs
            )

            tracked_order = self._order_tracker.fetch_tracked_order(order_id)
            if tracked_order:
                tracked_order.update_exchange_order_id(exchange_order_id)

        except Exception as e:
            self.logger().error(f"Failed to create order {order_id}: {e}")
            self.stop_tracking_order(order_id)
            raise
