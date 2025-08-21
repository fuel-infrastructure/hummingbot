# TODO: Just a template, needs modifying

import asyncio
import logging
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, TYPE_CHECKING, Tuple

import hummingbot.connector.exchange.bitget.bitget_constants as CONSTANTS
import hummingbot.connector.exchange.bitget.bitget_utils as bitget_utils
import hummingbot.connector.exchange.bitget.bitget_web_utils as web_utils
from hummingbot.connector.exchange.bitget.bitget_api_order_book_data_source import BitgetAPIOrderBookDataSource
from hummingbot.connector.exchange.bitget.bitget_api_user_stream_data_source import BitgetAPIUserStreamDataSource
from hummingbot.connector.exchange.bitget.bitget_auth import BitgetAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.estimate_fee import build_trade_fee
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter


class BitgetExchange(ExchangePyBase):
    """
    Bitget exchange connector for spot trading
    """

    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0
    POLL_INTERVAL = 1.0

    _logger: Optional[logging.Logger] = None

    @classmethod
    def logger(cls) -> logging.Logger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(
        self,
        client_config_map: "ClientConfigAdapter",
        bitget_api_key: str,
        bitget_secret_key: str,
        bitget_passphrase: str,
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        self._domain = domain
        self._time_synchronizer = TimeSynchronizer()
        self._throttler = web_utils.create_throttler()
        self._auth: BitgetAuth = BitgetAuth(
            api_key=bitget_api_key,
            secret_key=bitget_secret_key,
            passphrase=bitget_passphrase,
            time_provider=self._time_synchronizer,
        )
        self._api_factory = web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth,
        )

        super().__init__(client_config_map)

        self._trading_pairs = trading_pairs or []
        self._trading_required = trading_required

    @staticmethod
    def bitget_order_type(order_type: OrderType) -> str:
        return CONSTANTS.ORDER_TYPE_LIMIT if order_type is OrderType.LIMIT else CONSTANTS.ORDER_TYPE_MARKET

    @staticmethod
    def to_hb_order_type(bitget_type: str) -> OrderType:
        return OrderType.LIMIT if bitget_type == CONSTANTS.ORDER_TYPE_LIMIT else OrderType.MARKET

    @property
    def authenticator(self) -> BitgetAuth:
        return self._auth

    @property
    def name(self) -> str:
        if self._domain == "bitget_main":
            return "bitget"
        else:
            return f"bitget_{self._domain}"

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return self._domain

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def check_network_request_path(self):
        return CONSTANTS.SERVER_TIME_PATH_URL

    @property
    def trading_fees_request_path(self):
        return CONSTANTS.EXCHANGE_FEE_RATE_PATH_URL

    @property
    def user_stream_tracker_data_source_type(self) -> type:
        return BitgetAPIUserStreamDataSource

    @property
    def order_book_tracker_data_source_type(self) -> type:
        return BitgetAPIOrderBookDataSource

    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.LIMIT, OrderType.MARKET]

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        error_description = str(request_exception)
        is_time_synchronizer_related = ("timestamp" in error_description and "window" in error_description)
        return is_time_synchronizer_related

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return CONSTANTS.RET_CODE_ORDER_NOT_FOUND in str(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return CONSTANTS.RET_CODE_ORDER_NOT_FOUND in str(cancelation_exception)

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return self._api_factory

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return BitgetAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            domain=self._domain,
            api_factory=self._api_factory,
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return BitgetAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            domain=self._domain,
            api_factory=self._api_factory,
        )

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
        """
        Places an order on Bitget exchange
        """
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        api_params = {
            "symbol": symbol,
            "side": CONSTANTS.SIDE_BUY if trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL,
            "orderType": self.bitget_order_type(order_type),
            "quantity": str(amount),
            "clientOid": order_id,
        }

        if order_type is OrderType.LIMIT:
            api_params["price"] = str(price)
            api_params["timeInForce"] = CONSTANTS.TIME_IN_FORCE_GTC

        response = await self._api_request(
            path_url=CONSTANTS.ORDER_PLACE_PATH_URL,
            method=RESTMethod.POST,
            data=api_params,
            is_auth_required=True,
            trading_pair=trading_pair,
        )

        if not web_utils.is_rest_response_success(response):
            raise ValueError(f"Failed to place order: {web_utils.get_rest_response_error_message(response)}")

        data = web_utils.get_rest_response_data(response)
        return str(data["orderId"]), self.current_timestamp

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        """
        Cancels an order on Bitget exchange
        """
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        api_params = {
            "symbol": symbol,
            "orderId": tracked_order.exchange_order_id,
        }

        response = await self._api_request(
            path_url=CONSTANTS.ORDER_CANCEL_PATH_URL,
            method=RESTMethod.POST,
            data=api_params,
            is_auth_required=True,
            trading_pair=tracked_order.trading_pair,
        )

        if not web_utils.is_rest_response_success(response):
            raise ValueError(f"Failed to cancel order: {web_utils.get_rest_response_error_message(response)}")

        return True

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        """
        Formats the trading rules received from the exchange
        """
        rules = []
        trading_pair_rules = exchange_info_dict.get("data", [])

        for rule_dict in trading_pair_rules:
            try:
                trading_pair = bitget_utils.convert_from_exchange_symbol(rule_dict["symbol"])

                if rule_dict.get("status") != "online":
                    continue

                min_order_size = Decimal(rule_dict.get("minTradeAmount", "0"))
                tick_size = Decimal(rule_dict.get("priceScale", "0.01"))
                step_size = Decimal(rule_dict.get("quantityScale", "0.001"))

                rules.append(TradingRule(
                    trading_pair=trading_pair,
                    min_order_size=min_order_size,
                    min_price_increment=tick_size,
                    min_base_amount_increment=step_size,
                ))

            except Exception:
                self.logger().error(f"Error parsing trading rule for {rule_dict}", exc_info=True)
                continue

        return rules

    async def _update_trading_fees(self):
        """
        Updates trading fees from the exchange
        """
        try:
            response = await self._api_request(
                path_url=CONSTANTS.EXCHANGE_FEE_RATE_PATH_URL,
                method=RESTMethod.GET,
                is_auth_required=True,
            )

            if web_utils.is_rest_response_success(response):
                data = web_utils.get_rest_response_data(response)
                for fee_info in data:
                    trading_pair = bitget_utils.convert_from_exchange_symbol(fee_info["symbol"])
                    maker_fee = Decimal(fee_info.get("makerFeeRate", "0.001"))
                    taker_fee = Decimal(fee_info.get("takerFeeRate", "0.001"))

                    self._trading_fees[trading_pair] = build_trade_fee(
                        self.name,
                        is_maker=True,
                        base_currency=trading_pair.split("-")[0],
                        quote_currency=trading_pair.split("-")[1],
                        order_type=OrderType.LIMIT,
                        order_side=TradeType.BUY,
                        amount=Decimal("1"),
                        price=Decimal("1"),
                    )
        except Exception:
            self.logger().error("Failed to update trading fees", exc_info=True)

    async def _update_balances(self):
        """
        Updates account balances from the exchange
        """
        try:
            response = await self._api_request(
                path_url=CONSTANTS.BALANCE_PATH_URL,
                method=RESTMethod.GET,
                is_auth_required=True,
            )

            if web_utils.is_rest_response_success(response):
                data = web_utils.get_rest_response_data(response)
                self._account_balances.clear()
                self._account_available_balances.clear()

                for balance_entry in data:
                    asset = balance_entry["coin"]
                    total_balance = Decimal(balance_entry.get("available", "0")) + Decimal(balance_entry.get("frozen", "0"))
                    available_balance = Decimal(balance_entry.get("available", "0"))

                    self._account_balances[asset] = total_balance
                    self._account_available_balances[asset] = available_balance

        except Exception:
            self.logger().error("Failed to update balances", exc_info=True)

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        """
        Gets all trade updates for a specific order
        """
        try:
            symbol = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
            params = {
                "symbol": symbol,
                "orderId": order.exchange_order_id,
            }

            response = await self._api_request(
                path_url=CONSTANTS.TRADE_HISTORY_PATH_URL,
                method=RESTMethod.GET,
                params=params,
                is_auth_required=True,
                trading_pair=order.trading_pair,
            )

            trade_updates = []
            if web_utils.is_rest_response_success(response):
                data = web_utils.get_rest_response_data(response)
                for trade_data in data:
                    trade_update = TradeUpdate(
                        trade_id=str(trade_data["tradeId"]),
                        client_order_id=order.client_order_id,
                        exchange_order_id=str(trade_data["orderId"]),
                        trading_pair=order.trading_pair,
                        fee=build_trade_fee(
                            self.name,
                            is_maker=trade_data.get("isMaker", False),
                            base_currency=order.trading_pair.split("-")[0],
                            quote_currency=order.trading_pair.split("-")[1],
                            order_type=order.order_type,
                            order_side=order.trade_type,
                            amount=Decimal(trade_data["quantity"]),
                            price=Decimal(trade_data["price"]),
                        ),
                        fill_base_amount=Decimal(trade_data["quantity"]),
                        fill_quote_amount=Decimal(trade_data["quantity"]) * Decimal(trade_data["price"]),
                        fill_price=Decimal(trade_data["price"]),
                        fill_timestamp=int(trade_data["time"]) / 1000,
                    )
                    trade_updates.append(trade_update)

            return trade_updates

        except Exception:
            self.logger().error(f"Failed to get trade updates for order {order.client_order_id}", exc_info=True)
            return []

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        """
        Requests the status of a tracked order
        """
        try:
            symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
            params = {
                "symbol": symbol,
                "orderId": tracked_order.exchange_order_id,
            }

            response = await self._api_request(
                path_url=CONSTANTS.GET_ORDER_PATH_URL,
                method=RESTMethod.GET,
                params=params,
                is_auth_required=True,
                trading_pair=tracked_order.trading_pair,
            )

            if web_utils.is_rest_response_success(response):
                data = web_utils.get_rest_response_data(response)
                order_status = CONSTANTS.ORDER_STATE.get(data["status"], OrderState.OPEN)

                order_update = OrderUpdate(
                    client_order_id=tracked_order.client_order_id,
                    exchange_order_id=str(data["orderId"]),
                    trading_pair=tracked_order.trading_pair,
                    update_timestamp=int(data.get("updateTime", time.time() * 1000)) / 1000,
                    new_state=order_status,
                )

                return order_update
            else:
                raise ValueError(f"Failed to get order status: {web_utils.get_rest_response_error_message(response)}")

        except Exception as e:
            self.logger().error(f"Failed to request order status for {tracked_order.client_order_id}: {e}")
            raise

    def get_fee(self,
                base_currency: str,
                quote_currency: str,
                order_type: OrderType,
                order_side: TradeType,
                amount: Decimal,
                price: Optional[Decimal] = None,
                is_maker: Optional[bool] = None) -> TradeFeeBase:
        """
        Calculates the estimated fee for a trade
        """
        is_maker = is_maker or (order_type is OrderType.LIMIT)
        return build_trade_fee(
            self.name,
            is_maker,
            base_currency=base_currency,
            quote_currency=quote_currency,
            order_type=order_type,
            order_side=order_side,
            amount=amount,
            price=price or Decimal("1"),
        )

    async def _user_stream_event_listener(self):
        """
        Listens for user stream events and processes them
        """
        async for event_message in self._iter_user_event_queue():
            try:
                # Process different types of events
                if "arg" in event_message and "data" in event_message:
                    channel = event_message["arg"].get("channel")

                    if channel == CONSTANTS.WS_SUBSCRIPTION_ORDERS_ENDPOINT_NAME:
                        await self._process_order_event(event_message)
                    elif channel == CONSTANTS.WS_SUBSCRIPTION_EXECUTIONS_ENDPOINT_NAME:
                        await self._process_trade_event(event_message)
                    elif channel == CONSTANTS.WS_SUBSCRIPTION_WALLET_ENDPOINT_NAME:
                        await self._process_balance_event(event_message)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await self._sleep(5.0)

    async def _process_order_event(self, event_message: Dict[str, Any]):
        """
        Processes order update events from user stream
        """
        # TODO: Implement order event processing
        pass

    async def _process_trade_event(self, event_message: Dict[str, Any]):
        """
        Processes trade/fill events from user stream
        """
        # TODO: Implement trade event processing
        pass

    async def _process_balance_event(self, event_message: Dict[str, Any]):
        """
        Processes balance update events from user stream
        """
        # TODO: Implement balance event processing
        pass
