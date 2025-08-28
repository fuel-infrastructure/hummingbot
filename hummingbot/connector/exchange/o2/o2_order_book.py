from typing import Any, Dict, Optional

from hummingbot.connector.exchange.o2 import o2_constants as CONSTANTS
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_row import OrderBookRow


class O2OrderBook(OrderBook):
    """
    O2 exchange-specific order book implementation.

    Provides message formatting and conversion utilities for transforming
    raw O2 exchange data into standardized Hummingbot OrderBookMessage objects.
    """

    @classmethod
    def snapshot_rest_message_from_exchange(cls,
                                            msg: Dict[str, Any],
                                            timestamp: float,
                                            metadata: Optional[Dict] = None) -> OrderBookMessage:
        if metadata:
            msg.update(metadata)

        orders = msg.get("orders", {})
        update_id = msg.get("timestamp", int(timestamp * 1000))

        trading_pair = msg.get("trading_pair", "")
        connector = msg.get("connector")

        if not trading_pair:
            raise ValueError(f"Missing trading_pair in order book snapshot message: {msg}")

        if trading_pair and connector:
            base_asset, quote_asset = trading_pair.split("-")
            amount_decimals = connector.get_asset_decimals(base_asset)
            price_decimals = connector.get_asset_decimals(quote_asset)
        elif trading_pair:
            base_asset, quote_asset = trading_pair.split("-")
            amount_decimals = CONSTANTS.ASSETS_DECIMALS_MAP.get(base_asset, CONSTANTS.DEFAULT_ASSET_DECIMALS)
            price_decimals = CONSTANTS.ASSETS_DECIMALS_MAP.get(quote_asset, CONSTANTS.DEFAULT_ASSET_DECIMALS)
        else:
            amount_decimals = CONSTANTS.DEFAULT_ASSET_DECIMALS
            price_decimals = CONSTANTS.DEFAULT_ASSET_DECIMALS

        bids = []
        for order in orders.get("buys", []):
            try:
                price = float(order["price"]) / (10 ** price_decimals)
                quantity = float(order["quantity"]) / (10 ** amount_decimals)
                bids.append(OrderBookRow(price, quantity, update_id))
            except (KeyError, ValueError, TypeError):
                continue
        asks = []
        for order in orders.get("sells", []):
            try:
                # Convert from scaled integers using asset-specific decimals
                price = float(order["price"]) / (10 ** price_decimals)
                quantity = float(order["quantity"]) / (10 ** amount_decimals)
                asks.append(OrderBookRow(price, quantity, update_id))
            except (KeyError, ValueError, TypeError):
                continue

        content = {
            "trading_pair": trading_pair,
            "update_id": update_id,
            "bids": bids,
            "asks": asks
        }

        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, content, timestamp=timestamp)

    @classmethod
    def snapshot_ws_message_from_exchange(cls,
                                          msg: Dict[str, Any],
                                          timestamp: float,
                                          metadata: Optional[Dict] = None) -> OrderBookMessage:
        if metadata:
            msg.update(metadata)

        view = msg.get("view", {})
        update_id = msg.get("timestamp", int(timestamp * 1000))

        trading_pair = msg.get("trading_pair", "")
        connector = msg.get("connector")

        if not trading_pair:
            raise ValueError(f"Missing trading_pair in order book snapshot message: {msg}")

        if trading_pair and connector:
            base_asset, quote_asset = trading_pair.split("-")
            amount_decimals = connector.get_asset_decimals(base_asset)
            price_decimals = connector.get_asset_decimals(quote_asset)
        elif trading_pair:
            base_asset, quote_asset = trading_pair.split("-")
            amount_decimals = CONSTANTS.ASSETS_DECIMALS_MAP.get(base_asset, CONSTANTS.DEFAULT_ASSET_DECIMALS)
            price_decimals = CONSTANTS.ASSETS_DECIMALS_MAP.get(quote_asset, CONSTANTS.DEFAULT_ASSET_DECIMALS)
        else:
            amount_decimals = CONSTANTS.DEFAULT_ASSET_DECIMALS
            price_decimals = CONSTANTS.DEFAULT_ASSET_DECIMALS

        bids = []
        for order in view.get("buys", []):
            try:
                price = float(order["price"]) / (10 ** price_decimals)
                quantity = float(order["quantity"]) / (10 ** amount_decimals)
                bids.append(OrderBookRow(price, quantity, update_id))
            except (KeyError, ValueError, TypeError):
                continue
        asks = []
        for order in view.get("sells", []):
            try:
                # Convert from scaled integers using asset-specific decimals
                price = float(order["price"]) / (10 ** price_decimals)
                quantity = float(order["quantity"]) / (10 ** amount_decimals)
                asks.append(OrderBookRow(price, quantity, update_id))
            except (KeyError, ValueError, TypeError):
                continue

        content = {
            "trading_pair": trading_pair,
            "update_id": update_id,
            "bids": bids,
            "asks": asks
        }

        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, content, timestamp=timestamp)

    @classmethod
    def diff_message_from_exchange(cls,
                                   msg: Dict[str, Any],
                                   timestamp: Optional[float] = None,
                                   metadata: Optional[Dict] = None) -> OrderBookMessage:
        if metadata:
            msg.update(metadata)

        # Extract order changes from O2 WebSocket message
        changes = msg.get("changes", {})
        update_id = int((timestamp or 0) * 1000)

        # Get trading pair to determine decimals
        trading_pair = msg.get("trading_pair", "")
        connector = msg.get("connector")

        if trading_pair and connector:
            base_asset, quote_asset = trading_pair.split("-")
            # Try to get decimals from connector (dynamic) first
            amount_decimals = connector.get_asset_decimals(base_asset)
            price_decimals = connector.get_asset_decimals(quote_asset)
        elif trading_pair:
            base_asset, quote_asset = trading_pair.split("-")
            # Fallback to constants if connector not available
            amount_decimals = CONSTANTS.ASSETS_DECIMALS_MAP.get(base_asset, CONSTANTS.DEFAULT_ASSET_DECIMALS)
            price_decimals = CONSTANTS.ASSETS_DECIMALS_MAP.get(quote_asset, CONSTANTS.DEFAULT_ASSET_DECIMALS)
        else:
            # Fallback to defaults if trading pair not found
            amount_decimals = CONSTANTS.DEFAULT_ASSET_DECIMALS
            price_decimals = CONSTANTS.DEFAULT_ASSET_DECIMALS

        # Process bid changes (O2 uses signed quantities)
        bids = []
        for order in changes.get("buys", []):
            try:
                # Convert from scaled integers using asset-specific decimals
                price = float(order["price"]) / (10 ** price_decimals)
                quantity = float(order["quantity"]) / (10 ** amount_decimals)
                # O2 diff protocol: positive = add/update, negative/zero = remove
                final_quantity = quantity if quantity > 0 else 0
                bids.append(OrderBookRow(price, final_quantity, update_id))
            except (KeyError, ValueError, TypeError):
                continue

        # Process ask changes (O2 uses signed quantities)
        asks = []
        for order in changes.get("sells", []):
            try:
                # Convert from scaled integers using asset-specific decimals
                price = float(order["price"]) / (10 ** price_decimals)
                quantity = float(order["quantity"]) / (10 ** amount_decimals)
                # O2 diff protocol: positive = add/update, negative/zero = remove
                final_quantity = quantity if quantity > 0 else 0
                asks.append(OrderBookRow(price, final_quantity, update_id))
            except (KeyError, ValueError, TypeError):
                continue

        content = {
            "trading_pair": trading_pair,
            "update_id": update_id,
            "bids": bids,
            "asks": asks
        }

        return OrderBookMessage(OrderBookMessageType.DIFF, content, timestamp=timestamp)

    @classmethod
    def trade_message_from_exchange(cls, msg: Dict[str, Any], metadata: Optional[Dict] = None) -> OrderBookMessage:
        """
        Creates a trade message from O2 WebSocket trade event.

        Expected O2 format (from actual source code):
        {
            "action": "subscribe_trades",
            "market_id": "0x...",  // hex-encoded Bytes32
            "trade": {
                "trade_id": 12345,    // TradeId (number)
                "side": "Buy" or "Sell",  // Side enum
                "total": 75000000,    // u128: quantity * price
                "quantity": 1500000,  // u64: scaled quantity
                "price": 50000000,    // u64: scaled price
                "timestamp": 1234567890  // u128: timestamp
            }
        }

        :param msg: Raw trade message from O2 WebSocket
        :param metadata: Additional metadata (must include trading_pair)
        :return: OrderBookMessage with trade data
        """
        if metadata:
            msg.update(metadata)

        # Extract trade data from O2 message
        trade_data = msg.get("trade", msg.get("trades", {}))
        if isinstance(trade_data, list) and trade_data:
            trade_data = trade_data[0]  # Take first trade if multiple

        # Parse trade information
        timestamp = trade_data.get("timestamp", msg.get("timestamp", 0))
        if isinstance(timestamp, str):
            timestamp = float(timestamp)

        # Determine trade type from O2 side field
        side = trade_data.get("side", "").lower()
        trade_type = float(TradeType.BUY.value) if side == "buy" else float(TradeType.SELL.value)

        # Get trading pair to determine decimals
        trading_pair = msg.get("trading_pair", "")
        connector = msg.get("connector")

        if trading_pair and connector:
            base_asset, quote_asset = trading_pair.split("-")
            # Try to get decimals from connector (dynamic) first
            amount_decimals = connector.get_asset_decimals(base_asset)
            price_decimals = connector.get_asset_decimals(quote_asset)
        elif trading_pair:
            base_asset, quote_asset = trading_pair.split("-")
            # Fallback to constants if connector not available
            amount_decimals = CONSTANTS.ASSETS_DECIMALS_MAP.get(base_asset, CONSTANTS.DEFAULT_ASSET_DECIMALS)
            price_decimals = CONSTANTS.ASSETS_DECIMALS_MAP.get(quote_asset, CONSTANTS.DEFAULT_ASSET_DECIMALS)
        else:
            # Fallback to defaults if trading pair not found
            amount_decimals = CONSTANTS.DEFAULT_ASSET_DECIMALS
            price_decimals = CONSTANTS.DEFAULT_ASSET_DECIMALS

        content = {
            "trading_pair": msg["trading_pair"],
            "trade_type": trade_type,
            "trade_id": trade_data.get("trade_id", str(timestamp)),
            "update_id": int(timestamp * 1000) if timestamp else 0,
            # Convert from scaled integers using asset-specific decimals
            "price": float(trade_data.get("price", 0)) / (10 ** price_decimals),
            "amount": float(trade_data.get("quantity", 0)) / (10 ** amount_decimals)
        }

        return OrderBookMessage(OrderBookMessageType.TRADE, content, timestamp=timestamp)
