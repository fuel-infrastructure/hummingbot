# TODO: Just a template, needs modifying

from typing import Dict, Optional, Any
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage


class BitgetOrderBook(OrderBook):
    """
    Order book implementation for Bitget exchange
    """

    @classmethod
    def snapshot_message_from_exchange(cls,
                                       msg: Dict[str, Any],
                                       timestamp: float,
                                       metadata: Optional[Dict] = None) -> OrderBookMessage:
        """
        Convert exchange order book snapshot message to OrderBookMessage
        """
        # Implementation will be added when WebSocket functionality is completed
        raise NotImplementedError

    @classmethod
    def diff_message_from_exchange(cls,
                                   msg: Dict[str, Any],
                                   timestamp: float,
                                   metadata: Optional[Dict] = None) -> OrderBookMessage:
        """
        Convert exchange order book diff message to OrderBookMessage
        """
        # Implementation will be added when WebSocket functionality is completed
        raise NotImplementedError

    @classmethod
    def trade_message_from_exchange(cls,
                                    msg: Dict[str, Any],
                                    timestamp: float,
                                    metadata: Optional[Dict] = None) -> OrderBookMessage:
        """
        Convert exchange trade message to OrderBookMessage
        """
        # Implementation will be added when WebSocket functionality is completed
        raise NotImplementedError
