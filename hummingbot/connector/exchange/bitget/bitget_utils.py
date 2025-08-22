from decimal import Decimal
from typing import Any, Dict

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
import hummingbot.connector.exchange.bitget.bitget_constants as CONSTANTS
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTCUSDT"
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.002"),
    taker_percent_fee_decimal=Decimal("0.002"),
    buy_percent_fee_deducted_from_returns=True
)


def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    """
    Verifies if a trading pair is enabled to operate with based on its exchange information
    :param exchange_info: the exchange spot information for a trading pair.
    :return: True if the trading pair is enabled, False otherwise
    """
    return exchange_info.get("status", None) == CONSTANTS.PAIR_ACTIVE_STR


class BitgetConfigMap(BaseConnectorConfigMap):
    connector: str = "bitget"
    bitget_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Bitget API key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    bitget_api_secret: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Bitget API secret",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    bitget_passphrase: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Bitget passphrase",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True
        }
    )
    model_config = ConfigDict(title="bitget")


KEYS = BitgetConfigMap.model_construct()
