from decimal import Decimal
from typing import Any, Dict

from pydantic import ConfigDict, Field, field_validator

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.connector.exchange.o2.o2_constants import DEFAULT_DOMAIN
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "FUEL-USDC"

# O2 doesn't have fees yet since it's in development
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.0"),
    taker_percent_fee_decimal=Decimal("0.0"),
    buy_percent_fee_deducted_from_returns=True,
)


def normalize_market_id(market_id: str) -> str:
    if not market_id:
        return market_id

    # Handle case-insensitive check for '0x' or '0X' prefix
    if market_id.lower().startswith("0x"):
        # If it has a prefix, ensure it's lowercase '0x'
        return "0x" + market_id[2:]
    else:
        # If no prefix, add it
        return f"0x{market_id}"


def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    # Basic validation - ensure we have required fields for new format
    required_fields = ["market_id", "base", "quote"]
    for field in required_fields:
        if field not in exchange_info:
            return False

    # Validate base info has required fields
    base_info = exchange_info["base"]
    required_base_fields = ["symbol", "asset", "decimals"]
    if not all(key in base_info for key in required_base_fields):
        return False

    # Validate quote info has required fields
    quote_info = exchange_info["quote"]
    required_quote_fields = ["symbol", "asset", "decimals"]
    if not all(key in quote_info for key in required_quote_fields):
        return False

    return True


class O2ConfigMap(BaseConnectorConfigMap):
    """Configuration for O2 Testnet connector"""
    connector: str = "o2"

    trading_account: str = Field(
        json_schema_extra={
            "prompt": "Enter your Fuel trading account address",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )

    model_config = ConfigDict(title="o2")

    @field_validator("trading_account", mode="before")
    @classmethod
    def validate_trading_account(cls, v: str):
        if not v:
            raise ValueError("Trading account is required")
        # Add 0x prefix if not present
        if not v.startswith("0x"):
            v = "0x" + v
        # Check length after potentially adding prefix
        if len(v) != 66:  # 0x + 64 hex characters
            raise ValueError(f"Invalid trading account length (expected 66 characters including 0x, got {len(v)})")
        return v


class O2LocalConfigMap(BaseConnectorConfigMap):
    """Configuration for O2 Local connector (for development)"""
    connector: str = "o2_local"

    trading_account: str = Field(
        json_schema_extra={
            "prompt": "Enter your Fuel trading account address (local development)",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )

    model_config = ConfigDict(title="o2_local")

    @field_validator("trading_account", mode="before")
    @classmethod
    def validate_trading_account(cls, v: str):
        if not v:
            raise ValueError("Trading account is required")
        # Add 0x prefix if not present
        if not v.startswith("0x"):
            v = "0x" + v
        # Check length after potentially adding prefix
        if len(v) != 66:  # 0x + 64 hex characters
            raise ValueError(f"Invalid trading account length (expected 66 characters including 0x, got {len(v)})")
        return v


# Default keys for the main O2 connector (testnet)
KEYS = O2ConfigMap.model_construct()

# Configuration for additional domains
OTHER_DOMAINS = ["o2_local"]
OTHER_DOMAINS_PARAMETER = {"o2_local": "local"}
OTHER_DOMAINS_EXAMPLE_PAIR = {"o2_local": "FUEL-USDC"}
OTHER_DOMAINS_DEFAULT_FEES = {"o2_local": DEFAULT_FEES}
OTHER_DOMAINS_KEYS = {"o2_local": O2LocalConfigMap.model_construct()}
