from decimal import Decimal

from hummingbot.strategy.hedge.hedge import HedgeStrategy
from hummingbot.strategy.hedge.hedge_config_map_pydantic import MAX_CONNECTOR, HedgeConfigMap
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple

#######################
###   None config   ###
#######################


def start(self):
    c_map: HedgeConfigMap = {
        "strategy": "hedge",
        "hedge_ratio": Decimal(1.0),
        "hedge_interval": Decimal(30),
        "min_trade_size": Decimal(0.0),
        "slippage": Decimal(0.02),
        "hedge_connector": "o2_local",
        "hedge_markets": ["FUEL-USDC"],
        "hedge_offsets": [Decimal(0.0)],
        "hedge_leverage": Decimal(1),
        "hedge_position_mode": "ONEWAY",
        "enable_auto_set_position_mode": False,
        "connector_0": {"connector": "o2", "markets": ["FUEL-USDC"], "offsets": [Decimal(-1850000)]},
        "connector_1": {"connector": None, "markets": None, "offsets": None},
        "connector_2": {"connector": None, "markets": None, "offsets": None},
        "connector_3": {"connector": None, "markets": None, "offsets": None},
        "connector_4": {"connector": None, "markets": None, "offsets": None},
        "value_mode": False,
    }
    hedge_connector = c_map["hedge_connector"].lower()
    hedge_markets = c_map["hedge_markets"]
    hedge_offsets = c_map["hedge_offsets"]
    offsets_dict = {hedge_connector: hedge_offsets}
    initialize_markets = [(hedge_connector, hedge_markets)]
    for i in range(MAX_CONNECTOR):
        connector_config = c_map[f"connector_{i}"]
        connector = connector_config["connector"]
        if not connector:
            continue
        connector = connector.lower()
        markets = connector_config["markets"]
        offsets_dict[connector] = connector_config["offsets"]
        initialize_markets.append((connector, markets))
    self.initialize_markets(initialize_markets)
    self.market_trading_pair_tuples = []
    offsets_market_dict = {}
    for connector, markets in initialize_markets:
        offsets = offsets_dict[connector]
        for market, offset in zip(markets, offsets):
            base, quote = market.split("-")
            market_info = MarketTradingPairTuple(self.markets[connector], market, base, quote)
            self.market_trading_pair_tuples.append(market_info)
            offsets_market_dict[market_info] = offset
    index = len(hedge_markets)
    hedge_market_pairs = self.market_trading_pair_tuples[0:index]
    market_pairs = self.market_trading_pair_tuples[index:]
    self.strategy = HedgeStrategy(
        config_map=c_map, hedge_market_pairs=hedge_market_pairs, market_pairs=market_pairs, offsets=offsets_market_dict
    )
