from PairTrading.authentication import AlpacaAuth
from PairTrading.authentication.enums import ConfigType

from alpaca.trading.client import TradingClient
from alpaca.trading.enums import AssetClass, AssetExchange, AssetStatus, OrderSide, TimeInForce
from alpaca.trading.requests import GetAssetsRequest, MarketOrderRequest
from alpaca.trading.models import Order, Position


class AlpacaTradingClient:
    def __init__(self, auth:AlpacaAuth):
        self.client:TradingClient = TradingClient(
            api_key=auth.api_key,
            secret_key=auth.secret_key,
            paper=auth.isPaper
        ) 
    
    @classmethod
    def create(cls, alpacaAuth:AlpacaAuth):
        if alpacaAuth.configType != ConfigType.ALPACA:
            raise AttributeError("the auth object is not for Alpaca client")
        return cls(auth=alpacaAuth)
    
    def getViableStocks(self) -> list[str]:
        
        allAssets:list = self.client.get_all_assets(
            GetAssetsRequest(
                status=AssetStatus.ACTIVE,
                asset_class=AssetClass.US_EQUITY
            )
        )      
        validAssets:list = [asset.symbol for asset in allAssets if (asset.fractionable==True and \
                                            asset.shortable==True and \
                                            asset.easy_to_borrow==True and \
                                            asset.exchange in (AssetExchange.NYSE, AssetExchange.AMEX, AssetExchange.NASDAQ) and \
                                            "." not in asset.symbol)] 
        
        return validAssets
    
    def openPositions(self, stockPair:list, notional:float) -> list[Order, Order]:
        
        # short the first stock
        shortOrder:Order = self.client.submit_order(order_data=MarketOrderRequest(
            symbol=stockPair[0],
            notional=notional,
            side=OrderSide.SELL,
            time_in_force=TimeInForce.DAY            
        ))
        
        # long the second stock
        longOrder:Order = self.client.submit_order(order_data=MarketOrderRequest(
            symbol=stockPair[1],
            notional=notional,
            side=OrderSide.BUY,
            time_in_force=TimeInForce.DAY            
        ))
        
        return [shortOrder, longOrder]
    
    def closePositions(self, stockPair:list) -> list[Order, Order]:
        
        # closed short position
        closedShortOrder:Order = self.client.close_position(stockPair[0])
        
        # closed long position
        closedLongOrder:Order = self.client.close_position(stockPair[1])
        
        return [closedShortOrder, closedLongOrder]