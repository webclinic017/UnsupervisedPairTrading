from PairTrading.authentication import AlpacaAuth
from PairTrading.authentication.enums import ConfigType
from PairTrading.util.read import getRecentlyClosed

from alpaca.trading.client import TradingClient
from alpaca.trading.enums import AssetClass, AssetExchange, AssetStatus, OrderSide, TimeInForce
from alpaca.trading.requests import GetAssetsRequest, MarketOrderRequest, GetOrdersRequest
from alpaca.trading.models import Order, Position, TradeAccount, Asset, Clock

from datetime import date, datetime, timezone

class AlpacaTradingClient:
    # the trading client implements the singleton pattern
    _instance = None     
    def __new__(cls, auth:AlpacaAuth):
        if not cls._instance:
            cls._instance = super(AlpacaTradingClient, cls).__new__(AlpacaTradingClient)
        return cls._instance
    
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
    
    def getTimeTillMarketOpensInSeconds(self) -> float:
        clock:Clock = self.client.get_clock()
        
        if clock.is_open:
            return 0
        
        return (clock.next_open.replace(tzinfo=None) - datetime.now()).total_seconds()
    
    def getViableStocks(self) -> list[str]:
        
        allAssets:list[Asset] = self.client.get_all_assets(
            GetAssetsRequest(
                status=AssetStatus.ACTIVE,
                asset_class=AssetClass.US_EQUITY
            )
        )      
        recentlyClosed:dict[str, date] = getRecentlyClosed() if getRecentlyClosed() else {}
        validAssets:list[str] = [asset.symbol for asset in allAssets if (asset.fractionable==True and \
                                            asset.shortable==True and \
                                            asset.easy_to_borrow==True and \
                                            asset.exchange in (AssetExchange.NYSE, AssetExchange.AMEX, AssetExchange.NASDAQ) and \
                                            "." not in asset.symbol and \
                                            asset.symbol not in recentlyClosed)] 
        
        return validAssets
    
    def getAccountDetail(self) -> TradeAccount:
        return self.client.get_account()
    
    def getAllOpenPositions(self) -> dict[str, Position]:        
        openPositions:list[Position] = self.client.get_all_positions()
        res:dict[str, Position] = {position.symbol:position for position in openPositions}
        return res
    
    def getPairOrders(self, pairSymbols:tuple) -> list[Order]:
        return self.client.get_orders(
            GetOrdersRequest(
                symbols=list(pairSymbols)
            )
        )
    
    def openPositions(self, stockPair:tuple, notional:float) -> tuple[Order, Order]:
        
        if notional <= 2:
            raise ValueError("You cannot trade for less than 1 dollars")
        
        # short the first stock
        shortOrder:Order = self.client.submit_order(order_data=MarketOrderRequest(
            symbol=stockPair[0],
            notional=notional/2,
            side=OrderSide.SELL,
            time_in_force=TimeInForce.DAY            
        ))
        
        # long the second stock
        longOrder:Order = self.client.submit_order(order_data=MarketOrderRequest(
            symbol=stockPair[1],
            notional=notional/2,
            side=OrderSide.BUY,
            time_in_force=TimeInForce.DAY            
        ))
        
        return (shortOrder, longOrder)
    
    def closePositions(self, stockPair:tuple) -> tuple[Order, Order]:
        
        # closed short position
        closedShortOrder:Order = self.client.close_position(stockPair[0])
        
        # closed long position
        closedLongOrder:Order = self.client.close_position(stockPair[1])
        
        return (closedShortOrder, closedLongOrder)