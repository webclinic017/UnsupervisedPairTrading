from authentication.auth import AlpacaAuth
from authentication.enums import ConfigType
from PairTrading.util.read import getRecentlyClosed
from lib.patterns import Singleton, Base 

from alpaca.trading.client import TradingClient
from alpaca.trading.enums import AssetClass, AssetExchange, AssetStatus, OrderSide, TimeInForce
from alpaca.trading.requests import GetAssetsRequest, MarketOrderRequest, GetOrdersRequest
from alpaca.trading.models import Order, Position, TradeAccount, Asset, Clock

from datetime import date, datetime, timezone
import time

class AlpacaTradingClient(Base, metaclass=Singleton):
    
    def __init__(self, auth:AlpacaAuth):
        self.client:TradingClient = TradingClient(
            api_key=auth.api_key,
            secret_key=auth.secret_key,
            paper=auth.isPaper
        ) 
        self._allStocks:list[Asset] = self.client.get_all_assets(
            GetAssetsRequest(
                status=AssetStatus.ACTIVE,
                asset_class=AssetClass.US_EQUITY
            )
        )
    
    @classmethod
    def create(cls, alpacaAuth:AlpacaAuth):
        if alpacaAuth.configType not in (ConfigType.ALPACA_MAIN, ConfigType.ALPACA_SIDE):
            raise AttributeError("the auth object is not for Alpaca client")
        return cls(auth=alpacaAuth)
    
    @property
    def allTradableStocks(self) -> list[str]:
        return [asset for asset in self._allStocks if asset.tradable and asset.fractionable]
    
    @property
    def clock(self) -> Clock:
        return self.client.get_clock()
    
    @property
    def secondsTillMarketOpens(self) -> int:
        clock:Clock = self.clock
        
        if clock.is_open:
            return 0
        
        return int((clock.next_open.replace(tzinfo=None) - clock.timestamp.replace(tzinfo=None)).total_seconds())

    
    
    def getViableStocks(self) -> list[str]:
             
        recentlyClosed:dict[str, date] = getRecentlyClosed() if getRecentlyClosed() else {}
        validAssets:list[str] = [asset.symbol for asset in self.allTradableStocks if (asset.fractionable==True and \
                                            asset.shortable==True and \
                                            asset.easy_to_borrow==True and \
                                            asset.exchange in (AssetExchange.NYSE, AssetExchange.AMEX, AssetExchange.NASDAQ) and \
                                            "." not in asset.symbol and \
                                            asset.symbol not in recentlyClosed)] 
        
        return validAssets
    
    @property
    def accountDetail(self) -> TradeAccount:
        return self.client.get_account()
    
    @property
    def openedPositions(self) -> dict[str, Position]:        
        openPositions:list[Position] = self.client.get_all_positions()
        res:dict[str, Position] = {position.symbol:position for position in openPositions}
        return res
    
    def getPairOrders(self, pairSymbols:tuple) -> list[Order]:
        self.client.get
        return self.client.get_orders(
            GetOrdersRequest(
                status="closed",
                symbols=list(pairSymbols)
            )
        )
        
        
    def openMACDPosition(self, symbol:str, entryAmount:float) -> Order:
        
        order = None
        
        if entryAmount < 0:
            raise ValueError(f"entry amount: ${round(entryAmount, 2)}  entry amount must be positive")
        
        for _ in range(3):
            try:
                order:Order = self.client.submit_order(
                    order_data=MarketOrderRequest(
                        symbol=symbol,
                        qty=entryAmount,
                        side=OrderSide.BUY,
                        time_in_force=TimeInForce.DAY
                    )
                )
            except:
                time.sleep(1)
        
        return order            
            
    def closeMACDPosition(self, symbol:str) -> Order:
        
        for _ in range(3):
            try:
                order:Order = self.client.close_position(symbol)
            except:
                time.sleep(1)
            finally:
                return order
                
   
    
    def openArbitragePositions(self, stockPair:tuple, shortQty:float) -> tuple[Order, Order]:
        
        if shortQty < 1:
            raise ValueError(f"{stockPair[0]} - {stockPair[1]}: insufficient shares number forecasted")
        
        # short the first stock
        for i in range(3):
            try:               
                shortOrder:Order = self.client.submit_order(order_data=MarketOrderRequest(
                    symbol=stockPair[0],
                    qty=shortQty,
                    side=OrderSide.SELL,
                    time_in_force=TimeInForce.DAY            
                ))
            except:
                time.sleep(1)
            finally:
                break
        
        time.sleep(1)
        filledShortOrder:Order = self.client.get_order_by_id(shortOrder.id)
        longNotional:float = float(filledShortOrder.filled_qty) * float(filledShortOrder.filled_avg_price)
        
        # long the second stock
        for i in range(3):
            try:
                longOrder:Order = self.client.submit_order(order_data=MarketOrderRequest(
                    symbol=stockPair[1],
                    notional=longNotional,
                    side=OrderSide.BUY,
                    time_in_force=TimeInForce.DAY            
                ))
            except:
                time.sleep(1)
            finally:
                break
        time.sleep(1)
        filledLongOrder:Order = self.client.get_order_by_id(longOrder.id)
        return (filledShortOrder, filledLongOrder)
    
    def closeArbitragePositions(self, stockPair:tuple) -> tuple[Order, Order]:
        
        # closed short position
        closedShortOrder:Order = self.client.close_position(stockPair[0])
        
        # closed long position
        closedLongOrder:Order = self.client.close_position(stockPair[1])
        
        return (closedShortOrder, closedLongOrder)