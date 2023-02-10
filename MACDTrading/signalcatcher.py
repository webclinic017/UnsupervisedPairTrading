from ta.trend import MACD, SMAIndicator
from ta.volatility import AverageTrueRange
from lib.dataEngine import AlpacaDataClient
from pandas import Series, DataFrame
from datetime import datetime, date 

from dateutil.relativedelta import relativedelta
from alpaca.trading.models import Position, Order
import copy 


class SignalCatcher:
    
    def __init__(self, dataClient:AlpacaDataClient):
        self.client:AlpacaDataClient = dataClient 
        
    @classmethod
    def create(cls, dataClient:AlpacaDataClient):
        return cls(
            dataClient=dataClient
        )
        
    def getATR(self, symbol:str) -> float:
        priceDF = self.client.getDaily(symbol)
        avr = AverageTrueRange(
            high=priceDF["high"], 
            low=priceDF["low"], 
            close=priceDF["close"]
        )
        return avr.average_true_range().iloc[-1]
        
          
    def canOpen(self, symbol:str) -> bool:
          
        try:
            dailyBars:Series = self.client.getLongDaily(symbol)
            minuteBars = self.client.getMinutes(symbol).loc[symbol].loc[date.today().strftime("%Y-%m-%d")]
        except Exception as ex:
            print(f"{symbol}: {ex}")
            return False 
        
        todayOpen:float = minuteBars.iloc[0]["open"]
        latestClose:float = minuteBars.iloc[-1]["close"]
        
        macdCurr:Series = MACD(
            close=dailyBars["close"]
        ).macd().loc[symbol]
        
        macdPrev:Series = copy.deepcopy(macdCurr)
        while macdPrev.index[-1].date() >= date.today():
            macdPrev = macdPrev.iloc[:-1]
        
        
        return (
                macdCurr.loc[date.today().strftime("%Y-%m-%d")][0] > 0 and 
                (macdPrev.iloc[-21:] >= 0).sum() == 0 and 
                latestClose > todayOpen 
            )
        
    def canClose(self, symbol:str, position:Position, order:Order)-> bool:
        closePrice:Series = self.client.getLongDaily(symbol)["close"]
        minuteBars = self.client.getMinutes(symbol).loc[symbol].loc[date.today().strftime("%Y-%m-%d")]
        latestClose:float = minuteBars.iloc[-1]["close"]
        
        sma31:Series = SMAIndicator(
            close=closePrice, 
            window=31
        ).sma_indicator()
        sma60:Series = SMAIndicator(
            close=closePrice, 
            window=60
        ).sma_indicator()
        
        profitPercent:float = float(position.unrealized_plpc)
        daysElapsed = (date.today() - order.submitted_at.date()).days
        
        stopLoss:float = sma31.iloc[-1] if sma31.iloc[-1] > sma60.iloc[-1] else \
            sma60.iloc[-1] - (sma60.iloc[-1] - sma31.iloc[-1])/4
            
        stopLoss:float = stopLoss if stopLoss < minuteBars["close"].min() else minuteBars["close"].min()
        
        return (latestClose < stopLoss) or (daysElapsed <= 3 and profitPercent >= 0.15) or \
            (daysElapsed <= 10 and profitPercent >= 0.3) or (daysElapsed <= 20 and profitPercent >= 0.4)