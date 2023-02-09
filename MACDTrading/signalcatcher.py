from ta.trend import MACD, SMAIndicator
from ta.volatility import AverageTrueRange
from lib.dataEngine import AlpacaDataClient
from pandas import Series, DataFrame
from datetime import datetime, date 

from dateutil.relativedelta import relativedelta


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
        
        macdInd:Series = MACD(
            close=dailyBars["close"]
        ).macd()
        sma60:Series = SMAIndicator(
            close=dailyBars["close"], 
            window=60
        ).sma_indicator()
        
        return (
                macdInd.iloc[-1] > 0 and 
                (macdInd.iloc[-21:-2] > 0).sum() == 0 and 
                latestClose > sma60.iloc[-1] and  
                latestClose > todayOpen and 
                (dailyBars["close"] < sma60).iloc[-2:].any()
            )
        
    def canClose(self, symbol:str)-> bool:
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
        
        stopLoss:float = sma31.iloc[-1] if sma31.iloc[-1] > sma60.iloc[-1] else \
            sma60.iloc[-1] - (sma60.iloc[-1] - sma31.iloc[-1])/4
            
        stopLoss:float = stopLoss if stopLoss < minuteBars["close"].min() else minuteBars["close"].min()
        
        return latestClose < stopLoss