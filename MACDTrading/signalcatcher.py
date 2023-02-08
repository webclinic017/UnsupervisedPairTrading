from ta.trend import MACD, SMAIndicator
from ta.volatility import AverageTrueRange
from lib.dataEngine import AlpacaDataClient
from pandas import Series, DataFrame
from datetime import date 


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
        
          
    def canOpen(self, minuteBars:DataFrame, dailyCloses:DataFrame) -> bool:
        
        
        
        todayOpen:float = minuteBars.iloc[0]["open"]
        latestClose:float = minuteBars.iloc[-1]["close"]
        
        macdInd:Series = MACD(
            close=dailyCloses.loc[date.today().strftime("%Y-%m-%d")]["close"]
        ).macd()
        sma60:Series = SMAIndicator(
            close=dailyCloses.loc[date.today().strftime("%Y-%m-%d")]["close"], 
            window=60
        ).sma_indicator()
        
        return (
                macdInd.iloc[-1] > 0 and 
                (macdInd.iloc[-21:-1] > 0).sum() == 0 and
                latestClose > sma60.iloc[-1] and  
                (
                    latestClose > todayOpen or 
                    (dailyCloses.iloc[-3:]["close"] > dailyCloses.iloc[-3:]["open"]).any()
                ) and 
                (closePrice < sma60).iloc[-3:].any() 
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