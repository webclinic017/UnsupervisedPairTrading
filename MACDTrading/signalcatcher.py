from ta.trend import MACD, SMAIndicator
from ta.volatility import AverageTrueRange
from lib.dataEngine import AlpacaDataClient
from pandas import Series
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
        
          
    def canOpen(self, symbol:str) -> bool:
        closePrice:Series = self.client.getLongDaily(symbol)["close"]
        minuteBars = self.client.getMinutes(symbol).loc[symbol]
        todayOpen:float = minuteBars.loc[date.today().strftime("%Y-%m-%d")].iloc[0]["open"]
        latestClose:float = minuteBar.iloc[-1]["close"]
        
        macdInd:MACD = MACD(
            close=closePrice
        )
        sma60:SMAIndicator = SMAIndicator(
            close=closePrice, 
            window=60
        )
        
        return (
                macdInd.macd().iloc[-1] > 0 and 
                (macdInd.macd().iloc[-31:-1] > 0).sum() == 0 and
                latestClose > sma60.sma_indicator().iloc[-1] and  
                latestClose > todayOpen and 
                (closePrice < sma60.sma_indicator()).iloc[-2:].any() 
            )
        
    def canClose(self, symbol:str)-> bool:
        closePrice:Series = self.client.getLongDaily(symbol)["close"]
        latestClose:float = self.client.getLastMinute(symbol)
        
        sma31:SMAIndicator = SMAIndicator(
            close=closePrice, 
            window=31
        )
        sma60:SMAIndicator = SMAIndicator(
            close=closePrice, 
            window=60
        )
        
        stopLoss:float = sma31.sma_indicator().iloc[-1] if sma31.sma_indicator().iloc[-1] > sma60.sma_indicator().iloc[-1] else \
            sma60.sma_indicator().iloc[-1] - (sma60.sma_indicator().iloc[-1] - sma31.sma_indicator().iloc[-1])/4
        
        return latestClose < stopLoss