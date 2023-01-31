from ta.trend import MACD, SMAIndicator
from ta.volatility import AverageTrueRange
from lib.dataEngine import AlpacaDataClient
from pandas import Series


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
        latestClose:float = self.client.getLastMinute(symbol)
        todayMinute:Series = self.client.getHourly(symbol, days=1)["close"]
        
        macdInd:MACD = MACD(
            close=closePrice
        )
        sma31:SMAIndicator = SMAIndicator(
            close=closePrice, 
            window=31
        )
        sma21:SMAIndicator = SMAIndicator(
            close=closePrice, 
            window=21
        )
        sma60:SMAIndicator = SMAIndicator(
            close=closePrice, 
            window=60
        )
        
        return (
            macdInd.macd().iloc[-1] > 0 and 
            latestClose > sma31.sma_indicator().iloc[-1] and 
            (
                (closePrice - sma31.sma_indicator() < 0).iloc[-3:].any() or
                (todayMinute < sma31.sma_indicator().iloc[-1]).any() or 
                (
                    latestClose > sma21.sma_indicator().iloc[-1] and 
                    (closePrice < sma21.sma_indicator()).iloc[-3:].any() and 
                    not (closePrice < sma31.sma_indicator()).iloc[-3:].any()
                )
            ) and
            latestClose > sma60.sma_indicator().iloc[-1])
        
    def canClose(self, symbol:str)-> bool:
        closePrice:Series = self.client.getLongDaily(symbol)["close"]
        latestClose:float = self.client.getLastMinute(symbol)
        
        sma31:SMAIndicator = SMAIndicator(
            close=closePrice, 
            window=31
        )
        
        return latestClose < sma31.sma_indicator().iloc[-1]