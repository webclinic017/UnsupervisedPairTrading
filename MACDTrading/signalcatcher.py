from ta.trend import MACD, SMAIndicator
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
          
    def canOpen(self, symbol:str) -> bool:
        closePrice:Series = self.client.getLongDaily(symbol)["close"]
        latestClose:float = self.client.getLastMinute(symbol)
        
        macdInd:MACD = MACD(
            close=closePrice
        )
        sma31:SMAIndicator = SMAIndicator(
            close=closePrice, 
            window=31
        )
        sma60:SMAIndicator = SMAIndicator(
            close=closePrice, 
            window=60
        )
        
        return (
            macdInd.macd().iloc[-1] > 0 and 
            latestClose > sma31.sma_indicator().iloc[-1] and 
            latestClose > sma60.sma_indicator().iloc[-1])
        
    def canClose(self, symbol:str)-> bool:
        closePrice:Series = self.client.getLongDaily(symbol)["close"]
        latestClose:float = self.client.getLastMinute(symbol)
        
        sma31:SMAIndicator = SMAIndicator(
            close=closePrice, 
            window=31
        )
        
        return latestClose < sma31.sma_indicator().iloc[-1]