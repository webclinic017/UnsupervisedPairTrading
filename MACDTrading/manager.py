from MACDTrading.signalcatcher import SignalCatcher
from MACDTrading.etfs import ETFs
from lib.dataEngine import AlpacaDataClient
from lib.tradingClient import AlpacaTradingClient
from lib.patterns import Base, Singleton
from alpaca.trading.models import Position
import logging
from pandas import Series
import time 
from datetime import date 

logger = logging.getLogger(__name__)


class MACDManager(Base, metaclass=Singleton):
    
    def __init__(self, dataClient:AlpacaDataClient, tradingClient:AlpacaTradingClient, entryPercent:float):
        self.tradingClient:AlpacaTradingClient = tradingClient
        self.dataClient:AlpacaDataClient = dataClient
        self.entryPercent:float = entryPercent
        self.signalcatcher:SignalCatcher = SignalCatcher.create(self.dataClient)
        self.etfs:ETFs = ETFs.create(
            tradingClient=self.tradingClient, 
            dataClient=self.dataClient)
        
        self.candidates:dict = self.etfs.getAllCandidates()
        
    @classmethod
    def create(cls, dataClient:AlpacaDataClient, tradingClient:AlpacaTradingClient, entryPercent:float):
        return cls(
            dataClient=dataClient,
            tradingClient=tradingClient,
            entryPercent=entryPercent
        )

    
    def _getEnterableEquities(self, openedPositions:dict[str, Position]={}) -> list:
        logger.info("start retrieving enterable equities ... ")
        start = time.perf_counter()
        equities = list(Series({stock:self.signalcatcher.getATR(stock) for stock in self.candidates if stock not in openedPositions.keys() and 
                    self.signalcatcher.canOpen(stock)}).sort_index(ascending=False).index)
        logger.info(f"retrieval complete. time taken: {round((time.perf_counter() - start)/60, 2)} minutes")
            
        return equities
    
    
    def openPositions(self) -> list:
        openedPositions:dict[str, Position] = self.tradingClient.openedPositions
        openedPositionSums:float = sum([abs(float(p.cost_basis)) for p in openedPositions.values()])
        
        stockCandidates:list = self._getEnterableEquities(openedPositions)       
        logger.info(f"enterable stocks: {stockCandidates}")
        availableCash:float = float(self.tradingClient.accountDetail.equity) * self.entryPercent - openedPositionSums
        logger.info(f"available cash: ${round(availableCash, 2)}")
        
        res:list = []
        
        if self.tradingClient.clock.is_open:
            entryNum:int = min(40 - len(openedPositions), len(stockCandidates))
            for i in range(entryNum):
                order = self.tradingClient.openMACDPosition(stockCandidates[i], availableCash/(40-len(openedPositions)))
                logger.info(f"{stockCandidates[i]} bought    ----   entered amount: ${round(float(order.notional), 2)}")
                res.append(stockCandidates[i])
                
        return res
            
            
    
    def _getCloseableStocks(self, openedPositions:dict[str, Position], openedToday:list) -> list:       
        return [stock for stock in openedPositions.keys() if stock not in openedToday and self.signalcatcher.canClose(stock)]
    
    
    def closePositions(self, openedToday:list=[]) -> None:
        openedPositions:dict[str, Position] = self.tradingClient.openedPositions
        closeableStocks:list = self._getCloseableStocks(openedPositions, openedToday=openedToday)
        
        if self.tradingClient.clock.is_open:
            for symbol in closeableStocks:
                self.tradingClient.closeMACDPosition(symbol)
                logger.info(f"{symbol} position closed")