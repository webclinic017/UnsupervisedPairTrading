from MACDTrading.signalcatcher import SignalCatcher
from MACDTrading.etfs import ETFs, ETF_TYPES
from lib.dataEngine import AlpacaDataClient
from lib.tradingClient import AlpacaTradingClient
from lib.patterns import Base, Singleton
from alpaca.trading.models import Position
import logging
from pandas import Series
import time 

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
        
    
    def _getEnterableETFs(self, openedPositions:dict[str, Position]) -> list:
        
        leveragedOptions:list = self.candidates["leveraged"][ETF_TYPES.OPTIONS]
        leveragedNonOptions:list = self.candidates["leveraged"][ETF_TYPES.NON_OPTIONS]
        unleveragedOptions:list = self.candidates["unleveraged"][ETF_TYPES.OPTIONS]
        unleveragedNonOptions:list = self.candidates["unleveraged"][ETF_TYPES.NON_OPTIONS]
        
        candidates:list = leveragedOptions + leveragedNonOptions + unleveragedOptions + unleveragedNonOptions                     
        candidates:list = [stock for stock in candidates if stock not in openedPositions.keys() and \
                            self.signalcatcher.canOpen(stock)]
        
        if len(candidates) > 5 - len(openedPositions):
            candidates = candidates[:5]
            
        return candidates
    
    def _getEnterableEquities(self, openedPositions:dict[str, Position]) -> list:
        logger.info("start retrieving enterable equities ... ")
        start = time.perf_counter()
        equities = list(Series({stock:self.signalcatcher.getATR(stock) for stock in self.candidates if stock not in openedPositions.keys() and 
                    self.signalcatcher.canOpen(stock)}).sort_index(ascending=False).index)
        logger.info(f"retrieval complete. time taken: {round(time.perf_counter() - start, 2)} minutes")
            
        return equities
    
    
    def openPositions(self) -> None:
        openedPositions:dict[str, Position] = self.tradingClient.openedPositions
        openedPositionSums:float = sum([abs(float(p.cost_basis)) for p in openedPositions.values()])
        
        stockCandidates:list = self._getEnterableEquities(openedPositions)       
        logger.info(f"enterable stocks: {stockCandidates}")
        availableCash:float = float(self.tradingClient.accountDetail.equity) * self.entryPercent - openedPositionSums
        logger.info(f"available cash: ${round(availableCash, 2)}")
        
        if self.tradingClient.clock.is_open:
            entryNum:int = min(20 - len(openedPositions), len(stockCandidates))
            for i in range(entryNum):
                order = self.tradingClient.openMACDPosition(stockCandidates[i], availableCash/entryNum)
                logger.info(f"{stockCandidates[i]} bought    ----   entered amount: ${round(float(order.notional), 2)}")
            
            
    
    def _getCloseableStocks(self, openedPositions:dict[str, Position]) -> list:       
        return [stock for stock in openedPositions.keys() if self.signalcatcher.canClose(stock)]
    
    
    def closePositions(self) -> None:
        openedPositions:dict[str, Position] = self.tradingClient.openedPositions
        closeableStocks:list = self._getCloseableStocks(openedPositions)
        
        if self.tradingClient.clock.is_open:
            for symbol in closeableStocks:
                self.tradingClient.closeMACDPosition(symbol)
                logger.info(f"{symbol} position closed")