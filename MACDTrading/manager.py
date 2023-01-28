from MACDTrading.signalcatcher import SignalCatcher
from MACDTrading.etfs import ETFs, ETF_TYPES
from lib.dataEngine import AlpacaDataClient
from lib.tradingClient import AlpacaTradingClient
from lib.patterns import Base, Singleton
from alpaca.trading.models import Position


class MACDManager(Base, metaclass=Singleton):
    
    def __init__(self, dataClient:AlpacaDataClient, tradingClient:AlpacaTradingClient, entryPercent:float):
        self.tradingClient:AlpacaTradingClient = tradingClient
        self.dataClient:AlpacaDataClient = dataClient
        self.entryPercent:float = entryPercent
        self.signalcatcher:SignalCatcher = SignalCatcher.create(self.dataClient)
        self.etfs:ETFs = ETFs.create(
            tradingClient=self.tradingClient, 
            dataClient=self.dataClient)
        
        self.candidates:dict = self.etfs.getCandidates()
        
    @classmethod
    def create(cls, dataClient:AlpacaDataClient, tradingClient:AlpacaTradingClient, entryPercent:float):
        return cls(
            dataClient=dataClient,
            tradingClient=tradingClient,
            entryPercent=entryPercent
        )
        
    
    def _getEnterableStocks(self, openedPositions:dict[str, Position]) -> list:
        
        leveragedOptions:list = self.candidates["leveraged"][ETF_TYPES.OPTIONS]
        leveragedNonOptions:list = self.candidates["leveraged"][ETF_TYPES.NON_OPTIONS]
        unleveragedOptions:list = self.candidates["unleveraged"][ETF_TYPES.OPTIONS]
        unleveragedNonOptions:list = self.candidates["unleveraged"][ETF_TYPES.NON_OPTIONS]
        
        candidates:list = leveragedOptions + leveragedNonOptions + unleveragedOptions + unleveragedNonOptions                     
        candidates:list = [stock for stock in candidates if stock not in openedPositions.keys() and \
                            self.signalcatcher.canOpen(stock)]
        
        if len(candidates) > 8 - len(openedPositions):
            candidates = candidates[:8]
            
        return candidates
    
    
    def openPositions(self) -> None:
        openedPositions:dict[str, Position] = self.tradingClient.openedPositions
        openedPositionSums:float = sum([abs(float(p.cost_basis)) for p in openedPositions.values()])
        
        stockCandidates:list = self._getEnterableStocks(openedPositions)        
        availableCash:float = float(self.tradingClient.accountDetail.equity) * self.entryPercent - openedPositionSums
        
        for symbol in stockCandidates:
            self.tradingClient.openMACDPosition(symbol, availableCash/len(stockCandidates))
            
    
    def _getCloseableStocks(self, openedPositions:dict[str, Position]) -> list:       
        return [stock for stock in openedPositions.keys() if self.signalcatcher.canClose(stock)]
    
    
    def closePositions(self) -> None:
        openedPositions:dict[str, Position] = self.tradingClient.openedPositions
        closeableStocks:list = self._getCloseableStocks(openedPositions)
        
        for symbol in closeableStocks:
            self.tradingClient.closeMACDPosition(symbol)