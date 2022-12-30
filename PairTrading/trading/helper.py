from PairTrading.lib import AlpacaTradingClient
from PairTrading.util.read import getRecentlyClosed, getPairsFromTrainingJson
from PairTrading.util.write import dumpRecentlyClosed
from PairTrading.util.patterns import Singleton

from alpaca.trading.models import Position

from datetime import date, datetime

class PairInfoRetriever(metaclass=Singleton): 
    
    def __init__(
        self,
        tradingClient: AlpacaTradingClient
    ):
        self.tradingClent:AlpacaTradingClient = tradingClient
        
    @classmethod
    def create(cls, tradingClient:AlpacaTradingClient):
        return cls(tradingClient)
    
    @property 
    def trainedPairs(self) -> dict[tuple, list]:
        return getPairsFromTrainingJson()["final_pairs"]
        
    @property
    def recentlyClosedPositions(self) -> dict[str, date]:
        return getRecentlyClosed()
    
    @recentlyClosedPositions.setter
    def recentlyClosedPositions(self, rec:dict[str, date]) -> None:
        dumpRecentlyClosed(rec)
    
    def getTradablePairs(self, pairs:dict[tuple, list], openedPositions:dict[str, Position]) -> dict[tuple, list]:
        if not pairs:
            return None
        res:dict[tuple, list] = pairs.copy()
        for stock1, stock2 in pairs.keys():
            if stock1 in openedPositions or stock2 in openedPositions:
                del res[(stock1, stock2)]
            elif stock1 in self.recentlyClosedPositions or stock2 in self.recentlyClosedPositions:
                del res[(stock1, stock2)]
        return res
    
    def getCurrentlyOpenedPairs(self, pairs:dict[tuple, float], openedPositions:dict[str, Position]) -> dict[tuple, list]:
        if not pairs:
            return None 
        res:dict[tuple, list] = {}
        for stock1, stock2 in pairs.keys():
            if stock1 in openedPositions and stock2 in openedPositions:
                res[(stock1, stock2)] = [openedPositions[stock1], openedPositions[stock2]]
               
        return res      
    
    