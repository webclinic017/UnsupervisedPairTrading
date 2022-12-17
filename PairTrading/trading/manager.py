from PairTrading.lib.tradingClient import AlpacaTradingClient
from PairTrading.util.read import readFromJson
from PairTrading.util.write import writeToJson
from PairTrading.authentication import AlpacaAuth

from alpaca.trading.models import TradeAccount, Position

import os
from datetime import date
class TradingManager:
    def __init__(self, tradingClient:AlpacaTradingClient, entryPercent:float, openedPositions:list[tuple]):
        self.tradingPairs:list[tuple] = []
        self.tradingClient:AlpacaTradingClient = None 
        self.entryPercent:float = 0
        
    @classmethod
    def create(cls, alpacaAuth:AlpacaAuth, entryPercent:float):
        tradingClient:AlpacaTradingClient = AlpacaTradingClient.create(alpacaAuth)
        return cls(
            tradingClient=tradingClient,
            entryPercent=entryPercent
        )
        
    
    def _readPairs(self) -> dict[tuple][list]:
        pairsDict:dict = readFromJson("saveddata/pairs/pairs.json")
        if not pairsDict:
            return None
        res:dict[tuple][list] = {(p.split(",")[0], p.split(",")[1]):pairsDict[p] for p in pairsDict.keys()}
        return res
    
    def _filterExistingPairPositions(self, pairs:dict[tuple][list], openedPositions:dict[str][Position]) -> dict[tuple][list]:
        if not pairs:
            return None
        res:dict[tuple][list] = pairs
        for stock1, stock2 in res.keys():
            if stock1 in openedPositions or stock2 in openedPositions:
                del res[(stock1, stock2)]
        return res
    
    def _fetchExistingPairPositions(self, pairs:dict[tuple][list], openedPositions:dict[str][Position]) -> dict[tuple][list]:
        if not pairs:
            return None 
        res:dict[tuple][list] = {}
        for stock1, stock2 in pairs.keys():
            if stock1 in openedPositions and stock2 in openedPositions:
                res[(stock1, stock2)] = [openedPositions[stock1], openedPositions[stock2]]
               
        return res        
    
    def _getTradingRecord(self) -> dict[tuple, date]:
        if os.path.exists("saveddata/pairs/pairs.json"):
            res:dict = readFromJson("saveddata/pairs/pairs.json")
            for pair, submitTime in res.items():
                res[(pair.split(",")[0], pair.split(",")[1])] = date.strptime(submitTime, "%Y-%m-%d")
        return res 
    
    def _dumpTradingRecord(self, record:dict[tuple, date]) -> None:
        
        for pair, submitTime in record.items():
            record[",".join([pair[0], pair[1]])] = submitTime.strftime("%Y-%m-%d")
            
        writeToJson(record, "saveddata/pairs/pairs.json")
    
    def openPositions(self) -> None:
        
        currOpenedPositions:dict[str][Position] = self.tradingClient.getAllOpenPositions()
        tradingPairs:dict[tuple, list] = self._filterExistingPairPositions(
            pairs=self._readPairs(),
            openedPositions=currOpenedPositions
        )
        if not tradingPairs:
            print("no viable trading pairs...")
            return
       
        availableCash:float = float(self.tradingClient.getAccountDetail().cash) * self.entryPercent
        
        tradeNums:int = (availableCash//(float(currOpenedPositions.values()[0].cost_basis)*2)) if \
            currOpenedPositions and (availableCash//(float(currOpenedPositions.values()[0].cost_basis)*2)) <= len(tradingPairs) else len(tradingPairs)
            
        notionalAmount:float = float(currOpenedPositions.values()[0].cost_basis)*2 if currOpenedPositions else (availableCash)/tradeNums
            
        tradingRecord:dict[tuple, date] = self._getTradingRecord()
        for i in range(tradeNums):
            shortOrder, longOrder = self.tradingClient.openPositions(
                stockPair=(self.tradingPairs[i][0], self.tradingPairs[i][1]), 
                notional=notionalAmount
            )           
            tradingRecord[(self.tradingPairs[i][0], self.tradingPairs[i][1])] = shortOrder.submitted_at.date()
            print(f"short {self.tradingPairs[i][0]} long {self.tradingPairs[i][1]} pair position opened")
            
        self._dumpTradingRecord(tradingRecord)
            
        
            
                         
                         
    def _getCloseablePairs(self) -> list[tuple][str]:
        res:list[tuple] = []        
        viablePairs:dict[tuple][list] = self._readPairs()
        currOpenedPositions:dict[str][Position] = self.tradingClient.getAllOpenPositions()      
         
        openedPairs:dict[tuple][list] = self._fetchExistingPairPositions(
            pairs=viablePairs, 
            openedPositions=currOpenedPositions
        )
        
        for pair, positions in openedPairs.items():
            meanPriceRatio:float = viablePairs[pair][1]
            currPriceRatio:float = float(positions[0].current_price) / float(positions[1].current_price)
        
            if currPriceRatio <= meanPriceRatio:
                res.append(pair)
                
        for pair, submitTime in self._getTradingRecord().items():
            if float((date.today() - submitTime).days) > 30:
                res.append(pair)
                      
        return res 
    
    def closePositions(self) -> None:
        currOpenedPositions:dict[str][Position] = self.tradingClient.getAllOpenPositions()           
        closeablePairs:list[tuple] = self._getCloseablePairs()
        
        if not closeablePairs:
            return 
        
        for pair in closeablePairs:
            order1, order2 = self.tradingClient.closePositions(pair)
            profit:float = (float(currOpenedPositions[pair[0]].avg_entry_price)-float(order1.filled_avg_price))*float(order1.filled_qty) + \
                (float(order2.filled_avg_price) - float(currOpenedPositions[pair[1]].avg_entry_price))*float(order2.filled_qty)
            print(f"closed {pair[0]} <-> {pair[1]} pair position. Profit: ${round(profit, 2)}")
        
    
        
    