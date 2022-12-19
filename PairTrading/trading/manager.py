from PairTrading.lib.tradingClient import AlpacaTradingClient
from PairTrading.util.read import readFromJson, getRecentlyClosed, getTradingRecord, getPairsFromTrainingJson
from PairTrading.util.write import writeToJson, dumpRecentlyClosed, dumpTradingRecord
from PairTrading.authentication import AlpacaAuth

from alpaca.trading.models import TradeAccount, Position, Order

import os
from datetime import date, datetime
class TradingManager:
    _instance = None 
    def __new__(cls, tradingClient:AlpacaTradingClient, entryPercent:float):
        if not cls._instance:
            cls._instance = super(TradingManager, cls).__new__(TradingManager)
        return cls._instance 
    
    def __init__(self, tradingClient:AlpacaTradingClient, entryPercent:float):
        self.tradingClient:AlpacaTradingClient = tradingClient
        self.entryPercent:float = 0    
        
    @classmethod
    def create(cls, alpacaAuth:AlpacaAuth, entryPercent:float):
        tradingClient:AlpacaTradingClient = AlpacaTradingClient.create(alpacaAuth)
        return cls(
            tradingClient=tradingClient,
            entryPercent=entryPercent
        )
    
    @property
    def tradingPairs(self) -> dict[tuple, list]:
        getPairsFromTrainingJson()["final_pairs"]
    
    def _filterExistingPairPositions(self, pairs:dict[tuple, list], openedPositions:dict[str, Position]) -> dict[tuple, list]:
        if not pairs:
            return None
        res:dict[tuple, list] = pairs
        for stock1, stock2 in res.keys():
            if stock1 in openedPositions or stock2 in openedPositions:
                del res[(stock1, stock2)]
        return res
    
    def _fetchExistingPairPositions(self, pairs:dict[tuple, float], openedPositions:dict[str, Position]) -> dict[tuple, list]:
        if not pairs:
            return None 
        res:dict[tuple, list] = {}
        for stock1, stock2 in pairs.keys():
            if stock1 in openedPositions and stock2 in openedPositions:
                res[(stock1, stock2)] = [openedPositions[stock1], openedPositions[stock2]]
               
        return res        
    
    def openPositions(self) -> None:
        
        currOpenedPositions:dict[str, Position] = self.tradingClient.openedPositions
        tradingPairs:dict[tuple, list] = self._filterExistingPairPositions(
            pairs=self.tradingPairs,
            openedPositions=currOpenedPositions
        )
        if not tradingPairs:
            return
       
        availableCash:float = float(self.tradingClient.accountDetail.cash) * self.entryPercent
        
        tradeNums:int = (availableCash//(float(currOpenedPositions.values()[0].cost_basis)*2)) if \
            currOpenedPositions and (availableCash//(float(currOpenedPositions.values()[0].cost_basis)*2)) <= len(tradingPairs) else len(tradingPairs)
            
        notionalAmount:float = float(currOpenedPositions.values()[0].cost_basis)*2 if currOpenedPositions else (availableCash)/tradeNums
            
        tradingRecord:dict[tuple, float] = getTradingRecord()
        for i in range(tradeNums):
            shortOrder, longOrder = self.tradingClient.openPositions(
                stockPair=(tradingPairs[i][0], tradingPairs[i][1]), 
                notional=notionalAmount
            )           
            shortOrder.dict
            tradingRecord[(tradingPairs[i][0], tradingPairs[i][1])] = self.tradingPairs[(tradingPairs[i][0], tradingPairs[i][1])][1]
            print(f"short {tradingPairs[i][0]} long {tradingPairs[i][1]} pair position opened")
            
        dumpTradingRecord(tradingRecord)
            
        
            
                         
                         
    def _getCloseablePairs(self, currOpenedPositions:dict[str, Position]) -> list[tuple]:
        res:list[tuple] = []        
        openedPairs:dict[tuple, float] = getTradingRecord()            
        openedPairsPositions:dict[tuple, list] = self._fetchExistingPairPositions(
            pairs=openedPairs, 
            openedPositions=currOpenedPositions
        )
        
        for pair, positions in openedPairsPositions.items():
            meanPriceRatio:float = openedPairs[pair]
            currPriceRatio:float = float(positions[0].current_price) / float(positions[1].current_price)
        
            if currPriceRatio <= meanPriceRatio:
                res.append(pair)
                
            ordersList:list[Order] = self.tradingClient.getPairOrders(pair)
            if (date.today() - ordersList[0].submitted_at.date()).days > 30:
                res.append(pair)
                      
        return res 
    
    def closePositions(self) -> None:
        currOpenedPositions:dict[str, Position] = self.tradingClient.getAllOpenPositions()           
        closeablePairs:list[tuple] = self._getCloseablePairs()
        
        if not closeablePairs:
            return 
        
        tradingRecord:dict[tuple, float] = getTradingRecord()
        recentlyClosed:dict[str, date] = getRecentlyClosed()
        
        for pair in closeablePairs:
            order1, order2 = self.tradingClient.closePositions(pair)
            profit:float = (float(currOpenedPositions[pair[0]].avg_entry_price)-float(order1.filled_avg_price))*float(order1.filled_qty) + \
                (float(order2.filled_avg_price) - float(currOpenedPositions[pair[1]].avg_entry_price))*float(order2.filled_qty)
            print(f"closed {pair[0]} <-> {pair[1]} pair position. Profit: ${round(profit, 2)}")
            del tradingRecord[pair]
            recentlyClosed[order1.symbol] = order1.submitted_at.date()
            recentlyClosed[order2.symbol] = order2.submitted_at.date()
        dumpTradingRecord(tradingRecord)
        dumpRecentlyClosed(recentlyClosed)
        
    
        
    