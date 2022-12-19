from PairTrading.lib.tradingClient import AlpacaTradingClient
from PairTrading.lib.dataEngine import AlpacaDataClient
from PairTrading.util.read import readFromJson, getRecentlyClosed, getTradingRecord, getPairsFromTrainingJson
from PairTrading.util.write import writeToJson, dumpRecentlyClosed, dumpTradingRecord
from PairTrading.authentication import AlpacaAuth

from alpaca.trading.models import TradeAccount, Position, Order


import os
from datetime import date, datetime
class TradingManager:
    _instance = None 
    def __new__(cls, tradingClient:AlpacaTradingClient, dataClient:AlpacaDataClient, entryPercent:float):
        if not cls._instance:
            cls._instance = super(TradingManager, cls).__new__(TradingManager)
        return cls._instance 
    
    def __init__(self, tradingClient:AlpacaTradingClient, dataClient:AlpacaDataClient, entryPercent:float):
        self.tradingClient:AlpacaTradingClient = tradingClient
        self.dataClient:AlpacaDataClient = dataClient
        self.entryPercent:float = entryPercent
        
    @classmethod
    def create(cls, alpacaAuth:AlpacaAuth, entryPercent:float):
        tradingClient:AlpacaTradingClient = AlpacaTradingClient.create(alpacaAuth)
        dataClient:AlpacaDataClient = AlpacaDataClient.create(alpacaAuth)
        return cls(
            tradingClient=tradingClient,
            dataClient=dataClient,
            entryPercent=entryPercent
        )
    
    @property
    def tradingPairs(self) -> dict[tuple, list]:
        return getPairsFromTrainingJson()["final_pairs"]
        
    @property 
    def tradingRecord(self) -> dict[tuple, float]:
        return getTradingRecord()
    
    @tradingRecord.setter
    def tradingRecord(self, rec:dict[tuple, float]) -> None:
        dumpTradingRecord(rec)
        
    @property 
    def recentlyClosed(self) -> dict[str, date]:
        return getRecentlyClosed() 
    
    @recentlyClosed.setter 
    def recentlyClosed(self, rec:dict[str, date]) -> None:
        dumpRecentlyClosed(rec)
    
    def _filterExistingPairPositions(self, pairs:dict[tuple, list], openedPositions:dict[str, Position]) -> dict[tuple, list]:
        if not pairs:
            return None
        res:dict[tuple, list] = pairs.copy()
        for stock1, stock2 in pairs.keys():
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
    
    def _getShortableQty(self, symbol:str, notionalAmount) -> float:
        latestBidPrice:float = self.dataClient.getLatestQuote(symbol).bid_price
        return notionalAmount // latestBidPrice
    
    def openPositions(self) -> None:
        
        currOpenedPositions:dict[str, Position] = self.tradingClient.openedPositions
        tradingPairs:dict[tuple, list] = self._filterExistingPairPositions(
            pairs=self.tradingPairs,
            openedPositions=currOpenedPositions
        )
        if not tradingPairs:
            print("No trading pairs detected")
            return
       
        availableCash:float = float(self.tradingClient.accountDetail.equity) * self.entryPercent
        
        tradeNums:int = (availableCash//(float(list(currOpenedPositions.values())[0].cost_basis)*2)) if \
            currOpenedPositions and (availableCash//(float(list(currOpenedPositions.values())[0].cost_basis)*2)) <= len(tradingPairs) else len(tradingPairs)
            
        notionalAmount:float = float(list(currOpenedPositions.values())[0].cost_basis)*2 if currOpenedPositions else (availableCash)/tradeNums
        tradeNums -= len(currOpenedPositions)//2 
        
        
        if tradeNums < 1:
            print("No more trades can be placed currently")
            return 
            
        tradingRecord:dict[tuple, float] = self.tradingRecord
        pairsList:list[tuple] = list(tradingPairs.keys())
        for i in range(tradeNums):
            try:
                pair:tuple = pairsList[i]
                shortOrder, longOrder = self.tradingClient.openPositions(
                    stockPair=(pair[0], pair[1]), 
                    shortQty=self._getShortableQty(pair[0], notionalAmount/2)
                )           
                tradingRecord[pair] = self.tradingPairs[pair][1]
                print(f"short {pair[0]} long {pair[1]} pair position opened")
                self.tradingRecord = tradingRecord
            except:
                continue
        
            
        
            
                         
                         
    def _getCloseablePairs(self, currOpenedPositions:dict[str, Position]) -> list[tuple]:
        res:list[tuple] = []        
        openedPairs:dict[tuple, float] = self.tradingRecord          
        openedPairsPositions:dict[tuple, list] = self._fetchExistingPairPositions(
            pairs=openedPairs, 
            openedPositions=currOpenedPositions
        )
        if not openedPairsPositions:
            print("No pairs opened")
            return
        
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
        currOpenedPositions:dict[str, Position] = self.tradingClient.openedPositions          
        closeablePairs:list[tuple] = self._getCloseablePairs(currOpenedPositions)
        
        if not closeablePairs:
            print("no closeable pairs currently")
            return 
        
        tradingRecord:dict[tuple, float] = self.tradingRecord
        recentlyClosed:dict[str, date] = self.recentlyClosed
        
        for pair in closeablePairs:
            order1, order2 = self.tradingClient.closePositions(pair)
            profit:float = (float(currOpenedPositions[pair[0]].avg_entry_price)-float(order1.filled_avg_price))*float(order1.filled_qty) + \
                (float(order2.filled_avg_price) - float(currOpenedPositions[pair[1]].avg_entry_price))*float(order2.filled_qty)
            print(f"closed {pair[0]} <-> {pair[1]} pair position. Profit: ${round(profit, 2)}")
            del tradingRecord[pair]
            recentlyClosed[order1.symbol] = order1.submitted_at.date()
            recentlyClosed[order2.symbol] = order2.submitted_at.date()
        
        self.tradingRecord = tradingRecord
        self.recentlyClosed = recentlyClosed
        
    
        
    