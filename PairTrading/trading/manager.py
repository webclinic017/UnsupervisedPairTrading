from PairTrading.lib.tradingClient import AlpacaTradingClient
from PairTrading.lib.dataEngine import AlpacaDataClient
from PairTrading.util.read import readFromJson, getRecentlyClosed, getTradingRecord, getPairsFromTrainingJson
from PairTrading.util.write import writeToJson, dumpRecentlyClosed, dumpTradingRecord
from PairTrading.authentication import AlpacaAuth

from alpaca.trading.models import TradeAccount, Position, Order


import os
import logging
from datetime import date, datetime

logger = logging.getLogger(__name__)


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
        rawQty:float = notionalAmount // latestBidPrice
        offset:float = (rawQty % 100) if (rawQty % 100) >= 50 else 0
        return ((rawQty // 100) * 100) + offset
    
    def _getViableTradesNum(self, entryAmount:float) -> int:
        res:int = 0
        for pair, _ in self.tradingPairs.items():
            if self._getShortableQty(pair[0], entryAmount):
                res += 1 
                
        return res 
    
    def _getOptimalTradingNum(self, tradingPairs, availableCash:float, currOpenedPositions:dict[str, Position]) -> (int, float):
        if availableCash <= 0:
            return (0, 0)
        res:int = 0
        if currOpenedPositions:           
            tmp:float = 0
            for stock, position in currOpenedPositions.items():
                tmp += abs(float(position.avg_entry_price) * float(position.qty))
            avgEntryAmount = tmp / len(currOpenedPositions)
            res = min(availableCash//avgEntryAmount, self._getViableTradesNum(avgEntryAmount))
        else:
            tradingNum:int = len(tradingPairs)
            avgEntryAmount = availableCash / tradingNum
            while tradingNum > self._getViableTradesNum(avgEntryAmount):
                tradingNum -= 1
                avgEntryAmount = availableCash / tradingNum
            res = tradingNum
        
        if res > len(tradingPairs):
            res = len(tradingPairs)
            avgEntryAmount = availableCash / res 
        return (res, avgEntryAmount)
                  
    
    def openPositions(self) -> None:
        
        currOpenedPositions:dict[str, Position] = self.tradingClient.openedPositions
        tradingPairs:dict[tuple, list] = self._filterExistingPairPositions(
            pairs=self.tradingPairs,
            openedPositions=currOpenedPositions
        )
        if not tradingPairs:
            logger.debug("No trading pairs detected")
            return
       
        tradingAccount:TradeAccount = self.tradingClient.accountDetail
        totalPosition:float = sum([abs(float(p.cost_basis)) for p in currOpenedPositions.values()])
        availableCash:float = (float(tradingAccount.cash) * self.entryPercent - totalPosition) / 2
        logger.info(f"available cash: ${round(availableCash, 2)*2}")
        
        tradeNums, notionalAmount = self._getOptimalTradingNum(tradingPairs, availableCash, currOpenedPositions)           
        if tradeNums < 1:
            logger.info("No more trades can be placed currently")
            return 
            
        tradingRecord:dict[tuple, float] = self.tradingRecord
        pairsList:list[tuple] = list(tradingPairs.keys())
        executedTrades:int = 0
        for pair in pairsList:
            if executedTrades >= tradeNums:
                break
            try:
                shortOrder, longOrder = self.tradingClient.openPositions(
                    stockPair=(pair[0], pair[1]), 
                    shortQty=self._getShortableQty(pair[0], notionalAmount)
                )           
                tradingRecord[pair] = self.tradingPairs[pair][1]
                logger.info(f"short {pair[0]} long {pair[1]} pair position opened")
                self.tradingRecord = tradingRecord
                executedTrades += 1
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
            logger.debug("No pairs opened")
            return
        
        for pair, positions in openedPairsPositions.items():
            meanPriceRatio:float = openedPairs[pair]
            currPriceRatio:float = float(positions[0].current_price) / float(positions[1].current_price)
            logger.info(f"{pair[0]}--{pair[1]}: curr_ratio: {currPriceRatio}, mean_ratio: {meanPriceRatio}")
            if currPriceRatio <= meanPriceRatio:
                res.append(pair)
            else:   
                ordersList:list[Order] = self.tradingClient.getPairOrders(pair)
                if (date.today() - ordersList[0].submitted_at.date()).days > 30:
                    res.append(pair)
                      
        return res 
    
    def closePositions(self) -> bool:
        currOpenedPositions:dict[str, Position] = self.tradingClient.openedPositions          
        closeablePairs:list[tuple] = self._getCloseablePairs(currOpenedPositions)
        
        if not closeablePairs:
            logger.debug("no closeable pairs detected currently")
            return False
        
        tradingRecord:dict[tuple, float] = self.tradingRecord
        recentlyClosed:dict[str, date] = self.recentlyClosed
        
        
        for pair in closeablePairs:
            order1, order2 = self.tradingClient.closePositions(pair)
            del tradingRecord[pair]
            recentlyClosed[order1.symbol] = order1.submitted_at.date()
            recentlyClosed[order2.symbol] = order2.submitted_at.date()       
            self.tradingRecord = tradingRecord
            logger.info(f"recently closed: {recentlyClosed}")
            self.recentlyClosed = recentlyClosed           

            logger.info(f"closed {pair[0]} <-> {pair[1]} pair position.")
            
        return True
        
        
    
        
    