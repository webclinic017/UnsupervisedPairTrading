from lib.tradingClient import AlpacaTradingClient
from lib.dataEngine import AlpacaDataClient
from PairTrading.util.read import readFromJson, getRecentlyClosed, getTradingRecord, getPairsFromTrainingJson
from PairTrading.util.write import writeToJson, dumpRecentlyClosed, dumpTradingRecord
from lib.patterns import Singleton, Base
from PairTrading.trading.helper import PairInfoRetriever
from authentication.auth import AlpacaAuth

from alpaca.trading.models import TradeAccount, Position, Order
from alpaca.data.models import Quote


import os
import logging
import numpy as np 
from datetime import date, datetime
from pandas import Series

logger = logging.getLogger(__name__)


class TradingManager(Base, metaclass=Singleton):
    
    def __init__(self, tradingClient:AlpacaTradingClient, dataClient:AlpacaDataClient, entryPercent:float, maxPositions:int):
        self.tradingClient:AlpacaTradingClient = tradingClient
        self.dataClient:AlpacaDataClient = dataClient
        self.pairInfoRetriever:PairInfoRetriever = PairInfoRetriever.create(tradingClient)
        self.entryPercent:float = entryPercent
        self.maxPositions:int = maxPositions
        self.openedPositions:dict[str, Position] = self.tradingClient.openedPositions
        self.lastLogTime:datetime = datetime.now()
        
    @classmethod
    def create(cls, alpacaAuth:AlpacaAuth, entryPercent:float, maxPositions:int):
        tradingClient:AlpacaTradingClient = AlpacaTradingClient.create(alpacaAuth)
        dataClient:AlpacaDataClient = AlpacaDataClient.create(alpacaAuth)
        return cls(
            tradingClient=tradingClient,
            dataClient=dataClient,
            entryPercent=entryPercent,
            maxPositions=maxPositions
        )
        
    @property 
    def tradingRecord(self) -> dict[tuple, float]:
        return getTradingRecord()
    
    @tradingRecord.setter
    def tradingRecord(self, rec:dict[tuple, float]) -> None:
        dumpTradingRecord(rec)
         
    def _getShortableQty(self, symbol:str, notionalAmount) -> float:
        
        latestPrice:float = self.dataClient.getLastMinute(symbol)
        rawQty:float = notionalAmount // latestPrice
        offset:float = (rawQty % 100) if (rawQty % 100) >= 50 else 0
        return ((rawQty // 100) * 100) + offset
    
    def _getViableTradesNum(self, entryAmount:float, tradingPairs:dict[tuple, list]) -> int:
        res:int = 0
        
        for pair, _ in tradingPairs.items():
            shortQty:float = self._getShortableQty(pair[0], entryAmount)
            if shortQty:
                res += 1 
        return res 
    
    def _getOptimalTradingNum(self, tradingPairs, availableCash:float, currOpenedPositions:dict[str, Position]) -> (int, float):
        if availableCash <= 0:
            return (0, 0)
               
        openedEquities:list = []
        tradingNum:int = 0
        avgEntryAmount:float = 0
        
        if currOpenedPositions:           
            for stock, position in currOpenedPositions.items():
                openedEquities.append(abs(float(position.avg_entry_price) * float(position.qty)))
            openedEquities:np.array = np.array(openedEquities)
            avgEntryAmount:float = (np.sum(openedEquities) + availableCash) // self.maxPositions
            tradingNum:int = min([
                availableCash//avgEntryAmount, 
                self._getViableTradesNum(avgEntryAmount, tradingPairs), 
                self.maxPositions-len(currOpenedPositions) if self.maxPositions-len(currOpenedPositions) > 0 else 0
                ])
                
        else:
            tradingNum:int = len(tradingPairs)
            avgEntryAmount = availableCash / tradingNum
            while tradingNum > self._getViableTradesNum(avgEntryAmount, tradingPairs) or tradingNum > self.maxPositions:
                tradingNum -= 1
                avgEntryAmount = availableCash / tradingNum
                
            if tradingNum < 20:
                logger.warn(f"Too few available pairs enterable ({tradingNum} pairs), aborting entry...")
                return (0, 0)
        
        if tradingNum > len(tradingPairs):
            tradingNum = len(tradingPairs)
            avgEntryAmount = availableCash / tradingNum 

        return (tradingNum, avgEntryAmount)
                  
    
    def openPositions(self) -> None:
        
        tradingPairs:dict[tuple, list] = self.pairInfoRetriever.getTradablePairs(
            pairs=self.pairInfoRetriever.trainedPairs, 
            openedPositions=self.openedPositions
        )
        if not tradingPairs:
            logger.debug("No trading pairs detected")
            return
       
        tradingAccount:TradeAccount = self.tradingClient.accountDetail
        totalPosition:float = sum([abs(float(p.cost_basis)) for p in self.openedPositions.values()])
        availableCash:float = (min(float(tradingAccount.equity), float(tradingAccount.cash)) * self.entryPercent - totalPosition) / 2
        logger.info(f"available cash: ${round(availableCash, 2)*2}")
        
        tradeNums, notionalAmount = self._getOptimalTradingNum(tradingPairs, availableCash, self.openedPositions)          
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
                shortOrder, longOrder = self.tradingClient.openArbitragePositions(
                    stockPair=(pair[0], pair[1]), 
                    shortQty=self._getShortableQty(pair[0], notionalAmount)
                )           
                tradingRecord[pair] = self.pairInfoRetriever.trainedPairs[pair]
                logger.info(f"short {pair[0]} long {pair[1]} pair position opened")
                self.tradingRecord = tradingRecord
                executedTrades += 1
            except Exception:
                continue
            
        if executedTrades > 0:
            self.openedPositions = self.tradingClient.openedPositions
        
            
        
    def _getLatestProfit(self, position:Position, is_short:bool) -> float:        
        quote:Quote = self.dataClient.getLatestQuote(position.symbol)
        if is_short:
            return (float(position.avg_entry_price) - quote.ask_price) / float(position.avg_entry_price)
        else:
            return (quote.bid_price - float(position.avg_entry_price)) / float(position.avg_entry_price)
            
                         
                         
    def _getCloseablePairs(self, currOpenedPositions:dict[str, Position]) -> list[tuple]:
        updateLogTime:bool = (datetime.now() - self.lastLogTime).total_seconds() >= 60
        clock = self.tradingClient.clock
        res:list[tuple] = []        
        openedPairs:dict[tuple, float] = self.tradingRecord     
        openedPairsPositions:dict[tuple, list] = self.pairInfoRetriever.getCurrentlyOpenedPairs(
            pairs=openedPairs, 
            openedPositions=currOpenedPositions)     
        
        if not openedPairsPositions:
            logger.debug("No pairs opened")
            return
        
        for pair, positions in openedPairsPositions.items():
            
            currProfit:float = (self._getLatestProfit(positions[0], True) + self._getLatestProfit(positions[1], False)) / 2
            ordersList:list[Order] = self.tradingClient.getOrders(pair)
            daysElapsed:int = (date.today() - ordersList[0].submitted_at.date()).days
            
            if updateLogTime:
                logger.info(f"{pair[0]}--{pair[1]}, curr_profit: {round(currProfit*100, 2)}%, days_elapsed: {daysElapsed}")
            
            if currProfit > 0.1 or currProfit < -0.1:
                res.append(pair)
            else:                 
                if daysElapsed > 30 and (clock.next_close - clock.timestamp).total_seconds() <= 900:
                    res.append(pair)
                    
        if updateLogTime:
            print()
            print("========================================================================")
            print()
        self.lastLogTime:datetime = datetime.now() if updateLogTime else self.lastLogTime
        return res 
    
    def closePositions(self) -> bool:        
        closeablePairs:list[tuple] = self._getCloseablePairs(self.openedPositions)
        
        if not closeablePairs:
            logger.debug("no closeable pairs detected currently")
            return False
        
        tradingRecord:dict[tuple, float] = self.tradingRecord
        recentlyClosed:dict[str, date] = self.pairInfoRetriever.recentlyClosedPositions
        
        tradesExecuted:int = 0
        for pair in closeablePairs:
            order1, order2 = self.tradingClient.closeArbitragePositions(pair)
            tradesExecuted += 1
            del tradingRecord[pair]
            recentlyClosed[order1.symbol] = order1.submitted_at.date()
            recentlyClosed[order2.symbol] = order2.submitted_at.date()       
            self.tradingRecord = tradingRecord
            logger.info(f"recently closed: {list(recentlyClosed.keys())}")
            self.pairInfoRetriever.recentlyClosedPositions = recentlyClosed      

            logger.info(f"closed {pair[0]} <-> {pair[1]} pair position.")
            
        if tradesExecuted > 0:
            self.openedPositions = self.tradingClient.openedPositions
        return True
        
        
    
        
    