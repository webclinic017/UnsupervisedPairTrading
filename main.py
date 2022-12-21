from PairTrading.util import cleanClosedTrades, getPairsFromTrainingJson, writeToJson
from PairTrading.trading import TradingManager
from PairTrading.lib.dataEngine import AlpacaDataClient
from PairTrading.authentication.auth import AlpacaAuth, EodAuth
from PairTrading.authentication.authLoader import getAuth
from PairTrading.pairs.createpairs import PairCreator

from alpaca.trading.models import Order

from train import getTrainAssign
from log import Logger
from pandas import DataFrame, read_csv
from datetime import datetime, date
from tqdm import tqdm
import time
import sys

ENTRY_PERCENT = 0.3
REFRESH_DATA = False


if __name__ == "__main__":
    
        
    # see if we need to review trades that were recently closed 
    cleanClosedTrades()
    
    # initialize authentication objects 
    alpacaAuth:AlpacaAuth = getAuth("alpaca")
    eodAuth:EodAuth = getAuth("eod")
    # get recently trained final pairs data 
    pairsDict:dict = getPairsFromTrainingJson()
    
    todayTrained:bool = (date.today() - datetime.strptime(pairsDict["time"], "%Y-%m-%d").date()).days == 0
    if (date.today().day==2 and not todayTrained) or REFRESH_DATA:
        reason:str = "overdue for training" if (date.today().day==2 and not todayTrained) else "manual decision for new training"
        print(f"new training needs to be conducted -- {reason}")
        getTrainAssign(alpacaAuth, eodAuth, True) 
        
    #initialize pair-creator 
    cluster:DataFrame = read_csv("saveddata/cluster.csv", index_col=0)
    pairCreator:PairCreator = PairCreator.create(cluster, AlpacaDataClient.create(alpacaAuth))
    
    # update viable pairs
    newPairs:dict = pairCreator.getFinalPairs()
    writeToJson(newPairs, "saveddata/pairs/pairs.json")
    
    # initialize trading manager
    manager = TradingManager.create(alpacaAuth, ENTRY_PERCENT)
    
    timeTillMarketOpens:int = manager.tradingClient.secondsTillMarketOpens   
    if timeTillMarketOpens:
        print("waiting for market to open")
        time.sleep(timeTillMarketOpens + 60)
    else:
        print("the market is currently open")
        
    
    
        
    # start trading
    while manager.tradingClient.clock.is_open:       
        manager.openPositions()
        time.sleep(10)
        closed:bool = manager.closePositions()
        if closed:
            newPairs:dict = pairCreator.getFinalPairs()
            writeToJson(newPairs, "saveddata/pairs/pairs.json")
            print("new pairs created")
        time.sleep(60*5) # sleep for 5 minutes
        
        # update viable pairs
        newPairs:dict = pairCreator.getFinalPairs()
        writeToJson(newPairs, "saveddata/pairs/pairs.json")