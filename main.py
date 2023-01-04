from PairTrading.util import cleanClosedTrades, getPairsFromTrainingJson, writeToJson
from PairTrading.trading import TradingManager
from PairTrading.lib.dataEngine import AlpacaDataClient
from PairTrading.authentication.auth import AlpacaAuth, EodAuth
from PairTrading.authentication.authLoader import getAuth
from PairTrading.pairs.createpairs import PairCreator

from alpaca.trading.models import Order

from train import getTrainAssign
import logging
from pandas import DataFrame, read_csv
from datetime import datetime, date
import time
import sys

ENTRY_PERCENT = 0.4
REFRESH_DATA = False


if __name__ == "__main__":
    # logging.basicConfig(filename="saveddata/log/trading.log", filemode="w", format="%(asctime)s - %(message)s", level=logging.INFO)   
    logging.basicConfig(stream=sys.stdout, format="%(asctime)s - %(message)s", level=logging.INFO)
    logger = logging.getLogger(__name__)
        
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
        logger.info(f"new training needs to be conducted -- {reason}")
        getTrainAssign(alpacaAuth, eodAuth, True) 
        
    #initialize pair-creator
    logger.info("initializing pair creator")
    cluster:DataFrame = read_csv("saveddata/cluster.csv", index_col=0)
    pairCreator:PairCreator = PairCreator.create(cluster, AlpacaDataClient.create(alpacaAuth))
    
    # update viable pairs
    logger.info("getting latest pairs")
    trainedPairs = getPairsFromTrainingJson()
    trainDate:date = datetime.strptime(trainedPairs["time"], "%Y-%m-%d").date()
    newPairs:dict = pairCreator.getFinalPairs(trainDate)
    print(newPairs)
    writeToJson(newPairs, "saveddata/pairs/pairs.json")
    
    # initialize trading manager
    manager = TradingManager.create(alpacaAuth, ENTRY_PERCENT)
    
    timeTillMarketOpens:int = manager.tradingClient.secondsTillMarketOpens  
    while not manager.tradingClient.clock.is_open:
        if 0 < timeTillMarketOpens <= 3600 * 8:
            logger.info("waiting for market to open")
            time.sleep(timeTillMarketOpens + 60)
        elif timeTillMarketOpens > 3600 * 8:
            logger.info("market is not open today")
            sys.exit()
        else:
            logger.info(f"anomaly... {timeTillMarketOpens/60} minutes before market opens")
            time.sleep(60)
        timeTillMarketOpens:int = manager.tradingClient.secondsTillMarketOpens            

    logger.info("the market is currently open")
           
        
    # start trading
    while manager.tradingClient.clock.is_open:       
        manager.openPositions()
        time.sleep(10)
        closed:bool = manager.closePositions()
        if closed:
            newPairs:dict = pairCreator.getFinalPairs(trainDate)
            writeToJson(newPairs, "saveddata/pairs/pairs.json")
            logger.info("new pairs created")
        time.sleep(60*5) # sleep for 5 minutes
        
        # update viable pairs
        newPairs:dict = pairCreator.getFinalPairs(trainDate)
        writeToJson(newPairs, "saveddata/pairs/pairs.json")
