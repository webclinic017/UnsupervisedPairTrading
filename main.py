from PairTrading.util import cleanClosedTrades, getPairsFromTrainingJson
from PairTrading.trading import TradingManager
from PairTrading.authentication.auth import AlpacaAuth, EodAuth
from PairTrading.authentication.authLoader import getAuth

from alpaca.trading.models import Order

from train import getTrainAssign
from datetime import datetime, date
from tqdm import tqdm
import time

ENTRY_PERCENT = 0.2
REFRESH_DATA = True

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
        reason:str = "30 days elapsed" if thirtyDaysElapsed else "manual decision for new training"
        print(f"new training needs to be conducted -- {reason}")
        getTrainAssign(alpacaAuth, eodAuth, REFRESH_DATA) 
        
    # initialize trading manager
    manager = TradingManager.create(alpacaAuth, ENTRY_PERCENT)
    
    timeTillMarketOpens:int = manager.tradingClient.secondsTillMarketOpens
    
    if timeTillMarketOpens:
        for i in tqdm(range((timeTillMarketOpens//10)+1), desc=f"{round(timeTillMarketOpens/3600, 2)} hours before the market opens"):
            time.sleep(10)
    else:
        print("the market is currently open")
        
    # start trading
    while manager.tradingClient.clock.is_open:
        manager.openPositions()
        manager.closePositions()
        time.sleep(60*10) # sleep for 10 minutes