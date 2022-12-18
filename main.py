from PairTrading.util import cleanClosedTrades, getPairsFromTrainingJson
from PairTrading.trading import TradingManager
from PairTrading.authentication.auth import AlpacaAuth, EodAuth
from PairTrading.authentication.authLoader import getAuth

from alpaca.trading.models import Order

from train import getTrainAssign
from datetime import datetime, date
from tqdm import tqdm
import time

ENTRY_PERCENT = 0.1
REFRESH_DATA = True

if __name__ == "__main__":
        
    # see if we need to review trades that were recently closed 
    cleanClosedTrades()
    
    # initialize authentication objects 
    alpacaAuth:AlpacaAuth = getAuth("alpaca")
    eodAuth:EodAuth = getAuth("eod")
    # get recently trained final pairs data 
    pairsDict:dict = getPairsFromTrainingJson()
    
    thirtyDaysElapsed:bool = (date.today() - datetime.strptime(pairsDict["time"], "%Y-%m-%d").date()).days > 30
    if (date.today() - datetime.strptime(pairsDict["time"], "%Y-%m-%d").date()).days > 30 or REFRESH_DATA:
        reason:str = "30 days elapsed" if thirtyDaysElapsed else "manual decision for new training"
        print(f"new training needs to be conducted -- {reason}")
        getTrainAssign(alpacaAuth, eodAuth) 
        
    # create the authen
    # initialize trading manager
    manager = TradingManager.create(alpacaAuth, ENTRY_PERCENT)
    
    timeTillMarketOpens:float = manager.tradingClient.getTimeTillMarketOpensInSeconds()
    
    if timeTillMarketOpens:
        print(f"{round(timeTillMarketOpens/3600, 2)} hours before the market opens")
        for i in tqdm(range(int(timeTillMarketOpens//10)+1), desc="waiting till the Market opens"):
            time.sleep(10)
    else:
        print("the market is currently open")
        
    # start trading
    while manager.tradingClient.client.get_clock().is_open:
        manager.openPositions()
        manager.closePositions()
        time.sleep(60*5)