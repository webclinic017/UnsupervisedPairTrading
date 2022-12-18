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

if __name__ == "__main__":
        
    # see if we need to review trades that were recently closed 
    cleanClosedTrades()
    
    # initialize authentication objects 
    alpacaAuth:AlpacaAuth = getAuth("alpaca")
    eodAuth:EodAuth = getAuth("eod")
    # get recently trained final pairs data 
    pairsDict = getPairsFromTrainingJson()
    
    print(pairsDict)
    
    if (date.today() - datetime.strptime(pairsDict["time"], "%Y-%m-%d").date()).days > 31:
        print("pairs data past 31 days -- new training needs to be conducted")
        getTrainAssign() 
        
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