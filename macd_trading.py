from config.model import Config, CONFIG_TYPE
from config.configloader import configLoader
from authentication.authLoader import getAuth
from authentication.auth import AlpacaAuth
from lib.dataEngine import AlpacaDataClient
from lib.tradingClient import AlpacaTradingClient
from alpaca.trading.models import Position

from MACDTrading import MACDManager, ETFs
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
import time
import sys
from tqdm import tqdm

config:Config = configLoader(configType=CONFIG_TYPE.MACD_TRADING)
print(config)

alpacaAuth:AlpacaAuth = getAuth("alpaca_side", config.IS_PAPER)
dataClient:AlpacaDataClient = AlpacaDataClient.create(alpacaAuth)
tradingClient:AlpacaTradingClient = AlpacaTradingClient.create(alpacaAuth)

manager:MACDManager = MACDManager.create(
    dataClient=dataClient, 
    tradingClient=tradingClient, 
    entryPercent=config.ENTRYPERCENT)



logging.basicConfig(stream=sys.stdout, format="%(asctime)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    openedPositions:dict[str, Position] = tradingClient.openedPositions
    timeTillMarketOpens:int = manager.tradingClient.secondsTillMarketOpens  
    while not manager.tradingClient.clock.is_open:
        if 0 < timeTillMarketOpens <= 3600 * 8:
            logger.info("waiting for market to open")
            time.sleep(timeTillMarketOpens + 60)
        elif timeTillMarketOpens > 3600 * 8:
            logger.info("market is not open today")
            sys.exit()
        else:
            logger.info(f"anomaly... {round(timeTillMarketOpens/60, 2)} minutes before market opens")
            time.sleep(300*60 + timeTillMarketOpens)
        timeTillMarketOpens:int = manager.tradingClient.secondsTillMarketOpens   
        
        
    # wait till 10 minutes before the market closes
    secondsLeft:int = int((manager.tradingClient.clock.next_close.replace(tzinfo=None) - relativedelta(minutes=15) -
                       manager.tradingClient.clock.timestamp.replace(tzinfo=None)).total_seconds())
    logger.info(f"{round(secondsLeft/3600, 2)} hours left before the bot can start operating")   
    for _ in tqdm(range(secondsLeft), desc="waiting till the bot can act ..."):
        time.sleep(1)
    
    
    while manager.tradingClient.clock.is_open:
        manager.openPositions()
        manager.closePositions()
        time.sleep(30)

