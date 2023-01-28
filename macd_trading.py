from config.model import Config, CONFIG_TYPE
from config.configloader import configLoader
from authentication.authLoader import getAuth
from authentication.auth import AlpacaAuth
from lib.dataEngine import AlpacaDataClient
from lib.tradingClient import AlpacaTradingClient
from alpaca.trading.models import Position

from MACDTrading import MACDManager, ETFs
import logging
import sys


config:Config = configLoader(configType=CONFIG_TYPE.MACD_TRADING)

alpacaAuth:AlpacaAuth = getAuth("alpaca_side")
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
    etfs = ETFs.create(tradingClient, dataClient)
    print(etfs.getCandidates())
    print(manager._getEnterableStocks(openedPositions))

