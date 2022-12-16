from PairTrading.lib.dataEngine import AlpacaDataClient, EodDataClient
from PairTrading.authentication.authLoader import getAuth
from PairTrading.authentication.base import BaseAuth
from PairTrading.data import FundamentalsData, TechnicalData
from PairTrading.lib.tradingClient import AlpacaTradingClient
from PairTrading.util.write import writeToJson

import json
import pandas as pd


if __name__ == "__main__":
    
    # get authentication object
    alpacaAuth:BaseAuth = getAuth("alpaca")
    eodAuth:BaseAuth = getAuth("eod")
    
    print(alpacaAuth)
    print(eodAuth)

    alpacaClient:AlpacaDataClient = AlpacaDataClient.create(alpacaAuth)
    eodClient:EodDataClient = EodDataClient.create(eodAuth)
    
    tradingClient:AlpacaTradingClient = AlpacaTradingClient.create(alpacaAuth)
    
    assets = tradingClient.getViableStocks()
    print(assets)
