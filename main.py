from PairTrading import FeatureGenerator
from PairTrading.lib.tradingClient import AlpacaTradingClient
from PairTrading.authentication.authLoader import getAuth
from PairTrading.authentication.base import BaseAuth


import json
from pandas import DataFrame


if __name__ == "__main__":
    
    # get authentication object
    alpacaAuth:BaseAuth = getAuth("alpaca")
    eodAuth:BaseAuth = getAuth("eod")
    
    print(alpacaAuth)
    print(eodAuth)
    
    tradingClient:AlpacaTradingClient = AlpacaTradingClient.create(alpacaAuth)
    stockList:list = tradingClient.getViableStocks()

    generator:FeatureGenerator = FeatureGenerator(alpacaAuth, eodAuth, stockList)
    
    trainingData:DataFrame = generator.getFeatureData(
        useExistingFiles=True,
        writeToFile=True,
        cleanOldData=True
    )
    
    trainingData.to_csv("training.csv")
    
