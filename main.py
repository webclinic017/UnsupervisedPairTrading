from PairTrading import FeatureGenerator
from PairTrading.lib.tradingClient import AlpacaTradingClient
from PairTrading.lib.dataEngine import AlpacaDataClient, EodDataClient
from PairTrading.authentication.authLoader import getAuth
from PairTrading.authentication.base import BaseAuth

from PairTrading.training import Clustering
from PairTrading.pairs.createpairs import PairCreator
from PairTrading.util.write import writeToJson
from PairTrading.util.clean import cleanClosedTrades


import json
from pandas import DataFrame, read_csv
import warnings
import numpy as np

warnings.filterwarnings("ignore")


if __name__ == "__main__":
    
    cleanClosedTrades()
    
    
    # # get authentication object
    alpacaAuth:BaseAuth = getAuth("alpaca")
    eodAuth:BaseAuth = getAuth("eod")
    
    # print(alpacaAuth)
    # print(eodAuth)
    
    tradingClient:AlpacaTradingClient = AlpacaTradingClient.create(alpacaAuth)
    dataClient = AlpacaDataClient.create(alpacaAuth)
    print(dataClient)

    
    stockList:list = tradingClient.getViableStocks()

    generator:FeatureGenerator = FeatureGenerator.create(alpacaAuth, eodAuth, stockList)
    
    trainingData:DataFrame = generator.getFeatureData(
        useExistingFiles=False,
        writeToFile=True,
        cleanOldData=True
    )
    
    trainingData.to_csv("saveddata/training.csv")
    trainingData = read_csv("saveddata/training.csv", index_col=0)
    
    ac = Clustering(trainingData)
    
    res = ac.run()
    res.to_csv("saveddata/cluster.csv")
    
    pairCreator = PairCreator.create(res, dataClient)
    res = pairCreator.getFinalPairs()
    print(res)
    
    writeToJson(res, "saveddata/pairs/pairs.json")
    
 
    
    