from PairTrading import FeatureGenerator
from lib.tradingClient import AlpacaTradingClient
from lib.dataEngine import AlpacaDataClient, EodDataClient
from authentication.authLoader import getAuth
from authentication.base import BaseAuth

from PairTrading.training import Clustering
from PairTrading.pairs.createpairs import PairCreator
from PairTrading.util.write import writeToJson
from PairTrading.util.clean import cleanClosedTrades


import json
from pandas import DataFrame, read_csv
from datetime import date 
import warnings
import numpy as np

warnings.filterwarnings("ignore")

def getTrainAssign(alpacaAuth, eodAuth:BaseAuth, useExistingFile:bool=True) -> None:
    
    # create trading and data clients
    dataClient:AlpacaDataClient = AlpacaDataClient.create(alpacaAuth)
    tradingClient:AlpacaTradingClient = AlpacaTradingClient.create(alpacaAuth)    
    stockList:list = tradingClient.getViableStocks()

    # generate technical and fundamental features
    generator:FeatureGenerator = FeatureGenerator.create(alpacaAuth, eodAuth, stockList)
    trainingData:DataFrame = generator.getFeatureData(
        useExistingFiles=useExistingFile,
        writeToFile=True,
        cleanOldData=True
    )
    trainingData.to_csv("saveddata/training.csv")
    trainingData = read_csv("saveddata/training.csv", index_col=0)
    
    # find clusters using agglomerative clustering
    ac = Clustering(trainingData)  
    clusters:DataFrame = ac.run()
    clusters.to_csv("saveddata/cluster.csv")
    

    
 
    
    