from PairTrading.lib.dataEngine import AlpacaDataClient, EodDataClient
from PairTrading.authentication.authLoader import getAuth
from PairTrading.authentication.base import BaseAuth
from PairTrading.data import FundamentalsData
from PairTrading.util.write import writeToJson

import json


if __name__ == "__main__":
    
    # get authentication object
    alpacaAuth:BaseAuth = getAuth("alpaca")
    eodAuth:BaseAuth = getAuth("eod")
    
    print(alpacaAuth)
    print(eodAuth)

    alpacaClient:AlpacaDataClient = AlpacaDataClient.create(alpacaAuth)
    eodClient:EodDataClient = EodDataClient.create(eodAuth)
    

    fData = eodClient.getFundamentals("AAPL")
    
    bars = alpacaClient.getAllBars("AAPL")
    
    fundamentals = FundamentalsData.create(fData)
    fundamentals.setTechnicalBars(bars)
    
    metrics = fundamentals.getFundamentals()
    
    print(metrics)

    
