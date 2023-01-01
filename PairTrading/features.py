from PairTrading.lib.dataEngine import AlpacaDataClient, EodDataClient
from PairTrading.data.fundamentals import FundamentalsData
from PairTrading.data.technicals import TechnicalData
from PairTrading.authentication import AlpacaAuth, EodAuth
from PairTrading.util.write import writeToJson
from PairTrading.util.read import readFromJson
from PairTrading.util.patterns import Singleton
from PairTrading.authentication.enums import ConfigType

from pandas import DataFrame, Series, concat
import json 
import os
import shutil
import numpy as np

from tqdm import tqdm
import warnings




class FeatureGenerator(metaclass=Singleton):
    
    def __init__(
        self,
        alpacaAuth:AlpacaAuth, 
        eodAuth:EodAuth,
        stocks:list
        ):
        self.alpacaClient:AlpacaDataClient = AlpacaDataClient.create(alpacaAuth)
        self.eodClient:EodDataClient = EodDataClient.create(eodAuth)
        self.stocks:list = stocks
        
    @classmethod
    def create(cls, alpacaAuth:AlpacaAuth, eodAuth:EodAuth, stockList:list):
        if (alpacaAuth.configType==ConfigType.ALPACA and eodAuth.configType==ConfigType.EOD):
            return cls(
                alpacaAuth=alpacaAuth,
                eodAuth=eodAuth,
                stocks=stockList
            )        
        else:
            raise AttributeError("invalid auth object detected")
    
    def getFeatureData(self, useExistingFiles:bool=False, writeToFile:bool=True, cleanOldData:bool=False) -> DataFrame:
        res:DataFrame = DataFrame()
        storedStockList = os.listdir("saveddata/tmp") if os.path.exists("saveddata/tmp") else []
        
        if cleanOldData and not useExistingFiles:
            if os.path.exists("saveddata/tmp"):
                shutil.rmtree("saveddata/tmp")
                os.mkdir("saveddata/tmp")
                
        
        for stock in tqdm(self.stocks, desc="calculate technical and fundamental features"):
            priceData:DataFrame = self.alpacaClient.getMonthly(stock)
            
            # we will not consider stocks that have less than 4 years of data
            if priceData.shape[0] < 49:
                continue
            else:
                priceData:DataFrame = priceData.iloc[priceData.shape[0]-49:]
            
            if useExistingFiles and f"{stock}.json" in storedStockList:
                fundamentals:dict = readFromJson(f"saveddata/tmp/{stock}.json")
            else:
                fundamentals:dict = self.eodClient.getFundamentals(stock)
                       
            try:
                # initialize generators
                technicalsGenerator:TechnicalData = TechnicalData.create(priceData)
                firmCharGenerator:FundamentalsData = FundamentalsData.create(fundamentals)
                firmCharGenerator.setTechnicalBars(self.alpacaClient.getAllBars(stock))
                
                momentums:Series = technicalsGenerator.getMomentums()
                firmChars:Series = firmCharGenerator.getFundamentals()
                
                combinedFeatures = DataFrame([momentums.append(firmChars).rename(stock)])
                res = combinedFeatures if res.empty else concat([res, combinedFeatures])
                
                if writeToFile and f"{stock}.json" not in storedStockList:
                    if not os.path.exists("saveddata/tmp"):
                        os.makedirs("saveddata/tmp")
                    writeToJson(fundamentals, f"saveddata/tmp/{stock}.json")           
            except:
                continue
        
        res.replace([np.inf, -np.inf], np.nan, inplace=True)
        res.fillna(0, inplace=True)
        return res
            
            
            
            
            
        
        