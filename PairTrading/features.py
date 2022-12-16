from PairTrading.lib.dataEngine import AlpacaDataClient, EodDataClient
from PairTrading.data.fundamentals import FundamentalsData
from PairTrading.data.technicals import TechnicalData
from PairTrading.authentication import AlpacaAuth, EodAuth
from PairTrading.util.write import writeToJson
from PairTrading.util.read import readFromJson
from PairTrading.authentication.enums import ConfigType

from pandas import DataFrame, Series, concat
import json 
import os
import shutil

from tqdm import tqdm


class FeatureGenerator:
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
        storedStockList = os.listdir("tmp") if os.path.exists("tmp") else []
        
        if cleanOldData and not useExistingFiles:
            if os.path.exists("tmp"):
                shutil.rmtree("tmp")
                
        
        for stock in tqdm(self.stocks):
            priceData:DataFrame = self.alpacaClient.getMonthly(stock)
            
            if useExistingFiles and f"{stock}.json" in storedStockList:
                fundamentals:dict = readFromJson(f"tmp/{stock}.json")
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
                
                if writeToFile:
                    if not os.path.exists("tmp"):
                        os.makedirs("tmp")
                    writeToJson(fundamentals, f"tmp/{stock}.json")
            
            except:
                continue
        
        return res
            
            
            
            
            
        
        