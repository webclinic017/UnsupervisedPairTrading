from pandas import DataFrame, Series
from numpy import array, dot
from sklearn.preprocessing import StandardScaler
from datetime import date, datetime
from dateutil.relativedelta import relativedelta

from PairTrading.lib.dataEngine import AlpacaDataClient
from PairTrading.util.patterns import Singleton, Base
from PairTrading.pairs.cointegration import CointTest

from tqdm import tqdm




class PairCreator(Base, metaclass=Singleton):
    
    def __init__(self, clusterDF:DataFrame, dataClient:AlpacaDataClient):
        self.clusterDF:DataFrame = clusterDF
        self.dataClient:AlpacaDataClient = dataClient
        
        
    @classmethod
    def create(cls, clusterDF:DataFrame, client:AlpacaDataClient):
        return cls(clusterDF, client)
    
    def getFinalPairs(self, trainDate:date) -> dict[str, list]:
        self._getMomentum()
        res = {"time": trainDate.strftime("%Y-%m-%d")}
        finalPairs:dict = {}
        pairsDF:DataFrame = self._getTradeablePairs()
        viablePairs:list = [(val.split(",")[0], val.split(",")[1]) for val in pairsDF.index]
        
        tmpDict:dict = {}
        for pair in viablePairs:           
            tmpDict[",".join(pair)] = pairsDF.loc[",".join(pair)]["momentum_zscore"]
                
        for pair in list(tmpDict.keys()):
            finalPairs[pair] = tmpDict[pair]
        res["final_pairs"] = finalPairs 
        return res
    
    
    def _getTradeablePairs(self) -> DataFrame:
        
        pairCandidates:Series = Series(self._formPairs()).sort_values(ascending=False)
        pairData:array = array(pairCandidates).reshape(-1, 1)
        
        sc = StandardScaler()
        pairsDF:DataFrame = DataFrame(sc.fit_transform(pairData), index=pairCandidates.index, columns=["momentum_zscore"])     
        
        return pairsDF.loc[pairsDF["momentum_zscore"] > 1].sort_values(by=["momentum_zscore"], ascending=False)
                      
        
    def _formPairs(self) -> dict:
        pairCandidates:dict = {}       
        for clusterID in self.clusterDF["cluster_id"].unique():
            clusterDF = self.clusterDF.loc[self.clusterDF["cluster_id"] == clusterID].sort_values(by="momentum", ascending=False)
            head, tail = 0, len(clusterDF)-1
            while head < tail:
                pairCandidates[f"{clusterDF.iloc[head].name},{clusterDF.iloc[tail].name}"] = \
                abs(clusterDF.iloc[head]["momentum"] - clusterDF.iloc[tail]["momentum"])
                head += 1
                tail -= 1
                
        return pairCandidates
    
    def _getMomentum(self) -> None:
        for stock in tqdm(self.clusterDF.index, desc="get latest momentum data"):
            currPrice:float = self.dataClient.getHourly(stock).iloc[-1]["close"]
            prevPrice:float = self.dataClient.getMonthly(stock).iloc[-2]["close"]            
            self.clusterDF.loc[stock]["momentum"] = (currPrice - prevPrice) / prevPrice
    
