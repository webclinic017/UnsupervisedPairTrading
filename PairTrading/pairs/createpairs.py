from pandas import DataFrame, Series
from numpy import array, dot
from sklearn.preprocessing import StandardScaler
from datetime import date

from PairTrading.lib.dataEngine import AlpacaDataClient
from PairTrading.util.patterns import Singleton, Base
from PairTrading.pairs.cointegration import CointTest



class PairCreator(Base, metaclass=Singleton):
    
    def __init__(self, clusterDF:DataFrame, dataClient:AlpacaDataClient):
        self.clusterDF:DataFrame = clusterDF
        self.dataClient:AlpacaDataClient = dataClient
        
    @classmethod
    def create(cls, clusterDF:DataFrame, client:AlpacaDataClient):
        return cls(clusterDF, client)
    
    def getFinalPairs(self, trainDate:date) -> dict[str, list]:
        res = {"time": trainDate.strftime("%Y-%m-%d")}
        finalPairs:dict = {}
        pairsDF:DataFrame = self._getTradeablePairs()
        viablePairs:list = [(val.split(",")[0], val.split(",")[1]) for val in pairsDF.index]
        
        tmpDict:dict = {}
        for pair1, pair2 in viablePairs:
            pair1DailyDF:array = array(self.dataClient.getLongDaily(pair1)["close"]).flatten()
            pair2DailyDF:array = array(self.dataClient.getLongDaily(pair2)["close"]).flatten()           
            minSize:int = min(pair1DailyDF.size, pair2DailyDF.size)
            
            priceRatio:array = pair1DailyDF[:minSize]/ pair2DailyDF[:minSize]
            
            zscore = Series(priceRatio).sub(Series(priceRatio).rolling(30).mean()).div(Series(priceRatio).rolling(30).std())
            if CointTest.isCointegrated(pair1DailyDF[:minSize], pair2DailyDF[:minSize]) and zscore[-1] > 1:
                tmpDict[",".join([pair1, pair2])] = (zscore, priceRatio.mean())
                print(zscore)
        for pair in list(tmpDict.keys()):
            finalPairs[pair] = (tmpDict[pair][0], tmpDict[pair][1])
        res["final_pairs"] = finalPairs 
        return res
    
    
    def _getTradeablePairs(self) -> DataFrame:
        
        pairCandidates:Series = Series(self._formPairs()).sort_values(ascending=False)
        pairData:array = array(pairCandidates).reshape(-1, 1)
        
        sc = StandardScaler()
        pairsDF:DataFrame = DataFrame(sc.fit_transform(pairData), index=pairCandidates.index, columns=["momentum_zscore"])     
        
        return pairsDF.sort_values(by=["momentum_zscore"], ascending=False)
                      
        
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
    
