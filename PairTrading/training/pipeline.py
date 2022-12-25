from sklearn.pipeline import Pipeline 
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import AgglomerativeClustering

from PairTrading.util.patterns import Singleton

from pandas import DataFrame
import numpy as np

class Clustering(metaclass=Singleton):
    
    def __init__(self, inputData:DataFrame):
        self.inputData:DataFrame = inputData
        self.dataPipeline:Pipeline = Pipeline([
            ("scaler", StandardScaler()),
            ("pca", PCA(n_components=0.99)),
        ])
        self.trainingPipeline:Pipeline = Pipeline([
            ("agglomerative_clustering", 
             AgglomerativeClustering(n_clusters=None, affinity="cosine", linkage="average", distance_threshold=0.3))
        ])
        
    def _processFeatures(self, inputData:DataFrame) -> DataFrame:
        processedData:np.array = self.dataPipeline.fit_transform(inputData)
        return DataFrame(processedData, index=inputData.index)
    
    def _train_predict(self, scaledData:DataFrame) -> DataFrame:
        predictionNP:np.array = self.trainingPipeline.fit_predict(scaledData)
        res = DataFrame(predictionNP, index=scaledData.index, columns=["cluster_id"])
        res["momentum"] = self.inputData["m47"]
        
        return res
    
    def run(self) -> DataFrame:
        return self._train_predict(
            self._processFeatures(self.inputData)
        )
        