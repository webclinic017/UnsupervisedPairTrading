from statsmodels.tsa.stattools import coint
from numpy import array

class CointTest:
        
    @staticmethod
    def isCointegrated(stock1, stock2:array) -> bool:
        _, pvalue, _ = coint(stock1, stock2, maxlag=1)
        return pvalue < 0.05

