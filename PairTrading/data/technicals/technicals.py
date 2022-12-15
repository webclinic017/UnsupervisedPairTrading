import pandas as pd

class TechnicalData:
    def __init__(self, rawFile:pd.DataFrame):
        self.priceData:pd.DataFrame = rawFile
        self.symbol:str = rawFile.index[0][0]
        
    @staticmethod
    def create(rawFile:pd.DataFrame) -> TechnicalData:
        return TechnicalData(rawFile)
    
    @staticmethod
    def _isFileValid(rawFile:pd.DataFrame) -> bool:
        if len(rawFile) >= 49:
            return True 
        return False 
    
    def getMomentums(self) -> pd.Series:
        return self.priceData["close"]\
            .pct_change()\
            .dropna()\
            .reset_index(drop=True)\
            .add_prefix("m")\
            .rename(self.symbol)
        