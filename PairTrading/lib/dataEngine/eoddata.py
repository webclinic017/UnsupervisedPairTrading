from eod import EodHistoricalData
from PairTrading.authentication.base import BaseAuth
from PairTrading.lib.dataEngine.basedata import BaseDataClient
from PairTrading.authentication.enums import ConfigType

class EodDataClient:
    _instance = None
    def __new__(cls, auth:BaseAuth):
        if cls._instance is None:
            cls._instance = super(EodDataClient, cls).__new__(EodDataClient)
        return cls._instance
    
    def __init__(self, auth:BaseAuth):
        self.dataClient:EodHistoricalData = EodHistoricalData(auth.api_key)
        
    @classmethod
    def create(cls, auth:BaseAuth):
        if not cls._isAuthValid(auth):
            raise ValueError("wrong authentication object detected (not belonging to EOD)")
        return cls(auth)
    
    @staticmethod
    def _isAuthValid(auth:BaseAuth) -> bool:
        if auth.configType == ConfigType.EOD and auth.api_key:
            return True 
        return False
    
    def getFundamentals(self, symbol:str) -> dict:
        return self.dataClient.get_fundamental_equity(symbol)