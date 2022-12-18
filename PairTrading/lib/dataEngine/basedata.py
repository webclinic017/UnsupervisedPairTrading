from overrides import EnforceOverrides
from PairTrading.authentication.base import BaseAuth
from PairTrading.authentication.enums import ConfigType

class BaseDataClient(EnforceOverrides):
    
    def __new__(cls):
        pass 
    
    def __init__(self, auth):
        self.dataClient = None
        
    @classmethod
    def create(cls, auth):
        return cls(auth=auth)
    
    @staticmethod
    def _isAuthValid(auth) -> bool:
        pass