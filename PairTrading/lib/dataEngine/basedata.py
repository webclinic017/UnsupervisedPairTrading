from overrides import EnforceOverrides
from PairTrading.authentication.base import BaseAuth
from PairTrading.authentication.enums import ConfigType

class BaseDataClient(EnforceOverrides):
    def __init__(self, auth:BaseAuth):
        self.dataClient = None
        
    @classmethod
    def create(cls, auth:BaseAuth):
        return cls(auth=auth)
    
    @staticmethod
    def _isAuthValid(auth:BaseAuth) -> bool:
        pass