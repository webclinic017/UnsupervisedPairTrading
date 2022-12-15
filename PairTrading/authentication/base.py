from PairTrading.authentication.enums import ConfigType
from overrides import EnforceOverrides

class BaseAuth(EnforceOverrides):
    def __init__(self, api_key, secret_key:str):
        self.configType = ConfigType.BASE
        self.api_key:str = api_key
        self.secret_key:str = secret_key if secret_key else ""
        
    @classmethod
    def create(cls, rawDict:dict, isLive:bool=False):
        pass 
    
    def __str__(self):
        return str({
            "type": self.configType,
            "api_key": self.api_key
        })