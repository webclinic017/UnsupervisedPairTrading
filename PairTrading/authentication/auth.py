from PairTrading.authentication.base import BaseAuth
from PairTrading.authentication.enums import ConfigType
from overrides import override

class AlpacaAuth(BaseAuth):
    def __init__(self, api_key, secret_key:str):
        self.configType = ConfigType.ALPACA
        self.api_key:str = api_key
        self.secret_key:str = secret_key 
        
    @classmethod
    @override
    def create(cls, rawDict, isLive:bool):
        if isLive:
            return cls(
                api_key=rawDict["live"]["api_key"],
                secret_key=rawDict["live"]["secret_key"])
        else:
            return cls(
                api_key=rawDict["paper"]["api_key"],
                secret_key=rawDict["paper"]["secret_key"])
        
    @override
    def __str__(self):
        return str({
            "type": self.configType,
            "api_key": self.api_key,
            "secret_key": self.secret_key
        })
    
        
class EodAuth(BaseAuth):
    def __init__(self, api_key:str):
        self.configType = ConfigType.EOD
        self.api_key:str = api_key
        
    @classmethod
    @override
    def create(cls, rawDict, isLive=False):
        return cls(
            api_key=rawDict["api_key"])
        
    @override
    def __str__(self):
        return str({
            "type": self.configType,
            "api_key": self.api_key
        })