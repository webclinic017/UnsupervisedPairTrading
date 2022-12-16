from PairTrading.authentication.base import BaseAuth
from PairTrading.authentication.enums import ConfigType
from overrides import override

class AlpacaAuth(BaseAuth):
    def __init__(self, api_key, secret_key:str, isPaper:bool=True):
        super().__init__(api_key, secret_key)
        self.configType = ConfigType.ALPACA
        self.isPaper:bool = isPaper
        
    @classmethod
    @override
    def create(cls, rawDict, isPaper:bool):
        if isPaper:
            return cls(
                api_key=rawDict["paper"]["api_key"],
                secret_key=rawDict["paper"]["secret_key"],
                isPaper=True)
        else:
            return cls(
                api_key=rawDict["live"]["api_key"],
                secret_key=rawDict["live"]["secret_key"],
                isPaper=False)
        
    @override
    def __str__(self):
        return str({
            "type": self.configType,
            "api_key": self.api_key,
            "secret_key": self.secret_key
        })
    
        
class EodAuth(BaseAuth):
    def __init__(self, api_key:str):
        super().__init__(api_key, None)
        self.configType = ConfigType.EOD
        
    @classmethod
    @override
    def create(cls, rawDict, isPaper=True):
        return cls(
            api_key=rawDict["api_key"])
        
    @override
    def __str__(self):
        return str({
            "type": self.configType,
            "api_key": self.api_key
        })