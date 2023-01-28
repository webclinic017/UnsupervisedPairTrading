from authentication.enums import ConfigType
from PairTrading.util.patterns import Base


class BaseAuth(Base):
    def __init__(self, api_key, secret_key:str, isPaper:bool=True):
        self.configType = ConfigType.BASE
        self.api_key:str = api_key
        self.secret_key:str = secret_key if secret_key else ""
        self.isPaper:bool = isPaper
        
    @classmethod
    def create(cls, rawDict:dict, isPaper:bool=True):
        pass 
    
    def __str__(self):
        return str({
            "type": self.configType,
            "api_key": self.api_key
        })