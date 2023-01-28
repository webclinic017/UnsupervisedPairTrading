import yaml 
from authentication.auth import AlpacaAuth, EodAuth
from authentication.enums import ConfigType

def getAuth(fileName:str, isPaper:bool=True):
    with open(f"credentials/{fileName}.yaml", "r") as inFile:
        authDict:dict = yaml.load(inFile, Loader=yaml.Loader)
        
    if fileName == ConfigType.ALPACA_MAIN:
        return AlpacaAuth.create(rawDict=authDict, isPaper=isPaper, configType=ConfigType.ALPACA_MAIN)
    
    elif fileName == ConfigType.ALPACA_SIDE:
        return AlpacaAuth.create(rawDict=authDict, isPaper=isPaper, configType=ConfigType.ALPACA_SIDE)
    
    elif fileName == ConfigType.EOD:
        return EodAuth.create(authDict)