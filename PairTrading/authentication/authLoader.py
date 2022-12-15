import yaml 
from PairTrading.authentication.base import BaseAuth
from PairTrading.authentication.auth import *
from PairTrading.authentication.enums import ConfigType

def getAuth(fileName:str, isLive:bool=False) -> BaseAuth:
    with open(f"credentials/{fileName}.yaml", "r") as inFile:
        authDict:dict = yaml.load(inFile, Loader=yaml.Loader)
        
    if fileName == ConfigType.ALPACA:
        return AlpacaAuth.create(rawDict=authDict, isLive=isLive)
    
    elif fileName == ConfigType.EOD:
        return EodAuth.create(authDict)