import yaml
from config.model import Config


def configLoader() -> Config:

    with open(f"config/config.yaml", "r") as inFile:
        configDict:dict = yaml.load(inFile, Loader=yaml.Loader)
        
    return Config(
        ENTRYPERCENT=configDict["entry_percent"],
        REFRESH_DATA=configDict["refresh_data"],
        OVERWRITE_FUNDAMENTALS=configDict["overwrite_fundamentals"],
        IS_PAPER=configDict["is_paper"]
    )