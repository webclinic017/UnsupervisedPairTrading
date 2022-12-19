import json 
from datetime import datetime, date
import os

def readFromJson(filePath:str) -> dict:
    
    try:
        with open(filePath, "r") as inFile:
            jsonStr = inFile.read()
            return json.loads(jsonStr)
            
    except Exception as ex:
        print(ex)
        return {}
    
def getRecentlyClosed() -> dict[str, date]:
    if os.path.exists(("saveddata/pairs/recently_closed.json")):
        res:dict[str, str] = readFromJson("saveddata/pairs/recently_closed.json")
        for symbol, submitTime in res.items():
            res[symbol] = datetime.strptime(submitTime, "%Y-%m-%d").date()
        return res
    
def getTradingRecord() -> dict[tuple, float]:
    if os.path.exists("saveddata/openedpairs.json"):
        source:dict[str, float] = readFromJson("saveddata/openedpairs.json")       
        if not source:
            return {}
        res:dict[tuple, float] = {(pair.split(",")[0], pair.split(",")[1]):meanRatio
                                 for pair, meanRatio in source.items()}
        return res 
    
def getPairsFromTrainingJson() -> dict:
    if not os.path.exists("saveddata/pairs/pairs.json"):
        print("pairs.json file does not exist")
        return {}

    pairs:dict = readFromJson("saveddata/pairs/pairs.json")
    pairs["final_pairs"]:dict = {(p.split(",")[0], p.split(",")[1]):val for p, val in pairs["final_pairs"].items()}
    
    return pairs 
        