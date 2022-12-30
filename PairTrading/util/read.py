import json 
from datetime import datetime, date
import logging
import os


logger = logging.getLogger(__name__)

def readFromJson(filePath:str) -> dict:
    res = {}
    try:
        with open(filePath, "r") as inFile:
            jsonStr = inFile.read()
            res = json.loads(jsonStr)
    except:      
        pass 
    return res if res else {}
    
def getRecentlyClosed() -> dict[str, date]:
    if os.path.exists(("saveddata/recently_closed.json")):
        res:dict[str, str] = readFromJson("saveddata/recently_closed.json")
        for symbol, submitTime in res.items():
            res[symbol] = datetime.strptime(submitTime, "%Y-%m-%d").date()
        return res if res else {}
    
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
        logger.debug("pairs.json file does not exist")
        return {"time":datetime.today().strftime("%Y-%m-%d")}

    pairs:dict = readFromJson("saveddata/pairs/pairs.json")
    pairs["final_pairs"]:dict = {(p.split(",")[0], p.split(",")[1]):val for p, val in pairs["final_pairs"].items()}
    
    return pairs 
        
