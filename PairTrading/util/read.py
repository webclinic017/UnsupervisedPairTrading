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
    
def getTradingRecord() -> dict[tuple, date]:
    if os.path.exists("saveddata/pairs/pairs.json"):
        res:dict[str, str] = readFromJson("saveddata/pairs/pairs.json")
        for pair, submitTime in res.items():
            res[(pair.split(",")[0], pair.split(",")[1])] = datetime.strptime(submitTime, "%Y-%m-%d").date()
        return res 