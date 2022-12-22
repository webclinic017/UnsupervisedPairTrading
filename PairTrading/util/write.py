import json 
from datetime import datetime, date
import logging 

logger = logging.getLogger(__name__)

def writeToJson(data:dict, filePath:str) -> bool:
    
    try:
        with open(filePath, "w") as outFile:
            dataStr:str = json.dumps(data, indent=4)
            outFile.write(dataStr)
            return True
            
    except Exception as ex:
        logger.error(ex)
        return False
        
def dumpTradingRecord(record:dict[tuple, float]) -> None:
    res:dict[str, float] = {}
    for pair, meanRatio in record.items():
        res[",".join([pair[0], pair[1]])] = meanRatio
        
    writeToJson(res, "saveddata/openedpairs.json")
    
def dumpRecentlyClosed(recentlyClosed:dict[str, date]) -> None:
    for symbol, submitTime in recentlyClosed.items():
        recentlyClosed[symbol] = submitTime.strftime("%Y-%m-%d")
        
    writeToJson(recentlyClosed, "saveddata/recently_closed.json")
