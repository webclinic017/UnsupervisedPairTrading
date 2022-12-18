import json 
from datetime import datetime, date

def writeToJson(data:dict, filePath:str) -> bool:
    
    try:
        with open(filePath, "w") as outFile:
            dataStr:str = json.dumps(data, indent=4)
            outFile.write(dataStr)
            return True
            
    except Exception as ex:
        print(ex)
        return False
        
def dumpTradingRecord(record:dict[tuple, date]) -> None:
    for pair, submitTime in record.items():
        record[",".join([pair[0], pair[1]])] = submitTime.strftime("%Y-%m-%d")
        
    writeToJson(record, "saveddata/pairs/pairs.json")
    
def dumpRecentlyClosed(recentlyClosed:dict[str, date]) -> None:
    for symbol, submitTime in recentlyClosed.items():
        recentlyClosed[symbol] = submitTime.strftime("%Y-%m-%d")
        
    writeToJson(recentlyClosed, "saveddata/pairs/recently_closed.json")
