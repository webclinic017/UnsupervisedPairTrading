from PairTrading.util.read import getRecentlyClosed
from PairTrading.util.write import dumpRecentlyClosed

from datetime import datetime, date 


def cleanClosedTrades() -> None:    
    closedTrades:dict[str, date] = getRecentlyClosed()
    if not closedTrades:
        print("There are no trades that were conducted less than 31 days ago")
        return 
    today:date = date.today()
    
    delNum:int = 0
    for symbol, time in closedTrades.items():
        if (today - time).days > 31:
            del closedTrades[symbol]
            delNum += 1
            
    dumpRecentlyClosed(closedTrades)
    print(f"{delNum} past trading records removed")
    