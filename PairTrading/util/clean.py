from PairTrading.util.read import getRecentlyClosed
from PairTrading.util.write import dumpRecentlyClosed

from datetime import datetime, date 


def cleanClosedTrades() -> None:    
    closedTrades:dict[str, date] = getRecentlyClosed()
    if not closedTrades:
        return
    today:date = date.today()
    
    for symbol, time in closedTrades.items():
        if (today - time).days > 30:
            del closedTrades[symbol]
            
    dumpRecentlyClosed(closedTrades)
    