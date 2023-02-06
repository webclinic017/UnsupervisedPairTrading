from lib.dataEngine import AlpacaDataClient
from lib.tradingClient import AlpacaTradingClient
from lib.patterns import Base, Singleton

from alpaca.trading.models import Asset
from alpaca.trading.requests import GetAssetsRequest

from tqdm import tqdm

class ETFs(Base, metaclass=Singleton):

    
    def __init__(self, tradingClient: AlpacaTradingClient, dataClient: AlpacaDataClient):
        self.tradingClient: AlpacaTradingClient = tradingClient
        self.dataClient: AlpacaDataClient = dataClient
        
    @classmethod
    def create(cls, tradingClient: AlpacaTradingClient, dataClient: AlpacaDataClient):
        return cls(
            tradingClient=tradingClient,
            dataClient=dataClient
        )
        
    def getAllCandidates(self) -> list[str]:
        tradableStocksSymbols = [asset.symbol for asset in self.tradingClient.allTradableStocks]
        res = []
        for symbol in tqdm(tradableStocksSymbols, desc="filter for viable stocks"):
            try:
                if self.dataClient.getMarketCap(symbol) > 1_500_000:
                    res.append(symbol)
            except:
                continue
                
        return res 
        
