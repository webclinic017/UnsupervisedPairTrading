from PairTrading.authentication import AlpacaAuth
from PairTrading.authentication.enums import ConfigType

from alpaca.trading.client import TradingClient
from alpaca.trading.enums import AssetClass, AssetExchange, AssetStatus
from alpaca.trading.requests import GetAssetsRequest


class AlpacaTradingClient:
    def __init__(self, auth:AlpacaAuth):
        self.client:TradingClient = TradingClient(
            api_key=auth.api_key,
            secret_key=auth.secret_key,
            paper=auth.isPaper
        ) 
    
    @classmethod
    def create(cls, alpacaAuth:AlpacaAuth):
        if alpacaAuth.configType != ConfigType.ALPACA:
            raise AttributeError("the auth object is not for Alpaca client")
        return cls(auth=alpacaAuth)
    
    def getViableStocks(self) -> list[str]:
        
        allAssets:list = self.client.get_all_assets(
            GetAssetsRequest(
                status=AssetStatus.ACTIVE,
                asset_class=AssetClass.US_EQUITY
            )
        )      
        validAssets:list = [asset.symbol for asset in allAssets if (asset.fractionable==True and \
                                            asset.shortable==True and \
                                            asset.easy_to_borrow==True and \
                                            asset.exchange in (AssetExchange.NYSE, AssetExchange.AMEX, AssetExchange.NASDAQ) and \
                                            "." not in asset.symbol)]
        
        return validAssets