from PairTrading.lib.dataEngine import AlpacaDataClient, EodDataClient
from PairTrading.authentication import AlpacaAuth, EodAuth
class FeatureGenerator:
    def __init__(
        self,
        alpacaAuth:AlpacaAuth,
        eodAuth:EodAuth,
        stockCandidates:list
        ):
        self.alpacaClient:AlpacaDataClient = AlpacaDataClient.create(alpacaAuth)
        self.eodClient:EodDataClient = EodDataClient.create(eodAuth)