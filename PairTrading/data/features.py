from lib.dataEngine import AlpacaDataClient
class FeatureGenerator:
    def __init__(self):
        self.alpacaClient:AlpacaDataClient = AlpacaDataClient.create(auth)
        