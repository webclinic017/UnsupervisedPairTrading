from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, StockLatestQuoteRequest
from alpaca.data.models import Quote
from alpaca.data.timeframe import TimeFrame
from alpaca.data.enums import Adjustment, DataFeed
from PairTrading.authentication.auth import AlpacaAuth
from PairTrading.authentication.base import BaseAuth
from PairTrading.lib.dataEngine.common import BarCollection
from PairTrading.util.patterns import Singleton, Base

import pandas as pd 
from datetime import datetime
from dateutil.relativedelta import relativedelta

class AlpacaDataClient(Base, metaclass=Singleton):
    
    def __init__(self, auth:AlpacaAuth):
        self.dataClient:StockHistoricalDataClient = StockHistoricalDataClient(
            api_key=auth.api_key,
            secret_key=auth.secret_key
        )   
    
    @classmethod
    def create(cls, auth:AlpacaAuth):
        if cls._isAuthValid(auth):
            return cls(auth=auth)
        else:
            raise AttributeError("the auth object is invalid")
    
    @staticmethod
    def _isAuthValid(auth:AlpacaAuth) -> bool:
        if auth.api_key and auth.secret_key:
            return True 
        return False 
    
    def getMonthly(self, symbol:str) -> pd.DataFrame:
        return self.dataClient.get_stock_bars(
            StockBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=TimeFrame.Month,
                adjustment=Adjustment.ALL,
                limit=49,
                feed=DataFeed.SIP,
                start=datetime.today() - relativedelta(months=49),
                end=datetime.today()
            )
        ).df 
        
    def getWeekly(self, symbol:str) -> pd.DataFrame:
        return self.dataClient.get_stock_bars(
            StockBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=TimeFrame.Week,
                adjustment=Adjustment.ALL,
                feed=DataFeed.SIP,
                start=datetime.today() - relativedelta(years=3),
                end=datetime.today()
            )
        ).df 
        
    def getDaily(self, symbol:str) -> pd.DataFrame:
        return self.dataClient.get_stock_bars(
            StockBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=TimeFrame.Day,
                adjustment=Adjustment.ALL,
                feed=DataFeed.SIP,
                limit=30,
                start=datetime.today() - relativedelta(days=30),
                end=datetime.today()
            )
        ).df 
        
    def getLongDaily(self, symbol:str) -> pd.DataFrame:
        return self.dataClient.get_stock_bars(
            StockBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=TimeFrame.Day,
                adjustment=Adjustment.ALL,
                feed=DataFeed.SIP,
                limit=90,
                start=datetime.today() - relativedelta(days=90),
                end=datetime.today()
            )
        ).df
        
    def getAllBars(self, symbol:str) -> BarCollection:
        daily:pd.DataFrame = self.getDaily(symbol)
        weekly:pd.DataFrame = self.getWeekly(symbol)
        monthly:pd.DataFrame = self.getMonthly(symbol)
        
        return BarCollection(daily, weekly, monthly)
    
    def getLatestQuote(self, symbol:str) -> Quote:
        return self.dataClient.get_stock_latest_quote(
            StockLatestQuoteRequest(
                symbol_or_symbols=symbol,
                feed=DataFeed.SIP
            )
        )[symbol]
        