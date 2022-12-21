from abc import ABC 

class FundamentalsBase(ABC):
    
    def __init__(self, rawFile:dict):
        self._rawFile:dict = rawFile
        self.general:dict = None
        # technical summary
        self.technicals:dict = None 
        self.highlights:dict = None 
        self.shareStats:dict = None
        # quarterly statements
        self.incomeStatement:list = None 
        self.cashFlow:list = None
        self.balanceSheet:list = None 
        # init methods 
        self._readIncomeBalanceCash()
        self._readFundamentalSummary()
        

    def _readIncomeBalanceCash(self) -> None:
        self.incomeStatement = list(self._rawFile["Financials"]["Income_Statement"]["quarterly"].values())
        self.balanceSheet = list(self._rawFile["Financials"]["Balance_Sheet"]["quarterly"].values())
        self.cashFlow = list(self._rawFile["Financials"]["Cash_Flow"]["quarterly"].values())
       

    def _readFundamentalSummary(self) -> None:
        self.general = self._rawFile["General"]
        self.highlights = self._rawFile["Highlights"]
        self.technicals = self._rawFile["Technicals"]
        self.shareStats = self._rawFile["SharesStats"]