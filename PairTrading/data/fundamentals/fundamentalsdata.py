from PairTrading.data.fundamentals.firmCharacteristics import FirmCharGetter
from PairTrading.lib.dataEngine.common import BarCollection
from pandas import Series

class FundamentalsData:
    
    def __init__(
        self,
        rawFile: dict
    ) -> None:
        self._rawFile:dict = rawFile
        self.firmCharacteristics:FirmCharGetter = FirmCharGetter.create(rawFile)

        
    @classmethod
    def create(cls, rawFile:dict):
        if not FundamentalsData._isFileValid(rawFile):
            raise ValueError("key fields missing from the raw json file")
        
        return cls(rawFile)
    
    def setTechnicalBars(self, bars:BarCollection) -> None:
        self.firmCharacteristics.setDailyBar(bars.daily)
        self.firmCharacteristics.setWeeklyBar(bars.weekly)
        self.firmCharacteristics.setMonthlyBar(bars.monthly)
    
    def getFundamentals(self) -> Series:
        if not self.firmCharacteristics:
            raise AttributeError("No firm characteristics getter detected")
        
        fundamentalsDict:dict = {}
        
        getList:list = [method for method in dir(self.firmCharacteristics) if method.startswith("get")]
        
        for getMethod in getList:
            fundamentalsDict[getMethod.split("get")[1]] = getattr(self.firmCharacteristics, getMethod)
            
        return Series(fundamentalsDict)
    
    @staticmethod
    def _isFileValid(rawFile:dict) -> bool:
        return 
        ("Highlights" in rawFile.keys() and 
        "Technicals" in rawFile.keys() and 
        "ShareStats" in rawFile.keys() and
        "Financials" in rawFile.keys() and
        rawFile["Technicals"]["beta"] and 
        rawFile["Highlights"]["EarningsShare"] and
        len(rawFile["Financials"]["Income_Statement"]["quarterly"].values()) >= 13 and 
        len(rawFile["Financials"]["Balance_Sheet"]["quarterly"].values()) >= 13 and 
        len(rawFile["Financials"]["Cash_Flow"]["quarterly"].values()) >= 13)
        
    def _readIncomeBalanceCash(self) -> None:
        self.incomeStatement = self.rawFile["Income_Statement"]["quarterly"]
        self.balanceSheet = self.rawFile["Balance_Sheet"]["quarterly"]
        self.cashFlow = self.rawFile["Cash_Flow"]["quarterly"]
        
    
    