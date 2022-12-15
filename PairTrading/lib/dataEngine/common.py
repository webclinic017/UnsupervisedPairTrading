from pandas import DataFrame 

class BarCollection:
    def __init__(
        self, 
        daily, 
        weekly, 
        monthly:DataFrame
    ):
        self.daily:DataFrame = daily
        self.weekly:DataFrame = weekly
        self.monthly:DataFrame = monthly