from enum import Enum

class ConfigType(str, Enum):   
    BASE = "base"
    ALPACA_MAIN = "alpaca_main"
    ALPACA_SIDE = "alpaca_side"
    EOD = "eod"