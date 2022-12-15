from enum import Enum

class ConfigType(str, Enum):   
    BASE = "base"
    ALPACA = "alpaca"
    EOD = "eod"