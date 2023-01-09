import yaml 
from dataclasses import dataclass

@dataclass(frozen=True) 
class Config:
    ENTRYPERCENT: float 
    REFRESH_DATA: bool 
    OVERWRITE_FUNDAMENTALS: bool 
    IS_PAPER: bool 
    
    def __repr__(self):
        return self.asdict()
    