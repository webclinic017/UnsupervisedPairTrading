from abc import ABC

class Base(ABC):    
    @classmethod   
    def create(cls, *args, **kwargs):
        pass 
    
    