from abc import ABCMeta, abstractclassmethod

class Base(metaclass=ABCMeta):    
    
    @abstractclassmethod 
    def create(cls, *args, **kwargs):
        pass 
    
    