#objects for operations on data

from information_service.InfoMaster.basic_entities import  OperationSet, ResultDescription
from abc import ABC, abstractmethod

#API
#РАСКИДАТЬ АБСТРАКТНЫЕ КЛАССЫ И ФАБРИКИ ПО РАЗНЫМ МОДУЛЯМ ВО ИЗБЕЖАНИЕ КРУГОВЫХ ЗАВИСИМОСТЕЙ

class InfobaseConnection(ABC):

    @abstractmethod
    def __init__(self, infobase:str, **settings):
        pass

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def create_infobase(self):
        return ResultDescription(True)

    @abstractmethod
    def drop_infobase(self):
        return ResultDescription(True)

    @abstractmethod
    def execute_operations(self, operations:OperationSet, transaction:bool = True, *args, **kwargs):
        return ResultDescription(True)

    @abstractmethod
    def close(self):
        pass

