#main library module
import json, os, pathlib
from information_service.metadata import Metadata
from information_service.data_objects import DataManager
from data_managers import infobase_manager_factory

#global variables
__directory = os.path.dirname(__file__)
infobase_settings_file = pathlib.Path(__directory, "config", "infobase_settings.cfg")

class InfobaseList:

    _data_managers = ["InfoMaster"]
    _dms = ["PostgreSQL"]

    def __init__(self):
        self.__infobases = self.__read_infobases()

    def __getitem__(self, item):
        if not item in self.__infobases or not self.__infobases[item].get("active"):
            raise ValueError(f"There is no active infobase {item}. Check \information_service\config\infobase_settings.cfg")
        return _Infobase(item)

    @property
    def infobases(self):
        return self.__infobases

    def create_infobase(self, name:str, data_manager:str, dms:str):
        self.__check_creation_settings(name, data_manager, dms)
        infobase_manager_factory.create(data_manager).create_infobase(name, dms)
        self.__add_infobase(name, data_manager, dms)
        Metadata(name).create_metadata_storage()
        return _Infobase(name)

    #А ВОТ ЭТОТ КЛАСС ПУСТЬ БАЗЫ ДАННЫХ И СОЗДАЕТ. ЗАНЯТЬСЯ ЭТИМ 19.01.23

    def __read_infobases(self):
        with open(infobase_settings_file, "r") as settings:
            infobase_settings = json.load(settings)
        return infobase_settings

    def __check_creation_settings(self, name:str, data_manager:str, dms:str):
        if name in self.__infobases:
            raise ValueError(f"Infobase {name} already exists!")
        if not data_manager in self._data_managers:
            raise ValueError(f"Data manager {data_manager} is not in supported data managers\n{self._data_managers}")
        if not dms in self._dms:
            raise ValueError(f"Data management system {dms} is not in supported dms\n{self._dms}")

    def __add_infobase(self, name:str, data_manager:str, dms:str):
        self.__infobases = self.__read_infobases()
        self.__infobases[name] = {"active":True, "data_manager":data_manager, "dms":dms}
        with open(infobase_settings_file, "w") as settings:
            json.dump(self.__infobases, settings)

class _Infobase:
    def __init__(self, name:str):
        self.__name = name

    def metadata(self):
        return Metadata(self.__name)

    def data_manager(self):
        return DataManager(self.__name)
