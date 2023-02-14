#ПРИМЕЧАНИЕ - НЕ ЗАБУДЬ ПРО ЮНИТ-ТЕСТЫ ДЛЯ ВСЕГО ЭТОГО ДОБРА

import information_service.InfoMaster.dms_integration.PostgreSQL as PostgreSQL
import information_service.InfoMaster.basic_entities as basic_entities, json, os, pathlib
from filelock import FileLock
from dms_integration.integration_objects import InfobaseConnection

#global variables
__directory = os.path.dirname(__file__)
dms_settings_file = pathlib.Path(__directory, "config", "dms_settings.cfg")
infobase_settings_file = pathlib.Path(__directory, "config", "infobase_settings.cfg")

#ПРИМЕЧАНИЕ - ДОБАВИТЬ СОЗДАНИЕ КОНФИГУРАЦИОННЫХ ФАЙЛОВ С СООТВЕТСТВУЮЩИМИ ПРАВАМИ НА ЧТЕНИЕ И ЗАПИСЬ, ЕСЛИ ИХ НЕ СУЩЕСТВУЕТ
#ПРИМЕЧАНИЕ - МОЖНО ЗАПИСЫВАТЬ НАСТРОЙКИ С Active = False, ЕСЛИ ПРОКАТИЛО СОЗДАНИЕ БАЗЫ - МЕНЯТЬ Active на True
#ПОТОМ МОЖНО ДОБАВИТЬ МЕТОД, КОТОРЫЙ ПРОВЕРЯЛ БЫ НЕАКТИВНЫЕ И ВКЛЮЧАЛ БЫ АКТИВНОСТЬ, ЕСЛИ ВСЕ ОК (БАЗА ЕСТЬ, ТАБЛИЦА МЕТАДАННЫХ ЕСТЬ)
#ПРИМЕЧАНИЕ - ПОТОМ СОЗДАТЬ ОТРЕФАКТОРЕННУЮ ВЕРСИЮ МОДУЛЯ (см. методы create и drop - там каша).
#ПРИМЕЧАНИЕ - ЗАПИСЬ АРГУМЕНТОВ ПРИ ОБЪЯВЛЕНИИ МЕТОДОВ ТОЖЕ ОТРЕФАКТОРИТЬ - СМ. ТИПОВЫЕ БИБЛИОТЕКИ (например json)
#ПРИМЕЧАНИЕ - И ВООБЩЕ ИЗУЧИТЬ ПИТОНОВСКИЕ СОГЛАШЕНИЯ ПРИ НАПИСАНИИ КОДА
class InfobaseList:

    def __init__(self):
        self.__infobases = {}
        self.__dms = {}
        self._read_settings()

    def __repr__(self):
        return str(self.__infobases)

    @property
    def bases_list(self):
        return self.__infobases

    @property
    def dms_list(self):
        return self.__dms

    def get_infobase(self, name):
        if not name in self.__infobases:
            raise ValueError(f"Infobase {name} doesn't exist. Check infobase settings!")
        infobase_settings = self.__infobases[name]
        if not infobase_settings.get("active"):
            raise Exception(f"Infobase {name} isn't active. Operations on deactivated database is forbidden")
        return Infobase(name, infobase_settings, self.__dms[infobase_settings["dms"]])

    @basic_entities.WithChangeSettings(infobase_settings_file)
    def create(self, name:str, dms:str, db_name:str, **additional_settings):
        self._read_settings()
        #ПРИМЕЧАНИЕ - ОТРЕФАКТОРИТЬ (ДЕКОМПОЗИРОВАТЬ МЕТОД)
        #ПРИМЕЧАНИЕ - ДОБАВИТЬ ПОДДЕРЖКУ ЛОГИРОВАНИЯ
        check_result = self.__check_creation_settings(name, db_name, dms)
        if not check_result.success:
            return check_result
        operation_result = self.__execute_dms_operations(dms, db_name, self.__create_infobase, False)
        if not operation_result.success:
            return operation_result
        #ПРИМЕЧАНИЕ - ЭТОТ КУСОК ТОЖЕ В ОТДЕЛЬНЫЙ МЕТОД УПИХАТЬ
        #не принципиально, но поправить - дополнительные настройки пихать в конец, а в начало. а то не очень красиво получается
        infobase_settings = additional_settings.copy()
        infobase_settings["dms"] = dms
        infobase_settings["db_name"] = db_name
        infobase_settings["active"] = True
        self.__infobases[name] = infobase_settings
        return self.__infobase_manipulation_result()

    @basic_entities.WithChangeSettings(infobase_settings_file)
    def drop(self, name:str, delete_infobase:bool = False):
        self._read_settings()
        check_result = self.__check_drop_settings(name)
        if not check_result.success:
            return check_result
        dms = check_result.result["dms"]
        db_name = check_result.result["db_name"]
        dms_connection = connection_factory.create(dms, db_name, **self.__dms[dms])
        if delete_infobase:
            operation_result = self.__execute_dms_operations(dms, db_name, self.__drop_infobase, False)
            if not operation_result.success:
                return operation_result
            self.__infobases.pop(name)
        else:
            self.__infobases[name]["active"] = False
        return self.__infobase_manipulation_result()

    def __infobase_manipulation_result(self):
        #ПРИМЕЧАНИЕ - ЭТОТ ЗЛОДЕЙ НЕКРАСИВО ФОРМАТИРУЕТ КОНЕЧНЫЙ ТЕКСТ. ПОТОМ РАЗОБРАТЬСЯ, КАК МОЖНО СДЕЛАТЬ АККУРАТНЕЕ
        result = json.JSONEncoder(separators=(',\n', ':\n')).encode(self.__infobases)
        return basic_entities.ResultDescription(True, result)

    #ПРИМЕЧАНИЕ - ОПЕРАЦИИ НАД ДАННЫМИ НЕ ЗДЕСЬ, А В КЛАССЕ Infobase. ЗДЕСЬ ПРОСТО КОНСТРУКТОР ЭКЗЕМПЛЯРОВ Infobase прикрутить
    def __execute_dms_operations(self, dms:str, db_name:str, function, connect_to_infobase:bool = True):
        # ПРИМЕЧАНИЕ - ВМЕСТО ПОПЫТОК ОБРАБАТЫВАТЬ ТО, ЧТО ДОЛЖНО ВОЗВРАЩАТЬ СОЕДИНЕНИЕ С СУБД?
        dms_connection = connection_factory.create(dms, db_name, **self.__dms[dms], connect_to_infobase = connect_to_infobase)
        connection_result = dms_connection.connect()
        if not connection_result.success:
            return connection_result
        result = function(dms_connection)
        dms_connection.close()
        return result

    def __create_infobase(self, dms_connection):
        return dms_connection.create_infobase()

    def __drop_infobase(self, dms_connection):
        return dms_connection.drop_infobase()

    def _read_infobase_settings(self):
        self.__infobases = _get_settings(infobase_settings_file)

    def _read_dms_settings(self):
        self.__dms = _get_settings(dms_settings_file)

    def _read_settings(self):
        self._read_infobase_settings()
        self._read_dms_settings()

    def __check_creation_settings(self, name, db_name, dms):
        result = basic_entities.ResultDescription()
        if name in self.__infobases:
            result.result = ValueError(f"Infobase {name} already exists!")
            return result
        for bases_settings in self.__infobases.values():
            if db_name == bases_settings["db_name"]:
                result.result = ValueError(f"Database {db_name} already exists!")
                return result
        if not dms in self.__dms:
            result.result = ValueError(f"DMS {dms} isn't supported!")
            return result
        result.success = True
        return result

    def __check_drop_settings(self, name):
        result = basic_entities.ResultDescription()
        if not name in self.__infobases:
            result.result = ValueError(f"Infobase {name} doesn't exist")
            return result
        result.success = True
        result.result  = self.__infobases[name]
        return result

class Infobase:

    #ПРИМЕЧАНИЕ - ПРОВЕРКИ ТАБЛИЦ МЕТАДАННЫХ ПРИКРУТИТЬ (МОГУТ БЫТЬ ПОБИТЫ ПЛЮС В ТОМ ЖЕ Postgre база создается с автокоммитом)
    #ПРИМЕЧАНИЕ - ДОБАВИТЬ ФУНКЦИОНАЛ ЛОГИРОВАНИЯ (РЕАЛИЗОВАТЬ ЧЕРЕЗ ДЕКОРАТОР). НЕ ПЕРВООЧЕРЕДНАЯ ЗАДАЧА
    #ПРИМЕЧАНИЕ - СДЕЛАТЬ ПРОСТОЙ АНАЛИЗАТОР КОММЕНТОВ В КОДЕ (ПОИСК ПО ТЕГАМ). ТУТ МОЖНО С РЕГУЛЯРНЫМИ ВЫРАЖЕНИЯМИ ПОКОПАТЬСЯ
    #ПРИМЕЧАНИЕ - ЗАПИСЬ МЕТАДАННЫХ ЧЕРЕЗ ЭТОТ ОБЪЕКТ?

    def __init__(self, name:str, infobase_settings:dict = None, dms_settings:dict = None, *args, **kwargs):
        self.__name = name
        self.__infobase_settings = None
        self.__dms_settings = None
        self.__connection = None
        self.__set_settings(infobase_settings, dms_settings)

    @property
    def connection(self):
        return self.__connection

    #ПРИМЕЧАНИЕ - МОЖНО БЛОКИРОВАТЬ НАСТРОЙКИ БАЗЫ ДАННЫХ ОТ ИЗМЕНЕНИЯ ПРИ ПОДКЛЮЧЕНИИ К БАЗЕ ДАННЫХ. ПОТОМ (ПОКА БУДЕМ СЧИТАТЬ, ЧТО ПАРАЛЛЕЛЬНО ТАМ НИКТО КОВЫРЯТЬСЯ НЕ БУДЕТ)
    #ПРИМЕЧАНИЕ - ПИСАТЬ НАСТРОЙКИ НЕ В ФАЙЛ А В РЕЕСТР? А С НИКСАМИ ТОГДА КАК? ПОДУМАТЬ НАД ЭТИМ
    #ПРИМЕЧАНИЕ - ПРОВЕРИТЬ РАБОТОСПОБНОСТЬ ФУНКЦИОНАЛА ЧТЕНИЯ ФАЙЛОВ В НИКСАХ (РАЗВЕРНУТЬ ВИРУТАЛЬНУЮ МАШИНУ С Ubuntu)
    #ПРИМЕЧАНИЕ - ГДЕ-ТО ДОЛЖНЫ БЫТЬ В ЯВНОМ ВИДЕ ОПИСАНЫ ВХОДНЫЕ ПАРАМЕТРЫ ДЛЯ КАЖДОЙ ОПЕРАЦИИ (см. SQL_integration) ЧТОБЫ ВИДНО БЫЛО
    def execute_operations(self, operations:basic_entities.OperationSet, transaction:bool) -> basic_entities.ResultDescription:
        #ПРИМЕЧАНИЕ - ПОТОМ ОТРЕФАКТОРИТЬ (У КЛАССА InfobaseList ЕСТЬ ПОХОЖИЙ ПО ФОРМЕ И СМЫСЛУ КОД РАБОТЫ С СУБД)
        connection_result = self.__connection.connect()
        if not connection_result.success:
            return connection_result.result
        result = self.__connection.execute_operations(operations, transaction)
        self.__connection.close()
        return result

    #ПРИМЕЧАНИЕ - потом прикрутить проверку на изменение файла настроек или базы какой-то злонамеренной сволочью
    #ПРИМЕЧАНИЕ - КОГДА ПОЯВЯТСЯ ЛОГИ - ОТКОРРЕТИРОВАТЬ НА ВОЗВРАЩЕНИЕ ЗНАЧЕНИЯ (ЧТОБЫ ОНО ПИСАЛОСЬ В ЛОГ ЧЕРЕЗ ДЕКОРАТОР)
    #ПРИМЕЧЕНИЕ - КРОМЕ ЛОГА-ДЕКОРАТОРА МОЖНО ДОБАВИТЬ И ПРОСТОЙ ЛОГИРУЮЩИЙ МЕТОД (ИЛИ КЛАСС?), ЧТОБЫ ВСТАВЛЯТЬ ТУДАБ ГДЕ ДЕКОРАТОР НЕ КО ДВОРУ
    def __set_settings(self, infobase_settings:dict = None, dms_settings:dict = None):
        self.__set_infobase_settings(infobase_settings)
        self.__set_dms_settings(dms_settings)
        self.__set_connection()

    def __set_infobase_settings(self, infobase_settings:dict = None):
        if infobase_settings:
            self.__infobase_settings = infobase_settings
        else:
            _infobase_settings = _get_settings(infobase_settings_file).get(self.__name)
            if not _infobase_settings or not _infobase_settings.get("active"):
                raise Exception(f"Infobase {self.__name} isn't exist or don't active!")
            self.__infobase_settings = _infobase_settings

    def __set_dms_settings(self, dms_settings:dict = None):
        dms = self.__infobase_settings["dms"]
        if dms_settings:
            self.__dms_settings = dms_settings
        else:
            conn_settings = _get_settings(dms_settings_file).get(dms)
            if not conn_settings:
                raise Exception(f"DMS {dms} isn't supported!")
            self.__dms_settings = conn_settings

    def __set_connection(self):
        self.__connection = connection_factory.create(self.__infobase_settings["dms"],
                                                      self.__infobase_settings["db_name"], **self.__dms_settings, connect_to_infobase = True)

    #ПРИМЕЧАНИЕ - НЕ ЗАБУДЬ МЕТОД ЧТЕНИЯ МЕТАДАННЫХ. ПРИ КАЖДОМ ИЗМЕНЕНИИ МЕТАДАННЫХ ВНОСИМ ИЗМЕНЕНИЯ В ТАБЛИЦЫ МЕТАДАННЫХ
    #ПРИМЕЧАНИЕ - НЕ ЗАБУДЬ ОБНОВЛЯТЬ МЕТАДАННЫЕ ПРИ ВНЕСЕНИИ ИЗМЕНЕНИЙ И ВЫПОЛНЕНИИ ОПЕРАЦИЙ (ЛИБО ИЗОБРЕТИ РЕЖИМ КОНФИГУРАТОРА УЖЕ В ИНТЕРФЕЙСЕ)


#Factories

class _ConnectionFactory:
    def __init__(self):
        self.__objects = {}

    def register_object(self, key, builder):
        #ПРИМЕЧАНИЕ - ДОБАВИТЬ ПРОВЕРКУ ПОДКЛЮЧАЕМЫХ БИЛДЕРОВ НА ПРИНАДЛЕЖНОСТЬ К ПОТОМКАМ integration_objects.InfobaseConnection?
        self.__objects[key] = builder

    def create(self, key, infobase, **settings):
        builder = self.__objects.get(key)
        if not builder:
           raise ValueError(f"Supported DMS are {tuple(self.__objects.keys())}.DMS {key} is not supported!")
        return builder(infobase, **settings)

#service methods
def _get_settings(file:str):
    #тут можно вставить проверку наличия и корректности содержимого файла. потом
    with open(file, "r") as settings:
        settings_list = json.load(settings)
    return settings_list

#init

connection_factory = _ConnectionFactory()
connection_factory.register_object("PostgreSQL", PostgreSQL.DatabaseConnection)
