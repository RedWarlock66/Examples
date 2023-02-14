#This module contains a functionality designed to work with PostgreSQL DMBS
import datetime, uuid

import psycopg2, psycopg2.extensions as extensions
from psycopg2 import Error
from json.decoder import JSONDecoder
import information_service.InfoMaster.dms_integration.SQL_itegration as SQL_integration
from information_service.InfoMaster.dms_integration.integration_objects import InfobaseConnection
import information_service.InfoMaster.basic_entities as basic_entities

#EXTERNAL API
#ПОИСКАТЬ АНАЛОГ ОБЛАСТЕЙ В ПИТОНЕ

class DatabaseConnection(InfobaseConnection):

    default_isolation_level = "ISOLATION_LEVEL_READ_COMMITED"

    @property
    def connected(self):
        if not self.__connection:
            return False
        else:
            return True

    def __init__(self, infobase:str, **settings):
        self.__infobase            = infobase
        self.__connection_settings = settings["connection_settings"]
        self.__isolation_level     = self.__get_isolation_level(settings.get("isolation_level"))
        self.__connect_to_infobase = settings.get("connect_to_infobase")
        self.__connection = None

    def connect(self):
        if not self.__connection:
            settings = self.__connection_settings
            if self.__connect_to_infobase:
                settings["database"] = self.__infobase
            try:
                self.__connection = psycopg2.connect(**settings)
                return basic_entities.ResultDescription(True)
            except Exception as error:
                return basic_entities.ResultDescription(False, error)

    def create_infobase(self):
        return self.__database_manipulation("CREATE")

    def drop_infobase(self):
        return self.__database_manipulation("DROP")

    def execute_operations(self, operations:basic_entities.OperationSet, transaction:bool, *args, **kwargs):
        #ПРИМЕЧАНИЕ - СДЕЛАТЬ ПРОВЕРКУ АРГУМЕНТОВ ОПЕРАЦИЙ!!!
        if operations.is_empty():
            return basic_entities.ResultDescription(True)

        self.__connection.set_session(isolation_level = self.__isolation_level, autocommit = not transaction)
        cursor = self.__connection.cursor()

        #ПРИМЕЧАНИЕ - ПРОТЕСТИТЬ РАБОТУ В РЕЖИМЕ "СОБРАТЬ ВСЕ ТЕКСТЫ ЗАПРОСОВ В ПАКЕТ И ПЕРЕДАТЬ ПАКЕТ КУРСОРУ НА ВЫПОЛНЕНИЕ"
        #ПРИМЕЧАНИЕ - КАК ПРИ ЭТОМ БУДЕТ РАБОТАТЬ АВТОКОММИТ? ЗАКОММИТИТ ОПЕРАЦИИ ДО ОБЛОМА?
        #ПРИМЕЧАНИЕ - ПРОТЕСТИТЬ ЭТО ПОЗЖЕ (МЕХАНИКУ ЗАПОЛНЕНИЯ РЕЗУЛЬТАТОВ ОПЕРАЦИИ ПРИДЕТСЯ ПЕРЕДЕЛЫВАТЬ, ДЛЯ ЭТОГО НУЖНО В psycopg2 КОВЫРЯТЬСЯ, ТАК ЧТО НЕ БЫСТРО)
        for operation in operations:
            arguments = self.__get_arguments(operation)
            query_text = SQL_integration.QueryTextGenerator(operation.Table).query_text(operation.Type, **arguments)
            try:
                cursor.execute(query_text)
            except Exception as error:
                if transaction:
                    self.__connection.rollback() #ПРИМЕЧАНИЕ - НУЖНО ОТКАТЫВАТЬ ТРАЗАКЦИЮ ЯВНО?
                cursor.close()
                operation.Result = error
                return basic_entities.ResultDescription(False, error)
            else:
                operation.Result = self.__operation_result(operation.Type, cursor)

        if transaction:
            self.__connection.commit()

        cursor.close()
        return basic_entities.ResultDescription(True, operations)

    def close(self):
        if self.__connection:
            self.__connection.close()
            self.__connection = None

    @classmethod
    def __get_isolation_level(cls, isolation_level:str = None):
        if not isolation_level:
            isolation_level = cls.default_isolation_level
        return getattr(extensions, isolation_level)

    def __database_manipulation(self, manipulation:str):
        #ПРИМЕЧАНИЕ - ВЫНЕСТИ ОБЩИЕ С execute_operations ОПЕРАЦИИ В ОТДЕЛЬНЫЙ МЕТОД? (ЕСТЬ НЕКОТОРОЕ СООТВЕТСТВИЕ)
        self.__connection.set_isolation_level(self.__get_isolation_level("ISOLATION_LEVEL_AUTOCOMMIT"))
        cursor = self.__connection.cursor()
        try:
            cursor.execute(f"{manipulation} DATABASE {self.__infobase}")
            cursor.close()
            return basic_entities.ResultDescription(True)
        except (Exception, Error) as error:
            cursor.close()
            return basic_entities.ResultDescription(False, error)


    def __get_arguments(self, operation):
        #ПРИМЕЧАНИЕ - ПРОВЕРКУ АРГУМЕНТОВ СЮДА ПРИСОБАЧИТЬ?
        #ПРИМЕЧАНИЕ - СДЕЛАТЬ ПРОВЕРКУ ТИПОВ ЗНАЧЕНИЙ В ТАБЛИЦЕ для Insert и словаре для Update. ПРОВЕРКА НА NULL ТОЖЕ ГДЕ-ТО ДОЛЖНА БЫТЬ
        #ПРИМЕЧАНИЕ - ПРИСОБАЧИТЬ ПРЕОБРАЗОВАНИЕ ТИПОВ для Insert и Update, в частности - datetime.datetime
        if operation.Settings:
            arguments = operation.Settings.copy()
            if operation.Type == "create":
                arguments["description"] = self.__convert_types(arguments["description"])
            elif operation.Type == "add_attribute" or operation.Type == "alter_attribute":
                arguments["description"] = self.__convert_type(arguments["description"])
        else:
            arguments = {}
        return arguments

    #SERVICE methods
    #ПРИМЕЧАНИЕ - ОТРЕФАКТОРИТЬ? ДВА МЕТОДА НИЖЕ ПОХОЖИ ПО СМЫСЛУ
    #converting types for insert query.
    def __convert_types(self, description:basic_entities.DBRelationDescription):
        сonverted_description = description.copy_attributes()
        сonverted_description.attributes_add({"Type":None}, True)
        for attribute_description in description:
            attribute_dict = attribute_description.to_dict()
            attribute_dict["Type"] = self.__postgre_type(attribute_description.Type)
            сonverted_description.records_add(attribute_dict)
        return сonverted_description

    #converting types for alter attribute query
    def __convert_type(self, description:basic_entities.RelationAttributeDescription):
        if description.Type == None:
            return description
        converted_description = basic_entities.RelationAttributeDescription(True)
        description_values = description.values
        converted_description.Type = self.__postgre_type(description_values.pop("Type"))
        converted_description.fill(description_values)
        return converted_description

    def __postgre_type(self, python_type: type):
        if python_type in type_dict:
            return type_dict[python_type]
        else:
            raise TypeError(f"Type {python_type} is not supported!")

    def __operation_result(self, operation_type:str, cursor):
        if operation_type == "select":
            return self.__result_to_table(cursor)
        else:
            return True

    def __result_to_table(self, cursor: extensions.cursor):
        attributes = {cstring.name:self.__oid_to_type(cstring.type_code) for cstring in cursor.description}
        relation = basic_entities.Relation(attributes)
        #ПРИМЕЧАНИЕ - ТУТ НАВЕРНОЕ МОЖНО КАК-ТО НА ГЕНЕРАТОРЫ ПЕРЕВЕСТИ. ПОДУМАТЬ НАД РЕФАКТОРИНГОМ ПОТОМ
        #ПРИМЕЧАНИЕ - ПОТОМ ПРОВЕРИТЬ, КАК ПРЕОБРАЗОВЫВАЮТСЯ ТИПЫ
        for row in cursor.fetchall():
            index = 0
            record = relation.records_add()
            for attribute in attributes:
                setattr(record, attribute, row[index])
                index = index + 1
        return relation

    def __oid_to_type(self, oid):
        if not oid in oid_dict:
            return None
        else:
            return oid_dict[oid]

def __set_range(oid_dict: dict, start: int, stop: int, value: type):
    for i in range(start, stop):
        oid_dict[i] = value

#ПРИМЕЧАНИЕ - ДОБАВИТЬ КВАЛИФИКАТОРЫ ПРИМИТИВНЫХ ТИПОВ
type_dict = {}
type_dict[bool] = "bool"
type_dict[float] = "real"
type_dict[int] = "integer"
type_dict[str] = "varchar"
type_dict[datetime.datetime] = "timestamp"
type_dict[uuid.UUID] = "uuid"

oid_dict = {}
oid_dict[16] = bool
oid_dict[18] = str
oid_dict[25] = str
oid_dict[1043] = str
oid_dict[1114] = datetime.datetime
__set_range(oid_dict, 20, 24, int)
__set_range(oid_dict, 700, 702, float)































