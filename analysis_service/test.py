import datetime
import os, re
from abc import ABC, abstractmethod
from chardet.universaldetector import UniversalDetector

class _LinkRecord:
    def __init__(self, link_subject:str, link_object:str, weight:int):
        self.__link_subject = link_subject
        self.__link_object = link_object
        self.__weight = weight

    def __str__(self):
        return str(self.values)

    @property
    def link_subject(self):
        return self.__link_subject

    @property
    def link_object(self):
        return self.__link_object

    @property
    def weight(self):
        return self.__weight

    @weight.setter
    def weight(self, value):
        self.__weight = value
    
    @property
    def values(self):
        return {"link_subject":self.link_subject, "link_object":self.link_object, "weight":self.weight}

    def match(self, link_subject:str, link_object:str):
        return self.link_subject == link_subject and self.link_object == link_object

class LinkMatrix:
    def __init__(self):
        self.__records = []

    def __getitem__(self, item):
        return self.__records[item]

    def __iter__(self):
        return iter(self.__records)

    def __str__(self):
        return "\n".join([str(record) for record in self])

    @property
    def values(self):
        return [record.values for record in self]

    def set_record(self, link_subject:str, link_object:str, weight:int) -> _LinkRecord:
        link_record = _LinkRecord(link_subject, link_object, weight)
        record_index = self.record_index(link_subject, link_object)
        if record_index == -1:
            self.__records.append(link_record)
        else:
            self.__records[record_index] = link_record
        return link_record

    def drop_record(self, index:int = None, record:_LinkRecord = None, link_subject:str = None, link_object:str = None):
        if not index is None:
            self.__records.pop(index)
        elif not record is None:
            self.__records.remove(record)
        elif not link_subject is None and not link_object is None:
            index = self.record_index(link_subject, link_object)
            self.__records.pop(index)
        else:
            raise ValueError("Index or record or link_subject and link_object shouldn't be None!")

    def record_index(self, link_subject:str, link_object:str) -> int:
        index = 0
        for record in self.__records:
            if record.match(link_subject, link_object):
                return index
            index = index + 1
        return -1

    def find_record(self, link_subject:str, link_object:str) -> _LinkRecord or None:
        for record in self.__records:
            if record.match(link_subject, link_object):
                return record
        return None

class Analyzer(ABC):
    @abstractmethod
    def __init__(self, **settings):
        #ПРИМЕЧАНИЕ - ВХОДЯЩИЕ АРГУМЕНТЫ ЛУЧШЕ УНИФИЦИРОВАТЬ - поSOLIDнее БУДЕТ
        pass

    @abstractmethod
    def link_matrix(self) -> LinkMatrix:
        pass


class PythonAnalizer(Analyzer):

    _source_extension = ".py"
    _default_encoding = "utf-8"
    _site_packages = ["\Lib", "\site-packages", "\Scripts"]
    _import_tags = ["import ", "from "]
    _comment_tag = "#"

    def __init__(self, path:str, without_site_packages:bool = False, **settings):
        self.__path = path
        self.__without_site_packages = without_site_packages

    def link_matrix(self) -> LinkMatrix:
        link_matrix = LinkMatrix()
        self.__fill_link_matrix(link_matrix)
        return link_matrix

    def __fill_link_matrix(self, link_matrix:LinkMatrix):
        for descr in os.walk(self.__path):
            if not self.__is_site_package(descr[0]) or not self.__without_site_packages:
                for filename in descr[2]:
                    if filename.endswith(self._source_extension):
                        filepath = os.path.join(descr[0], filename)
                        #ПРИМЕЧАНИЕ - ЗДЕСЬ НУЖЕН МЕТОД, ПРЕОБРАЗУЮЩИЙ ИМЯ ФАЙЛА В ИМЯ МОДУЛЯ
                        #(имя_каталога.имя_подкаталога.имя_модуля), чтобы это стыковалось с полученным из файлов
                        #прикрутить 25.01.2023
                        module_name = self.__module_name(filename)
                        self.__fill_from_source(module_name, filepath, link_matrix)

    def __module_name(self, filename:str):
        return filename.replace(self._source_extension, "")

    def __fill_from_source(self, module_name:str, filepath:str, link_matrix:LinkMatrix):
        encoding = self.__encoding(filepath)
        if not self.__is_readable(filepath, encoding):
            return
        imported_objects = {}
        for line in open(filepath, "r", encoding=encoding):
            if line.isspace() or line.lstrip("\t").lstrip().startswith(self._comment_tag):
                continue
            #ПРИМЕЧАНИЕ - ПОТОМ УЧЕСТЬ СИТУАЦИИ, КОГДА ШАРП ВСТРЕЧАЕТСЯ ВНУТРИ КАВЫЧЕК
            #И СИТУАЦИИ, КОГДА СУЩЕСТВУЮТ КОММЕНТАРИИ В '''. УПАКОВАТЬ ЭТО В ОТДЕЛЬНЫЙ МЕТОД
            #ПРИМЕЧАНИЕ - ПОСМОТРЕТЬ, МОЖНО ЛИ АККУРАТНЕЕ РАЗБИРАТЬ СТРОКИ ТАК, КАК ТУТ НАДО
            #ПОСРЕДСТВОМ РЕГУЛЯРОК, А НЕ КУЧИ РУЧНЫХ ПРОВЕРОК
            if self._comment_tag in line:
                code_line = line[0:self.__tag_postiton(line, self._comment_tag)]
            else:
                code_line = line.rstrip("\n")
            line_imported_objects = self.__imported_objects(code_line)
            #ПРИМЕЧАНИЕ - АНАЛИЗ СТРОК ПОКА НЕ ОПТИМАЛЕН
            #(ДЛЯ КАЖДОЙ НЕ ПУСТОЙ СТРОКИ КОДА ИЩЕТСЯ ВХОЖДЕНИЕ ЛЮБОГО ЭЛЕМЕНТА ИЗ ИМПОРТА ПЕРЕБОРОМ)
            #ПОТОМ ОПТИМИЗИРОВАТЬ, ЕСЛИ ПОТРЕБУЕТСЯ
            if not line_imported_objects is None: #it's an import line
                imported_objects.update(line_imported_objects)
            else: # it's code line which may use importer objects by their aliases
                self.__add_importer_objects(module_name, code_line, imported_objects, link_matrix)

    def __add_importer_objects(self, module_name:str, code_line:str,
                               imported_objects:dict[tuple[str, str]:str], link_matrix:LinkMatrix):
        for imported_object, module in imported_objects.items():
            #ПРИМЕЧАНИЕ - ТУТ ЕСТЬ ЕЩЕ ИМЕНА КОНКРЕТНЫХ ОБЪЕКТОВ ИЗ МОДУЛЕЙ. ПОКА НЕ ИСПОЛЬЗУЕМ
            #ПРИГОДЯТСЯ, КОГДА (ЕСЛИ) БУДЕМ ПРИКРУЧИВАТЬ ДЕТАЛИЗАЦИЮ СВЯЗЕЙ ДО ОБЪЕКТОВ (ПОКА ДЕТАЛИЗАЦИЯ СВЯЗЕЙ БУДЕТ ДО МОДУЛЕЙ)
            alias_entries_count = self.__alias_entries_count(code_line, imported_object[1])
            if alias_entries_count > 0:
                #ПРИМЕЧАНИЕ - АЛГОРИТМ ДОБАВЛЕНИЯ ЗАПИСЕЙ В МАТРИЦУ СВЯЗЕЙ НЕОПТИМАЛЕН (ОТЪЕДАЕТ ДО ТРЕТИ ВРЕМЕНИ РАБОТЫ МЕТОДА)
                #ПОТОМ ОПТИМИЗИРОВАТЬ
                link_record = link_matrix.find_record(link_subject=module_name, link_object=module)
                if link_record is None:
                    link_matrix.set_record(link_subject=module_name, link_object=module, weight=alias_entries_count)
                else:
                    link_record.weight = link_record.weight + alias_entries_count

    def __alias_entries_count(self, code_line:str, object_alias:str) -> int:
        #НАЧАЛО - КОСТЫЛЬ (ПОКА НЕ РАЗБЕРЕМСЯ С КРИВЫМИ АЛИАСАМИ)
        if not object_alias:
            return 0
        is_alpha = False
        for _symbol in object_alias:
            if _symbol.isalpha():
                is_alpha = True
                break
        if not is_alpha:
            return 0
        #ОКОНЧАНИЕ - КОСТЫЛЬ (ПОКА НЕ РАЗБЕРЕМСЯ С КРИВЫМИ АЛИАСАМИ)

        result = 0
        #ПРИМЕЧАНИЕ - ПОТОМ УЧЕСТЬ СИТУАЦИИ, КОГДА АЛИАС ВСТРЕЧАЕТСЯ ВНУТРИ КАВЫЧЕК
        #ПРИМЕЧАНИЕ - ПОТОМ РАЗОБРАТЬСЯ, ПОЧЕМУ re.finditer ГЛЮЧИТ. ПОКА СВОЙ МЕТОД ИСПОЛЬЗУЕМ
        for entry in self.__finditer(object_alias, code_line):
            #if alias is not a part of some bigger variable description - add 1 to aliases entries count
            if self.__check_symbol(code_line, entry[0] - 1) and self.__check_symbol(code_line, entry[1]):
                result = result + 1
        return result

    def __finditer(self, pattern: str, string: str) -> list[tuple[int, int]]:
        result = []
        index = 0
        _len = len(pattern)
        while index + _len <= len(string):
            if string[index:index + _len] == pattern:
                result.append((index, index + _len))
                index = index + _len
            else:
                index = index + 1
        return result

    def __check_symbol(self, code_line:str, index:int) -> bool:
        if index < 0:
            return True
        elif index >= len(code_line):
            return True
        _symbol = code_line[index]
        #ПРИМЕЧАНИЕ - ПОТОМ ПЕРЕПРОВЕРИТЬ УСЛОВИЕ НИЖЕ (кавычки должны отсекаться в __object_in_line)
        return not (_symbol.isalpha() or _symbol.isdigit() or _symbol == "_")

    def __imported_objects(self, line:str) -> dict[tuple[str, str]:str] or None:
        #ПРИМЕЧАНИЕ - СИТУАЦИИ ТИПА * И (') ПОКА НЕ АНАЛИЗИРУЕМ. ИХ АНАЛИЗ ПРИКРУТИТЬ ПОТОМ - ДЛЯ РАБОЧЕЙ ВЕРСИИ
        #ПРИМЕЧАНИЕ - КРИВОЙ НЕЗАКОНЧЕННЫЙ СИНТАКСИС ТОЖЕ ПОКА ИГНОРИМ. ПАРСИМ ТОЛЬКО СИНТАКСИЧЕСКИ КОРРЕКТНЫЕ ВЕЩИ
        stripped_line = line.strip("\n").strip()
        if stripped_line.startswith(self._import_tags[0]):
            modules = self.__get_imported_objects(stripped_line, start=len(self._import_tags[0])-1)
            #ПРИМЕЧАНИЕ - ПОТОМ РАЗОБРАТЬСЯ, ОТКУДА БЕРУТСЯ КРИВЫЕ АЛИАСЫ И УБРАТЬ
            return {module:module[1] for module in modules}
        elif (stripped_line.startswith(self._import_tags[1]) and self._import_tags[0] in stripped_line):
            module_tag_position_end = self.__tag_postiton(stripped_line, self._import_tags[1], True)
            objects_tag_position_start = self.__tag_postiton(stripped_line, self._import_tags[0])
            objects_tag_position_end = objects_tag_position_start + len(self._import_tags[0])
            module = self.__get_imported_objects(stripped_line, module_tag_position_end, objects_tag_position_start)[0][1]
            _objects = self.__get_imported_objects(stripped_line, objects_tag_position_end)
            return {_object:module for _object in _objects}
        else:
            return None

    def __tag_postiton(self, line:str, tag:str, after_tag:bool = False) -> int:
        tag_position = line.index(tag)
        if after_tag:
            tag_position = tag_position + len(tag)
        return tag_position

    def __get_imported_objects(self, line:str, start:int = None, end:int = None) -> list[tuple[str, str]]:
        if start is None:
            start = 0
        if end is None:
            end = len(line)
        stripped_line = line[start:end].strip("\n")
        return [self.__get_name_pair(object_line) for object_line in stripped_line.split(",")]

    def __get_name_pair(self, descr:str) -> tuple[str, str]:
        descr_array = descr.split(" as ")
        if len(descr_array) == 1:
            stripped_name = descr_array[0].strip()
            return (stripped_name, stripped_name)
        else:
            return (descr_array[0].strip(), descr_array[1].strip())

    def __module_name(self, filename:str) -> str:
        return filename.replace(self._source_extension, "")

    def __encoding(self, filename):
        detector = UniversalDetector()
        for line in open(filename, "rb"):
            detector.feed(line)
            if detector.done:
                break
        detector.close()
        if detector.result["encoding"] is None:
            return self._default_encoding
        else:
            return detector.result["encoding"]

    def __is_readable(self, filename:str, encoding:str):
        with open(filename, "r", encoding=encoding) as file:
            try:
                file.readline()
            except:
                return False
        return True

    def __is_site_package(self, path:str):
        for site_package_dir in self._site_packages:
            if site_package_dir in path:
                return True
        return False

path = "D:\Python\Projects\InfoMaster"
print(f"Begins in {datetime.datetime.now()}")
analyzer = PythonAnalizer(path, False)
link_matrix = analyzer.link_matrix()
print(link_matrix)
print(f"Ends in {datetime.datetime.now()}")
#ПРИМЕЧАНИЕ - ДОВЕСТИ ДО УМА СЕРВЕРНУЮ ЧАСТЬ МЕХАНИЗМА АНАЛИЗА (ПЕРЕЧИТАТЬ ТЕХПРОЕКТ
#ПРИМЕЧАНИЕ - В МАТРИЦЕ СВЯЗЕЙ ХРАНИТЬ НЕ ТОЛЬКО КОНЕЧНЫЕ МОДУЛИ, НО И ИХ РАСПОЛОЖЕНИЕ
#(ДЛЯ ПИТОНА - ПАПКИ ВЫШЕ УРОВНЕМ, ГДЕ ЕСТЬ __init__.py). СВЯЗАТЬ С АЛГОРИТМОМ, ПОЛУЧАЮЩИМ ТАКИЕ ДАННЫЕ ПРИ ЧТЕНИИ ФАЙЛОВ

