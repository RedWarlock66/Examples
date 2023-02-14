#common objects for integration with SQL dbms'
import information_service.InfoMaster.basic_entities as basic_entities

def _value_repr(value, None_repr='NULL'):
    if value == None:
        return None_repr
    if isinstance(value, str):
        return f"'{value}'"
    else:
        return str(value)

#generatot of query texts. only works with single db table
class QueryTextGenerator:
    def __init__(self, table:str):
        self.__table = table

    def query_text(self, operation_type:str, *args, **kwargs):
        if not operation_type in operation_functions:
            raise ValueError(f"Invalid operation {operation_type}. Only {operation_functions} are possible")
        return operation_functions[operation_type](self, *args, **kwargs)

    def create(self, description:basic_entities.DBRelationDescription):
        attributes = ",\n".join([self.__attribute_description(record) for record in description])
        return f"CREATE TABLE {self.__table} (\n{attributes}\n)"

    def rename(self, name:str):
        return f"ALTER TABLE {self.__table} RENAME TO {name}"

    def drop(self):
        return f"DROP TABLE {self.__table}"

    def add_attribute(self, description:basic_entities.RelationAttributeDescription):
        return f"ALTER TABLE {self.__table}\nADD COLUMN {self.__attribute_description(description)}"

    def drop_attribute(self, name:str):
        return f"ALTER TABLE {self.__table}\nDROP COLUMN {name}"

    def alter_attribute(self, name:str, description:basic_entities.RelationAttributeDescription):
        query_text = "\n".join(
            [f"{self.__alter_attribute_description(name, attribute, value)}" for attribute, value in description.values.items()]
        ).strip()
        if description.Name and description.Name != name:
            query_text = query_text + f"\nALTER TABLE {self.__table}\nRENAME {name} TO {description.Name}"
        return query_text

    def select(self, attributes:tuple = None, filter:basic_entities.OperationFilter = None, order:basic_entities.OperationResultOrder = None):
        return f"SELECT {self.__select_attributes(attributes)}\nFROM {self.__table}{self.__filter(filter)}{self.__order(order)}"

    def insert(self,  values:basic_entities.Relation):
        _attributes = f"({', '.join(list(values.attributes))})"
        _values = ",\n".join(
            [f"({', '.join([_value_repr(value) for value in record.values.values()]).strip()})" for record in values])
        return f"INSERT INTO {self.__table} {_attributes}\nVALUES \n{_values}"

    def update(self, values:dict, filter:basic_entities.OperationFilter = None):
        set = ", ".join([f"{key} = {_value_repr(value)}" for key, value in values.items()])
        return f"UPDATE {self.__table}\nSET {set}{self.__filter(filter)}"

    def delete(self, filter:basic_entities.OperationFilter = None):
        return f"DELETE FROM {self.__table}{self.__filter(filter)}"

    def index(self, description:basic_entities.RelationIndexDescription):
        attributes_description = ','.join(description.attributes)
        cluster_description = self.cluster(description.name, description.cluster)
        return f"CREATE INDEX {description.name} ON {self.__table} ({attributes_description}){cluster_description}"

    def cluster(self, index:str, cluster:bool):
        if cluster:
            return f";\nCLUSTER {self.__table} USING {index}"
        else:
            return ""

    def drop_index(self, name, if_exists:bool = False):
        if if_exists:
            if_exists_text = " IF EXISTS "
        else:
            if_exists_text = " "
        return f"DROP INDEX{if_exists_text}{name}"

    #service
    def __alter_attribute_description(self, attribute:str, name:str, value):
        if value is None:
            return ""
        if name == "Name" or "":
            return "" #rename query must be the last, so we are passing name in this method
        alter_text = f"ALTER TABLE {self.__table}\nALTER COLUMN {attribute}"
        if name in attribute_flags:
            if value:
                action = "SET"
            else:
                action = "DROP"
            return f"{alter_text} {action} {attribute_flags[name]};"
        if name == "Type":
            prefix = "TYPE "
        else:
            prefix = "SET "
        return f"{alter_text} {self.__attribute_property(name, value, prefix)};"

    def __attribute_description(self, properties):
        return " ".join([self.__attribute_property(name, value) for name, value in properties.values.items()]).strip()

    def __attribute_property(self, name:str, value, prefix:str = ""):
        if not name in attribute_properties:
            raise ValueError(f"Invalid property! Property {name} ins't supported")
        if value == None:
            return ""
        else:
            return prefix + attribute_properties[name](value)

    def __None_repr(self, value, None_repr):
        if value == None:
            return None_repr
        else:
            return value

    def __select_attributes(self,attributes:tuple = None):
        if not attributes:
            return "*"
        else:
            return ','.join(attributes)

    def __filter(self, filter:basic_entities.OperationFilter = None):
        if not filter:
            return ""
        filter.records[len(filter.records)-1].Operation = "" #the last operation should always be empty
        description = "\n".join(
            [f"{record.Attribute} {record.Compare} {_value_repr(record.Value)} {self.__None_repr(record.Operation, '')}" for record in filter])
        return f"\nWHERE\n{description}"

    def __order(self, order:basic_entities.OperationResultOrder = None):
        if not order:
            return ""
        description = ",\n".join(
            [f"{record.Attribute} {self.__None_repr(record.Way, '')}" for record in order]
        )
        return f"\nORDER BY\n{description}"

operation_functions = {"create":QueryTextGenerator.create,
                       "drop":QueryTextGenerator.drop,
                       "rename":QueryTextGenerator.rename,
                       "add_attribute":QueryTextGenerator.add_attribute,
                       "drop_attribute":QueryTextGenerator.drop_attribute,
                       "alter_attribute":QueryTextGenerator.alter_attribute,
                       "select":QueryTextGenerator.select,
                       "insert":QueryTextGenerator.insert,
                       "update":QueryTextGenerator.update,
                       "delete":QueryTextGenerator.delete,
                       "index":QueryTextGenerator.index,
                       "cluster":QueryTextGenerator.cluster,
                       "drop_index":QueryTextGenerator.drop_index}
attribute_properties = {"Name":lambda value:value,
                        "Type":lambda value:str(value),
                        "Primary_key":lambda value:"PRIMARY KEY" if value else "",
                        "Not_null":lambda value:"NOT NULL" if value else "",
                        "Default":lambda value:f"DEFAULT {_value_repr(value)}"
                        }
attribute_flags = {"Not_null":"NOT NULL"}