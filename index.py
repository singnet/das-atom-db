from enum import Enum
from pymongo import MongoClient
from pymongo.errors import OperationFailure
from dataclasses import dataclass
from abc import ABC


class OrderIndexEnum(Enum):
    ASC = 1
    DESC = -1


class MongoIndex:
    def __init__(self, db) -> None:
        self.db = db

    def drop_all(self, collection_name):
        self.db[collection_name].drop_indexes()

    def _set(self):
        pass

    def _create_indexes(self, collection_name: str, indexes: list, refresh=False):
        if refresh:
            self.drop_all(collection_name)

        for index in indexes:
            try:
                self.db[collection_name].create_index(
                    index.get_keys(),
                    **index.get_options(),
                )
                print(f"Index created")
            except OperationFailure as e:
                print(f"Failed to create index: {e}")


    def create_node_indexes(self, indexes: list, refresh=False):
        return self._create_indexes("nodes", indexes, refresh)

    def create_links_indexes(self, indexes: list, refresh=False):
        return self._create_indexes("links_2", indexes, refresh)


class RemoteDAS:
    def __init__(self, mongo_uri, default_database="das"):
        self.client = MongoClient(mongo_uri)
        self.db = self.client.get_database(default_database)
        self.index = MongoIndex(self.db)


mongo_uri = "mongodb://dbadmin:dassecret@127.0.0.1:27017"

das = RemoteDAS(mongo_uri)


@dataclass
class IndexType(ABC):
    _options = {}
    _keys = {}

    def __init__(self, field_name=""):
        self._field_name = field_name

    def get_keys(self):
        return self._keys

    def get_options(self):
        return self._options


class IndexOpt(Enum):
    GREATER_EQUAL_THAN = "$gte"
    LESS_EQUAL_THAN = "$lte"
    GREATER_THAN = "$gt"
    LESS_THAN = "$lt"
    EQUAL = "$eq"


class FilterIndex(IndexType):
    def __init__(self, field_name: str, operations: dict):
        super().__init__(field_name)
        self._operations = operations
        self._options = self._get_partial_filter_expression()

    def _get_partial_filter_expression(self):
        partial_filter_expression = {}
        for opt, value in self._operations.items():
            partial_filter_expression[self._field_name] = {opt.value: value}
        return {"partialFilterExpression": partial_filter_expression}


class OrderIndex(IndexType):
    def __init__(self, field_name: str, order: OrderIndexEnum):
        super().__init__(field_name)
        self._keys = {f"{field_name}": order.value}


class TextIndex(IndexType):
    def __init__(self, field_name: str):
        super().__init__(field_name)
        self._keys = {f"{field_name}": "text"}


class MultipleFieldsIndex(IndexType):
    def __init__(self, indexes: list) -> None:
        self._indexes = indexes
        self._options = self._merged_options()
        self._keys = self._merged_keys()

    def _merged_keys(self):
        keys = []
        for index in self._indexes:
            keys.append(index.get_keys())

        return self._merge(*keys)

    def _merged_options(self):
        options = []
        for index in self._indexes:
            options.append(index.get_options())

        return self._merge(*options)

    def _merge(self, *options):
        result = {}

        for option in options:
            result = {**result, **option}

        return result


node_indexes = [
    MultipleFieldsIndex(
        [
            OrderIndex("name", OrderIndexEnum.ASC),
            FilterIndex(
                field_name="importance",
                operations={
                    IndexOpt.GREATER_EQUAL_THAN: 0,
                    IndexOpt.LESS_EQUAL_THAN: 1
                },
            ),
        ]
    ),
    MultipleFieldsIndex(
        [
            TextIndex("named_type"),
            OrderIndex("name", OrderIndexEnum.DESC),
        ]
    ),
    OrderIndex("named_type", OrderIndexEnum.DESC),
]


das.index.create_node_indexes(
    indexes=node_indexes,
    refresh=True,
)


links_indexes = [
    MultipleFieldsIndex(
        [
            TextIndex("named_type"),
            OrderIndex("name", OrderIndexEnum.DESC),
        ]
    ),
]

das.index.create_links_indexes(
    indexes=links_indexes,
    refresh=True,
)