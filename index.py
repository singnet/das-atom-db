from enum import Enum
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import OperationFailure


class OrderEnum(Enum):
    ASC = ASCENDING
    DESC = DESCENDING


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
                print(index.get_keys())
                print(index.get_options())
                self.db[collection_name].create_index(
                    index.get_keys(),
                    **index.get_options(),
                )
                print(f"Index created")
            except OperationFailure as e:
                print(f"Failed to create index: {e}")


    def create_node_indexes(self, indexes: list, refresh=False):
        return self._create_indexes("supplies", indexes, refresh)

    def create_links_indexes(self, indexes: list, refresh=False):
        return self._create_indexes("links_2", indexes, refresh)


class RemoteDAS:
    def __init__(self, mongo_uri, default_database="das"):
        self.client = MongoClient(mongo_uri)
        self.db = self.client.get_database(default_database)
        self.index = MongoIndex(self.db)


mongo_uri = "mongodb://dbadmin:dassecret@127.0.0.1:27017"

das = RemoteDAS(mongo_uri)

class PartialFilterExpression:
    def __init__(self, field_name: str, operations: dict):
        self._field_name = field_name
        self._operations = operations
        self._options = self._get_partial_filter_expression()

    def _get_partial_filter_expression(self):
        partial_filter_expression = {}
        
        for opt, value in self._operations.items():
            field = partial_filter_expression.setdefault(self._field_name, {})
            
            field[opt.value] = value

        return partial_filter_expression

    def get_options(self):
        return self._options
    


class Index:
    _options = {}
    _keys = {}

    def __init__(
        self,
        field_name: str,
        order: OrderEnum = OrderEnum.ASC,
        partial_filter_expression: PartialFilterExpression = None,
    ):
        self._keys = {f"{field_name}": order.value}
        self._partial_filter_expression = partial_filter_expression

    def get_keys(self):
        return self._keys

    def _build_options(self):
        options = {}

        if self._partial_filter_expression is not None:
            options = {**options, "partialFilterExpression": self._partial_filter_expression.get_options()}

        return options

    def get_options(self):
        return self._build_options()


class CompoundIndex:
    def __init__(self, indexes: list) -> None:
        self._indexes = indexes
        self._options = self._merged_options()
        self._keys = self._merged_keys()

    def get_keys(self):
        return self._keys

    def get_options(self):
        return self._options

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

class PartialFilterExpressionOptions(Enum):
    GREATER_EQUAL_THAN = "$gte"
    LESS_EQUAL_THAN = "$lte"
    GREATER_THAN = "$gt"
    LESS_THAN = "$lt"
    EQUAL = "$eq"
    EXISTS = "$exists"
    AND = "$and"
    OR = "$or"
    IN = "$in"


simple_node_indexes = [
   Index("name", OrderEnum.DESC), # Este índice é útil quando você faz consultas frequentes que envolvem o campo "name". O indice irá indexar name de maneira decrescente.
   Index("named_type"), # Este índice é útil quando você faz consultas frequentes que envolvem o campo "named_type". O índice irá indexar o campo "named_type" de maneira ascendente.
]
# Exemplo de busca que fará uso do indice acima
# db.nodes.find({ name: "human" })

das.index.create_node_indexes(
    indexes=simple_node_indexes,
    refresh=True
)

compound_node_indexes = [
    CompoundIndex(indexes=[ # Este é um exemplo de um índice composto no MongoDB. Um índice composto é aquele que é criado com base em vários campos. Os dados são agrupados pelo primeiro campo no índice e, em seguida, por cada campo subsequente.
        Index("name", OrderEnum.DESC),
        Index("named_type"),
    ])
]
# Exemplo de busca que fará uso do indice acima
# db.nodes.find({ name: "human", "named_type": "Concept" })

das.index.create_node_indexes(
    indexes=compound_node_indexes,
    refresh=True
)
    


partial_filter_expression_node_indexes = [
    Index("purchaseMethod",
        partial_filter_expression=PartialFilterExpression( #PartialFilterExpression é útil quando você deseja otimizar consultas específicas, excluindo documentos que não atendem a determinados critérios de filtragem. Por exemplo, se você tiver uma coleção de transações de compra (purchaseMethod) e quiser indexar apenas as transações de clientes com idades entre 18 e 30 anos, você pode usar um índice parcial como este para melhorar o desempenho de consultas que envolvem esse critério.
            field_name="customer.age",
            operations={
                PartialFilterExpressionOptions.GREATER_EQUAL_THAN: 18,
                PartialFilterExpressionOptions.LESS_EQUAL_THAN: 30
            }
        ),
    ),
]
# Exemplo de busca que fará uso do indice acima
# db.sales.find({ purchaseMethod: "Online", "customer.age": { $gte: 19, $lte: 25 } })


das.index.create_node_indexes(
    indexes=partial_filter_expression_node_indexes,
    refresh=True
)

# Quando você tem um array de objetos em seus documentos e deseja indexar um campo dentro desses objetos, você pode usar a notação de ponto (.) para especificar o caminho completo até o campo desejado.
array_object_node_indexes = [
   Index("items.name"),
]

das.index.create_node_indexes(
    indexes=array_object_node_indexes,
    refresh=True
)
# Exemplo de busca que fará uso do indice acima
# db.sales.find({ "items.name": "item01" })