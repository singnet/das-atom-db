import os
import pickle
from typing import Any, Dict, List, Optional, Tuple, Union

from pymongo import MongoClient
from pymongo.database import Database
from redis import Redis
from redis.cluster import RedisCluster

from hyperon_das_atomdb.constants.redis_mongo_db import (
    MongoCollectionNames,
    MongoFieldNames,
)
from hyperon_das_atomdb.constants.redis_mongo_db import (
    RedisCollectionNames as KeyPrefix,
)
from hyperon_das_atomdb.constants.redis_mongo_db import build_redis_key
from hyperon_das_atomdb.exceptions import (
    ConnectionMongoDBException,
    LinkDoesNotExistException,
    NodeDoesNotExistException,
)
from hyperon_das_atomdb.i_database import (
    UNORDERED_LINK_TYPES,
    WILDCARD,
    IAtomDB,
)
from hyperon_das_atomdb.logger import logger
from hyperon_das_atomdb.utils.expression_hasher import ExpressionHasher

USE_CACHED_NODES = True
USE_CACHED_LINK_TYPES = True
USE_CACHED_NODE_TYPES = True


class NodeDocuments:
    def __init__(self, collection) -> None:
        self.mongo_collection = collection
        self.cached_nodes = {}
        self.count = 0

    def add(self, node_id, document) -> None:
        if USE_CACHED_NODES:
            self.cached_nodes[node_id] = document
        self.count += 1

    def get(self, handle, default_value):
        if USE_CACHED_NODES:
            return self.cached_nodes.get(handle, default_value)
        else:
            mongo_filter = {MongoFieldNames.ID_HASH: handle}
            node = self.mongo_collection.find_one(mongo_filter)
            return node if node else default_value

    def size(self):
        if USE_CACHED_NODES:
            return len(self.cached_nodes)
        else:
            return self.count

    def values(self):
        for document in (
            self.cached_nodes.values()
            if USE_CACHED_NODES
            else self.mongo_collection.find()
        ):
            yield document


class RedisMongoDB(IAtomDB):
    """A concrete implementation using Redis and Mongo database"""

    def __repr__(self) -> str:
        return "<Atom database RedisMongo>"  # pragma no cover

    def __init__(self, database_name: str = 'das') -> None:
        """
        Initialize an instance of a custom class with Redis
        and MongoDB connections.
        """
        self.database_name = database_name
        self._setup_databases()
        self.mongo_link_collection = {
            '1': self.mongo_db.get_collection(
                MongoCollectionNames.LINKS_ARITY_1
            ),
            '2': self.mongo_db.get_collection(
                MongoCollectionNames.LINKS_ARITY_2
            ),
            'N': self.mongo_db.get_collection(
                MongoCollectionNames.LINKS_ARITY_N
            ),
        }
        self.mongo_nodes_collection = self.mongo_db.get_collection(
            MongoCollectionNames.NODES
        )
        self.mongo_types_collection = self.mongo_db.get_collection(
            MongoCollectionNames.ATOM_TYPES
        )
        self.wildcard_hash = ExpressionHasher._compute_hash(WILDCARD)
        self.named_type_hash = None
        self.named_type_hash_reverse = None
        self.named_types = None
        self.symbol_hash = None
        self.parent_type = None
        self.node_documents = None
        self.terminal_hash = None
        self.link_type_cache = None
        self.node_type_cache = None
        self.typedef_mark_hash = ExpressionHasher._compute_hash(":")
        self.typedef_base_type_hash = ExpressionHasher._compute_hash("Type")
        self.typedef_composite_type_hash = ExpressionHasher.composite_hash(
            [
                self.typedef_mark_hash,
                self.typedef_base_type_hash,
                self.typedef_base_type_hash,
            ]
        )
        self.use_targets = [KeyPrefix.PATTERNS, KeyPrefix.TEMPLATES]
        self.prefetch()
        logger().info("Prefetching data")
        logger().info("Database setup finished")

    def _setup_databases(self) -> None:
        self.mongo_db = self._connection_mongo_db()
        self.redis = self._connection_redis()

    def _connection_mongo_db(self) -> Database:
        mongo_hostname = os.environ.get('DAS_MONGODB_HOSTNAME')
        mongo_port = os.environ.get('DAS_MONGODB_PORT')
        mongo_username = os.environ.get('DAS_MONGODB_USERNAME')
        mongo_password = os.environ.get('DAS_MONGODB_PASSWORD')

        logger().info(
            f"Connecting to MongoDB at {mongo_hostname}:{mongo_port}"
        )

        try:
            self.mongo_db = MongoClient(
                f'mongodb://{mongo_username}:{mongo_password}@{mongo_hostname}:{mongo_port}'
            )[self.database_name]
            return self.mongo_db
        except ValueError as e:
            raise ConnectionMongoDBException(
                message='error creating a MongoClient', details=str(e)
            )

    def _connection_redis(self) -> Redis:
        redis_hostname = os.environ.get('DAS_REDIS_HOSTNAME')
        redis_port = os.environ.get('DAS_REDIS_PORT')

        if redis_port == "7000":
            logger().info(
                f"Connecting to Redis cluster at {redis_hostname}:{redis_port}"
            )
            self.redis = RedisCluster(
                host=redis_hostname, port=redis_port, decode_responses=False
            )
        else:
            logger().info(
                "Connecting to standalone Redis at "
                f"{redis_hostname}:{redis_port}"
            )
            self.redis = Redis(
                host=redis_hostname, port=redis_port, decode_responses=False
            )

        return self.redis

    def _get_atom_type_hash(self, atom_type):
        # TODO: implement a proper mongo collection to atom types so instead
        #      of this lazy hashmap, we should load the hashmap during prefetch
        named_type_hash = self.named_type_hash.get(atom_type, None)
        if named_type_hash is None:
            named_type_hash = ExpressionHasher.named_type_hash(atom_type)
            self.named_type_hash[atom_type] = named_type_hash
            self.named_type_hash_reverse[named_type_hash] = atom_type
        return named_type_hash

    def prefetch(self) -> None:
        self.named_type_hash = {}
        self.named_type_hash_reverse = {}
        self.named_types = {}
        self.symbol_hash = {}
        self.parent_type = {}
        self.terminal_hash = {}
        self.link_type_cache = {}
        self.node_type_cache = {}
        self.node_documents = NodeDocuments(self.mongo_nodes_collection)
        if USE_CACHED_NODES:
            for document in self.mongo_nodes_collection.find():
                node_id = document[MongoFieldNames.ID_HASH]
                document[MongoFieldNames.TYPE_NAME]
                document[MongoFieldNames.NODE_NAME]
                self.node_documents.add(node_id, document)
        else:
            self.node_documents.count = (
                self.mongo_nodes_collection.count_documents({})
            )
        if USE_CACHED_LINK_TYPES:
            for tag in ["1", "2", "N"]:
                for document in self.mongo_link_collection[tag].find():
                    self.link_type_cache[
                        document[MongoFieldNames.ID_HASH]
                    ] = document[MongoFieldNames.TYPE_NAME]
        if USE_CACHED_NODE_TYPES:
            for document in self.mongo_nodes_collection.find():
                self.node_type_cache[
                    document[MongoFieldNames.ID_HASH]
                ] = document[MongoFieldNames.TYPE_NAME]
        for document in self.mongo_types_collection.find():
            hash_id = document[MongoFieldNames.ID_HASH]
            named_type = document[MongoFieldNames.TYPE_NAME]
            named_type_hash = document[MongoFieldNames.TYPE_NAME_HASH]
            composite_type_hash = document[MongoFieldNames.TYPE]
            type_document = self.mongo_types_collection.find_one(
                {MongoFieldNames.ID_HASH: composite_type_hash}
            )
            self.named_type_hash[named_type] = named_type_hash
            self.named_type_hash_reverse[named_type_hash] = named_type
            if type_document is not None:
                self.named_types[named_type] = type_document[
                    MongoFieldNames.TYPE_NAME
                ]
                self.parent_type[named_type_hash] = type_document[
                    MongoFieldNames.TYPE_NAME_HASH
                ]
            self.symbol_hash[named_type] = hash_id

    def _retrieve_mongo_document(self, handle: str, arity=-1) -> dict:
        mongo_filter = {"_id": handle}
        if arity >= 0:
            if arity == 0:
                return self.mongo_nodes_collection.find_one(mongo_filter)
            elif arity == 2:
                return self.mongo_link_collection['2'].find_one(mongo_filter)
            elif arity == 1:
                return self.mongo_link_collection['1'].find_one(mongo_filter)
            else:
                return self.mongo_link_collection['N'].find_one(mongo_filter)
        # The order of keys in search is important. Greater to smallest
        # probability of proper arity
        for collection in [
            self.mongo_link_collection[key] for key in ['2', '1', 'N']
        ]:
            document = collection.find_one(mongo_filter)
            if document:
                return document
        return None

    def _retrieve_key_value(self, prefix: str, key: str) -> List[str]:
        members = self.redis.smembers(build_redis_key(prefix, key))
        if prefix in self.use_targets:
            return [pickle.loads(t) for t in members]
        else:
            return [*members]

    def _build_named_type_hash_template(
        self, template: Union[str, List[Any]]
    ) -> List[Any]:
        if isinstance(template, str):
            return self._get_atom_type_hash(template)
        else:
            answer = []
            for element in template:
                v = self._build_named_type_hash_template(element)
                answer.append(v)
            return answer

    def _build_named_type_template(
        self, template: Union[str, List[Any]]
    ) -> List[Any]:
        if isinstance(template, str):
            ret = self.named_type_hash_reverse.get(template, None)
            return ret
        else:
            answer = []
            for element in template:
                v = self._build_named_type_template(element)
                answer.append(v)
            return answer

    def _get_mongo_document_keys(self, document: Dict) -> List[str]:
        answer = document.get(MongoFieldNames.KEYS, None)
        if answer is not None:
            return answer
        answer = []
        index = 0
        while True:
            key = document.get(
                f'{MongoFieldNames.KEY_PREFIX.value}_{index}', None
            )
            if key is None:
                return answer
            else:
                answer.append(key)
            index += 1

    def _build_deep_representation(self, handle, arity=-1):
        answer = {}
        document = self.node_documents.get(handle, None)
        if document is None:
            document = self._retrieve_mongo_document(handle, arity)
            answer["type"] = document[MongoFieldNames.TYPE_NAME]
            answer["targets"] = []
            for target_handle in self._get_mongo_document_keys(document):
                answer["targets"].append(
                    self._build_deep_representation(target_handle)
                )
        else:
            answer["type"] = document[MongoFieldNames.TYPE_NAME]
            answer["name"] = document[MongoFieldNames.NODE_NAME]
        return answer

    def _create_node_handle(self, node_type: str, node_name: str) -> str:
        return ExpressionHasher.terminal_hash(node_type, node_name)

    def _create_link_handle(
        self, link_type: str, target_handles: List[str]
    ) -> str:
        return ExpressionHasher.expression_hash(
            self._get_atom_type_hash(link_type), target_handles
        )

    # DB interface methods

    def node_exists(self, node_type: str, node_name: str) -> bool:
        try:
            self.get_node_handle(node_type, node_name)
            return True
        except NodeDoesNotExistException:
            return False

    def link_exists(self, link_type: str, target_handles: List[str]) -> bool:
        try:
            self.get_link_handle(link_type, target_handles)
            return True
        except LinkDoesNotExistException:
            return False

    def get_node_handle(self, node_type: str, node_name: str) -> str:
        node_handle = self._create_node_handle(node_type, node_name)
        document = self._retrieve_mongo_document(node_handle, 0)
        if document is not None:
            return document['_id']
        else:
            raise NodeDoesNotExistException(
                message='This node does not exist',
                details=f'{node_type}:{node_name}',
            )

    def get_link_handle(
        self, link_type: str, target_handles: List[str]
    ) -> str:
        link_handle = self._create_link_handle(link_type, target_handles)
        document = self._retrieve_mongo_document(
            link_handle, len(target_handles)
        )
        if document is not None:
            return document['_id']
        else:
            raise LinkDoesNotExistException(
                message='This link does not exist',
                details=f'{link_type}:{target_handles}',
            )

    def get_link_targets(self, link_handle: str) -> List[str]:
        answer = self._retrieve_key_value(KeyPrefix.OUTGOING_SET, link_handle)
        if not answer:
            raise ValueError(f"Invalid handle: {link_handle}")
        return [h.decode() for h in answer]

    def is_ordered(self, link_handle: str) -> bool:
        document = self._retrieve_mongo_document(link_handle)
        if document is None:
            raise ValueError(f'Invalid handle: {link_handle}')
        return True

    def get_matched_links(
        self,
        link_type: str,
        target_handles: List[str],
        extra_parameters: Optional[Dict[str, Any]] = None,
    ):
        if link_type != WILDCARD and WILDCARD not in target_handles:
            try:
                link_handle = self.get_link_handle(link_type, target_handles)
                document = self._retrieve_mongo_document(
                    link_handle, len(target_handles)
                )
                return [link_handle] if document else []
            except ValueError:
                return []

        if link_type == WILDCARD:
            link_type_hash = WILDCARD
        else:
            link_type_hash = self._get_atom_type_hash(link_type)

        if link_type_hash is None:
            return []

        if link_type in UNORDERED_LINK_TYPES:
            target_handles = sorted(target_handles)

        pattern_hash = ExpressionHasher.composite_hash(
            [link_type_hash, *target_handles]
        )

        patterns_matched = self._retrieve_key_value(
            KeyPrefix.PATTERNS, pattern_hash
        )

        if len(patterns_matched) > 0:
            if extra_parameters and extra_parameters.get('toplevel_only'):
                return self._filter_non_toplevel(patterns_matched)

        return patterns_matched

    def get_all_nodes(self, node_type: str, names: bool = False) -> List[str]:
        node_type_hash = self._get_atom_type_hash(node_type)
        if node_type_hash is None:
            raise ValueError(f'Invalid node type: {node_type}')
        if names:
            return [
                document[MongoFieldNames.NODE_NAME]
                for document in self.node_documents.values()
                if document[MongoFieldNames.TYPE] == node_type_hash
            ]
        else:
            return [
                document[MongoFieldNames.ID_HASH]
                for document in self.node_documents.values()
                if document[MongoFieldNames.TYPE] == node_type_hash
            ]

    def get_matched_type_template(
        self,
        template: List[Any],
        extra_parameters: Optional[Dict[str, Any]] = None,
    ) -> List[str]:
        try:
            template = self._build_named_type_hash_template(template)
            template_hash = ExpressionHasher.composite_hash(template)
            templates_matched = self._retrieve_key_value(
                KeyPrefix.TEMPLATES, template_hash
            )
            if len(templates_matched) > 0:
                if extra_parameters and extra_parameters.get('toplevel_only'):
                    return self._filter_non_toplevel(templates_matched)
            return templates_matched
        except Exception as exception:
            raise ValueError(str(exception))

    def get_matched_type(
        self, link_type: str, extra_parameters: Optional[Dict[str, Any]] = None
    ) -> List[str]:
        named_type_hash = self._get_atom_type_hash(link_type)
        templates_matched = self._retrieve_key_value(
            KeyPrefix.TEMPLATES, named_type_hash
        )
        if len(templates_matched) > 0:
            if extra_parameters and extra_parameters.get('toplevel_only'):
                return self._filter_non_toplevel(templates_matched)
        return templates_matched

    def get_node_name(self, node_handle: str) -> str:
        answer = self._retrieve_key_value(
            KeyPrefix.NAMED_ENTITIES, node_handle
        )
        if not answer:
            raise ValueError(f"Invalid handle: {node_handle}")
        return answer[0].decode()

    def get_matched_node_name(self, node_type: str, substring: str) -> str:
        node_type_hash = self._get_atom_type_hash(node_type)
        mongo_filter = {
            MongoFieldNames.TYPE: node_type_hash,
            MongoFieldNames.NODE_NAME: {'$regex': substring},
        }
        return [
            document[MongoFieldNames.ID_HASH]
            for document in self.mongo_nodes_collection.find(mongo_filter)
        ]

    def get_atom_as_dict(self, handle, arity=-1) -> dict:
        answer = {}
        document = (
            self.node_documents.get(handle, None) if arity <= 0 else None
        )
        if document is None:
            document = self._retrieve_mongo_document(handle, arity)
            if document:
                answer["handle"] = document[MongoFieldNames.ID_HASH]
                answer["type"] = document[MongoFieldNames.TYPE_NAME]
                answer["template"] = self._build_named_type_template(
                    document[MongoFieldNames.COMPOSITE_TYPE]
                )
                answer["targets"] = self._get_mongo_document_keys(document)
        else:
            answer["handle"] = document[MongoFieldNames.ID_HASH]
            answer["type"] = document[MongoFieldNames.TYPE_NAME]
            answer["name"] = document[MongoFieldNames.NODE_NAME]
        return answer

    def get_atom_as_deep_representation(self, handle: str, arity=-1) -> str:
        return self._build_deep_representation(handle, arity)

    def get_link_type(self, link_handle: str) -> str:
        if USE_CACHED_LINK_TYPES:
            ret = self.link_type_cache[link_handle]
            return ret
        else:
            document = self.get_atom_as_dict(link_handle)
            return document["type"]

    def get_node_type(self, node_handle: str) -> str:
        if USE_CACHED_NODE_TYPES:
            return self.node_type_cache[node_handle]
        else:
            document = self.get_atom_as_dict(node_handle)
            return document["type"]

    def count_atoms(self) -> Tuple[int, int]:
        node_count = self.mongo_nodes_collection.estimated_document_count()
        link_count = 0
        for collection in self.mongo_link_collection.values():
            link_count += collection.estimated_document_count()
        return (node_count, link_count)

    def clear_database(self) -> None:
        """
        from the connected MongoDB and Redis databases.

        This method drops all collections in the MongoDB database and flushes
        all data from the Redis cache, effectively wiping the databases clean.
        """
        collections = self.mongo_db.list_collection_names()

        for collection in collections:
            self.mongo_db[collection].drop()

        self.redis.flushall()

    def _filter_non_toplevel(self, matches: list) -> list:
        if isinstance(matches[0], list):
            matches = matches[0]
        matches_toplevel_only = []
        for match in matches:
            link_handle = match[0]
            link = self._retrieve_mongo_document(link_handle, len(match[-1]))
            if link['is_toplevel']:
                matches_toplevel_only.append(match)
        return matches_toplevel_only
