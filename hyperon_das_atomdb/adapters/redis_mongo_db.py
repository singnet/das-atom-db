import pickle
import sys
from copy import deepcopy
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from pymongo import MongoClient
from pymongo.database import Database
from redis import Redis
from redis.cluster import RedisCluster

from hyperon_das_atomdb.database import UNORDERED_LINK_TYPES, WILDCARD, AtomDB
from hyperon_das_atomdb.exceptions import (
    AtomDoesNotExist,
    ConnectionMongoDBException,
    InvalidOperationException,
    LinkDoesNotExist,
    NodeDoesNotExist,
)
from hyperon_das_atomdb.logger import logger
from hyperon_das_atomdb.utils.expression_hasher import ExpressionHasher


def _build_redis_key(prefix, key):
    return prefix + ":" + key


class MongoCollectionNames(str, Enum):
    NODES = 'nodes'
    ATOM_TYPES = 'atom_types'
    LINKS_ARITY_1 = 'links_1'
    LINKS_ARITY_2 = 'links_2'
    LINKS_ARITY_N = 'links_n'
    DAS_CONFIG = 'das_config'


class MongoFieldNames(str, Enum):
    NODE_NAME = 'name'
    TYPE_NAME = 'named_type'
    TYPE_NAME_HASH = 'named_type_hash'
    ID_HASH = '_id'
    TYPE = 'composite_type_hash'
    COMPOSITE_TYPE = 'composite_type'
    KEY_PREFIX = 'key'
    KEYS = 'keys'


class KeyPrefix(str, Enum):
    INCOMING_SET = 'incomming_set'
    OUTGOING_SET = 'outgoing_set'
    PATTERNS = 'patterns'
    TEMPLATES = 'templates'
    NAMED_ENTITIES = 'names'


class NodeDocuments:
    def __init__(self, collection) -> None:
        self.mongo_collection = collection
        self.cached_nodes = {}
        self.count = 0

    def add(self) -> None:
        self.count += 1

    def get(self, handle, default_value):
        mongo_filter = {MongoFieldNames.ID_HASH: handle}
        node = self.mongo_collection.find_one(mongo_filter)
        return node if node else default_value

    def size(self):
        return self.count

    def values(self):
        for document in self.mongo_collection.find():
            yield document


class _HashableDocument:
    def __init__(self, base: Dict[str, Any]):
        self.base = base

    def __hash__(self):
        return hash(self.base["_id"])

    def __str__(self):
        return str(self.base)


class RedisMongoDB(AtomDB):
    """A concrete implementation using Redis and Mongo database"""

    def __repr__(self) -> str:
        return "<Atom database RedisMongo>"  # pragma no cover

    def __init__(self, **kwargs: Optional[Dict[str, Any]]) -> None:
        """
        Initialize an instance of a custom class with Redis and MongoDB connections.
        """
        self.database_name = 'das'
        self._setup_databases(**kwargs)
        self.mongo_link_collection = {
            "1": self.mongo_db.get_collection(MongoCollectionNames.LINKS_ARITY_1),
            "2": self.mongo_db.get_collection(MongoCollectionNames.LINKS_ARITY_2),
            "N": self.mongo_db.get_collection(MongoCollectionNames.LINKS_ARITY_N),
        }
        self.mongo_nodes_collection = self.mongo_db.get_collection(MongoCollectionNames.NODES)
        self.mongo_types_collection = self.mongo_db.get_collection(MongoCollectionNames.ATOM_TYPES)
        self.all_mongo_collections = [
            (
                MongoCollectionNames.LINKS_ARITY_1,
                self.mongo_link_collection["1"],
            ),
            (
                MongoCollectionNames.LINKS_ARITY_2,
                self.mongo_link_collection["2"],
            ),
            (
                MongoCollectionNames.LINKS_ARITY_N,
                self.mongo_link_collection["N"],
            ),
            (MongoCollectionNames.NODES, self.mongo_nodes_collection),
            (MongoCollectionNames.ATOM_TYPES, self.mongo_types_collection),
        ]
        self.mongo_das_config_collection = None
        self.wildcard_hash = ExpressionHasher._compute_hash(WILDCARD)
        self.named_type_hash = {}
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
        self.mongo_bulk_insertion_buffer = {
            collection_name: tuple([collection, set()])
            for collection_name, collection in self.all_mongo_collections
        }
        self.mongo_bulk_insertion_limit = 100000
        self.max_mongo_db_document_size = 16000000
        self._setup_indexes()
        logger().info("Database setup finished")

    def _setup_databases(
        self,
        mongo_hostname='localhost',
        mongo_port=27017,
        mongo_username='mongo',
        mongo_password='mongo',
        mongo_tls_ca_file=None,
        redis_hostname='localhost',
        redis_port=6379,
        redis_username=None,
        redis_password=None,
        redis_cluster=True,
        redis_ssl=True,
        **kwargs,
    ) -> None:
        self.mongo_db = self._connection_mongo_db(
            mongo_hostname,
            mongo_port,
            mongo_username,
            mongo_password,
            mongo_tls_ca_file,
        )
        self.redis = self._connection_redis(
            redis_hostname,
            redis_port,
            redis_username,
            redis_password,
            redis_cluster,
            redis_ssl,
        )

    def _connection_mongo_db(
        self,
        mongo_hostname,
        mongo_port,
        mongo_username,
        mongo_password,
        mongo_tls_ca_file,
    ) -> Database:
        message = f"Connecting to MongoDB at {mongo_username}:{mongo_password}@{mongo_hostname}:{mongo_port}"
        if mongo_tls_ca_file:
            message += f"?tls=true&tlsCAFile={mongo_tls_ca_file}"
        logger().info(message)
        try:
            if mongo_tls_ca_file:
                self.mongo_db = MongoClient(
                    f"mongodb://{mongo_username}:{mongo_password}@{mongo_hostname}:{mongo_port}?tls=true&tlsCAFile={mongo_tls_ca_file}&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"
                )[
                    self.database_name
                ]  # aws
            else:
                self.mongo_db = MongoClient(
                    f"mongodb://{mongo_username}:{mongo_password}@{mongo_hostname}:{mongo_port}"
                )[self.database_name]
            return self.mongo_db
        except ValueError as e:
            logger().error(
                f'An error occourred while creating a MongoDB client - Details: {str(e)}'
            )
            raise ConnectionMongoDBException(message="error creating a MongoClient", details=str(e))

    def _connection_redis(
        self,
        redis_hostname,
        redis_port,
        redis_username,
        redis_password,
        redis_cluster,
        redis_ssl,
    ) -> Redis:
        redis_type = 'Redis cluster' if redis_cluster else 'Standalone Redis'

        message = f"Connecting to {redis_type} at {redis_username}:{redis_password}@{redis_hostname}:{redis_port}. ssl: {redis_ssl}"

        logger().info(message)

        redis_connection = {
            "host": redis_hostname,
            "port": redis_port,
            "decode_responses": False,
            "ssl": redis_ssl,
        }

        if redis_password and redis_username:
            redis_connection["password"] = redis_password
            redis_connection["username"] = redis_username

        if redis_cluster:
            self.redis = RedisCluster(**redis_connection)
        else:
            self.redis = Redis(**redis_connection)

        return self.redis

    def _setup_indexes(self):
        self.default_pattern_index_templates = []
        for named_type in [True, False]:
            for pos0 in [True, False]:
                for pos1 in [True, False]:
                    for pos2 in [True, False]:
                        if named_type and pos0 and pos1 and pos2:
                            # not a pattern but an actual atom
                            continue
                        template = {}
                        template[MongoFieldNames.TYPE_NAME] = named_type
                        template["selected_positions"] = [
                            i for i, pos in enumerate([pos0, pos1, pos2]) if pos
                        ]
                        self.default_pattern_index_templates.append(template)
        if MongoCollectionNames.DAS_CONFIG in self.mongo_db.list_collection_names():
            self.pattern_index_templates = self.mongo_das_config_collection.find_one(
                {"_id": "pattern_index_templates"}
            )["templates"]
        else:
            self.pattern_index_templates = None

    def _get_atom_type_hash(self, atom_type):
        # TODO: implement a proper mongo collection to atom types so instead
        #      of this lazy hashmap, we should load the hashmap during prefetch
        named_type_hash = self.named_type_hash.get(atom_type, None)
        if named_type_hash is None:
            named_type_hash = ExpressionHasher.named_type_hash(atom_type)
            self.named_type_hash[atom_type] = named_type_hash
        return named_type_hash

    def _retrieve_mongo_document(self, handle: str, arity=-1) -> dict:
        mongo_filter = {MongoFieldNames.ID_HASH: handle}
        document = None
        if arity >= 0:
            if arity == 0:
                return self.mongo_nodes_collection.find_one(mongo_filter)
            elif arity == 2:
                document = self.mongo_link_collection["2"].find_one(mongo_filter)
                if document:
                    document["targets"] = self._get_mongo_document_keys(document)
                return document
            elif arity == 1:
                document = self.mongo_link_collection["1"].find_one(mongo_filter)
                if document:
                    document["targets"] = self._get_mongo_document_keys(document)
                return document
            else:
                document = self.mongo_link_collection["N"].find_one(mongo_filter)
                if document:
                    document["targets"] = self._get_mongo_document_keys(document)
                return document

        # The order of keys in search is important. Greater to smallest
        # probability of proper arity
        document = self.mongo_nodes_collection.find_one(mongo_filter)
        if document:
            return document
        for collection in [self.mongo_link_collection[key] for key in ["2", "1", "N"]]:
            document = collection.find_one(mongo_filter)
            if document:
                document["targets"] = self._get_mongo_document_keys(document)
                return document
        return None

    def _build_named_type_hash_template(self, template: Union[str, List[Any]]) -> List[Any]:
        if isinstance(template, str):
            return self._get_atom_type_hash(template)
        else:
            answer = []
            for element in template:
                v = self._build_named_type_hash_template(element)
                answer.append(v)
            return answer

    def _get_mongo_document_keys(self, document: Dict) -> List[str]:
        answer = document.get(MongoFieldNames.KEYS, None)
        if answer is not None:
            return answer
        answer = []
        index = 0
        while True:
            key = document.get(f"{MongoFieldNames.KEY_PREFIX.value}_{index}", None)
            if key is None:
                return answer
            else:
                answer.append(key)
            index += 1

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

    def get_node_handle(self, node_type: str, node_name: str) -> str:
        node_handle = self.node_handle(node_type, node_name)
        document = self._retrieve_mongo_document(node_handle, 0)
        if document is not None:
            return document[MongoFieldNames.ID_HASH]
        else:
            logger().error(
                f'Failed to retrieve node handle for {node_type}:{node_name}. This node may not exist.'
            )
            raise NodeDoesNotExist(
                message="This node does not exist",
                details=f"{node_type}:{node_name}",
            )

    def get_node_name(self, node_handle: str) -> str:
        answer = self._retrieve_name(node_handle)
        if not answer:
            logger().error(
                f'Failed to retrieve node name for handle: {node_handle}. The handle may be invalid or the corresponding node does not exist.'
            )
            raise ValueError(f"Invalid handle: {node_handle}")
        return answer

    def get_node_type(self, node_handle: str) -> str:
        document = self.get_atom(node_handle)
        return document[MongoFieldNames.TYPE_NAME]

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

    def get_all_nodes(self, node_type: str, names: bool = False) -> List[str]:
        if names:
            return [
                document[MongoFieldNames.NODE_NAME]
                for document in self.mongo_nodes_collection.find(
                    {MongoFieldNames.TYPE_NAME: node_type}
                )
            ]
        else:
            return [
                document[MongoFieldNames.ID_HASH]
                for document in self.mongo_nodes_collection.find(
                    {MongoFieldNames.TYPE_NAME: node_type}
                )
            ]

    def get_all_links(self, link_type: str) -> List[str]:
        links_handle = []
        for collection in [self.mongo_link_collection[key] for key in ["2", "1", "N"]]:
            documents = collection.find({MongoFieldNames.TYPE_NAME: link_type})
            for document in documents:
                links_handle.append(document[MongoFieldNames.ID_HASH])
        return links_handle

    def get_link_handle(self, link_type: str, target_handles: List[str]) -> str:
        link_handle = self.link_handle(link_type, target_handles)
        document = self._retrieve_mongo_document(link_handle, len(target_handles))
        if document is not None:
            return document[MongoFieldNames.ID_HASH]
        else:
            logger().error(
                f'Failed to retrieve link handle for {link_type}:{target_handles}. This link may not exist.'
            )
            raise LinkDoesNotExist(
                message="This link does not exist",
                details=f"{link_type}:{target_handles}",
            )

    def get_link_targets(self, link_handle: str) -> List[str]:
        answer = self._retrieve_outgoing_set(link_handle)
        if not answer:
            logger().error(
                f'Failed to retrieve link targets for handle: {link_handle}. The handle may be invalid or the corresponding link does not exist.'
            )
            raise ValueError(f"Invalid handle: {link_handle}")
        return answer

    def is_ordered(self, link_handle: str) -> bool:
        document = self._retrieve_mongo_document(link_handle)
        if document is None:
            logger().error(
                'Failed to retrieve document for link handle: {link_handle}. The handle may be invalid or the corresponding link does not exist.'
            )
            raise ValueError(f"Invalid handle: {link_handle}")
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
                document = self._retrieve_mongo_document(link_handle, len(target_handles))
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

        pattern_hash = ExpressionHasher.composite_hash([link_type_hash, *target_handles])
        patterns_matched = self._retrieve_pattern(pattern_hash)
        if len(patterns_matched) > 0:
            if extra_parameters and extra_parameters.get("toplevel_only"):
                return self._filter_non_toplevel(patterns_matched)

        return patterns_matched

    def get_incoming_links(
        self, atom_handle: str, **kwargs
    ) -> List[Union[Tuple[Dict[str, Any], List[Dict[str, Any]]], Dict[str, Any]]]:
        links = self._retrieve_incoming_set(atom_handle)

        if not links:
            return []

        if kwargs.get('handles_only', False):
            return links

        links_document = [self.get_atom(handle, **kwargs) for handle in links]

        return links_document

    def get_matched_type_template(
        self,
        template: List[Any],
        extra_parameters: Optional[Dict[str, Any]] = None,
    ) -> List[str]:
        try:
            template = self._build_named_type_hash_template(template)
            template_hash = ExpressionHasher.composite_hash(template)
            templates_matched = self._retrieve_template(template_hash)
            if len(templates_matched) > 0:
                if extra_parameters and extra_parameters.get("toplevel_only"):
                    return self._filter_non_toplevel(templates_matched)
            return templates_matched
        except Exception as exception:
            logger().error(f'Failed to get matched type template - Details: {str(exception)}')
            raise ValueError(str(exception))

    def get_matched_type(
        self, link_type: str, extra_parameters: Optional[Dict[str, Any]] = None
    ) -> List[str]:
        named_type_hash = self._get_atom_type_hash(link_type)
        templates_matched = self._retrieve_template(named_type_hash)
        if len(templates_matched) > 0:
            if extra_parameters and extra_parameters.get("toplevel_only"):
                return self._filter_non_toplevel(templates_matched)
        return templates_matched

    def get_link_type(self, link_handle: str) -> str:
        document = self.get_atom(link_handle)
        return document[MongoFieldNames.TYPE_NAME]

    def get_atom(self, handle: str, **kwargs) -> Dict[str, Any]:
        document = self._retrieve_mongo_document(handle)
        if document:
            atom = self._convert_atom_format(document, **kwargs)
            return atom
        else:
            logger().error(
                f'Failed to retrieve atom for handle: {handle}. This link may not exist. - Details: {kwargs}'
            )
            raise AtomDoesNotExist(
                message='This atom does not exist',
                details=f'handle: {handle}',
            )

    def get_atom_type(self, handle: str) -> str:
        atom = self._retrieve_mongo_document(handle)
        if atom is not None:
            return atom['named_type']

    def get_atom_as_dict(self, handle, arity=-1) -> dict:
        answer = {}
        document = self._retrieve_mongo_document(handle, arity)
        if document:
            answer["handle"] = document[MongoFieldNames.ID_HASH]
            answer["type"] = document[MongoFieldNames.TYPE_NAME]
            if "targets" in document:
                answer["targets"] = document["targets"]
            else:
                answer["name"] = document["name"]
        return answer

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

    def commit(self) -> None:
        id_tag = MongoFieldNames.ID_HASH
        for key, (
            collection,
            buffer,
        ) in self.mongo_bulk_insertion_buffer.items():
            if buffer:
                documents = [d.base for d in buffer]
                for document in documents:
                    collection.replace_one({id_tag: document[id_tag]}, document, upsert=True)
                if key == MongoCollectionNames.NODES:
                    self._update_node_index(documents)
                elif key == MongoCollectionNames.ATOM_TYPES:
                    logger().error('Failed to commit Atom Types. This operation is not allowed')
                    raise InvalidOperationException
                else:
                    self._update_link_index(documents)
            buffer.clear()

    def add_node(self, node_params: Dict[str, Any]) -> Dict[str, Any]:
        handle, node = self._add_node(node_params)
        if sys.getsizeof(node_params['name']) < self.max_mongo_db_document_size:
            _, buffer = self.mongo_bulk_insertion_buffer[MongoCollectionNames.NODES]
            buffer.add(_HashableDocument(node))
            if len(buffer) >= self.mongo_bulk_insertion_limit:
                self.commit()
            return node
        else:
            logger().warn("Discarding atom whose name is too large: {node_name}")

    def add_link(self, link_params: Dict[str, Any], toplevel: bool = True) -> Dict[str, Any]:
        handle, link, targets = self._add_link(link_params, toplevel)
        arity = len(targets)
        if arity == 1:
            collection_name = MongoCollectionNames.LINKS_ARITY_1
        elif arity == 2:
            collection_name = MongoCollectionNames.LINKS_ARITY_2
        else:
            collection_name = MongoCollectionNames.LINKS_ARITY_N
        _, buffer = self.mongo_bulk_insertion_buffer[collection_name]
        buffer.add(_HashableDocument(link))
        if len(buffer) >= self.mongo_bulk_insertion_limit:
            self.commit()
        return link

    def delete_atom(self, handle: str, **kwargs) -> None:
        pass

    def _apply_index_template(
        self, template: Dict[str, Any], named_type: str, targets: List[str], arity
    ) -> List[List[str]]:
        key = []
        key = [WILDCARD] if template[MongoFieldNames.TYPE_NAME] else [named_type]
        target_selected_pos = template["selected_positions"]
        for cursor in range(arity):
            key.append(WILDCARD if cursor in target_selected_pos else targets[cursor])
        return _build_redis_key(KeyPrefix.PATTERNS, ExpressionHasher.composite_hash(key))

    def _retrieve_incoming_set(self, handle: str) -> List[str]:
        key = _build_redis_key(KeyPrefix.INCOMING_SET, handle)
        return [member.decode() for member in self.redis.smembers(key)]

    def _retrieve_outgoing_set(self, handle: str) -> List[str]:
        key = _build_redis_key(KeyPrefix.OUTGOING_SET, handle)
        return [member.decode() for member in self.redis.lrange(key, 0, -1)]

    def _retrieve_name(self, handle: str) -> List[str]:
        key = _build_redis_key(KeyPrefix.NAMED_ENTITIES, handle)
        return self.redis.get(key).decode()

    def _retrieve_template(self, handle: str) -> List[str]:
        key = _build_redis_key(KeyPrefix.TEMPLATES, handle)
        members = self.redis.smembers(key)
        return [pickle.loads(t) for t in members]

    def _retrieve_pattern(self, handle: str) -> List[str]:
        key = _build_redis_key(KeyPrefix.PATTERNS, handle)
        members = self.redis.smembers(key)
        return [pickle.loads(t) for t in members]

    def _update_node_index(self, documents: Iterable[Dict[str, any]]) -> None:
        for document in documents:
            handle = document[MongoFieldNames.ID_HASH]
            node_name = document[MongoFieldNames.NODE_NAME]
            key = _build_redis_key(KeyPrefix.NAMED_ENTITIES, handle)
            self.redis.set(key, node_name)

    def _update_link_index(self, documents: Iterable[Dict[str, any]]) -> None:
        incoming_buffer = {}
        for document in documents:
            handle = document[MongoFieldNames.ID_HASH]
            targets = self._get_mongo_document_keys(document)
            arity = len(targets)
            named_type = document[MongoFieldNames.TYPE_NAME]
            named_type_hash = document[MongoFieldNames.TYPE_NAME_HASH]
            key = _build_redis_key(KeyPrefix.OUTGOING_SET, handle)
            self.redis.rpush(key, *targets)
            for target in targets:
                buffer = incoming_buffer.get(target, None)
                if buffer is None:
                    buffer = []
                    incoming_buffer[target] = buffer
                buffer.append(handle)
            value = pickle.dumps(tuple([handle, tuple(targets)]))
            for type_hash in [MongoFieldNames.TYPE, MongoFieldNames.TYPE_NAME_HASH]:
                key = _build_redis_key(KeyPrefix.TEMPLATES, document[type_hash])
                self.redis.sadd(key, value)
            if self.pattern_index_templates:
                index_templates = self.pattern_index_templates.get(named_type, [])
            else:
                index_templates = self.default_pattern_index_templates
            for template in index_templates:
                key = self._apply_index_template(template, named_type_hash, targets, arity)
                self.redis.sadd(key, value)
        for handle in incoming_buffer:
            key = _build_redis_key(KeyPrefix.INCOMING_SET, handle)
            self.redis.sadd(key, *incoming_buffer[handle])

    def reindex(self, pattern_index_templates: Optional[Dict[str, Dict[str, Any]]] = None):
        if pattern_index_templates is not None:
            self.pattern_index_templates = deepcopy(pattern_index_templates)
        self.redis.flushall()
        for collection in self.mongo_link_collection.values():
            self._update_link_index(collection.find({}))
