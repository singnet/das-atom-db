import base64
import pickle
import sys
from copy import deepcopy
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from pymongo import MongoClient
from pymongo import errors as pymongo_errors
from pymongo.collection import Collection
from pymongo.database import Database
from redis import Redis
from redis.cluster import RedisCluster

from hyperon_das_atomdb.database import UNORDERED_LINK_TYPES, WILDCARD, AtomDB, IncomingLinksT
from hyperon_das_atomdb.exceptions import (
    AtomDoesNotExist,
    ConnectionMongoDBException,
    InvalidOperationException,
    LinkDoesNotExist,
    NodeDoesNotExist,
)
from hyperon_das_atomdb.index import Index
from hyperon_das_atomdb.logger import logger
from hyperon_das_atomdb.utils.expression_hasher import ExpressionHasher


def _build_redis_key(prefix, key):
    return prefix + ":" + key


class MongoCollectionNames(str, Enum):
    ATOMS = 'atoms'
    ATOM_TYPES = 'atom_types'
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
    INCOMING_SET = 'incoming_set'
    OUTGOING_SET = 'outgoing_set'
    PATTERNS = 'patterns'
    TEMPLATES = 'templates'
    NAMED_ENTITIES = 'names'
    CUSTOM_INDEXES = 'custom_indexes'


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


class MongoDBIndex(Index):
    def __init__(self, collection: Collection) -> None:
        self.collection = collection

    def create(self, atom_type: str, field: str, **kwargs) -> Tuple[str, Any]:
        conditionals = {}

        for key, value in kwargs.items():
            conditionals = {key: {"$eq": value}}
            break  # only one key-value pair

        index_id = f"{atom_type}_{self.generate_index_id(field, conditionals)}"

        index_conditionals = {"name": index_id}

        if conditionals:
            index_conditionals["partialFilterExpression"] = conditionals

        index_list = [(field, 1)]  # store the index in ascending order

        if not self.index_exists(index_id):
            return self.collection.create_index(index_list, **index_conditionals), conditionals
        else:
            return index_id, conditionals

    def index_exists(self, index_id: str) -> bool:
        indexes = self.collection.list_indexes()
        index_ids = [index.get('name') for index in indexes]
        return True if index_id in index_ids else False


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
        self.mongo_atoms_collection = self.mongo_db.get_collection(MongoCollectionNames.ATOMS)
        self.mongo_types_collection = self.mongo_db.get_collection(MongoCollectionNames.ATOM_TYPES)
        self.all_mongo_collections = [
            (MongoCollectionNames.ATOMS, self.mongo_atoms_collection),
            (MongoCollectionNames.ATOM_TYPES, self.mongo_types_collection),
        ]
        self.mongo_das_config_collection = None
        self.wildcard_hash = ExpressionHasher._compute_hash(WILDCARD)
        self.named_type_hash = {}
        self.typedef_mark_hash = ExpressionHasher._compute_hash(":")
        self.typedef_base_type_hash = ExpressionHasher._compute_hash("Type")
        self.hash_length = len(self.typedef_base_type_hash)
        self.typedef_composite_type_hash = ExpressionHasher.composite_hash(
            [
                self.typedef_mark_hash,
                self.typedef_base_type_hash,
                self.typedef_base_type_hash,
            ]
        )
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
            "decode_responses": True,
            "ssl": redis_ssl,
        }

        if redis_password and redis_username:
            redis_connection["password"] = redis_password
            redis_connection["username"] = redis_username

        if redis_cluster:
            return RedisCluster(**redis_connection)
        else:
            return Redis(**redis_connection)

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

    def _retrieve_document(self, handle: str) -> dict:
        mongo_filter = {MongoFieldNames.ID_HASH: handle}
        document = self.mongo_atoms_collection.find_one(mongo_filter)
        if document := self.mongo_atoms_collection.find_one(mongo_filter):
            if self._is_document_link(document):
                document["targets"] = self._get_document_keys(document)
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

    def _get_document_keys(self, document: Dict) -> List[str]:
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
        matches_toplevel_only = []
        if len(matches) > 0:
            # if isinstance(matches[0], list):
            #    matches = matches[0]
            for match in matches:
                link_handle = match[0]
                link = self._retrieve_document(link_handle)
                if link['is_toplevel']:
                    matches_toplevel_only.append(match)
        return matches_toplevel_only

    def get_node_handle(self, node_type: str, node_name: str) -> str:
        node_handle = self.node_handle(node_type, node_name)
        document = self._retrieve_document(node_handle)
        if document is not None:
            return document[MongoFieldNames.ID_HASH]
        else:
            logger().error(
                f'Failed to retrieve node handle for {node_type}:{node_name}. This node may not exist.'
            )
            raise NodeDoesNotExist(
                message="Nonexistent node",
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
            for document in self.mongo_atoms_collection.find(mongo_filter)
        ]

    def get_all_nodes(self, node_type: str, names: bool = False) -> List[str]:
        if names:
            return [
                document[MongoFieldNames.NODE_NAME]
                for document in self.mongo_atoms_collection.find(
                    {MongoFieldNames.TYPE_NAME: node_type}
                )
            ]
        else:
            return [
                document[MongoFieldNames.ID_HASH]
                for document in self.mongo_atoms_collection.find(
                    {MongoFieldNames.TYPE_NAME: node_type}
                )
            ]

    def get_all_links(self, link_type: str, **kwargs) -> Tuple[int, List[str]]:
        pymongo_cursor = self.mongo_atoms_collection.find({MongoFieldNames.TYPE_NAME: link_type})

        if kwargs.get('cursor') is not None:
            cursor = kwargs.get('cursor')
            chunk_size = kwargs.get('chunk_size', 500)
            pymongo_cursor.skip(cursor).limit(chunk_size)

            handles = [document[MongoFieldNames.ID_HASH] for document in pymongo_cursor]

            if not handles:
                return 0, []

            if len(handles) < chunk_size:
                return 0, handles
            else:
                return cursor + chunk_size, handles

        return 0, [document[MongoFieldNames.ID_HASH] for document in pymongo_cursor]

    def get_link_handle(self, link_type: str, target_handles: List[str]) -> str:
        link_handle = self.link_handle(link_type, target_handles)
        document = self._retrieve_document(link_handle)
        if document is not None:
            return document[MongoFieldNames.ID_HASH]
        else:
            logger().error(
                f'Failed to retrieve link handle for {link_type}:{target_handles}. This link may not exist.'
            )
            raise LinkDoesNotExist(
                message="Nonexistent link",
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
        document = self._retrieve_document(link_handle)
        if document is None:
            logger().error(
                'Failed to retrieve document for link handle: {link_handle}. The handle may be invalid or the corresponding link does not exist.'
            )
            raise ValueError(f"Invalid handle: {link_handle}")
        return True

    def get_matched_links(
        self, link_type: str, target_handles: List[str], **kwargs
    ) -> Union[tuple, list]:
        if link_type != WILDCARD and WILDCARD not in target_handles:
            try:
                link_handle = self.get_link_handle(link_type, target_handles)
                if kwargs.get('cursor') is not None:
                    return 0, [link_handle]
                return [link_handle]
            except LinkDoesNotExist:
                if kwargs.get('cursor') is not None:
                    return 0, []
                return []

        if link_type == WILDCARD:
            link_type_hash = WILDCARD
        else:
            link_type_hash = self._get_atom_type_hash(link_type)

        if link_type_hash is None:
            if kwargs.get('cursor') is not None:
                return 0, []
            return []

        if link_type in UNORDERED_LINK_TYPES:
            target_handles = sorted(target_handles)

        pattern_hash = ExpressionHasher.composite_hash([link_type_hash, *target_handles])
        cursor, patterns_matched = self._retrieve_pattern(pattern_hash, **kwargs)
        toplevel_only = kwargs.get('toplevel_only', False)
        return self._process_matched_results(patterns_matched, cursor, toplevel_only)

    def get_incoming_links(
        self, atom_handle: str, **kwargs
    ) -> Union[Tuple[int, List[IncomingLinksT]], List[IncomingLinksT]]:
        cursor, links = self._retrieve_incoming_set(atom_handle, **kwargs)

        if kwargs.get('cursor') is not None:
            if kwargs.get('handles_only', False):
                return cursor, links
            else:
                return cursor, [self.get_atom(handle, **kwargs) for handle in links]
        else:
            if kwargs.get('handles_only', False):
                return links
            else:
                return [self.get_atom(handle, **kwargs) for handle in links]

    def get_matched_type_template(self, template: List[Any], **kwargs) -> Union[tuple, list]:
        try:
            template = self._build_named_type_hash_template(template)
            template_hash = ExpressionHasher.composite_hash(template)
            cursor, templates_matched = self._retrieve_template(template_hash, **kwargs)
            toplevel_only = kwargs.get('toplevel_only', False)
            return self._process_matched_results(templates_matched, cursor, toplevel_only)
        except Exception as exception:
            logger().error(f'Failed to get matched type template - Details: {str(exception)}')
            raise ValueError(str(exception))

    def get_matched_type(self, link_type: str, **kwargs) -> Union[tuple, list]:
        named_type_hash = self._get_atom_type_hash(link_type)
        cursor, templates_matched = self._retrieve_template(named_type_hash, **kwargs)
        toplevel_only = kwargs.get('toplevel_only', False)
        return self._process_matched_results(templates_matched, cursor, toplevel_only)

    def get_link_type(self, link_handle: str) -> str:
        document = self.get_atom(link_handle)
        return document[MongoFieldNames.TYPE_NAME]

    def get_atom(self, handle: str, **kwargs) -> Dict[str, Any]:
        document = self._retrieve_document(handle)
        if document:
            if not kwargs.get('no_target_format', False):
                return self._transform_to_target_format(document, **kwargs)
            else:
                return document
        else:
            logger().error(
                f'Failed to retrieve atom for handle: {handle}. This link may not exist. - Details: {kwargs}'
            )
            raise AtomDoesNotExist(
                message='Nonexistent atom',
                details=f'handle: {handle}',
            )

    def get_atom_type(self, handle: str) -> str:
        atom = self._retrieve_document(handle)
        if atom is not None:
            return atom['named_type']

    def get_atom_as_dict(self, handle) -> dict:
        answer = {}
        document = self._retrieve_document(handle)
        if document:
            answer["handle"] = document[MongoFieldNames.ID_HASH]
            answer["type"] = document[MongoFieldNames.TYPE_NAME]
            if "targets" in document:
                answer["targets"] = document["targets"]
            else:
                answer["name"] = document["name"]
        return answer

    def count_atoms(self) -> Tuple[int, int]:
        nodes_count = self.mongo_atoms_collection.count_documents(
            {MongoFieldNames.COMPOSITE_TYPE: {'$exists': False}}
        )
        links_count = self.mongo_atoms_collection.count_documents(
            {MongoFieldNames.COMPOSITE_TYPE: {'$exists': True}}
        )
        return (nodes_count, links_count)

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

    def commit(self, **kwargs) -> None:
        id_tag = MongoFieldNames.ID_HASH

        if kwargs.get('buffer'):
            try:
                for document in kwargs['buffer']:
                    self.mongo_atoms_collection.replace_one(
                        {id_tag: document[id_tag]}, document, upsert=True
                    )
                    self._update_atom_indexes([document])
            except Exception as e:
                logger().error(f'Failed to commit buffer - Details: {str(e)}')
                raise e
        else:
            for key, (collection, buffer) in self.mongo_bulk_insertion_buffer.items():
                if buffer:
                    if key == MongoCollectionNames.ATOM_TYPES:
                        logger().error('Failed to commit Atom Types. This operation is not allowed')
                        raise InvalidOperationException

                    for hashtable in buffer:
                        document = hashtable.base
                        collection.replace_one({id_tag: document[id_tag]}, document, upsert=True)
                        self._update_atom_indexes([document])

                buffer.clear()

    def add_node(self, node_params: Dict[str, Any]) -> Dict[str, Any]:
        handle, node = self._add_node(node_params)
        if sys.getsizeof(node_params['name']) < self.max_mongo_db_document_size:
            _, buffer = self.mongo_bulk_insertion_buffer[MongoCollectionNames.ATOMS]
            buffer.add(_HashableDocument(node))
            if len(buffer) >= self.mongo_bulk_insertion_limit:
                self.commit()
            return node
        else:
            logger().warn("Discarding atom whose name is too large: {node_name}")

    def add_link(self, link_params: Dict[str, Any], toplevel: bool = True) -> Dict[str, Any]:
        handle, link, targets = self._add_link(link_params, toplevel)
        _, buffer = self.mongo_bulk_insertion_buffer[MongoCollectionNames.ATOMS]
        buffer.add(_HashableDocument(link))
        if len(buffer) >= self.mongo_bulk_insertion_limit:
            self.commit()
        return link

    def _get_and_delete_links_by_handles(self, handles: List[str]) -> Dict[str, Any]:
        documents = []
        for handle in handles:
            if document := self.mongo_atoms_collection.find_one_and_delete(
                {MongoFieldNames.ID_HASH: handle}
            ):
                documents.append(document)
        return documents

    def _apply_index_template(
        self, template: Dict[str, Any], named_type: str, targets: List[str], arity
    ) -> List[List[str]]:
        key = []
        key = [WILDCARD] if template[MongoFieldNames.TYPE_NAME] else [named_type]
        target_selected_pos = template["selected_positions"]
        for cursor in range(arity):
            key.append(WILDCARD if cursor in target_selected_pos else targets[cursor])
        return _build_redis_key(KeyPrefix.PATTERNS, ExpressionHasher.composite_hash(key))

    def _retrieve_incoming_set(self, handle: str, **kwargs) -> Tuple[int, List[str]]:
        key = _build_redis_key(KeyPrefix.INCOMING_SET, handle)
        cursor, members = self._get_redis_members(key, **kwargs)
        return (cursor, [member for member in members])

    def _delete_smember_incoming_set(self, handle: str, smember: str) -> None:
        key = _build_redis_key(KeyPrefix.INCOMING_SET, handle)
        self.redis.srem(key, smember)

    def _retrieve_and_delete_incoming_set(self, handle: str) -> List[str]:
        key = _build_redis_key(KeyPrefix.INCOMING_SET, handle)
        data = [member for member in self.redis.smembers(key)]
        self.redis.delete(key)
        return data

    def _retrieve_outgoing_set(self, handle: str, delete=False) -> List[str]:
        key = _build_redis_key(KeyPrefix.OUTGOING_SET, handle)
        if delete:
            value = self.redis.getdel(key)
        else:
            value = self.redis.get(key)
        if value is None:
            return []
        arity = len(value) // self.hash_length
        return [
            value[(offset * self.hash_length) : ((offset + 1) * self.hash_length)]
            for offset in range(arity)
        ]

    def _retrieve_name(self, handle: str) -> str:
        key = _build_redis_key(KeyPrefix.NAMED_ENTITIES, handle)
        name = self.redis.get(key)
        if name:
            return name
        else:
            return None

    def _retrieve_hash_targets_value(
        self, key_prefix: str, handle: str, **kwargs
    ) -> Tuple[int, List[str]]:
        key = _build_redis_key(key_prefix, handle)
        cursor, members = self._get_redis_members(key, **kwargs)
        if len(members) == 0:
            return (cursor, [])
        else:
            n = len(next(iter(members))) // self.hash_length
            return (
                cursor,
                [
                    [
                        member[(offset * self.hash_length) : ((offset + 1) * self.hash_length)]
                        for offset in range(n)
                    ]
                    for member in members
                ],
            )

    def _retrieve_template(self, handle: str, **kwargs) -> Tuple[int, List[str]]:
        return self._retrieve_hash_targets_value(KeyPrefix.TEMPLATES, handle, **kwargs)

    def _delete_smember_template(self, handle: str, smember: str) -> None:
        key = _build_redis_key(KeyPrefix.TEMPLATES, handle)
        self.redis.srem(key, smember)

    def _retrieve_pattern(self, handle: str, **kwargs) -> Tuple[int, List[str]]:
        return self._retrieve_hash_targets_value(KeyPrefix.PATTERNS, handle, **kwargs)

    def _retrieve_custom_index(self, index_id: str) -> dict:
        try:
            key = _build_redis_key(KeyPrefix.CUSTOM_INDEXES, index_id)
            custom_index_str = self.redis.get(key)

            if custom_index_str is None:
                logger().info(f"Custom index with ID {index_id} not found in Redis")
                return None

            custom_index_bytes = base64.b64decode(custom_index_str)
            custom_index = pickle.loads(custom_index_bytes)

            if not isinstance(custom_index, dict):
                logger().error(f"Custom index with ID {index_id} is not a dictionary")
                raise ValueError("Custom index is not a dictionary")

            return custom_index
        except ConnectionError as e:
            logger().error(f"Error connecting to Redis: {e}")
            raise e
        except Exception as e:
            logger().error(f"Unexpected error retrieving custom index with ID {index_id}: {e}")
            raise e

    def _get_redis_members(self, key, **kwargs) -> Tuple[int, list]:
        """
        Retrieve members from a Redis set, with optional cursor-based paging.

        Args:
            key (str): The key of the set in Redis.
            **kwargs: Additional keyword arguments.
                cursor (int, optional): The cursor for pagination.
                chunk_size (int, optional): The size of each chunk to retrieve.

        Returns:
            Tuple[int, list]: The cursor and a list of members retrieved from Redis.
        """
        if (cursor := kwargs.get('cursor')) is not None:
            chunk_size = kwargs.get('chunk_size', 1000)
            cursor, members = self.redis.sscan(name=key, cursor=cursor, count=chunk_size)
        else:
            cursor = None
            members = self.redis.smembers(key)

        return cursor, members

    def _update_atom_indexes(self, documents: Iterable[Dict[str, any]], **kwargs) -> None:
        for document in documents:
            if self._is_document_link(document):
                self._update_link_index(document, **kwargs)
            else:
                self._update_node_index(document, **kwargs)

    def _update_node_index(self, document: Dict[str, Any], **kwargs) -> None:
        handle = document[MongoFieldNames.ID_HASH]
        node_name = document[MongoFieldNames.NODE_NAME]
        key = _build_redis_key(KeyPrefix.NAMED_ENTITIES, handle)
        if kwargs.get('delete_atom', False):
            self.redis.delete(key)
            if links_handle := self._retrieve_and_delete_incoming_set(handle):
                documents = self._get_and_delete_links_by_handles(links_handle)
                for document in documents:
                    self._update_link_index(document, delete_atom=True)
        else:
            self.redis.set(key, node_name)

    def _update_link_index(self, document: Dict[str, Any], **kwargs) -> None:
        handle = document[MongoFieldNames.ID_HASH]
        targets = self._get_document_keys(document)
        targets_str = "".join(targets)
        arity = len(targets)
        named_type = document[MongoFieldNames.TYPE_NAME]
        named_type_hash = document[MongoFieldNames.TYPE_NAME_HASH]
        value = f"{handle}{targets_str}"

        if self.pattern_index_templates:
            index_templates = self.pattern_index_templates.get(named_type, [])
        else:
            index_templates = self.default_pattern_index_templates

        if kwargs.get('delete_atom', False):
            links_handle = self._retrieve_and_delete_incoming_set(handle)

            if links_handle:
                docs = self._get_and_delete_links_by_handles(links_handle)
                for doc in docs:
                    self._update_link_index(doc, delete_atom=True)

            outgoing_atoms = self._retrieve_outgoing_set(handle, delete=True)

            for atom_handle in outgoing_atoms:
                self._delete_smember_incoming_set(atom_handle, handle)

            for type_hash in [MongoFieldNames.TYPE, MongoFieldNames.TYPE_NAME_HASH]:
                self._delete_smember_template(document[type_hash], value)

            for template in index_templates:
                key = self._apply_index_template(template, named_type_hash, targets, arity)
                self.redis.srem(key, value)
        else:
            incoming_buffer = {}
            key = _build_redis_key(KeyPrefix.OUTGOING_SET, handle)
            self.redis.set(key, targets_str)

            for target in targets:
                buffer = incoming_buffer.get(target, None)
                if buffer is None:
                    buffer = []
                    incoming_buffer[target] = buffer
                buffer.append(handle)

            for type_hash in [MongoFieldNames.TYPE, MongoFieldNames.TYPE_NAME_HASH]:
                key = _build_redis_key(KeyPrefix.TEMPLATES, document[type_hash])
                self.redis.sadd(key, value)

            for template in index_templates:
                key = self._apply_index_template(template, named_type_hash, targets, arity)
                self.redis.sadd(key, value)

            for handle in incoming_buffer:
                key = _build_redis_key(KeyPrefix.INCOMING_SET, handle)
                self.redis.sadd(key, *incoming_buffer[handle])

    def _process_matched_results(
        self, matched: list, cursor: int = None, toplevel_only: bool = False
    ) -> Union[tuple, list]:
        if toplevel_only:
            answer = self._filter_non_toplevel(matched)
        else:
            answer = matched

        if cursor is not None:
            return cursor, answer
        else:
            return answer

    def _is_document_link(self, document: Dict[str, Any]) -> bool:
        return True if MongoFieldNames.COMPOSITE_TYPE in document else False

    def _calculate_composite_type_hash(self, composite_type: List[Any]) -> str:
        def calculate_composite_type_hashes(composite_type: List[Any]) -> List[str]:
            response = []
            for type in composite_type:
                if isinstance(type, list):
                    _hash = calculate_composite_type_hashes(type)
                    response.append(ExpressionHasher.composite_hash(_hash))
                else:
                    response.append(ExpressionHasher.named_type_hash(type))
            return response

        composite_type_hashes_list = calculate_composite_type_hashes(composite_type)
        return ExpressionHasher.composite_hash(composite_type_hashes_list)

    def _retrieve_documents_by_index(
        self, collection: Collection, index_id: str, **kwargs
    ) -> Tuple[int, List[Dict[str, Any]]]:
        if MongoDBIndex(collection).index_exists(index_id):
            cursor = kwargs.pop('cursor', None)
            chunk_size = kwargs.pop('chunk_size', 500)

            try:
                kwargs.update(self._retrieve_custom_index(index_id))
            except Exception as e:
                raise e

            # Using the hint() method is an additional measure to ensure its use
            pymongo_cursor = collection.find(kwargs).hint(index_id)

            if cursor is not None:
                pymongo_cursor.skip(cursor).limit(chunk_size)

                documents = [document for document in pymongo_cursor]

                if not documents:
                    return 0, []

                if len(documents) < chunk_size:
                    return 0, documents
                else:
                    return cursor + chunk_size, documents

            return 0, [document for document in pymongo_cursor]
        else:
            raise ValueError(f"Index '{index_id}' does not exist in collection '{collection}'")

    def reindex(self, pattern_index_templates: Optional[Dict[str, Dict[str, Any]]] = None):
        if pattern_index_templates is not None:
            self.pattern_index_templates = deepcopy(pattern_index_templates)
        self.redis.flushall()
        self._update_atom_indexes(self.mongo_atoms_collection.find({}))

    def delete_atom(self, handle: str, **kwargs) -> None:
        self.commit()

        mongo_filter = {MongoFieldNames.ID_HASH: handle}

        document = self.mongo_atoms_collection.find_one_and_delete(mongo_filter)

        if not document:
            logger().error(
                f'Failed to delete atom for handle: {handle}. This atom may not exist. - Details: {kwargs}'
            )
            raise AtomDoesNotExist(
                message='Nonexistent atom',
                details=f'handle: {handle}',
            )
        self._update_atom_indexes([document], delete_atom=True)

    def create_field_index(
        self,
        atom_type: str,
        field: str,
        type: Optional[str] = None,
        composite_type: Optional[List[Any]] = None,
    ) -> str:
        if type and composite_type:
            raise ValueError("Both type and composite_type cannot be specified")

        kwargs = {}

        if type:
            kwargs = {MongoFieldNames.TYPE_NAME: type}
        elif composite_type:
            kwargs = {MongoFieldNames.TYPE: self._calculate_composite_type_hash(composite_type)}

        collection = self.mongo_atoms_collection

        index_id = ""

        try:
            exc = ""
            index_id, conditionals = MongoDBIndex(collection).create(atom_type, field, **kwargs)
            serialized_conditionals = pickle.dumps(conditionals)
            serialized_conditionals_str = base64.b64encode(serialized_conditionals).decode('utf-8')
            self.redis.set(
                _build_redis_key(KeyPrefix.CUSTOM_INDEXES, index_id),
                serialized_conditionals_str,
            )
        except pymongo_errors.OperationFailure as e:
            exc = e
            logger().error(f"Error creating index in collection '{collection}': {str(e)}")
        except Exception as e:
            exc = e
            logger().error(f"Error: {str(e)}")
        finally:
            if not index_id:
                return (
                    f"Index creation failed, Details: {str(exc)}"
                    if exc
                    else "Index creation failed"
                )

        return index_id

    def get_atoms_by_index(self, index_id: str, **kwargs) -> Union[Tuple[int, list], list]:
        try:
            documents = self._retrieve_documents_by_index(
                self.mongo_atoms_collection, index_id, **kwargs
            )
            cursor, documents = documents
            return cursor, [self.get_atom(document['_id']) for document in documents]
        except Exception as e:
            logger().error(f"Error retrieving atoms by index: {str(e)}")
            raise e

    def retrieve_all_atoms(self) -> List[Dict[str, Any]]:
        try:
            return [document for document in self.mongo_atoms_collection.find()]
        except Exception as e:
            logger().error(f"Error retrieving all atoms: {str(e)}")
            raise e

    def bulk_insert(self, documents: List[Dict[str, Any]]) -> None:
        try:
            _id = MongoFieldNames.ID_HASH
            [
                self.mongo_atoms_collection.replace_one({_id: document[_id]}, document, upsert=True)
                for document in documents
            ]
            self._update_atom_indexes(documents)
        except Exception:
            logger().error("Error bulk inserting documents")
            return None
