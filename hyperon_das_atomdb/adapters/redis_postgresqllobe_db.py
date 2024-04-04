# from copy import deepcopy
import json
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union

import psycopg2
from psycopg2.extensions import cursor as PostgreSQLCursor

from hyperon_das_atomdb.adapters.redis_mongo_db import RedisMongoDB
from hyperon_das_atomdb.exceptions import AtomDoesNotExist, NodeDoesNotExist

# from hyperon_das_atomdb.database import WILDCARD
# from hyperon_das_atomdb.exceptions import (
#     AtomDoesNotExist,
#     ConnectionMongoDBException,
#     InvalidOperationException,
#     LinkDoesNotExist,
#     NodeDoesNotExist,
# )
from hyperon_das_atomdb.logger import logger
from hyperon_das_atomdb.utils.expression_hasher import ExpressionHasher
from hyperon_das_atomdb.utils.mapper import Table, create_mapper


class FieldNames(str, Enum):
    NODE_NAME = 'name'
    TYPE_NAME = 'named_type'
    TYPE_NAME_HASH = 'named_type_hash'
    ID_HASH = '_id'
    TYPE = 'composite_type_hash'
    COMPOSITE_TYPE = 'composite_type'
    KEY_PREFIX = 'key'
    KEYS = 'keys'


class RedisPostgreSQLLobeDB(RedisMongoDB):
    """A concrete implementation using Redis and a PostgreSQL Lobe"""

    def __repr__(self) -> str:
        return "<Atom database RedisPostgreSQLLobe>"  # pragma no cover

    def __init__(self, **kwargs) -> None:
        self.database_name = 'das'
        self.pattern_index_templates = None
        self.table_names = []
        self.named_type_hash = {}
        self.typedef_base_type_hash = ExpressionHasher._compute_hash("Type")
        self.hash_length = len(self.typedef_base_type_hash)
        self.mapper = create_mapper(kwargs.get('mapper', 'sql2metta'))
        self._setup_databases(**kwargs)
        self._setup_indexes()
        self._fetch()
        logger().info("Database setup finished")

    def _setup_databases(
        self,
        postgresql_database_name='postgres',
        postgresql_hostname='localhost',
        postgresql_port=27017,
        postgresql_username='postgres',
        postgresql_password='postgres',
        redis_hostname='localhost',
        redis_port=6379,
        redis_username=None,
        redis_password=None,
        redis_cluster=True,
        redis_ssl=True,
        **kwargs,
    ) -> None:
        self.postgresql_db = self._connection_postgresql_db(
            postgresql_database_name,
            postgresql_hostname,
            postgresql_port,
            postgresql_username,
            postgresql_password,
        )
        self.redis = self._connection_redis(
            redis_hostname,
            redis_port,
            redis_username,
            redis_password,
            redis_cluster,
            redis_ssl,
        )

    def _setup_indexes(self):
        self.default_pattern_index_templates = []
        for named_type in [True, False]:
            for pos0 in [True, False]:
                for pos1 in [True, False]:
                    for pos2 in [True, False]:
                        if named_type and pos0 and pos1 and pos2:
                            continue
                        template = {}
                        template[FieldNames.TYPE_NAME] = named_type
                        template["selected_positions"] = [
                            i for i, pos in enumerate([pos0, pos1, pos2]) if pos
                        ]
                        self.default_pattern_index_templates.append(template)

    def _connection_postgresql_db(
        self,
        postgresql_database_name='postgres',
        postgresql_hostname='localhost',
        postgresql_port=5432,
        postgresql_username='postgres',
        postgresql_password='postgres',
    ) -> None:
        logger().info(
            f"Connecting to PostgreSQL at {postgresql_username}:{postgresql_password}://{postgresql_hostname}:{postgresql_port}/{postgresql_database_name}"
        )
        try:
            return psycopg2.connect(
                database=postgresql_database_name,
                host=postgresql_hostname,
                port=postgresql_port,
                user=postgresql_username,
                password=postgresql_password,
            )
        except psycopg2.OperationalError as e:
            logger().error(f'An error occourred when connection to PostgreSQL - Details: {str(e)}')
            raise e

    def _fetch(self) -> None:
        with self.postgresql_db.cursor() as cursor:
            cursor.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public';
                """
            )
            self.table_names = [table[0] for table in cursor.fetchall()]
            for table_name in self.table_names:
                table = self._parser(cursor, table_name)
                atoms = self.mapper.map_table(table)
                self._update_atom_indexes(atoms)
                self._insert_atoms(atoms)

    def _parser(self, cursor: PostgreSQLCursor, table_name: str) -> Table:
        table = Table(table_name)

        # Get information about the constrainst and data type
        cursor.execute(
            f"""
            SELECT 
                cols.column_name,
                cols.data_type,
                CASE 
                    WHEN cons.constraint_type = 'PRIMARY KEY' THEN 'PK'
                    WHEN cons.constraint_type = 'FOREIGN KEY' THEN 'FK'
                    ELSE ''
                END AS type
            FROM 
                information_schema.columns cols
            LEFT JOIN 
                (
                    SELECT 
                        kcu.column_name,
                        tc.constraint_type
                    FROM 
                        information_schema.key_column_usage kcu
                    JOIN 
                        information_schema.table_constraints tc 
                        ON kcu.constraint_name = tc.constraint_name
                        AND kcu.constraint_schema = tc.constraint_schema
                    WHERE 
                        tc.table_name = '{table_name}'
                ) cons 
                ON cols.column_name = cons.column_name
            WHERE 
                cols.table_name = '{table_name}'
            ORDER BY 
                CASE 
                    WHEN cons.constraint_type = 'PRIMARY KEY' THEN 0 
                    ELSE 1 
                END;
            """
        )
        columns = cursor.fetchall()
        for column in columns:
            table.add_column(*column)

        # Pk column
        cursor.execute(
            f"""
            SELECT column_name
            FROM information_schema.key_column_usage
            WHERE constraint_name = (
                SELECT constraint_name
                FROM information_schema.table_constraints
                WHERE table_name = '{table_name}' AND constraint_type = 'PRIMARY KEY'
            ) AND table_name = '{table_name}';       
            """
        )
        pk_column = cursor.fetchone()[0]

        # Pk Data
        cursor.execute(
            f"""
            SELECT {pk_column} FROM {table_name};
            """
        )
        pk_table = cursor.fetchall()

        # Non PK columns
        cursor.execute(
            f"""
            SELECT 
                string_agg(column_name, ',')
            FROM 
                information_schema.columns
            WHERE 
                table_name = '{table_name}'
                AND column_name != '{pk_column}'        
            """
        )
        non_pk_column = cursor.fetchone()[0]

        # Non PK Data
        cursor.execute(
            f"""
            SELECT {non_pk_column} FROM {table_name}
            """
        )
        non_pk_table = cursor.fetchall()

        # Sorted data where PK column is first
        rows = [(*k, *v) for k, v in zip(pk_table, non_pk_table)]

        for row in rows:
            table.add_row({key: value for key, value in zip(table.get_column_names(), row)})

        return table

    def _insert_atoms(self, atoms: Dict[str, Any]) -> None:
        for atom in atoms:
            key = f'atoms:{atom["_id"]}'
            self.redis.set(key, json.dumps(atom))

    def _value_exists_in_db(self, value: Any) -> bool:
        with self.postgresql_db.cursor() as cursor:
            for table_name in self.table_names:
                cursor.execute(
                    f"""
                    SELECT EXISTS (
                        SELECT 1
                        FROM {table_name}
                        WHERE ({table_name}::text ILIKE '%{value}%')
                    );
                    """
                )
                value_exists = cursor.fetchone()[0]
                if value_exists:
                    return True
            return False

    def _retrieve_document(self, handle: str) -> dict:
        key = f'atoms:{handle}'
        answer = self.redis.get(key)
        if answer is not None:
            document = json.loads(answer)
            if self._is_document_link(document):
                document["targets"] = self._get_document_keys(document)
            return document
        return None

    def _retrieve_all_documents(self, key: str = None, value: Any = None):
        answers = self.redis.mget(self.redis.keys('atoms:*'))
        all_documents = [json.loads(answer) for answer in answers]
        if key and value is not None:
            if value is True:
                return [document for document in all_documents if key in document]
            elif value is False:
                return [document for document in all_documents if key not in document]
            else:
                return [document for document in all_documents if document[key] == value]
        else:
            return all_documents

    def _get_and_delete_links_by_handles(self, handles: List[str]) -> Dict[str, Any]:
        pass

    def _retrieve_documents_by_index(
        self, collection: Any, index_id: str, **kwargs
    ) -> Tuple[int, List[Dict[str, Any]]]:
        pass

    def get_all_nodes(self, node_type: str, names: bool = False) -> List[str]:
        if names:
            return [
                document[FieldNames.NODE_NAME]
                for document in self._retrieve_all_documents(
                    key=FieldNames.TYPE_NAME, value=node_type
                )
            ]
        else:
            return [
                document[FieldNames.ID_HASH]
                for document in self._retrieve_all_documents(
                    key=FieldNames.TYPE_NAME, value=node_type
                )
            ]

    def get_all_links(self, link_type: str) -> List[str]:
        links_handle = []
        documents = self._retrieve_all_documents(key=FieldNames.TYPE_NAME, value=link_type)
        for document in documents:
            links_handle.append(document[FieldNames.ID_HASH])
        return links_handle

    def count_atoms(self) -> Tuple[int, int]:
        atoms = len(self._retrieve_all_documents())
        nodes = len(self._retrieve_all_documents(FieldNames.COMPOSITE_TYPE, False))
        return nodes, atoms - nodes

    def get_matched_node_name(self, node_type: str, substring: str) -> str:
        pass

    def commit(self) -> None:
        pass

    def add_node(self, node_params: Dict[str, Any]) -> Dict[str, Any]:
        pass

    def add_link(self, link_params: Dict[str, Any], toplevel: bool = True) -> Dict[str, Any]:
        pass

    def reindex(self, pattern_index_templates: Optional[Dict[str, Dict[str, Any]]] = None):
        pass

    def delete_atom(self, handle: str, **kwargs) -> None:
        pass

    def create_field_index(
        self,
        atom_type: str,
        field: str,
        type: Optional[str] = None,
        composite_type: Optional[List[Any]] = None,
    ) -> str:
        pass

    def get_atoms_by_index(self, index_id: str, **kwargs) -> Union[Tuple[int, list], list]:
        pass

    def bulk_insert(self, documents: List[Dict[str, Any]]) -> None:
        pass

    def clear_database(self) -> None:
        pass


if __name__ == "__main__":
    db = RedisPostgreSQLLobeDB(
        postgresql_database_name='postgres',
        postgresql_hostname='127.0.0.1',
        postgresql_port=5432,
        postgresql_username='postgres',
        postgresql_password='postgres',
        redis_hostname='127.0.0.1',
        redis_port=8379,
        redis_username=None,
        redis_password=None,
        redis_cluster=False,
        redis_ssl=False,
    )
    # marco = db.get_node_handle(node_type='Symbol', node_name='"Marco"')
    # recife = db.get_node_handle(node_type='Symbol', node_name='"Recife"')
    # contractor_name = db.get_node_handle(node_type='Symbol', node_name='contractor.name')

    # contractor_id_link = db.get_link_handle(
    #     link_type='Expression',
    #     target_handles=[
    #         db.get_node_handle('Symbol', 'contractor'),
    #         db.get_node_handle('Symbol', '"1"'),
    #     ],
    # )
    # contractor_name_link = db.get_link_handle(
    #     link_type='Expression', target_handles=[contractor_name, contractor_id_link, marco]
    # )

    # atom = db.get_matched_links(link_type='Expression', target_handles=[contractor_name, '*', '*'])

    # atom1 = db.get_atom(contractor_name_link)
    # atom2 = db.get_atom_type(contractor_name_link)
    # atom3 = db.get_atom_as_dict(contractor_name_link)

    # node1 = db.get_node_name(marco)
    # node2 = db.get_node_type(marco)
    # node3 = db.get_all_nodes('Symbol')

    # link1 = db.get_all_links('Expression')
    # link2 = db.get_link_targets(contractor_id_link)
    # link3 = db.is_ordered(contractor_id_link)
    # link4 = db.get_incoming_links(marco)
    # link5 = db.get_matched_type_template(['Expression', 'Symbol', 'Symbol'])
    # link6 = db.get_matched_type('Expression')
    # db.count_atoms()
    print('END')
