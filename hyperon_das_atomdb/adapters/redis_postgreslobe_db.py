import json
import time
from enum import Enum
from queue import Queue
from typing import Any, Dict, List, Optional, Tuple, Union

import psycopg2
from psycopg2.extensions import cursor as PostgresCursor

from hyperon_das_atomdb.adapters.redis_mongo_db import RedisMongoDB
from hyperon_das_atomdb.exceptions import InvalidSQL
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


class RedisPostgresLobeDB(RedisMongoDB):
    """A concrete implementation using Redis and a PostgresLobe"""

    def __repr__(self) -> str:
        return "<Atom database RedisPostgresLobe>"  # pragma no cover

    def __init__(self, **kwargs) -> None:
        self.database_name = 'das'
        self.pattern_index_templates = None
        self.table_names = []
        self.named_type_hash = {}
        self.typedef_base_type_hash = ExpressionHasher._compute_hash("Type")
        self.hash_length = len(self.typedef_base_type_hash)
        self.atom_queue = Queue()
        self.mapper = create_mapper(kwargs.get('mapper', 'sql2metta'))
        self._setup_databases(**kwargs)
        self._setup_indexes()
        self._fetch(tables=kwargs.get('tables', None), batch_size=kwargs.get('batch_size', 100000))
        logger().info("Database setup finished")

    def _setup_databases(
        self,
        postgres_database_name='postgres',
        postgres_hostname='localhost',
        postgres_port=27017,
        postgres_username='postgres',
        postgres_password='postgres',
        redis_hostname='localhost',
        redis_port=6379,
        redis_username=None,
        redis_password=None,
        redis_cluster=True,
        redis_ssl=True,
        **kwargs,
    ) -> None:
        self.postgres_db = self._connection_postgres_db(
            postgres_database_name,
            postgres_hostname,
            postgres_port,
            postgres_username,
            postgres_password,
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

    def _connection_postgres_db(
        self,
        postgres_database_name='postgres',
        postgres_hostname='localhost',
        postgres_port=5432,
        postgres_username='postgres',
        postgres_password='postgres',
    ) -> None:
        logger().info(
            f"Connecting to Postgres at {postgres_username}:{postgres_password}://{postgres_hostname}:{postgres_port}/{postgres_database_name}"
        )
        try:
            return psycopg2.connect(
                database=postgres_database_name,
                host=postgres_hostname,
                port=postgres_port,
                user=postgres_username,
                password=postgres_password,
            )
        except psycopg2.Error as e:
            logger().error(f'An error occourred when connection to Postgres - Details: {str(e)}')
            raise e

    def _fetch(self, tables: List[str] = None, batch_size: int = 100000) -> None:
        try:
            start0 = time.time()
            with self.postgres_db.cursor() as cursor:
                if tables is not None:
                    self.table_names = tables
                else:
                    cursor.execute(
                        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
                    )

                    self.table_names = [table[0] for table in cursor.fetchall()]

                print(f"\n** Quantiy of tables ** : {len(self.table_names)}")

                for table_name in self.table_names:
                    print(f"\n==> '{table_name}' table processing...")

                    start = time.time()
                    self._parser(cursor, table_name, batch_size)
                    print(f"|-> Time to parse all the table: {time.time() - start}")

                print(f"** Total time to fetch data **: {time.time() - start0}")
        except (psycopg2.Error, Exception) as e:
            logger().error(f"Error during fetching data from Postgres Lobe - Details: {str(e)}")
            raise e

    def _parser(self, cursor: PostgresCursor, table_name: str, batch_size: int = 100000) -> Table:
        try:
            start0 = time.time()

            # Get information about the constrainst and data type
            cursor.execute(
                f"""
                    SELECT 
                        cols.column_name,
                        cols.data_type,
                        CASE 
                            WHEN cons.constraint_type LIKE '%PRIMARY KEY%' THEN 'PK'
                            WHEN cons.constraint_type LIKE '%FOREIGN KEY%' THEN 'FK'
                            ELSE ''
                        END AS type
                    FROM 
                        information_schema.columns cols
                    LEFT JOIN 
                        (
                            SELECT 
                                cols.column_name,
                                STRING_AGG(cons.constraint_type, ', ') AS constraint_type
                            FROM 
                                information_schema.columns cols
                            LEFT JOIN 
                                information_schema.key_column_usage kcu 
                            ON 
                                cols.table_name = kcu.table_name 
                            AND 
                                cols.column_name = kcu.column_name
                            LEFT JOIN 
                                information_schema.table_constraints cons 
                            ON 
                                kcu.constraint_name = cons.constraint_name 
                            AND 
                                kcu.table_name = cons.table_name
                            WHERE 
                                cols.table_name = '{table_name}'
                            GROUP BY 
                                cols.column_name
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

            # Non PK columns
            cursor.execute(
                f"""
                SELECT 
                    string_agg(column_name, ',' ORDER BY ordinal_position)
                FROM (
                    SELECT column_name, ordinal_position
                    FROM information_schema.columns
                    WHERE table_name = '{table_name}' AND column_name != '{pk_column}'
                ) AS subquery;     
                """
            )
            non_pk_column = cursor.fetchone()[0]

            cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            total_rows = int(cursor.fetchone()[0])

            for i in range(0, total_rows, batch_size):
                start_a = time.time()
                # Pk Data
                cursor.execute(
                    f"""SELECT {pk_column} FROM {table_name} LIMIT {batch_size} OFFSET {i};"""
                )
                pk_table = cursor.fetchall()

                # Non PK Data
                cursor.execute(
                    f"""SELECT {non_pk_column} FROM {table_name} LIMIT {batch_size} OFFSET {i};"""
                )
                non_pk_table = cursor.fetchall()

                # Sorted data where PK column is first
                rows = [(*k, *v) for k, v in zip(pk_table, non_pk_table)]

                table = Table(table_name)

                [table.add_column(*column) for column in columns]

                column_names = table.get_column_names()
                [
                    table.add_row({key: value for key, value in zip(column_names, row)})
                    for row in rows
                ]

                print(
                    f"|-> Time to parse {min(total_rows - i, batch_size)} rows: {time.time() - start_a}"
                )

                start = time.time()
                atoms = self.mapper.map_table(table)
                print(f"|-> Time to map {len(atoms)} atoms: {time.time() - start}")

                start = time.time()
                self._update_atom_indexes(atoms)
                print(f"|-> Time to update indexes of {len(atoms)} atoms : {time.time() - start}")

                start = time.time()
                self._insert_atoms(atoms)
                print(f"|-> Time to insert {len(atoms)} atoms: {time.time() - start}")

                percentage = min(i + batch_size, total_rows) / total_rows * 100

                print(
                    f"\n|--> Parse progress: {percentage:.2f}% -- Time: {time.time() - start_a} -- Memory consumed: \n"
                )

            return table
        except (psycopg2.Error, TypeError, Exception) as e:
            print(f"|-> Error during parser. Time to error: {time.time() - start0} - {str(e)}")
            logger().error(f"Error: {e}")
            raise InvalidSQL(message=f"Error during parsing table '{table_name}'", details=str(e))

    # def _commit_atoms(self, atoms):
    #     try:
    #         start = time.time()
    #         self._update_atom_indexes(atoms)
    #         print(f"|-> Time to update indexes of {len(atoms)} atoms : {time.time() - start}")
    #         start = time.time()
    #         self._insert_atoms(atoms)
    #         print(f"|-> Time to insert {len(atoms)} atoms: {time.time() - start}")
    #     except Exception as e:
    #         raise e

    def _insert_atoms(self, atoms: Dict[str, Any]) -> None:
        for atom in atoms:
            key = f'atoms:{atom["_id"]}'
            self.redis.set(key, json.dumps(atom))

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
        raise NotImplementedError("The method 'get_matched_node_name' is not implemented yet")

    def commit(self) -> None:
        raise NotImplementedError("The method 'commit' is not implemented yet")

    def add_node(self, node_params: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError("The method 'add_node' is not implemented yet")

    def add_link(self, link_params: Dict[str, Any], toplevel: bool = True) -> Dict[str, Any]:
        raise NotImplementedError("The method 'add_link' is not implemented yet")

    def reindex(self, pattern_index_templates: Optional[Dict[str, Dict[str, Any]]] = None):
        raise NotImplementedError("The method 'reindex' is not implemented yet")

    def delete_atom(self, handle: str, **kwargs) -> None:
        raise NotImplementedError("The method 'delete_atom' is not implemented yet")

    def create_field_index(
        self,
        atom_type: str,
        field: str,
        type: Optional[str] = None,
        composite_type: Optional[List[Any]] = None,
    ) -> str:
        raise NotImplementedError("The method 'create_field_index' is not implemented yet")

    def get_atoms_by_index(self, index_id: str, **kwargs) -> Union[Tuple[int, list], list]:
        raise NotImplementedError("The method 'get_atoms_by_index' is not implemented yet")

    def bulk_insert(self, documents: List[Dict[str, Any]]) -> None:
        raise NotImplementedError("The method 'bulk_insert' is not implemented yet")

    def clear_database(self) -> None:
        raise NotImplementedError("The method 'clear_database' is not implemented yet")
