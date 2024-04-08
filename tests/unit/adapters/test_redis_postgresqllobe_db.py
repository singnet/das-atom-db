import json
from unittest import mock

import pytest

from hyperon_das_atomdb.adapters.redis_postgresqllobe_db import RedisPostgreSQLLobeDB


class TestRedisPostgreSQLLobeDB:
    @pytest.fixture()
    def sql_lobe(self):
        redis = mock.MagicMock()
        postgresql = mock.MagicMock()
        with mock.patch(
            'hyperon_das_atomdb.adapters.redis_postgresqllobe_db.RedisPostgreSQLLobeDB._connection_postgresql_db',
            return_value=postgresql,
        ), mock.patch(
            'hyperon_das_atomdb.adapters.redis_postgresqllobe_db.RedisPostgreSQLLobeDB._connection_redis',
            return_value=redis,
        ):
            yield RedisPostgreSQLLobeDB()

    def test_repr(self, sql_lobe):
        assert repr(sql_lobe) == "<Atom database RedisPostgreSQLLobe>"

    def test_setup_databases(self, sql_lobe):
        sql_lobe._setup_databases(
            postgresql_database_name='sql_lobe',
            postgresql_hostname='test',
            postgresql_port=5432,
            postgresql_username='test',
            postgresql_password='test',
            redis_hostname='test',
            redis_port=6379,
            redis_username='test',
            redis_password='test',
            redis_cluster=False,
            redis_ssl=False,
        )
        sql_lobe._connection_postgresql_db.assert_called_with(
            'sql_lobe', 'test', 5432, 'test', 'test'
        )
        sql_lobe._connection_redis.assert_called_with('test', 6379, 'test', 'test', False, False)

    def test_fetch(self):
        with mock.patch(
            'hyperon_das_atomdb.adapters.redis_postgresqllobe_db.RedisPostgreSQLLobeDB._setup_databases',
            return_value=mock.MagicMock(),
        ), mock.patch(
            'hyperon_das_atomdb.adapters.redis_postgresqllobe_db.RedisPostgreSQLLobeDB._setup_indexes',
            return_value=mock.MagicMock(),
        ), mock.patch(
            'hyperon_das_atomdb.adapters.redis_postgresqllobe_db.RedisPostgreSQLLobeDB._fetch',
            return_value=mock.MagicMock(),
        ):
            sql_lobe = RedisPostgreSQLLobeDB()

        sql_lobe._parser = mock.MagicMock()
        sql_lobe._update_atom_indexes = mock.MagicMock()
        sql_lobe._insert_atoms = mock.MagicMock()
        sql_lobe.mapper.map_table = mock.MagicMock()
        sql_lobe.postgresql_db = mock.MagicMock()

        cursor_mock = mock.MagicMock()
        cursor_mock.fetchall.return_value = [('table1',), ('table2',)]
        sql_lobe.postgresql_db.cursor.return_value.__enter__.return_value = cursor_mock

        sql_lobe._fetch()

        cursor_mock.execute.assert_called_once_with(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
        )

        assert sql_lobe._parser.call_count == 2
        assert sql_lobe._update_atom_indexes.call_count == 2
        assert sql_lobe._insert_atoms.call_count == 2
        assert sql_lobe.mapper.map_table.call_count == 2

    def test_parser(self, sql_lobe):
        cursor_mock = mock.MagicMock()
        cursor_mock.fetchall.side_effect = [
            [('id', 'integer', 'PK'), ('name', 'varchar', '')],
            [('1',), ('2',)],
            [('John',), ('Jane',)],
        ]
        cursor_mock.fetchone.side_effect = [('id',), ('name',)]
        table = sql_lobe._parser(cursor_mock, 'users')

        assert table.table_name == 'users'
        assert len(table.columns) == 2
        assert table.columns[0]['name'] == 'id'
        assert table.columns[0]['type'] == 'integer'
        assert table.columns[0]['constraint_type'] == 'PK'
        assert table.columns[1]['name'] == 'name'
        assert table.columns[1]['type'] == 'varchar'
        assert table.columns[1]['constraint_type'] == ''
        assert len(table.rows) == 2
        assert table.rows[0]['id'] == '1'
        assert table.rows[0]['name'] == 'John'
        assert table.rows[1]['id'] == '2'
        assert table.rows[1]['name'] == 'Jane'

    def test_insert_atoms(self, sql_lobe):
        atoms = [
            {"_id": "1", "name": "Atom 1"},
            {"_id": "2", "name": "Atom 2"},
            {"_id": "3", "name": "Atom 3"},
        ]
        expected_keys = ['atoms:1', 'atoms:2', 'atoms:3']
        expected_values = [json.dumps(atom) for atom in atoms]

        sql_lobe.redis.set = mock.MagicMock()

        sql_lobe._insert_atoms(atoms)

        assert sql_lobe.redis.set.call_count == len(atoms)
        for key, value in zip(expected_keys, expected_values):
            sql_lobe.redis.set.assert_any_call(key, value)

    def test_retrieve_document(self, sql_lobe):
        handle = "1"
        key = f'atoms:{handle}'
        document = {"_id": "1", "name": "Atom 1"}
        json_document = json.dumps(document)

        sql_lobe.redis.get = mock.MagicMock(return_value=json_document)

        result = sql_lobe._retrieve_document(handle)

        sql_lobe.redis.get.assert_called_once_with(key)
        assert result == document

    def test_retrieve_document_not_found(self, sql_lobe):
        handle = "1"
        key = f'atoms:{handle}'

        sql_lobe.redis.get = mock.MagicMock(return_value=None)

        result = sql_lobe._retrieve_document(handle)

        sql_lobe.redis.get.assert_called_once_with(key)
        assert result is None

    def test_retrieve_document_with_document_link(self, sql_lobe):
        handle = "1"
        document = {'_id': '1', 'name': 'Atom 1', 'link': 'atoms:2', 'targets': ['atoms:2']}
        json_document = json.dumps(document)
        linked_document = {"_id": "2", "name": "Atom 2"}

        sql_lobe.redis.get = mock.MagicMock(
            side_effect=[json_document, json.dumps(linked_document)]
        )
        sql_lobe._is_document_link = mock.MagicMock(return_value=True)
        sql_lobe._get_document_keys = mock.MagicMock(return_value=["atoms:2"])

        result = sql_lobe._retrieve_document(handle)

        sql_lobe._is_document_link.assert_called_once_with(document)
        sql_lobe._get_document_keys.assert_called_once_with(document)
        document["targets"] = ["atoms:2"]
        assert result == document

    def test_retrieve_all_documents(self, sql_lobe):
        sql_lobe.redis.mget = mock.MagicMock(
            return_value=[
                json.dumps({"_id": "1", "name": "Atom 1"}),
                json.dumps({"_id": "2", "name": "Atom 2"}),
                json.dumps({"_id": "3", "name": "Atom 3"}),
            ]
        )

        result = sql_lobe._retrieve_all_documents()
        assert len(result) == 3
        assert result == [
            {"_id": "1", "name": "Atom 1"},
            {"_id": "2", "name": "Atom 2"},
            {"_id": "3", "name": "Atom 3"},
        ]

        result = sql_lobe._retrieve_all_documents(key="_id", value="2")
        assert len(result) == 1
        assert result == [{"_id": "2", "name": "Atom 2"}]

        result = sql_lobe._retrieve_all_documents(key="_id", value=True)
        assert len(result) == 3
        assert result == [
            {"_id": "1", "name": "Atom 1"},
            {"_id": "2", "name": "Atom 2"},
            {"_id": "3", "name": "Atom 3"},
        ]

        result = sql_lobe._retrieve_all_documents(key="_id", value=False)
        assert len(result) == 0
        assert result == []
