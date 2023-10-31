from unittest import mock

import pytest
import requests_mock

from hyperon_das_atomdb.adapters import ServerDB
from hyperon_das_atomdb.exceptions import (
    LinkDoesNotExistException,
    NodeDoesNotExistException,
)
from hyperon_das_atomdb.utils.expression_hasher import ExpressionHasher


class TestServerDB:
    @pytest.fixture()
    def database(self):
        with requests_mock.Mocker() as r_moc:
            r_moc.post('http://0.0.0.0:8080/function/atomdb', status_code=200)
            db = ServerDB(ip_address='0.0.0.0')
        return db

    def test_get_node_handle(self, requests_mock, database):
        handle = "<Concept human>"
        requests_mock.post(
            database.url, text='\n{"message":"<Concept human>"}\n'
        )
        node_type = 'Concept'
        node_name = 'human'
        resp = database.get_node_handle(node_type, node_name)
        assert resp['message'] == handle

    def test_get_node_name(self, requests_mock, database):
        requests_mock.post(database.url, text='\n{"message":"human"}\n')
        node_type = 'Concept'
        node_name = 'human'
        resp = database.get_node_handle(node_type, node_name)
        assert resp['message'] == "human"

    def test_get_node_type(self, requests_mock, database):
        requests_mock.post(database.url, text='\n{"message":"Concept"}\n')
        resp_node = database.get_node_type("<Concept monkey>")
        assert 'Concept' == resp_node['message']

    def test_get_matched_node_name(self, requests_mock, database):
        requests_mock.post(
            database.url,
            text="\n{'message':['<Concept human>', '<Concept mammal>','<Concept animal>']}\n",
        )
        expected = sorted(
            ['<Concept human>', '<Concept mammal>', '<Concept animal>']
        )
        actual = database.get_matched_node_name('Concept', 'ma')
        assert expected == sorted(actual['message'])

    def test_get_all_nodes(self, requests_mock, database):
        requests_mock.post(
            database.url,
            text="\n{'message':['<Concept human>', '<Concept mammal>','<Concept animal>']}\n",
        )
        ret = database.get_all_nodes('Concept')
        assert len(ret['message']) == 3

    def test_node_exists_true(self, requests_mock, database: ServerDB):
        requests_mock.post(database.url, text='\n{"message":"OK"}\n')
        node_type = 'Concept-fake'
        node_name = 'human-fake'
        resp = database.node_exists(node_type, node_name)
        assert resp is True

    def test_node_exists_false(self, requests_mock, database: ServerDB):
        requests_mock.post(database.url, text='\n{"error":"OK"}\n')
        node_type = 'Concept-fake'
        node_name = 'human-fake'
        resp = database.node_exists(node_type, node_name)
        assert resp is False

    def test_get_link_handle(self, requests_mock, database):
        requests_mock.post(database.url, text='\n{"message":"OK"}\n')
        resp = database.get_link_handle(
            link_type='Similarity',
            target_handles=['<Concept human>', '<Concept chimp>'],
        )
        assert resp is not None
