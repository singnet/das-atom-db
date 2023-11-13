from unittest import mock

import pytest
import requests_mock

from hyperon_das_atomdb.adapters import ServerDB
from hyperon_das_atomdb.utils.settings import config


class TestServerDB:
    @pytest.fixture()
    def database(self):
        with requests_mock.Mocker() as r_moc:
            r_moc.post('http://0.0.0.0/prod/atomdb', status_code=200)
            db = ServerDB(host='0.0.0.0')
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


class TestServerDBAWSIntegration:
    @pytest.fixture()
    def server(self):
        with mock.patch(
            'hyperon_das_atomdb.adapters.server_db.ServerDB._connect_server',
            return_value=f"http://{config.get('DEFAULT_HOST_AWS_LAMBDA')}/prod/atomdb",
            # return_value=f"http://127.0.0.1:8000/v1/atomdb",
        ):
            return ServerDB(host=config.get('DEFAULT_HOST_AWS_LAMBDA'))

    def test_get_node_handle(self, server):
        ret = server.get_node_handle(node_type='Concept', node_name='human')
        assert ret == server._node_handle('Concept', 'human')

    def test_get_node_name(self, server):
        human_handle = server._node_handle('Concept', 'human')
        ret = server.get_node_name(node_handle=human_handle)
        assert ret == 'human'

    def test_get_node_type(self, server):
        human_handle = server._node_handle('Concept', 'human')
        ret = server.get_node_type(node_handle=human_handle)
        assert ret == 'Concept'

    def test_get_matched_node_name(self, server):
        ret = server.get_matched_node_name(node_type='Concept', substring='ma')
        # names = [server.get_node_name(item) for item in ret]
        assert len(ret) == 3
        # assert set(names) == set(['human', 'animal', 'mammal'])

    def test_get_all_nodes(self, server):
        ret = server.get_all_nodes(node_type='Concept')
        assert len(ret) == 14

    def test_get_link_handle(self, server):
        human_handle = server._node_handle('Concept', 'human')
        monkey_handle = server._node_handle('Concept', 'monkey')
        handle = server._link_handle(
            link_type='Similarity',
            target_handles=[human_handle, monkey_handle],
        )
        ret = server.get_link_handle(
            link_type='Similarity',
            target_handles=[human_handle, monkey_handle],
        )
        assert ret == handle

    def test_get_link_targets(self, server):
        human_handle = server._node_handle('Concept', 'human')
        monkey_handle = server._node_handle('Concept', 'monkey')
        handle = server._link_handle(
            link_type='Similarity',
            target_handles=[human_handle, monkey_handle],
        )
        ret = server.get_link_targets(link_handle=handle)
        assert len(ret) == 2
        assert set(ret) == set([human_handle, monkey_handle])

    def test_is_ordered(self, server):
        human_handle = server._node_handle('Concept', 'human')
        monkey_handle = server._node_handle('Concept', 'monkey')
        handle = server._link_handle(
            link_type='Similarity',
            target_handles=[human_handle, monkey_handle],
        )
        ret = server.is_ordered(link_handle=handle)
        assert ret is True

    def test_get_link_type(self, server):
        human_handle = server._node_handle('Concept', 'human')
        monkey_handle = server._node_handle('Concept', 'monkey')
        handle = server._link_handle(
            link_type='Similarity',
            target_handles=[human_handle, monkey_handle],
        )
        ret = server.get_link_type(link_handle=handle)
        assert ret == 'Similarity'

    def test_get_matched_links(self, server):
        ret = server.get_matched_links(
            link_type='Similarity', target_handles=['*', '*']
        )
        assert len(ret) == 14

    def test_get_matched_type_template(self, server):
        ret = server.get_matched_type_template(
            template=['Similarity', 'Concept', 'Concept']
        )
        assert len(ret) == 14

    def test_get_matched_type(self, server):
        ret = server.get_matched_type(link_type='Similarity')
        assert len(ret) == 14

    def test_get_atom_as_dict(self, server):
        human_handle = server._node_handle('Concept', 'human')
        monkey_handle = server._node_handle('Concept', 'monkey')
        link_handle = server._link_handle(
            link_type='Similarity',
            target_handles=[human_handle, monkey_handle],
        )
        ret = server.get_atom_as_dict(handle=human_handle)
        assert ret == {
            'handle': human_handle,
            'type': 'Concept',
            'name': 'human',
        }

        ret = server.get_atom_as_dict(handle=link_handle, arity=2)
        assert ret == {
            'handle': link_handle,
            'type': 'Similarity',
            'template': ['Similarity', 'Concept', 'Concept'],
            'targets': [human_handle, monkey_handle],
        }

    def test_get_atom_as_deep_representation(self, server):
        human_handle = server._node_handle('Concept', 'human')
        monkey_handle = server._node_handle('Concept', 'monkey')
        link_handle = server._link_handle(
            link_type='Similarity',
            target_handles=[human_handle, monkey_handle],
        )
        ret = server.get_atom_as_deep_representation(handle=human_handle)
        assert ret == {
            'type': 'Concept',
            'name': 'human',
        }

        ret = server.get_atom_as_deep_representation(
            handle=link_handle, arity=2
        )
        assert ret == {
            'type': 'Similarity',
            'targets': [
                {'type': 'Concept', 'name': 'human'},
                {'type': 'Concept', 'name': 'monkey'},
            ],
        }

    def test_count_atoms(self, server):
        ret = server.count_atoms()
        assert ret[0] == 14
        assert ret[1] == 26


class TestServerDBVultrIntegration:
    @pytest.fixture()
    def server(self):
        with mock.patch(
            'hyperon_das_atomdb.adapters.server_db.ServerDB._connect_server',
            return_value=f"http://{config.get('DEFAULT_HOST_OPENFAAS')}:{config.get('DEFAULT_PORT_OPENFAAS')}/function/atomdb",
        ):
            return ServerDB(host=config.get('DEFAULT_HOST_AWS_LAMBDA'))

    def test_get_node_handle(self, server):
        ret = server.get_node_handle(node_type='Concept', node_name='human')
        assert ret == server._node_handle('Concept', 'human')

    def test_get_node_name(self, server):
        human_handle = server._node_handle('Concept', 'human')
        ret = server.get_node_name(node_handle=human_handle)
        assert ret == 'human'

    def test_get_node_type(self, server):
        human_handle = server._node_handle('Concept', 'human')
        ret = server.get_node_type(node_handle=human_handle)
        assert ret == 'Concept'

    def test_get_matched_node_name(self, server):
        ret = server.get_matched_node_name(node_type='Concept', substring='ma')
        # names = [server.get_node_name(item) for item in ret]
        assert len(ret) == 3
        # assert set(names) == set(['human', 'animal', 'mammal'])

    def test_get_all_nodes(self, server):
        ret = server.get_all_nodes(node_type='Concept')
        assert len(ret) == 14

    def test_get_link_handle(self, server):
        human_handle = server._node_handle('Concept', 'human')
        monkey_handle = server._node_handle('Concept', 'monkey')
        handle = server._link_handle(
            link_type='Similarity',
            target_handles=[human_handle, monkey_handle],
        )
        ret = server.get_link_handle(
            link_type='Similarity',
            target_handles=[human_handle, monkey_handle],
        )
        assert ret == handle

    def test_get_link_targets(self, server):
        human_handle = server._node_handle('Concept', 'human')
        monkey_handle = server._node_handle('Concept', 'monkey')
        handle = server._link_handle(
            link_type='Similarity',
            target_handles=[human_handle, monkey_handle],
        )
        ret = server.get_link_targets(link_handle=handle)
        assert len(ret) == 2
        assert set(ret) == set([human_handle, monkey_handle])

    def test_is_ordered(self, server):
        human_handle = server._node_handle('Concept', 'human')
        monkey_handle = server._node_handle('Concept', 'monkey')
        handle = server._link_handle(
            link_type='Similarity',
            target_handles=[human_handle, monkey_handle],
        )
        ret = server.is_ordered(link_handle=handle)
        assert ret is True

    def test_get_link_type(self, server):
        human_handle = server._node_handle('Concept', 'human')
        monkey_handle = server._node_handle('Concept', 'monkey')
        handle = server._link_handle(
            link_type='Similarity',
            target_handles=[human_handle, monkey_handle],
        )
        ret = server.get_link_type(link_handle=handle)
        assert ret == 'Similarity'

    def test_get_matched_links(self, server):
        ret = server.get_matched_links(
            link_type='Similarity', target_handles=['*', '*']
        )
        assert len(ret) == 14

    def test_get_matched_type_template(self, server):
        ret = server.get_matched_type_template(
            template=['Similarity', 'Concept', 'Concept']
        )
        assert len(ret) == 14

    def test_get_matched_type(self, server):
        ret = server.get_matched_type(link_type='Similarity')
        assert len(ret) == 14

    def test_get_atom_as_dict(self, server):
        human_handle = server._node_handle('Concept', 'human')
        monkey_handle = server._node_handle('Concept', 'monkey')
        link_handle = server._link_handle(
            link_type='Similarity',
            target_handles=[human_handle, monkey_handle],
        )
        ret = server.get_atom_as_dict(handle=human_handle)
        assert ret == {
            'handle': human_handle,
            'type': 'Concept',
            'name': 'human',
        }

        ret = server.get_atom_as_dict(handle=link_handle, arity=2)
        assert ret == {
            'handle': link_handle,
            'type': 'Similarity',
            'template': ['Similarity', 'Concept', 'Concept'],
            'targets': [human_handle, monkey_handle],
        }

    def test_get_atom_as_deep_representation(self, server):
        human_handle = server._node_handle('Concept', 'human')
        monkey_handle = server._node_handle('Concept', 'monkey')
        link_handle = server._link_handle(
            link_type='Similarity',
            target_handles=[human_handle, monkey_handle],
        )
        ret = server.get_atom_as_deep_representation(handle=human_handle)
        assert ret == {
            'type': 'Concept',
            'name': 'human',
        }

        ret = server.get_atom_as_deep_representation(
            handle=link_handle, arity=2
        )
        assert ret == {
            'type': 'Similarity',
            'targets': [
                {'type': 'Concept', 'name': 'human'},
                {'type': 'Concept', 'name': 'monkey'},
            ],
        }

    def test_count_atoms(self, server):
        ret = server.count_atoms()
        assert ret[0] == 14
        assert ret[1] == 26
