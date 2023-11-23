from unittest import mock

import pytest

from hyperon_das_atomdb.adapters import ServerDB
from hyperon_das_atomdb.utils.settings import config


class TestServerDBAWSIntegration:
    @pytest.fixture()
    def server(self):
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
        return ServerDB(host=config.get('DEFAULT_HOST_OPENFAAS'))

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
