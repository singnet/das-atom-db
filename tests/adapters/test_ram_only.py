import pytest

from hyperon_das_atomdb.adapters.ram_only import InMemoryDB
from hyperon_das_atomdb.exceptions import (
    AddLinkException,
    AddNodeException,
    LinkDoesNotExistException,
    NodeDoesNotExistException,
)
from hyperon_das_atomdb.utils.expression_hasher import ExpressionHasher


class TestInMemoryDB:
    @pytest.fixture()
    def all_nodes(self):
        return [
            {'type': 'Concept', 'name': 'human'},
            {'type': 'Concept', 'name': 'monkey'},
            {'type': 'Concept', 'name': 'chimp'},
            {'type': 'Concept', 'name': 'snake'},
            {'type': 'Concept', 'name': 'earthworm'},
            {'type': 'Concept', 'name': 'rhino'},
            {'type': 'Concept', 'name': 'triceratops'},
            {'type': 'Concept', 'name': 'vine'},
            {'type': 'Concept', 'name': 'ent'},
            {'type': 'Concept', 'name': 'mammal'},
            {'type': 'Concept', 'name': 'animal'},
            {'type': 'Concept', 'name': 'reptile'},
            {'type': 'Concept', 'name': 'dinosaur'},
            {'type': 'Concept', 'name': 'plant'},
        ]

    @pytest.fixture()
    def all_links(self):
        return [
            {
                'type': 'Similarity',
                'targets': [
                    {'type': 'Concept', 'name': 'human'},
                    {'type': 'Concept', 'name': 'monkey'},
                ],
            },
            {
                'type': 'Similarity',
                'targets': [
                    {'type': 'Concept', 'name': 'human'},
                    {'type': 'Concept', 'name': 'chimp'},
                ],
            },
            {
                'type': 'Similarity',
                'targets': [
                    {'type': 'Concept', 'name': 'chimp'},
                    {'type': 'Concept', 'name': 'monkey'},
                ],
            },
            {
                'type': 'Similarity',
                'targets': [
                    {'type': 'Concept', 'name': 'snake'},
                    {'type': 'Concept', 'name': 'earthworm'},
                ],
            },
            {
                'type': 'Similarity',
                'targets': [
                    {'type': 'Concept', 'name': 'rhino'},
                    {'type': 'Concept', 'name': 'triceratops'},
                ],
            },
            {
                'type': 'Similarity',
                'targets': [
                    {'type': 'Concept', 'name': 'snake'},
                    {'type': 'Concept', 'name': 'vine'},
                ],
            },
            {
                'type': 'Similarity',
                'targets': [
                    {'type': 'Concept', 'name': 'human'},
                    {'type': 'Concept', 'name': 'ent'},
                ],
            },
            {
                'type': 'Inheritance',
                'targets': [
                    {'type': 'Concept', 'name': 'human'},
                    {'type': 'Concept', 'name': 'mammal'},
                ],
            },
            {
                'type': 'Inheritance',
                'targets': [
                    {'type': 'Concept', 'name': 'monkey'},
                    {'type': 'Concept', 'name': 'mammal'},
                ],
            },
            {
                'type': 'Inheritance',
                'targets': [
                    {'type': 'Concept', 'name': 'chimp'},
                    {'type': 'Concept', 'name': 'mammal'},
                ],
            },
            {
                'type': 'Inheritance',
                'targets': [
                    {'type': 'Concept', 'name': 'mammal'},
                    {'type': 'Concept', 'name': 'animal'},
                ],
            },
            {
                'type': 'Inheritance',
                'targets': [
                    {'type': 'Concept', 'name': 'reptile'},
                    {'type': 'Concept', 'name': 'animal'},
                ],
            },
            {
                'type': 'Inheritance',
                'targets': [
                    {'type': 'Concept', 'name': 'snake'},
                    {'type': 'Concept', 'name': 'reptile'},
                ],
            },
            {
                'type': 'Inheritance',
                'targets': [
                    {'type': 'Concept', 'name': 'dinosaur'},
                    {'type': 'Concept', 'name': 'reptile'},
                ],
            },
            {
                'type': 'Inheritance',
                'targets': [
                    {'type': 'Concept', 'name': 'triceratops'},
                    {'type': 'Concept', 'name': 'dinosaur'},
                ],
            },
            {
                'type': 'Inheritance',
                'targets': [
                    {'type': 'Concept', 'name': 'earthworm'},
                    {'type': 'Concept', 'name': 'animal'},
                ],
            },
            {
                'type': 'Inheritance',
                'targets': [
                    {'type': 'Concept', 'name': 'rhino'},
                    {'type': 'Concept', 'name': 'mammal'},
                ],
            },
            {
                'type': 'Inheritance',
                'targets': [
                    {'type': 'Concept', 'name': 'vine'},
                    {'type': 'Concept', 'name': 'plant'},
                ],
            },
            {
                'type': 'Inheritance',
                'targets': [
                    {'type': 'Concept', 'name': 'ent'},
                    {'type': 'Concept', 'name': 'plant'},
                ],
            },
            {
                'type': 'Similarity',
                'targets': [
                    {'type': 'Concept', 'name': 'monkey'},
                    {'type': 'Concept', 'name': 'human'},
                ],
            },
            {
                'type': 'Similarity',
                'targets': [
                    {'type': 'Concept', 'name': 'chimp'},
                    {'type': 'Concept', 'name': 'human'},
                ],
            },
            {
                'type': 'Similarity',
                'targets': [
                    {'type': 'Concept', 'name': 'monkey'},
                    {'type': 'Concept', 'name': 'chimp'},
                ],
            },
            {
                'type': 'Similarity',
                'targets': [
                    {'type': 'Concept', 'name': 'earthworm'},
                    {'type': 'Concept', 'name': 'snake'},
                ],
            },
            {
                'type': 'Similarity',
                'targets': [
                    {'type': 'Concept', 'name': 'triceratops'},
                    {'type': 'Concept', 'name': 'rhino'},
                ],
            },
            {
                'type': 'Similarity',
                'targets': [
                    {'type': 'Concept', 'name': 'vine'},
                    {'type': 'Concept', 'name': 'snake'},
                ],
            },
            {
                'type': 'Similarity',
                'targets': [
                    {'type': 'Concept', 'name': 'ent'},
                    {'type': 'Concept', 'name': 'human'},
                ],
            },
        ]

    @pytest.fixture()
    def database(self, all_nodes, all_links):
        db = InMemoryDB()
        for node in all_nodes:
            db.add_node(node)
        for link in all_links:
            db.add_link(link)
        return db

    def test_get_node_handle(self, database: InMemoryDB):
        actual = database.get_node_handle(
            node_type="Concept", node_name="human"
        )
        expected = ExpressionHasher.terminal_hash('Concept', 'human')
        assert expected == actual

    def test_get_node_handle_not_exist(self, database: InMemoryDB):
        with pytest.raises(NodeDoesNotExistException) as exc_info:
            database.get_node_handle(
                node_type="Concept-Fake", node_name="fake"
            )
        assert exc_info.type is NodeDoesNotExistException
        assert exc_info.value.args[0] == "This node does not exist"

    def test_get_link_handle(self, database: InMemoryDB):
        human = database.get_node_handle('Concept', 'human')
        chimp = database.get_node_handle('Concept', 'chimp')
        actual = database.get_link_handle(
            link_type='Similarity', target_handles=[human, chimp]
        )
        expected = 'b5459e299a5c5e8662c427f7e01b3bf1'
        assert expected == actual

    def test_get_link_handle_not_exist(self, database: InMemoryDB):
        with pytest.raises(LinkDoesNotExistException) as exc_info:
            database.get_link_handle(
                link_type='Singularity', target_handles=['Fake-1', 'Fake-2']
            )
        assert exc_info.type is LinkDoesNotExistException
        assert exc_info.value.args[0] == "This link does not exist"

    def test_node_exists_true(self, database: InMemoryDB):
        ret = database.node_exists(node_type="Concept", node_name="human")
        assert ret is True

    def test_node_exists_false(self, database: InMemoryDB):
        ret = database.node_exists(
            node_type="Concept-Fake", node_name="human-fake"
        )
        assert ret is False

    def test_link_exists_true(self, database: InMemoryDB):
        human = database.get_node_handle('Concept', 'human')
        monkey = database.get_node_handle('Concept', 'monkey')
        ret = database.link_exists(
            link_type="Similarity", target_handles=[human, monkey]
        )
        assert ret is True

    def test_link_exists_false(self, database: InMemoryDB):
        ret = database.link_exists(
            link_type="Concept-Fake", target_handles=['Fake1, Fake2']
        )
        assert ret is False

    def test_get_link_targets(self, database: InMemoryDB):
        human = database.get_node_handle('Concept', 'human')
        mammal = database.get_node_handle('Concept', 'mammal')
        handle = database.get_link_handle('Inheritance', [human, mammal])
        ret = database.get_link_targets(handle)
        assert ret is not None

    def test_get_link_targets_invalid(self, database: InMemoryDB):
        with pytest.raises(LinkDoesNotExistException) as exc_info:
            database.get_link_targets(f'link_handle_Fake')
        assert exc_info.type is LinkDoesNotExistException
        assert exc_info.value.args[0] == "This link does not exist"

    def test_is_ordered_true(self, database: InMemoryDB):
        human = database.get_node_handle('Concept', 'human')
        mammal = database.get_node_handle('Concept', 'mammal')
        handle = database.get_link_handle('Inheritance', [human, mammal])
        ret = database.is_ordered(handle)
        assert ret is True

    def test_is_ordered_false(self, database: InMemoryDB):
        ret = database.is_ordered('handle_123')
        assert ret is False

    def test_get_matched_links_without_wildcard(self, database):
        link_type = 'Similarity'
        human = ExpressionHasher.terminal_hash('Concept', 'human')
        monkey = ExpressionHasher.terminal_hash('Concept', 'monkey')
        link_handle = database.get_link_handle(link_type, [human, monkey])
        expected = [link_handle]
        actual = database.get_matched_links(link_type, [human, monkey])

        assert expected == actual

    def test_get_matched_links_link_equal_wildcard(self, database: InMemoryDB):
        human = ExpressionHasher.terminal_hash('Concept', 'human')
        chimp = ExpressionHasher.terminal_hash('Concept', 'chimp')
        expected = [
            (
                "b5459e299a5c5e8662c427f7e01b3bf1",
                (
                    "af12f10f9ae2002a1607ba0b47ba8407",
                    "5b34c54bee150c04f9fa584b899dc030",
                ),
            )
        ]
        actual = database.get_matched_links('*', [human, chimp])

        assert expected == actual

    def test_get_matched_links_link_diff_wildcard(self, database: InMemoryDB):
        link_type = 'Similarity'
        chimp = ExpressionHasher.terminal_hash('Concept', 'chimp')
        expected = [
            (
                'b5459e299a5c5e8662c427f7e01b3bf1',
                (
                    'af12f10f9ae2002a1607ba0b47ba8407',
                    '5b34c54bee150c04f9fa584b899dc030',
                ),
            ),
            (
                '31535ddf214f5b239d3b517823cb8144',
                (
                    '1cdffc6b0b89ff41d68bec237481d1e1',
                    '5b34c54bee150c04f9fa584b899dc030',
                ),
            ),
        ]

        actual = database.get_matched_links(link_type, ['*', chimp])
        assert expected == actual

    def test_get_matched_links_link_does_not_exist(self, database: InMemoryDB):
        link_type = 'Similarity-Fake'
        chimp = ExpressionHasher.terminal_hash('Concept', 'chimp')
        with pytest.raises(LinkDoesNotExistException) as exc_info:
            database.get_matched_links(link_type, [chimp, chimp])
        assert exc_info.type is LinkDoesNotExistException
        assert exc_info.value.args[0] == "This link does not exist"

    def test_get_all_nodes(self, database):
        ret = database.get_all_nodes('Concept')
        assert len(ret) == 14
        ret = database.get_all_nodes('Concept', True)
        assert len(ret) == 14
        ret = database.get_all_nodes('ConceptFake')
        assert len(ret) == 0

    def test_get_matched_type_template(self, database: InMemoryDB):
        v1 = database.get_matched_type_template(
            ['Inheritance', 'Concept', 'Concept']
        )
        v2 = database.get_matched_type_template(
            ['Similarity', 'Concept', 'Concept']
        )
        v3 = database.get_matched_type_template(
            ['Inheritance', 'Concept', 'blah']
        )
        v4 = database.get_matched_type_template(
            ['Similarity', 'blah', 'Concept']
        )
        v5 = database.get_matched_links('Inheritance', ['*', '*'])
        v6 = database.get_matched_links('Similarity', ['*', '*'])
        assert len(v1) == 12
        assert len(v2) == 14
        assert len(v3) == 0
        assert len(v4) == 0
        assert v1 == v5
        assert v2 == v6

    def test_get_matched_type(self, database: InMemoryDB):
        inheritance = database.get_matched_type('Inheritance')
        similarity = database.get_matched_type('Similarity')
        assert len(inheritance) == 12
        assert len(similarity) == 14

    def test_get_node_name(self, database):
        handle = database.get_node_handle('Concept', 'monkey')
        db_name = database.get_node_name(handle)

        assert db_name == 'monkey'

    def test_get_node_name_error(self, database):
        with pytest.raises(NodeDoesNotExistException) as exc_info:
            database.get_node_name('handle-test')
        assert exc_info.type is NodeDoesNotExistException
        assert exc_info.value.args[0] == "This node does not exist"

    def test_get_node_type(self, database):
        handle = database.get_node_handle('Concept', 'monkey')
        db_type = database.get_node_type(handle)

        assert db_type == 'Concept'

    def test_get_node_type_error(self, database):
        with pytest.raises(NodeDoesNotExistException) as exc_info:
            database.get_node_type('handle-test')
        assert exc_info.type is NodeDoesNotExistException
        assert exc_info.value.args[0] == "This node does not exist"

    def test_get_matched_node_name(self, database: InMemoryDB):
        expected = sorted(
            [
                database.get_node_handle('Concept', 'human'),
                database.get_node_handle('Concept', 'mammal'),
                database.get_node_handle('Concept', 'animal'),
            ]
        )
        actual = sorted(database.get_matched_node_name('Concept', 'ma'))

        assert expected == actual
        assert sorted(database.get_matched_node_name('blah', 'Concept')) == []
        assert sorted(database.get_matched_node_name('Concept', 'blah')) == []

    def test_add_node_without_type_parameter(self, database: InMemoryDB):
        with pytest.raises(AddNodeException) as exc_info:
            database.add_node({'color': 'red', 'name': 'car'})
        assert exc_info.type is AddNodeException
        assert (
            exc_info.value.args[0]
            == 'The "name" and "type" fields must be sent'
        )

    def test_add_node_without_name_parameter(self, database: InMemoryDB):
        with pytest.raises(AddNodeException) as exc_info:
            database.add_node({'type': 'Concept', 'color': 'red'})
        assert exc_info.type is AddNodeException
        assert (
            exc_info.value.args[0]
            == 'The "name" and "type" fields must be sent'
        )

    def test_add_node(self, database: InMemoryDB):
        assert len(database.get_all_nodes('Concept')) == 14
        database.add_node({'type': 'Concept', 'name': 'car'})
        assert len(database.get_all_nodes('Concept')) == 15
        node_handle = database.get_node_handle('Concept', 'car')
        node_name = database.get_node_name(node_handle)
        assert node_name == 'car'

    def test_add_link_without_type_parameter(self, database: InMemoryDB):
        with pytest.raises(AddLinkException) as exc_info:
            database.add_link(
                {
                    'targets': [
                        {'type': 'Concept', 'name': 'human'},
                        {'type': 'Concept', 'name': 'monkey'},
                    ],
                    'quantity': 2,
                }
            )
        assert exc_info.type is AddLinkException
        assert (
            exc_info.value.args[0]
            == 'The "type" and "targets" fields must be sent'
        )

    def test_add_link_without_targets_parameter(self, database: InMemoryDB):
        with pytest.raises(AddLinkException) as exc_info:
            database.add_link({'source': 'fake', 'type': 'Similarity'})
        assert exc_info.type is AddLinkException
        assert (
            exc_info.value.args[0]
            == 'The "type" and "targets" fields must be sent'
        )

    def test_add_nested_links(self, database: InMemoryDB):
        assert len(database.get_matched_type('Evaluation')) == 0

        database.add_link(
            {
                'type': 'Evaluation',
                'targets': [
                    {'type': 'Predicate', 'name': 'Predicate:has_name'},
                    {
                        'type': 'Evaluation',
                        'targets': [
                            {
                                'type': 'Predicate',
                                'name': 'Predicate:has_name',
                            },
                            {
                                'targets': [
                                    {
                                        'type': 'Reactome',
                                        'name': 'Reactome:R-HSA-164843',
                                    },
                                    {
                                        'type': 'Concept',
                                        'name': 'Concept:2-LTR circle formation',
                                    },
                                ],
                                'type': 'Set',
                            },
                        ],
                    },
                ],
            }
        )

        assert len(database.get_matched_type('Evaluation')) == 2
