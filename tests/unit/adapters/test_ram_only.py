import pytest

from hyperon_das_atomdb import AtomDB
from hyperon_das_atomdb.adapters.ram_only import InMemoryDB
from hyperon_das_atomdb.exceptions import AddLinkException, AddNodeException, AtomDoesNotExist
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
        actual = database.get_node_handle(node_type="Concept", node_name="human")
        expected = ExpressionHasher.terminal_hash('Concept', 'human')
        assert expected == actual

    def test_get_node_handle_not_exist(self, database: InMemoryDB):
        with pytest.raises(AtomDoesNotExist) as exc_info:
            database.get_node_handle(node_type="Concept-Fake", node_name="fake")
        assert exc_info.type is AtomDoesNotExist
        assert exc_info.value.args[0] == "Nonexistent atom"

    def test_get_link_handle(self, database: InMemoryDB):
        human = database.get_node_handle('Concept', 'human')
        chimp = database.get_node_handle('Concept', 'chimp')
        actual = database.get_link_handle(link_type='Similarity', target_handles=[human, chimp])
        expected = 'b5459e299a5c5e8662c427f7e01b3bf1'
        assert expected == actual

    def test_get_link_handle_not_exist(self, database: InMemoryDB):
        with pytest.raises(AtomDoesNotExist) as exc_info:
            database.get_link_handle(link_type='Singularity', target_handles=['Fake-1', 'Fake-2'])
        assert exc_info.type is AtomDoesNotExist
        assert exc_info.value.args[0] == "Nonexistent atom"

    def test_node_exists_true(self, database: InMemoryDB):
        ret = database.node_exists(node_type="Concept", node_name="human")
        assert ret is True

    def test_node_exists_false(self, database: InMemoryDB):
        ret = database.node_exists(node_type="Concept-Fake", node_name="human-fake")
        assert ret is False

    def test_link_exists_true(self, database: InMemoryDB):
        human = database.get_node_handle('Concept', 'human')
        monkey = database.get_node_handle('Concept', 'monkey')
        ret = database.link_exists(link_type="Similarity", target_handles=[human, monkey])
        assert ret is True

    def test_link_exists_false(self, database: InMemoryDB):
        ret = database.link_exists(link_type="Concept-Fake", target_handles=['Fake1, Fake2'])
        assert ret is False

    def test_get_link_targets(self, database: InMemoryDB):
        human = database.get_node_handle('Concept', 'human')
        mammal = database.get_node_handle('Concept', 'mammal')
        handle = database.get_link_handle('Inheritance', [human, mammal])
        ret = database.get_link_targets(handle)
        assert ret is not None

    def test_get_link_targets_invalid(self, database: InMemoryDB):
        with pytest.raises(AtomDoesNotExist) as exc_info:
            database.get_link_targets('link_handle_Fake')
        assert exc_info.type is AtomDoesNotExist
        assert exc_info.value.args[0] == "Nonexistent atom"

    def test_is_ordered_true(self, database: InMemoryDB):
        human = database.get_node_handle('Concept', 'human')
        mammal = database.get_node_handle('Concept', 'mammal')
        handle = database.get_link_handle('Inheritance', [human, mammal])
        ret = database.is_ordered(handle)
        assert ret is True

    def test_is_ordered_false(self, database: InMemoryDB):
        with pytest.raises(AtomDoesNotExist) as exc_info:
            database.is_ordered('handle_123')

        assert exc_info.type is AtomDoesNotExist
        assert exc_info.value.args[0] == "Nonexistent atom"

    def test_get_matched_links_without_wildcard(self, database):
        link_type = 'Similarity'
        human = ExpressionHasher.terminal_hash('Concept', 'human')
        monkey = ExpressionHasher.terminal_hash('Concept', 'monkey')
        link_handle = database.get_link_handle(link_type, [human, monkey])
        expected = (None, [link_handle])
        actual = database.get_matched_links(link_type, [human, monkey])

        assert expected == actual

    def test_get_matched_links_link_equal_wildcard(self, database: InMemoryDB):
        human = ExpressionHasher.terminal_hash('Concept', 'human')
        chimp = ExpressionHasher.terminal_hash('Concept', 'chimp')
        expected = (
            None,
            [
                (
                    "b5459e299a5c5e8662c427f7e01b3bf1",
                    (
                        "af12f10f9ae2002a1607ba0b47ba8407",
                        "5b34c54bee150c04f9fa584b899dc030",
                    ),
                )
            ],
        )
        actual = database.get_matched_links('*', [human, chimp])

        assert expected == actual

    def test_get_matched_links_link_diff_wildcard(self, database: InMemoryDB):
        link_type = 'Similarity'
        chimp = ExpressionHasher.terminal_hash('Concept', 'chimp')
        expected = (
            None,
            sorted(
                [
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
                ],
                key=lambda i: i[0],
            ),
        )

        cursor, actual = database.get_matched_links(link_type, ['*', chimp])
        assert expected == (cursor, sorted(actual, key=lambda i: i[0]))

    def test_get_matched_links_link_does_not_exist(self, database: InMemoryDB):
        link_type = 'Similarity-Fake'
        chimp = ExpressionHasher.terminal_hash('Concept', 'chimp')
        with pytest.raises(AtomDoesNotExist) as exc_info:
            database.get_matched_links(link_type, [chimp, chimp])
        assert exc_info.type is AtomDoesNotExist
        assert exc_info.value.args[0] == "Nonexistent atom"

    def test_get_matched_links_toplevel_only(self, database: InMemoryDB):
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
        expected = (
            None,
            [
                (
                    '661fb5a7c90faabfeada7e1f63805fc0',
                    (
                        'a912032ece1826e55fa583dcaacdc4a9',
                        '260e118be658feeeb612dcd56d270d77',
                    ),
                )
            ],
        )
        cursor, actual = database.get_matched_links('Evaluation', ['*', '*'], toplevel_only=True)

        assert expected == (cursor, actual)
        assert len(actual) == 1

    def test_get_matched_links_wrong_parameter(self, database: InMemoryDB):
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
        cursor, actual = database.get_matched_links('Evaluation', ['*', '*'], toplevel=True)
        assert cursor is None
        assert len(actual) == 2

    @pytest.mark.skip(
        reason=(
            "get_matched_links does not support nested lists in the target_handles parameter. "
            "See: https://github.com/singnet/das-atom-db/issues/191"
        )
    )
    def test_get_matched_links_nested_lists(self, database: InMemoryDB):
        chimp = ExpressionHasher.terminal_hash('Concept', 'chimp')
        human = ExpressionHasher.terminal_hash('Concept', 'human')
        monkey = ExpressionHasher.terminal_hash('Concept', 'monkey')
        database.add_link(
            {
                'type': 'Nearness',
                'targets': [
                    {'type': 'Concept', 'name': 'chimp'},
                    {'type': 'Concept', 'name': 'human'},
                ],
            }
        )
        nearness_chimp_human_handle = database.get_link_handle('Nearness', [chimp, human])
        database.add_link(
            {
                'type': 'Nearness',
                'targets': [
                    {'type': 'Concept', 'name': 'chimp'},
                    {'type': 'Concept', 'name': 'monkey'},
                ],
            }
        )
        nearness_chimp_monkey_handle = database.get_link_handle('Nearness', [chimp, monkey])
        database.add_link(
            {
                'type': 'Connectivity',
                'targets': [
                    {
                        'type': 'Nearness',
                        'targets': [
                            {'type': 'Concept', 'name': 'chimp'},
                            {'type': 'Concept', 'name': 'human'},
                        ],
                    },
                    {
                        'type': 'Nearness',
                        'targets': [
                            {'type': 'Concept', 'name': 'chimp'},
                            {'type': 'Concept', 'name': 'monkey'},
                        ],
                    },
                ],
            }
        )
        target_handles = [
            [nearness_chimp_human_handle, nearness_chimp_monkey_handle],
        ]
        links = database.get_matched_links('Connectivity', target_handles)
        assert len(links) == 1

    def test_get_all_nodes(self, database):
        ret = database.get_all_nodes('Concept')
        assert len(ret) == 14
        ret = database.get_all_nodes('Concept', True)
        assert len(ret) == 14
        ret = database.get_all_nodes('ConceptFake')
        assert len(ret) == 0

    def test_get_matched_type_template(self, database: InMemoryDB):
        cursors = [-1] * 6
        cursors[0], v1 = database.get_matched_type_template(['Inheritance', 'Concept', 'Concept'])
        cursors[1], v2 = database.get_matched_type_template(['Similarity', 'Concept', 'Concept'])
        cursors[2], v3 = database.get_matched_type_template(['Inheritance', 'Concept', 'blah'])
        cursors[3], v4 = database.get_matched_type_template(['Similarity', 'blah', 'Concept'])
        cursors[4], v5 = database.get_matched_links('Inheritance', ['*', '*'])
        cursors[5], v6 = database.get_matched_links('Similarity', ['*', '*'])
        assert all(c is None for c in cursors)
        assert len(v1) == 12
        assert len(v2) == 14
        assert len(v3) == 0
        assert len(v4) == 0
        assert v1 == v5
        assert v2 == v6

    def test_get_matched_type_template_toplevel_only(self, database: InMemoryDB):
        database.add_link(
            {
                'type': 'Evaluation',
                'targets': [
                    {'type': 'Predicate', 'name': 'Predicate:has_name'},
                    {
                        'type': 'Evaluation',
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
                    },
                ],
            }
        )

        cursor, ret = database.get_matched_type_template(
            ['Evaluation', 'Reactome', 'Concept'], toplevel_only=True
        )
        assert cursor is None
        assert len(ret) == 0

        cursor, ret = database.get_matched_type_template(
            ['Evaluation', 'Reactome', 'Concept'], toplevel_only=False
        )
        assert cursor is None
        assert len(ret) == 1

    def test_get_matched_type(self, database: InMemoryDB):
        cursors = [-1] * 2
        cursors[0], inheritance = database.get_matched_type('Inheritance')
        cursors[1], similarity = database.get_matched_type('Similarity')
        assert all(c is None for c in cursors)
        assert len(inheritance) == 12
        assert len(similarity) == 14

    def test_get_matched_type_toplevel_only(self, database: InMemoryDB):
        database.add_link(
            {
                'type': 'EvaluationLink',
                'targets': [
                    {'type': 'Predicate', 'name': 'Predicate:has_name'},
                    {
                        'type': 'EvaluationLink',
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
                    },
                ],
            }
        )
        cursor, ret = database.get_matched_type('EvaluationLink')
        assert cursor is None
        assert len(ret) == 2

        cursor, ret = database.get_matched_type('EvaluationLink', toplevel_only=True)
        assert cursor is None
        assert len(ret) == 1

    def test_get_node_name(self, database):
        handle = database.get_node_handle('Concept', 'monkey')
        db_name = database.get_node_name(handle)

        assert db_name == 'monkey'

    def test_get_node_name_error(self, database):
        with pytest.raises(AtomDoesNotExist) as exc_info:
            database.get_node_name('handle-test')
        assert exc_info.type is AtomDoesNotExist
        assert exc_info.value.args[0] == "Nonexistent atom"

    def test_get_node_type(self, database):
        handle = database.get_node_handle('Concept', 'monkey')
        db_type = database.get_node_type(handle)

        assert db_type == 'Concept'

    def test_get_node_type_error(self, database):
        with pytest.raises(AtomDoesNotExist) as exc_info:
            database.get_node_type('handle-test')
        assert exc_info.type is AtomDoesNotExist
        assert exc_info.value.args[0] == "Nonexistent atom"

    def test_get_matched_node_name(self, database: InMemoryDB):
        expected = sorted(
            [
                database.get_node_handle('Concept', 'human'),
                database.get_node_handle('Concept', 'mammal'),
                database.get_node_handle('Concept', 'animal'),
            ]
        )
        actual = sorted(database.get_node_by_name('Concept', 'ma'))

        assert expected == actual
        assert sorted(database.get_node_by_name('blah', 'Concept')) == []
        assert sorted(database.get_node_by_name('Concept', 'blah')) == []

    def test_add_node_without_type_parameter(self, database: InMemoryDB):
        with pytest.raises(AddNodeException) as exc_info:
            database.add_node({'color': 'red', 'name': 'car'})
        assert exc_info.type is AddNodeException
        assert exc_info.value.args[0] == 'The "name" and "type" fields must be sent'

    def test_add_node_without_name_parameter(self, database: InMemoryDB):
        with pytest.raises(AddNodeException) as exc_info:
            database.add_node({'type': 'Concept', 'color': 'red'})
        assert exc_info.type is AddNodeException
        assert exc_info.value.args[0] == 'The "name" and "type" fields must be sent'

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
        assert exc_info.value.args[0] == 'The "type" and "targets" fields must be sent'

    def test_add_link_without_targets_parameter(self, database: InMemoryDB):
        with pytest.raises(AddLinkException) as exc_info:
            database.add_link({'source': 'fake', 'type': 'Similarity'})
        assert exc_info.type is AddLinkException
        assert exc_info.value.args[0] == 'The "type" and "targets" fields must be sent'

    def test_add_nested_links(self, database: InMemoryDB):
        cursor, answer = database.get_matched_type('Evaluation')
        assert cursor is None
        assert len(answer) == 0

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
        cursor, answer = database.get_matched_type('Evaluation')
        assert cursor is None
        assert len(answer) == 2

    def test_get_link_type(self, database: InMemoryDB):
        human = database.get_node_handle('Concept', 'human')
        chimp = database.get_node_handle('Concept', 'chimp')
        link_handle = database.get_link_handle(
            link_type='Similarity', target_handles=[human, chimp]
        )
        ret = database.get_link_type(link_handle=link_handle)
        assert ret == 'Similarity'

    def test_build_targets_list(self, database: InMemoryDB):
        targets = database._build_targets_list(
            {
                "blah": "h0",
            }
        )
        assert targets == []
        targets = database._build_targets_list(
            {
                "key_0": "h0",
            }
        )
        assert targets == ["h0"]
        targets = database._build_targets_list(
            {
                "key_0": "h0",
                "key_1": "h1",
            }
        )
        assert targets == ["h0", "h1"]

    def test_get_atom(self, database: InMemoryDB):
        h = database.get_node_handle('Concept', 'human')
        m = database.get_node_handle('Concept', 'monkey')
        s = database.get_link_handle('Similarity', [h, m])
        atom = database.get_atom(handle=s)
        assert atom['handle'] == s
        assert atom['targets'] == [h, m]

        with pytest.raises(AtomDoesNotExist) as exc:
            database.get_atom(handle='test')
        assert exc.value.message == 'Nonexistent atom'
        assert exc.value.details == 'handle: test'

    def test_get_atom_as_dict(self, database: InMemoryDB):
        h = database.get_node_handle('Concept', 'human')
        m = database.get_node_handle('Concept', 'monkey')
        s = database.get_link_handle('Similarity', [h, m])
        atom = database.get_atom_as_dict(handle=s)
        assert atom['handle'] == s
        assert atom['targets'] == [h, m]

    def test_get_incoming_links(self, database: InMemoryDB):
        h = database.get_node_handle('Concept', 'human')
        m = database.get_node_handle('Concept', 'monkey')
        s = database.get_link_handle('Similarity', [h, m])

        cursor, links = database.get_incoming_links(atom_handle=h, handles_only=False)
        assert cursor is None
        atom = database.get_atom(handle=s)
        assert atom in links

        cursor, links = database.get_incoming_links(
            atom_handle=h, handles_only=False, targets_document=True
        )
        assert cursor is None
        for link in links:
            for a, b in zip(link['targets'], link['targets_document']):
                assert a == b['handle']

        cursor, links = database.get_incoming_links(atom_handle=h, handles_only=True)
        assert cursor is None
        assert links == list(database.db.incoming_set.get(h))
        assert s in links

        cursor, links = database.get_incoming_links(atom_handle=m, handles_only=True)
        assert cursor is None
        assert links == list(database.db.incoming_set.get(m))

        cursor, links = database.get_incoming_links(atom_handle=s, handles_only=True)
        assert cursor is None
        assert links == []

    def test_get_atom_type(self, database: InMemoryDB):
        h = database.get_node_handle('Concept', 'human')
        m = database.get_node_handle('Concept', 'mammal')
        i = database.get_link_handle('Inheritance', [h, m])

        assert 'Concept' == database.get_atom_type(h)
        assert 'Concept' == database.get_atom_type(m)
        assert 'Inheritance' == database.get_atom_type(i)

    def test_get_all_links(self, database: InMemoryDB):
        cursors = [-1] * 2
        cursors[0], link_h = database.get_all_links('Similarity')
        cursors[1], link_i = database.get_all_links('Inheritance')
        assert all(c is None for c in cursors)
        assert len(link_h) == 14
        assert len(link_i) == 12
        assert (None, []) == database.get_all_links('snet')

    def test_delete_atom(self):
        cat_handle = AtomDB.node_handle('Concept', 'cat')
        dog_handle = AtomDB.node_handle('Concept', 'dog')
        mammal_handle = AtomDB.node_handle('Concept', 'mammal')
        inheritance_cat_mammal_handle = AtomDB.link_handle(
            'Inheritance', [cat_handle, mammal_handle]
        )
        inheritance_dog_mammal_handle = AtomDB.link_handle(
            'Inheritance', [dog_handle, mammal_handle]
        )

        db = InMemoryDB()

        assert db.count_atoms() == {'atom_count': 0, 'node_count': 0, 'link_count': 0}

        db.add_link(
            {
                'type': 'Inheritance',
                'targets': [
                    {'type': 'Concept', 'name': 'cat'},
                    {'type': 'Concept', 'name': 'mammal'},
                ],
            }
        )
        db.add_link(
            {
                'type': 'Inheritance',
                'targets': [
                    {'type': 'Concept', 'name': 'dog'},
                    {'type': 'Concept', 'name': 'mammal'},
                ],
            }
        )

        assert db.count_atoms() == {'atom_count': 5, 'node_count': 3, 'link_count': 2}
        assert db.db.incoming_set == {
            dog_handle: {inheritance_dog_mammal_handle},
            cat_handle: {inheritance_cat_mammal_handle},
            mammal_handle: {inheritance_cat_mammal_handle, inheritance_dog_mammal_handle},
        }
        assert db.db.outgoing_set == {
            inheritance_dog_mammal_handle: [dog_handle, mammal_handle],
            inheritance_cat_mammal_handle: [cat_handle, mammal_handle],
        }
        assert db.db.templates == {
            '41c082428b28d7e9ea96160f7fd614ad': {
                (inheritance_cat_mammal_handle, (cat_handle, mammal_handle)),
                (inheritance_dog_mammal_handle, (dog_handle, mammal_handle)),
            },
            'e40489cd1e7102e35469c937e05c8bba': {
                (inheritance_cat_mammal_handle, (cat_handle, mammal_handle)),
                (inheritance_dog_mammal_handle, (dog_handle, mammal_handle)),
            },
        }
        assert db.db.patterns == {
            "6e644e70a9fe3145c88b5b6261af5754": {
                (inheritance_cat_mammal_handle, (cat_handle, mammal_handle)),
                (inheritance_dog_mammal_handle, (dog_handle, mammal_handle)),
            },
            "5dd515aa7a451276feac4f8b9d84ae91": {
                (inheritance_cat_mammal_handle, (cat_handle, mammal_handle)),
                (inheritance_dog_mammal_handle, (dog_handle, mammal_handle)),
            },
            "a11d7cbf62bc544f75702b5fb6a514ff": {
                (inheritance_cat_mammal_handle, (cat_handle, mammal_handle)),
            },
            "f29daafee640d91aa7091e44551fc74a": {
                (inheritance_cat_mammal_handle, (cat_handle, mammal_handle)),
            },
            "7ead6cfa03894c62761162b7603aa885": {
                (inheritance_cat_mammal_handle, (cat_handle, mammal_handle)),
                (inheritance_dog_mammal_handle, (dog_handle, mammal_handle)),
            },
            "112002ff70ea491aad735f978e9d95f5": {
                (inheritance_cat_mammal_handle, (cat_handle, mammal_handle)),
                (inheritance_dog_mammal_handle, (dog_handle, mammal_handle)),
            },
            "3ba42d45a50c89600d92fb3f1a46c1b5": {
                (inheritance_cat_mammal_handle, (cat_handle, mammal_handle)),
            },
            "e55007a8477a4e6bf4fec76e4ffd7e10": {
                (inheritance_dog_mammal_handle, (dog_handle, mammal_handle))
            },
            "23dc149b3218d166a14730db55249126": {
                (inheritance_dog_mammal_handle, (dog_handle, mammal_handle))
            },
            "399751d7319f9061d97cd1d75728b66b": {
                (inheritance_dog_mammal_handle, (dog_handle, mammal_handle))
            },
        }

        db.delete_atom(inheritance_cat_mammal_handle)
        db.delete_atom(inheritance_dog_mammal_handle)
        assert db.count_atoms() == {'atom_count': 3, 'node_count': 3, 'link_count': 0}
        assert db.db.incoming_set == {
            dog_handle: set(),
            cat_handle: set(),
            mammal_handle: set(),
        }
        assert db.db.outgoing_set == {}
        assert db.db.templates == {
            '41c082428b28d7e9ea96160f7fd614ad': set(),
            'e40489cd1e7102e35469c937e05c8bba': set(),
        }
        assert db.db.patterns == {
            "6e644e70a9fe3145c88b5b6261af5754": set(),
            "5dd515aa7a451276feac4f8b9d84ae91": set(),
            "a11d7cbf62bc544f75702b5fb6a514ff": set(),
            "f29daafee640d91aa7091e44551fc74a": set(),
            "7ead6cfa03894c62761162b7603aa885": set(),
            "112002ff70ea491aad735f978e9d95f5": set(),
            "3ba42d45a50c89600d92fb3f1a46c1b5": set(),
            "e55007a8477a4e6bf4fec76e4ffd7e10": set(),
            "23dc149b3218d166a14730db55249126": set(),
            "399751d7319f9061d97cd1d75728b66b": set(),
        }

        db.add_link(
            {
                'type': 'Inheritance',
                'targets': [
                    {'type': 'Concept', 'name': 'cat'},
                    {'type': 'Concept', 'name': 'mammal'},
                ],
            }
        )
        db.add_link(
            {
                'type': 'Inheritance',
                'targets': [
                    {'type': 'Concept', 'name': 'dog'},
                    {'type': 'Concept', 'name': 'mammal'},
                ],
            }
        )

        db.delete_atom(mammal_handle)
        assert db.count_atoms() == {'atom_count': 2, 'node_count': 2, 'link_count': 0}
        assert db.db.incoming_set == {
            dog_handle: set(),
            cat_handle: set(),
        }
        assert db.db.outgoing_set == {}
        assert db.db.templates == {
            '41c082428b28d7e9ea96160f7fd614ad': set(),
            'e40489cd1e7102e35469c937e05c8bba': set(),
        }
        assert db.db.patterns == {
            "6e644e70a9fe3145c88b5b6261af5754": set(),
            "5dd515aa7a451276feac4f8b9d84ae91": set(),
            "a11d7cbf62bc544f75702b5fb6a514ff": set(),
            "f29daafee640d91aa7091e44551fc74a": set(),
            "7ead6cfa03894c62761162b7603aa885": set(),
            "112002ff70ea491aad735f978e9d95f5": set(),
            "3ba42d45a50c89600d92fb3f1a46c1b5": set(),
            "e55007a8477a4e6bf4fec76e4ffd7e10": set(),
            "23dc149b3218d166a14730db55249126": set(),
            "399751d7319f9061d97cd1d75728b66b": set(),
        }

        db.add_link(
            {
                'type': 'Inheritance',
                'targets': [
                    {'type': 'Concept', 'name': 'cat'},
                    {'type': 'Concept', 'name': 'mammal'},
                ],
            }
        )
        db.add_link(
            {
                'type': 'Inheritance',
                'targets': [
                    {'type': 'Concept', 'name': 'dog'},
                    {'type': 'Concept', 'name': 'mammal'},
                ],
            }
        )

        db.delete_atom(cat_handle)
        assert db.count_atoms() == {'atom_count': 3, 'node_count': 2, 'link_count': 1}
        assert db.db.incoming_set == {
            dog_handle: {inheritance_dog_mammal_handle},
            mammal_handle: {inheritance_dog_mammal_handle},
        }
        assert db.db.outgoing_set == {inheritance_dog_mammal_handle: [dog_handle, mammal_handle]}
        assert db.db.templates == {
            '41c082428b28d7e9ea96160f7fd614ad': {
                (inheritance_dog_mammal_handle, (dog_handle, mammal_handle))
            },
            'e40489cd1e7102e35469c937e05c8bba': {
                (inheritance_dog_mammal_handle, (dog_handle, mammal_handle))
            },
        }
        assert db.db.patterns == {
            "6e644e70a9fe3145c88b5b6261af5754": {
                (inheritance_dog_mammal_handle, (dog_handle, mammal_handle))
            },
            "5dd515aa7a451276feac4f8b9d84ae91": {
                (inheritance_dog_mammal_handle, (dog_handle, mammal_handle))
            },
            "a11d7cbf62bc544f75702b5fb6a514ff": set(),
            "f29daafee640d91aa7091e44551fc74a": set(),
            "7ead6cfa03894c62761162b7603aa885": {
                (inheritance_dog_mammal_handle, (dog_handle, mammal_handle))
            },
            "3ba42d45a50c89600d92fb3f1a46c1b5": set(),
            "112002ff70ea491aad735f978e9d95f5": {
                (inheritance_dog_mammal_handle, (dog_handle, mammal_handle))
            },
            "e55007a8477a4e6bf4fec76e4ffd7e10": {
                (inheritance_dog_mammal_handle, (dog_handle, mammal_handle))
            },
            "23dc149b3218d166a14730db55249126": {
                (inheritance_dog_mammal_handle, (dog_handle, mammal_handle))
            },
            "399751d7319f9061d97cd1d75728b66b": {
                (inheritance_dog_mammal_handle, (dog_handle, mammal_handle))
            },
        }

        db.add_link(
            {
                'type': 'Inheritance',
                'targets': [
                    {'type': 'Concept', 'name': 'cat'},
                    {'type': 'Concept', 'name': 'mammal'},
                ],
            }
        )

        db.delete_atom(dog_handle)
        assert db.count_atoms() == {'atom_count': 3, 'node_count': 2, 'link_count': 1}
        assert db.db.incoming_set == {
            cat_handle: {inheritance_cat_mammal_handle},
            mammal_handle: {inheritance_cat_mammal_handle},
        }
        assert db.db.outgoing_set == {inheritance_cat_mammal_handle: [cat_handle, mammal_handle]}
        assert db.db.templates == {
            '41c082428b28d7e9ea96160f7fd614ad': {
                (inheritance_cat_mammal_handle, (cat_handle, mammal_handle))
            },
            'e40489cd1e7102e35469c937e05c8bba': {
                (inheritance_cat_mammal_handle, (cat_handle, mammal_handle))
            },
        }
        assert db.db.patterns == {
            "6e644e70a9fe3145c88b5b6261af5754": {
                (inheritance_cat_mammal_handle, (cat_handle, mammal_handle)),
            },
            "5dd515aa7a451276feac4f8b9d84ae91": {
                (inheritance_cat_mammal_handle, (cat_handle, mammal_handle)),
            },
            "a11d7cbf62bc544f75702b5fb6a514ff": {
                (inheritance_cat_mammal_handle, (cat_handle, mammal_handle)),
            },
            "f29daafee640d91aa7091e44551fc74a": {
                (inheritance_cat_mammal_handle, (cat_handle, mammal_handle)),
            },
            "7ead6cfa03894c62761162b7603aa885": {
                (inheritance_cat_mammal_handle, (cat_handle, mammal_handle)),
            },
            "112002ff70ea491aad735f978e9d95f5": {
                (inheritance_cat_mammal_handle, (cat_handle, mammal_handle)),
            },
            "3ba42d45a50c89600d92fb3f1a46c1b5": {
                (inheritance_cat_mammal_handle, (cat_handle, mammal_handle)),
            },
            "e55007a8477a4e6bf4fec76e4ffd7e10": set(),
            "23dc149b3218d166a14730db55249126": set(),
            "399751d7319f9061d97cd1d75728b66b": set(),
        }

        db.clear_database()

        db.add_link(
            {
                'type': 'Inheritance',
                'targets': [
                    {
                        'type': 'Inheritance',
                        'targets': [
                            {'type': 'Concept', 'name': 'dog'},
                            {
                                'type': 'Inheritance',
                                'targets': [
                                    {'type': 'Concept', 'name': 'cat'},
                                    {'type': 'Concept', 'name': 'mammal'},
                                ],
                            },
                        ],
                    },
                    {'type': 'Concept', 'name': 'mammal'},
                ],
            }
        )

        db.delete_atom(inheritance_cat_mammal_handle)
        assert db.count_atoms() == {'atom_count': 3, 'node_count': 3, 'link_count': 0}
        assert db.db.incoming_set == {
            dog_handle: set(),
            cat_handle: set(),
            mammal_handle: set(),
        }
        assert db.db.outgoing_set == {}
        assert db.db.templates == {
            '41c082428b28d7e9ea96160f7fd614ad': set(),
            'e40489cd1e7102e35469c937e05c8bba': set(),
            '62bcbcec7fdc1bf896c0c9c99fe2f6b6': set(),
            '451c57cb0a3d43eb9ca208aebe11cf9e': set(),
        }
        assert db.db.patterns == {
            '6e644e70a9fe3145c88b5b6261af5754': set(),
            '5dd515aa7a451276feac4f8b9d84ae91': set(),
            'a11d7cbf62bc544f75702b5fb6a514ff': set(),
            'f29daafee640d91aa7091e44551fc74a': set(),
            '7ead6cfa03894c62761162b7603aa885': set(),
            '112002ff70ea491aad735f978e9d95f5': set(),
            '3ba42d45a50c89600d92fb3f1a46c1b5': set(),
            '1515eec36602aa53aa58a132cad99564': set(),
            'e55007a8477a4e6bf4fec76e4ffd7e10': set(),
            '1a81db4866eb3cc14dae6fd5a732a0b5': set(),
            '113b45c48122d22790870abb1152f218': set(),
            '399751d7319f9061d97cd1d75728b66b': set(),
            '3b23b5e8ecf01bb53c1e531018ee3b2a': set(),
            '1a8d5143240997c7179d99c846812ee1': set(),
            '1be2f1be6e8a65d5ddd8a9efbfb93233': set(),
        }

    def test_add_link_that_already_exists(self):
        db = InMemoryDB()
        db.add_link(
            {
                'type': 'Similarity',
                'targets': [
                    {'type': 'Test', 'name': 'test_1'},
                    {'type': 'Test', 'name': 'test_2'},
                ],
            }
        )
        db.add_link(
            {
                'type': 'Similarity',
                'targets': [
                    {'type': 'Test', 'name': 'test_1'},
                    {'type': 'Test', 'name': 'test_2'},
                ],
            }
        )

        assert db.db.incoming_set['167a378d17b1eda5587292814c8d0769'] == {
            '4a7f5140c0017fe270c8693605fd000a'
        }
        assert db.db.incoming_set['e24c839b9ffaf295c5d9be05171cf5d1'] == {
            '4a7f5140c0017fe270c8693605fd000a'
        }

        assert db.db.patterns['6e644e70a9fe3145c88b5b6261af5754'] == {
            (
                '4a7f5140c0017fe270c8693605fd000a',
                ('167a378d17b1eda5587292814c8d0769', 'e24c839b9ffaf295c5d9be05171cf5d1'),
            )
        }
        assert db.db.patterns['dab80dcb22dc4b246e3f8642a4e99449'] == {
            (
                '4a7f5140c0017fe270c8693605fd000a',
                ('167a378d17b1eda5587292814c8d0769', 'e24c839b9ffaf295c5d9be05171cf5d1'),
            )
        }
        assert db.db.patterns['957e33112374129ee9a7afacc702fe33'] == {
            (
                '4a7f5140c0017fe270c8693605fd000a',
                ('167a378d17b1eda5587292814c8d0769', 'e24c839b9ffaf295c5d9be05171cf5d1'),
            )
        }
        assert db.db.patterns['7fc3951816751ca77e6e14efecff2529'] == {
            (
                '4a7f5140c0017fe270c8693605fd000a',
                ('167a378d17b1eda5587292814c8d0769', 'e24c839b9ffaf295c5d9be05171cf5d1'),
            )
        }
        assert db.db.patterns['c48b5236102ae75ba3e71729a6bfa2e5'] == {
            (
                '4a7f5140c0017fe270c8693605fd000a',
                ('167a378d17b1eda5587292814c8d0769', 'e24c839b9ffaf295c5d9be05171cf5d1'),
            )
        }
        assert db.db.patterns['699ac93da51eeb8d573f9a20d7e81010'] == {
            (
                '4a7f5140c0017fe270c8693605fd000a',
                ('167a378d17b1eda5587292814c8d0769', 'e24c839b9ffaf295c5d9be05171cf5d1'),
            )
        }
        assert db.db.patterns['7d277b5039fb500cbf51806d06dbdc78'] == {
            (
                '4a7f5140c0017fe270c8693605fd000a',
                ('167a378d17b1eda5587292814c8d0769', 'e24c839b9ffaf295c5d9be05171cf5d1'),
            )
        }

        assert db.db.templates['4c201422342d157b2dded43181e7782d'] == {
            (
                '4a7f5140c0017fe270c8693605fd000a',
                ('167a378d17b1eda5587292814c8d0769', 'e24c839b9ffaf295c5d9be05171cf5d1'),
            )
        }
        assert db.db.templates['a9dea78180588431ec64d6bc4872fdbc'] == {
            (
                '4a7f5140c0017fe270c8693605fd000a',
                ('167a378d17b1eda5587292814c8d0769', 'e24c839b9ffaf295c5d9be05171cf5d1'),
            )
        }

    def test_bulk_insert(self):
        db = InMemoryDB()

        assert db.count_atoms() == {'atom_count': 0, 'node_count': 0, 'link_count': 0}

        documents = [
            {
                '_id': 'node1',
                'composite_type_hash': 'ConceptHash',
                'name': 'human',
                'named_type': 'Concept',
            },
            {
                '_id': 'node2',
                'composite_type_hash': 'ConceptHash',
                'name': 'monkey',
                'named_type': 'Concept',
            },
            {
                '_id': 'link1',
                'composite_type_hash': 'CompositeTypeHash',
                'is_toplevel': True,
                'composite_type': ['SimilarityHash', 'ConceptHash', 'ConceptHash'],
                'named_type': 'Similarity',
                'named_type_hash': 'SimilarityHash',
                'key_0': 'node1',
                'key_1': 'node2',
            },
        ]

        db.bulk_insert(documents)

        assert db.count_atoms() == {'atom_count': 3, 'node_count': 2, 'link_count': 1}

    def test_retrieve_all_atoms(self, database: InMemoryDB):
        expected = list(database.db.node.values()) + list(database.db.link.values())
        actual = database.retrieve_all_atoms()
        assert expected == actual

        with pytest.raises(Exception):
            database.db.node = None
            database.retrieve_all_atoms()
