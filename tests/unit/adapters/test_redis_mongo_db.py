import json
import pathlib
from unittest import mock

import pytest
from pymongo.errors import OperationFailure

from hyperon_das_atomdb.adapters import RedisMongoDB
from hyperon_das_atomdb.adapters.redis_mongo_db import KeyPrefix
from hyperon_das_atomdb.database import FieldIndexType, FieldNames, LinkT
from hyperon_das_atomdb.exceptions import AtomDoesNotExist
from hyperon_das_atomdb.utils.expression_hasher import ExpressionHasher
from tests.helpers import dict_to_link_params, dict_to_node_params
from tests.unit.fixtures import redis_mongo_db  # noqa: F401

FILE_CACHE = {}


def loader(file_name):
    global FILE_CACHE
    path = pathlib.Path(__file__).parent.resolve()
    filename = f"{path}/data/{file_name}"
    if filename not in FILE_CACHE:
        with open(filename) as f:
            FILE_CACHE[filename] = json.load(f)
    return FILE_CACHE[filename]


class TestRedisMongoDB:
    @pytest.fixture
    def database(self, redis_mongo_db: RedisMongoDB):  # noqa: F811
        atoms = loader("atom_mongo_redis.json")
        for atom in atoms:
            if "name" in atom:
                redis_mongo_db.add_node(dict_to_node_params(atom))
            else:
                is_toplevel = atom.pop("is_toplevel", True)
                redis_mongo_db.add_link(dict_to_link_params(atom), toplevel=is_toplevel)
        redis_mongo_db.commit()
        yield redis_mongo_db

    def test_node_exists(self, database: RedisMongoDB):
        node_type = "Concept"
        node_name = "monkey"
        resp = database.node_exists(node_type, node_name)
        assert resp is True

    def test_node_exists_false(self, database: RedisMongoDB):
        node_type = "Concept"
        node_name = "human-fake"

        resp = database.node_exists(node_type, node_name)

        assert resp is False

    def test_link_exists(self, database: RedisMongoDB):
        human = ExpressionHasher.terminal_hash("Concept", "human")
        monkey = ExpressionHasher.terminal_hash("Concept", "monkey")

        resp = database.link_exists("Similarity", [human, monkey])

        assert resp is True

    def test_link_exists_false(self, database: RedisMongoDB):
        human = ExpressionHasher.terminal_hash("Concept", "fake")
        monkey = ExpressionHasher.terminal_hash("Concept", "monkey")

        resp = database.link_exists("Similarity", [human, monkey])

        assert resp is False

    def test_get_node_handle(self, database: RedisMongoDB):
        node_type = "Concept"
        node_name = "human"

        resp = database.get_node_handle(node_type, node_name)

        assert resp == ExpressionHasher.terminal_hash("Concept", "human")

    @pytest.mark.parametrize(
        "node_type,node_name",
        [
            ("Concept", ""),
            ("Concept", "test"),
            ("Similarity", ""),
        ],
    )
    def test_get_node_handle_node_does_not_exist(
        self, node_type, node_name, database: RedisMongoDB
    ):
        with pytest.raises(AtomDoesNotExist) as exc_info:
            database.get_node_handle(node_type, node_name)
        assert exc_info.type is AtomDoesNotExist
        assert exc_info.value.args[0] == "Nonexistent atom"

    @pytest.mark.parametrize(
        "link_type,targets,expected",
        [
            (
                "Similarity",
                [("Concept", "human"), ("Concept", "chimp")],
                "b5459e299a5c5e8662c427f7e01b3bf1",
            ),
        ],
    )
    def test_get_link_handle(self, link_type, targets, expected, database: RedisMongoDB):
        resp = database.get_link_handle(
            link_type=link_type,
            target_handles=[ExpressionHasher.terminal_hash(*t) for t in targets],
        )
        assert resp is not None
        assert isinstance(resp, str)
        assert resp == expected

    @pytest.mark.parametrize(
        "link_type,targets",
        [
            ("Similarity", [("Concept", "brazil"), ("Concept", "travel")]),
            ("Similarity", [("Concept", "*"), ("Concept", "*")]),
            ("Similarity", [("Concept", "$"), ("Concept", "*")]),
            ("Concept", []),
            ("$", []),
        ],
    )
    def test_get_link_handle_link_does_not_exist(self, link_type, targets, database: RedisMongoDB):
        with pytest.raises(AtomDoesNotExist) as exc_info:
            database.get_link_handle(
                link_type=link_type,
                target_handles=[ExpressionHasher.terminal_hash(*t) for t in targets],
            )
        assert exc_info.type is AtomDoesNotExist
        assert exc_info.value.args[0] == "Nonexistent atom"

    @pytest.mark.parametrize(
        "link_type,targets,expected_count",
        [
            ("Similarity", [("Concept", "human"), ("Concept", "chimp")], 2),
            ("Inheritance", [("Concept", "human"), ("Concept", "mammal")], 2),
            ("Evaluation", [("Concept", "triceratops"), ("Concept", "rhino")], 2),
        ],
    )
    def test_get_link_targets(self, link_type, targets, expected_count, database: RedisMongoDB):
        handle = database.get_link_handle(
            link_type, [database.get_node_handle(*t) for t in targets]
        )
        targets = database.get_link_targets(handle)
        assert isinstance(targets, list)
        assert len(targets) == expected_count

    @pytest.mark.parametrize(
        "handle", ["handle", "2a8a69c01305563932b957de4b3a9ba6", "2a8a69c0130556=z32b957de4b3a9ba6"]
    )
    def test_get_link_targets_invalid(self, handle, database: RedisMongoDB):
        with pytest.raises(ValueError) as exc_info:
            database.get_link_targets(f"{handle}-Fake")
        assert exc_info.type is ValueError
        assert exc_info.value.args[0] == f"Invalid handle: {handle}-Fake"

    @pytest.mark.parametrize(
        "link_values,expected,expected_count",
        [
            (
                {"link_type": "Evaluation", "target_handles": ["*", "*"], "toplevel_only": True},
                {"bd2bb6c802a040b00659dfe7954e804d"},
                1,
            ),
            (
                {
                    "link_type": "*",
                    "target_handles": [
                        ExpressionHasher.terminal_hash("Concept", "human"),
                        ExpressionHasher.terminal_hash("Concept", "chimp"),
                    ],
                    "toplevel_only": False,
                },
                {"b5459e299a5c5e8662c427f7e01b3bf1"},
                1,
            ),
            (
                {
                    "link_type": "Similarity",
                    "target_handles": [
                        ExpressionHasher.terminal_hash("Concept", "human"),
                        ExpressionHasher.terminal_hash("Concept", "monkey"),
                    ],
                    "toplevel_only": False,
                },
                {"bad7472f41a0e7d601ca294eb4607c3a"},
                1,
            ),
            (
                {
                    "link_type": "Similarity",
                    "target_handles": ["*", ExpressionHasher.terminal_hash("Concept", "chimp")],
                    "toplevel_only": False,
                },
                {
                    "31535ddf214f5b239d3b517823cb8144",
                    "b5459e299a5c5e8662c427f7e01b3bf1",
                },
                2,
            ),
        ],
    )
    def test_get_matched_links_toplevel_only(
        self, link_values, expected, expected_count, database: RedisMongoDB
    ):
        actual = database.get_matched_links(**link_values)
        assert expected == actual
        assert len(actual) == expected_count

    @pytest.mark.parametrize(
        "node_type,names,expected",
        [
            ("Concept", True, 14),
            ("Concept", False, 14),
            ("Test", True, 0),
            ("Test", False, 0),
            ("Inheritance", True, 0),
            ("Inheritance", False, 0),
            ("Evaluation", True, 0),
            ("Evaluation", False, 0),
            ("Similarity", True, 0),
            ("Similarity", False, 0),
        ],
    )
    def test_get_all_nodes(self, node_type, names, expected, database: RedisMongoDB):
        if node_type in {"Inheritance", "Evaluation", "Similarity"}:
            pytest.skip(
                "Returning links, also break if it's a link and name is true"
                "https://github.com/singnet/das-atom-db/issues/210"
            )
        if names:
            ret = database.get_all_nodes_names(node_type)
        else:
            ret = database.get_all_nodes_handles(node_type)
        assert len(ret) == expected

    def test_get_matched_type_template(self, database: RedisMongoDB):
        v1 = database.get_matched_type_template(["Inheritance", "Concept", "Concept"])
        v2 = database.get_matched_type_template(["Similarity", "Concept", "Concept"])
        v3 = database.get_matched_type_template(["Inheritance", "Concept", "blah"])
        v4 = database.get_matched_type_template(["Similarity", "blah", "Concept"])
        v5 = database.get_matched_links("Inheritance", ["*", "*"])
        v6 = database.get_matched_links("Similarity", ["*", "*"])
        assert len(v1) == 12
        assert len(v2) == 14
        assert len(v3) == 0
        assert len(v4) == 0
        assert v1 == v5
        assert v2 == v6

    @pytest.mark.parametrize(
        "template",
        [
            ["Inheritance", "Concept", "Concept", {"aaa": "bbb"}],
            ["Inheritance", "Concept", "Concept", ["aaa", "bbb"]],
        ],
    )
    def test_get_matched_type_template_error(self, template, database: RedisMongoDB):
        with pytest.raises(ValueError) as exc_info:
            database.get_matched_type_template(template)
        assert exc_info.type is ValueError

    @pytest.mark.parametrize(
        "link_type,expected",
        [
            ("Evaluation", 2),
            ("Inheritance", 12),
            ("Similarity", 14),
            ("Concept", 0),
        ],
    )
    def test_get_matched_type(self, link_type, expected, database: RedisMongoDB):
        links = database.get_matched_type(link_type)
        assert len(links) == expected
        assert isinstance(links, set)

    @pytest.mark.parametrize(
        "link_type,top_level,expected",
        [
            ("Evaluation", True, 1),
            ("Evaluation", False, 2),
            ("Inheritance", True, 12),
            ("Inheritance", False, 12),
            ("Similarity", False, 14),
            ("Similarity", False, 14),
        ],
    )
    def test_get_matched_type_toplevel_only(
        self, link_type, top_level, expected, database: RedisMongoDB
    ):
        ret = database.get_matched_type(link_type, toplevel_only=top_level)
        assert len(ret) == expected
        assert isinstance(ret, set)

    @pytest.mark.parametrize(
        "node_type,node_name",
        [
            ("Concept", "monkey"),
            ("Concept", "human"),
            ("Concept", "mammal"),
        ],
    )
    def test_get_node_name(self, node_type, node_name, database: RedisMongoDB):
        handle = database.get_node_handle(node_type, node_name)
        db_name = database.get_node_name(handle)
        assert db_name == node_name

    @pytest.mark.parametrize(
        "handle,",
        ["handle", "2a8a69c01305563932b957de4b3a9ba6", "2a8a69c0130556=z32b957de4b3a9ba6"],
    )
    def test_get_node_name_value_error(self, handle, database: RedisMongoDB):
        with pytest.raises(ValueError) as exc_info:
            database.get_node_name("handle")
        assert exc_info.type is ValueError
        assert exc_info.value.args[0] == "Invalid handle: handle"

    @pytest.mark.parametrize(
        "node_type,node_name,expected",
        [
            (
                "Concept",
                "ma",
                sorted(
                    [
                        ExpressionHasher.terminal_hash("Concept", "human"),
                        ExpressionHasher.terminal_hash("Concept", "mammal"),
                        ExpressionHasher.terminal_hash("Concept", "animal"),
                    ]
                ),
            ),
            ("blah", "Concept", []),
            ("Concept", "blah", []),
            ("Similarity", "ma", []),
        ],
    )
    def test_get_matched_node_name(self, node_type, node_name, expected, database: RedisMongoDB):
        actual = sorted(database.get_node_by_name(node_type, node_name))
        assert expected == actual

    @pytest.mark.parametrize(
        "node_type,node_name,expected",
        [
            (
                "Concept",
                "ma",
                sorted(
                    [
                        ExpressionHasher.terminal_hash("Concept", "mammal"),
                    ]
                ),
            ),
            ("blah", "Concept", []),
            (
                "Concept",
                "h",
                sorted(
                    [
                        ExpressionHasher.terminal_hash("Concept", "human"),
                    ]
                ),
            ),
        ],
    )
    def test_get_startswith_node_name(self, node_type, node_name, expected, database: RedisMongoDB):
        actual = database.get_node_by_name_starting_with(node_type, node_name)
        assert expected == actual

    def test_get_node_by_field(self, database: RedisMongoDB):
        expected = [
            database.get_node_handle("Concept", "mammal"),
        ]
        actual = database.get_atoms_by_field([{"field": "name", "value": "mammal"}])

        assert expected == actual

    @pytest.mark.parametrize(
        "atom_type,fields,query,expected",
        [
            (
                "node",
                ["name"],
                [{"field": "name", "value": "mammal"}],
                [ExpressionHasher.terminal_hash("Concept", "mammal")],
            ),
            (
                "link",
                ["type"],
                [{"field": "type", "value": "Evaluation"}],
                ["bd2bb6c802a040b00659dfe7954e804d", "cadd63b3fd14e34819bca4803925bf2c"],
            ),
        ],
    )
    def test_get_atoms_by_index(self, atom_type, fields, query, expected, database: RedisMongoDB):
        result = database.create_field_index(atom_type, fields=fields)
        cursor, actual = database.get_atoms_by_index(result, query)
        assert cursor == 0
        assert isinstance(actual, list)
        assert all([a.handle in expected for a in actual])

    @pytest.mark.parametrize(
        "text_value,field,expected",
        [
            ("mammal", "name", [ExpressionHasher.terminal_hash("Concept", "mammal")]),
        ],
    )
    def test_get_node_by_text_field(self, text_value, field, expected, database: RedisMongoDB):
        actual = database.get_atoms_by_text_field(text_value, field)
        assert expected == actual

    @pytest.mark.parametrize(
        "handle,expected",
        [
            (ExpressionHasher.terminal_hash("Concept", "monkey"), "Concept"),
            (ExpressionHasher.terminal_hash("Concept", "human"), "Concept"),
            ("b5459e299a5c5e8662c427f7e01b3bf1", None),  # Similarity handle
        ],
    )
    def test_get_node_type(self, handle, expected, database: RedisMongoDB):
        resp_node = database.get_node_type(handle)
        if expected is None:
            assert resp_node is None
        else:
            assert expected == resp_node

    def test_get_node_type_without_cache(self, database: RedisMongoDB):
        from hyperon_das_atomdb.adapters import redis_mongo_db  # noqa: F811

        redis_mongo_db.USE_CACHED_NODE_TYPES = False
        monkey = database.get_node_handle("Concept", "monkey")
        resp_node = database.get_node_type(monkey)
        assert "Concept" == resp_node

    @pytest.mark.parametrize(
        "handle,expected",
        [
            (ExpressionHasher.terminal_hash("Concept", "monkey"), None),
            (ExpressionHasher.terminal_hash("Concept", "human"), None),
            ("b5459e299a5c5e8662c427f7e01b3bf1", "Similarity"),
        ],
    )
    def test_get_link_type(self, handle, expected, database: RedisMongoDB):
        resp_link = database.get_link_type(handle)
        if expected is None:
            assert resp_link is None
        else:
            assert expected == resp_link

    def test_get_link_type_without_cache(self, database: RedisMongoDB):
        from hyperon_das_atomdb.adapters import redis_mongo_db  # noqa: F811

        redis_mongo_db.USE_CACHED_LINK_TYPES = False
        human = database.get_node_handle("Concept", "human")
        chimp = database.get_node_handle("Concept", "chimp")
        link_handle = database.get_link_handle("Similarity", [human, chimp])
        resp_link = database.get_link_type(link_handle)
        assert "Similarity" == resp_link

    def test_atom_count(self, database: RedisMongoDB):
        response = database.count_atoms({"precise": True})
        assert response == {"atom_count": 42, "node_count": 14, "link_count": 28}

    def test_atom_count_fast(self, database: RedisMongoDB):
        response = database.count_atoms()
        assert response == {"atom_count": 42}

    def test_add_node(self, database: RedisMongoDB):
        assert {"atom_count": 42} == database.count_atoms()
        all_nodes_before = database.get_all_nodes_handles("Concept")
        database.add_node(dict_to_node_params({"type": "Concept", "name": "lion"}))
        database.commit()
        all_nodes_after = database.get_all_nodes_handles("Concept")
        assert len(all_nodes_before) == 14
        assert len(all_nodes_after) == 15
        assert {
            "atom_count": 43,
            "node_count": 15,
            "link_count": 28,
        } == database.count_atoms({"precise": True})
        new_node_handle = database.get_node_handle("Concept", "lion")
        assert new_node_handle == ExpressionHasher.terminal_hash("Concept", "lion")
        assert new_node_handle not in all_nodes_before
        assert new_node_handle in all_nodes_after
        new_node = database.get_atom(new_node_handle)
        assert new_node.handle == new_node_handle
        assert new_node.named_type == "Concept"
        assert new_node.name == "lion"

    def test_add_link(self, database: RedisMongoDB):
        assert {"atom_count": 42} == database.count_atoms()

        all_nodes_before = database.get_all_nodes_handles("Concept")
        similarity = database.get_all_links("Similarity")
        inheritance = database.get_all_links("Inheritance")
        evaluation = database.get_all_links("Evaluation")
        all_links_before = similarity.union(inheritance).union(evaluation)
        database.add_link(
            dict_to_link_params(
                {
                    "type": "Similarity",
                    "targets": [
                        {"type": "Concept", "name": "lion"},
                        {"type": "Concept", "name": "cat"},
                        {
                            "type": "Dumminity",
                            "targets": [
                                {"type": "Dummy", "name": "dummy1"},
                                {"type": "Dummy", "name": "dummy2"},
                                {
                                    "type": "Anidity",
                                    "targets": [
                                        {"type": "Any", "name": "any1"},
                                        {"type": "Any", "name": "any2"},
                                    ],
                                },
                            ],
                        },
                    ],
                }
            )
        )
        database.commit()
        all_nodes_after = database.get_all_nodes_handles("Concept")
        similarity = database.get_all_links("Similarity")
        inheritance = database.get_all_links("Inheritance")
        evaluation = database.get_all_links("Evaluation")
        all_links_after = similarity.union(inheritance).union(evaluation)
        assert len(all_nodes_before) == 14
        assert len(all_nodes_after) == 16
        assert len(all_links_before) == 28
        assert len(all_links_after) == 29
        assert {
            "atom_count": 51,
            "node_count": 20,
            "link_count": 31,
        } == database.count_atoms({"precise": True})

        new_node_handle = database.get_node_handle("Concept", "lion")
        assert new_node_handle == ExpressionHasher.terminal_hash("Concept", "lion")
        assert new_node_handle not in all_nodes_before
        assert new_node_handle in all_nodes_after
        new_node = database.get_atom(new_node_handle)
        assert new_node.handle == new_node_handle
        assert new_node.named_type == "Concept"
        assert new_node.name == "lion"

        new_node_handle = database.get_node_handle("Concept", "cat")
        assert new_node_handle == ExpressionHasher.terminal_hash("Concept", "cat")
        assert new_node_handle not in all_nodes_before
        assert new_node_handle in all_nodes_after
        new_node = database.get_atom(new_node_handle)
        assert new_node.handle == new_node_handle
        assert new_node.named_type == "Concept"
        assert new_node.name == "cat"

    @pytest.mark.parametrize(
        "node,expected_count",
        [
            (("Concept", "human"), 7),
            (("Concept", "monkey"), 5),
            (("Concept", "rhino"), 4),
            (("Concept", "reptile"), 3),
        ],
    )
    def test_get_incoming_links_by_node(self, node, expected_count, database: RedisMongoDB):
        handle = database.get_node_handle(*node)
        links = database.get_incoming_links_atoms(atom_handle=handle)
        link_handles = database.get_incoming_links_handles(atom_handle=handle)
        assert len(links) > 0
        assert all(isinstance(link, str) for link in link_handles)
        answer = database.redis.smembers(f"{KeyPrefix.INCOMING_SET.value}:{handle}")
        assert len(links) == len(answer) == expected_count
        assert sorted(link_handles) == sorted(answer)
        assert all([handle in link.targets for link in links])

    @pytest.mark.parametrize(
        "key",
        list(KeyPrefix),
    )
    def test_redis_keys(self, key, database: RedisMongoDB):
        assert str(key) not in {k.split(":")[0] for k in database.redis.cache.keys()}
        assert str(key.value) in {k.split(":")[0] for k in database.redis.cache.keys()}

    @pytest.mark.parametrize(
        "link_type,link_targets",
        [
            ("Similarity", [("Concept", "human"), ("Concept", "monkey")]),
            ("Inheritance", [("Concept", "snake"), ("Concept", "reptile")]),
            ("Evaluation", [("Concept", "triceratops"), ("Concept", "rhino")]),
            (
                "Evaluation",
                [
                    ("Concept", "triceratops"),
                    (
                        "Evaluation",
                        ["d03e59654221c1e8fcda404fd5c8d6cb", "99d18c702e813b07260baf577c60c455"],
                    ),
                ],
            ),
        ],
    )
    def test_get_incoming_links_by_links(self, link_type, link_targets, database: RedisMongoDB):
        handle = database.get_link_handle(
            link_type,
            [
                database.get_node_handle(*t)
                if all([isinstance(tt, str) for tt in t])
                else database.get_link_handle(*t)
                for t in link_targets
            ],
        )
        for target in link_targets:
            if all([isinstance(t, str) for t in target]):
                h = database.get_node_handle(*target)
            else:
                database.get_link_handle(*target)
            links = database.get_incoming_links_handles(atom_handle=h)
            assert len(links) > 0
            assert all(isinstance(link, str) for link in links)
            answer = database.redis.smembers(f"{KeyPrefix.INCOMING_SET.value}:{h}")
            assert sorted(links) == sorted(answer)
            assert handle in links
            links = database.get_incoming_links_atoms(atom_handle=h)
            atom = database.get_atom(handle=handle)
            assert atom.handle in [link.handle for link in links]
            links = database.get_incoming_links_atoms(atom_handle=h, targets_document=True)
            assert len(links) > 0
            assert all(isinstance(link, LinkT) for link in links)
            for link in links:
                for a, b in zip(link.targets, link.targets_documents):
                    assert a == b.handle

    @pytest.mark.parametrize(
        "link_type,link_targets,expected_count",
        [
            ("Similarity", ["*", "af12f10f9ae2002a1607ba0b47ba8407"], 3),
            ("Similarity", ["af12f10f9ae2002a1607ba0b47ba8407", "*"], 3),
            (
                "Inheritance",
                ["c1db9b517073e51eb7ef6fed608ec204", "b99ae727c787f1b13b452fd4c9ce1b9a"],
                1,
            ),
            (
                "Evaluation",
                ["d03e59654221c1e8fcda404fd5c8d6cb", "99d18c702e813b07260baf577c60c455"],
                1,
            ),
            (
                "Evaluation",
                ["d03e59654221c1e8fcda404fd5c8d6cb", "99d18c702e813b07260baf577c60c455"],
                1,
            ),
            ("Evaluation", ["*", "99d18c702e813b07260baf577c60c455"], 1),
        ],
    )
    def test_redis_patterns(self, link_type, link_targets, expected_count, database: RedisMongoDB):
        links = database.get_matched_links(link_type, link_targets)
        pattern_hash = ExpressionHasher.composite_hash(
            [ExpressionHasher.named_type_hash(link_type), *link_targets]
        )
        answer = database.redis.smembers(f"{KeyPrefix.PATTERNS.value}:{pattern_hash}")
        assert len(answer) == len(links) == expected_count
        assert sorted(links) == sorted(answer)
        assert len(links) == expected_count

    @pytest.mark.parametrize(
        "template_values,expected_count",
        [
            (["Inheritance", "Concept", "Concept"], 12),
            (["Inheritance"], 12),
            (["Inheritance", "Concept", "Concept"], 12),
            (["Inheritance"], 12),
            (["Similarity", "Concept", "Concept"], 14),
            (["Similarity"], 14),
            (["Evaluation", "Concept", "Concept"], 1),
            (["Evaluation"], 2),
            # (["Evaluation", "Concept", ["Evaluation", "Concept", ["Evaluation", "Concept", "Concept"]]], 1)
        ],
    )
    def test_redis_templates(self, template_values, expected_count, database: RedisMongoDB):
        links = database.get_matched_type_template(template_values)
        hash_base = database._build_named_type_hash_template(template_values)
        template_hash = ExpressionHasher.composite_hash(hash_base)
        answer = database.redis.smembers(f"{KeyPrefix.TEMPLATES.value}:{template_hash}")
        assert len(answer) == len(links) == expected_count
        assert sorted(links) == sorted(answer)
        assert len(links) == expected_count

    @pytest.mark.parametrize(
        "node_type,expected_count",
        [
            ("Concept", 14),
            ("Empty", 0),
        ],
    )
    def test_redis_names(self, node_type, expected_count, database: RedisMongoDB):
        nodes = database.get_all_nodes_handles(node_type)
        assert len(nodes) == expected_count
        assert all(
            [database.redis.smembers(f"{KeyPrefix.NAMED_ENTITIES.value}:{node}") for node in nodes]
        )

    @pytest.mark.parametrize(
        "link_type,expected_count",
        [
            ("Inheritance", 12),
            ("Similarity", 14),
            ("Evaluation", 2),
        ],
    )
    def test_redis_outgoing_set(self, link_type, expected_count, database: RedisMongoDB):
        links = database.get_all_links(link_type)
        assert len(links) == expected_count
        assert all(
            [database.redis.smembers(f"{KeyPrefix.OUTGOING_SET.value}:{link}") for link in links]
        )

    def test_get_atom_type(self, database: RedisMongoDB):
        h = database.get_node_handle("Concept", "human")
        m = database.get_node_handle("Concept", "mammal")
        i = database.get_link_handle("Inheritance", [h, m])

        assert "Concept" == database.get_atom_type(h)
        assert "Concept" == database.get_atom_type(m)
        assert "Inheritance" == database.get_atom_type(i)

    def test_create_field_index_node_collection(self, database: RedisMongoDB):
        database.mongo_atoms_collection = mock.Mock()
        database.mongo_atoms_collection.create_index.return_value = "name_index_asc"
        with mock.patch(
            "hyperon_das_atomdb.index.Index.generate_index_id",
            return_value="name_index_asc",
        ), mock.patch(
            "hyperon_das_atomdb.adapters.redis_mongo_db.MongoDBIndex.index_exists",
            return_value=False,
        ):
            result = database.create_field_index("node", ["name"], "Type")

        assert result == "name_index_asc"
        database.mongo_atoms_collection.create_index.assert_called_once_with(
            [("name", 1)],
            name="node_name_index_asc",
            partialFilterExpression={FieldNames.TYPE_NAME: {"$eq": "Type"}},
        )

    def test_create_field_index_link_collection(self, database: RedisMongoDB):
        database.mongo_atoms_collection = mock.Mock()
        database.mongo_atoms_collection.create_index.return_value = "field_index_asc"
        with mock.patch(
            "hyperon_das_atomdb.index.Index.generate_index_id",
            return_value="field_index_asc",
        ), mock.patch(
            "hyperon_das_atomdb.adapters.redis_mongo_db.MongoDBIndex.index_exists",
            return_value=False,
        ):
            result = database.create_field_index("link", ["field"], "Type")

        assert result == "field_index_asc"
        database.mongo_atoms_collection.create_index.assert_called_once_with(
            [("field", 1)],
            name="link_field_index_asc",
            partialFilterExpression={FieldNames.TYPE_NAME: {"$eq": "Type"}},
        )

    def test_create_text_index(self, database: RedisMongoDB):
        database.mongo_atoms_collection = mock.Mock()
        database.mongo_atoms_collection.create_index.return_value = "field_index_asc"
        with mock.patch(
            "hyperon_das_atomdb.index.Index.generate_index_id",
            return_value="field_index_asc",
        ), mock.patch(
            "hyperon_das_atomdb.adapters.redis_mongo_db.MongoDBIndex.index_exists",
            return_value=False,
        ):
            result = database.create_field_index(
                "link", ["field"], index_type=FieldIndexType.TOKEN_INVERTED_LIST
            )

        assert result == "field_index_asc"
        database.mongo_atoms_collection.create_index.assert_called_once_with(
            [("field", "text")], name="link_field_index_asc_text"
        )

    def test_create_text_index_type(self, database: RedisMongoDB):
        database.mongo_atoms_collection = mock.Mock()
        database.mongo_atoms_collection.create_index.return_value = "field_index_asc"
        with mock.patch(
            "hyperon_das_atomdb.index.Index.generate_index_id",
            return_value="field_index_asc",
        ), mock.patch(
            "hyperon_das_atomdb.adapters.redis_mongo_db.MongoDBIndex.index_exists",
            return_value=False,
        ):
            result = database.create_field_index(
                "link", ["field"], "Type", index_type=FieldIndexType.TOKEN_INVERTED_LIST
            )

        assert result == "field_index_asc"
        database.mongo_atoms_collection.create_index.assert_called_once_with(
            [("field", "text")],
            name="link_field_index_asc_text",
            partialFilterExpression={FieldNames.TYPE_NAME: {"$eq": "Type"}},
        )

    def test_create_compound_index_type(self, database: RedisMongoDB):
        database.mongo_atoms_collection = mock.Mock()
        database.mongo_atoms_collection.create_index.return_value = "field_index_asc"
        with mock.patch(
            "hyperon_das_atomdb.index.Index.generate_index_id",
            return_value="field_index_asc",
        ), mock.patch(
            "hyperon_das_atomdb.adapters.redis_mongo_db.MongoDBIndex.index_exists",
            return_value=False,
        ):
            result = database.create_field_index("link", fields=["field", "name"])

        assert result == "field_index_asc"
        database.mongo_atoms_collection.create_index.assert_called_once_with(
            [("field", 1), ("name", 1)],
            name="link_field_index_asc",
        )

    def test_create_compound_index_type_filter(self, database: RedisMongoDB):
        database.mongo_atoms_collection = mock.Mock()
        database.mongo_atoms_collection.create_index.return_value = "field_index_asc"
        with mock.patch(
            "hyperon_das_atomdb.index.Index.generate_index_id",
            return_value="field_index_asc",
        ), mock.patch(
            "hyperon_das_atomdb.adapters.redis_mongo_db.MongoDBIndex.index_exists",
            return_value=False,
        ):
            result = database.create_field_index(
                "link", named_type="Type", fields=["field", "name"]
            )

        assert result == "field_index_asc"
        database.mongo_atoms_collection.create_index.assert_called_once_with(
            [("field", 1), ("name", 1)],
            name="link_field_index_asc",
            partialFilterExpression={FieldNames.TYPE_NAME: {"$eq": "Type"}},
        )

    @pytest.mark.skip(reason="Change the way to handle this test")
    def test_create_field_index_invalid_collection(self, database: RedisMongoDB):
        with pytest.raises(ValueError):
            database.create_field_index("invalid_atom_type", ["field"], "type")

    def test_create_field_index_operation_failure(self, database: RedisMongoDB):
        database.mongo_atoms_collection = mock.Mock()
        database.mongo_atoms_collection.create_index.side_effect = OperationFailure(
            "Index creation failed"
        )
        with mock.patch(
            "hyperon_das_atomdb.adapters.redis_mongo_db.MongoDBIndex.index_exists",
            return_value=False,
        ):
            result = database.create_field_index("node", ["field"], "Type")

        assert result == "Index creation failed, Details: Index creation failed"

    def test_create_field_index_already_exists(self, database: RedisMongoDB):
        database.mongo_atoms_collection = mock.Mock()
        database.mongo_atoms_collection.list_indexes.return_value = []
        database.mongo_atoms_collection.create_index.return_value = "name_index_asc"
        with mock.patch(
            "hyperon_das_atomdb.index.Index.generate_index_id",
            return_value="name_index_asc",
        ), mock.patch(
            "hyperon_das_atomdb.adapters.redis_mongo_db.MongoDBIndex.index_exists",
            return_value=False,
        ):
            database.create_field_index("node", "name", "Type")
        assert database.create_field_index("node", ["name"], "Type") == "name_index_asc"
