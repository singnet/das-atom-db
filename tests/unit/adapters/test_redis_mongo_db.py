import json
import pathlib
from unittest import mock

import mongomock
import pytest
from pymongo.errors import OperationFailure
from redis import Redis

from hyperon_das_atomdb.adapters import RedisMongoDB
from hyperon_das_atomdb.adapters.redis_mongo_db import MongoCollectionNames
from hyperon_das_atomdb.database import FieldIndexType, FieldNames, LinkT
from hyperon_das_atomdb.exceptions import AtomDoesNotExist
from hyperon_das_atomdb.utils.expression_hasher import ExpressionHasher
from tests.helpers import dict_to_link_params, dict_to_node_params


def loader(file_name):
    path = pathlib.Path(__file__).parent.resolve()
    with open(f"{path}/data/{file_name}") as f:
        return json.load(f)


outgoing_set_redis_mock_data = loader("outgoing_set_redis_data.json")
incoming_set_redis_mock_data = loader("incoming_set_redis_data.json")
patterns_redis_mock_data = loader("patterns_redis_data.json")
templates_redis_mock_data = loader("templates_redis_data.json")
names_redis_mock_data = loader("names_redis_data.json")


class TestRedisMongoDB:
    @pytest.fixture()
    def mongo_db(self):
        return mongomock.MongoClient().db

    @pytest.fixture()
    def redis_db(self):
        redis_db = mock.MagicMock(spec=Redis)

        def smembers(key: str):
            if "incoming_set" in key:
                for data in incoming_set_redis_mock_data:
                    if list(data.keys())[0] == key:
                        return list(data.values())[0]
                return []
            elif "patterns" in key:
                return patterns_redis_mock_data.get(key, [])
            elif "templates" in key:
                return templates_redis_mock_data.get(key, [])
            else:
                assert False

        def lrange(key: str, start: int, end: int):
            if "outgoing_set" in key:
                for d in outgoing_set_redis_mock_data:
                    if key in d:
                        return d[key]
                return []
            else:
                assert False

        def get(key: str):
            if "names" in key:
                return names_redis_mock_data.get(key)
            elif "outgoing_set" in key:
                for d in outgoing_set_redis_mock_data:
                    if key in d:
                        return d[key]
                return []
            else:
                assert False

        def commit():
            pass

        redis_db.smembers = mock.Mock(side_effect=smembers)
        redis_db.lrange = mock.Mock(side_effect=lrange)
        redis_db.get = mock.Mock(side_effect=get)
        redis_db.commit = mock.Mock(side_effect=commit)
        return redis_db

    @pytest.fixture(scope="function")
    def database(self, mongo_db, redis_db):
        with mock.patch(
            "hyperon_das_atomdb.adapters.redis_mongo_db.RedisMongoDB._connection_mongo_db",
            return_value=mongo_db,
        ), mock.patch(
            "hyperon_das_atomdb.adapters.redis_mongo_db.RedisMongoDB._connection_redis",
            return_value=redis_db,
        ):
            db = RedisMongoDB()
            db.mongo_atoms_collection = mongo_db.collection
            db.mongo_types_collection = mongo_db.collection

            db.mongo_atoms_collection.insert_many(loader("atom_collection_data.json"))
            db.all_mongo_collections = [
                (MongoCollectionNames.ATOMS, db.mongo_atoms_collection),
                (MongoCollectionNames.ATOM_TYPES, db.mongo_types_collection),
            ]
            db.mongo_bulk_insertion_buffer = {
                MongoCollectionNames.ATOMS: tuple([db.mongo_atoms_collection, set()]),
                MongoCollectionNames.ATOM_TYPES: tuple([db.mongo_types_collection, set()]),
            }
        return db

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

    def test_get_node_handle_node_does_not_exist(self, database: RedisMongoDB):
        node_type = "Fake"
        node_name = "Fake2"

        with pytest.raises(AtomDoesNotExist) as exc_info:
            database.get_node_handle(node_type, node_name)
        assert exc_info.type is AtomDoesNotExist
        assert exc_info.value.args[0] == "Nonexistent atom"

    def test_get_link_handle(self, database: RedisMongoDB):
        human = ExpressionHasher.terminal_hash("Concept", "human")
        chimp = ExpressionHasher.terminal_hash("Concept", "chimp")

        resp = database.get_link_handle(link_type="Similarity", target_handles=[human, chimp])

        assert resp is not None

    def test_get_link_handle_link_does_not_exist(self, database: RedisMongoDB):
        brazil = ExpressionHasher.terminal_hash("Concept", "brazil")
        travel = ExpressionHasher.terminal_hash("Concept", "travel")

        with pytest.raises(AtomDoesNotExist) as exc_info:
            database.get_link_handle(link_type="Similarity", target_handles=[brazil, travel])
        assert exc_info.type is AtomDoesNotExist
        assert exc_info.value.args[0] == "Nonexistent atom"

    def test_get_link_targets(self, database: RedisMongoDB):
        human = database.get_node_handle("Concept", "human")
        mammal = database.get_node_handle("Concept", "mammal")
        handle = database.get_link_handle("Inheritance", [human, mammal])
        assert database.get_link_targets(handle)

    def test_get_link_targets_invalid(self, database: RedisMongoDB):
        human = database.get_node_handle("Concept", "human")
        mammal = database.get_node_handle("Concept", "mammal")
        handle = database.get_link_handle("Inheritance", [human, mammal])

        with pytest.raises(ValueError) as exc_info:
            database.get_link_targets(f"{handle}-Fake")
        assert exc_info.type is ValueError
        assert exc_info.value.args[0] == f"Invalid handle: {handle}-Fake"

    def test_get_matched_links_without_wildcard(self, database: RedisMongoDB):
        link_type = "Similarity"
        human = ExpressionHasher.terminal_hash("Concept", "human")
        monkey = ExpressionHasher.terminal_hash("Concept", "monkey")
        link_handle = database.get_link_handle(link_type, [human, monkey])
        expected = {link_handle}
        actual = database.get_matched_links(link_type, [human, monkey])

        assert expected == actual

    def test_get_matched_links_link_equal_wildcard(self, database: RedisMongoDB):
        link_type = "*"
        human = ExpressionHasher.terminal_hash("Concept", "human")
        chimp = ExpressionHasher.terminal_hash("Concept", "chimp")
        expected = {"b5459e299a5c5e8662c427f7e01b3bf1"}
        actual = database.get_matched_links(link_type, [human, chimp])

        assert expected == actual

    def test_get_matched_links_link_diff_wildcard(self, database: RedisMongoDB):
        link_type = "Similarity"
        chimp = ExpressionHasher.terminal_hash("Concept", "chimp")
        expected = {
            "31535ddf214f5b239d3b517823cb8144",
            "b5459e299a5c5e8662c427f7e01b3bf1",
        }
        actual = database.get_matched_links(link_type, ["*", chimp])

        assert expected == actual

    def test_get_matched_links_toplevel_only(self, database: RedisMongoDB):
        expected = {"d542caa94b57219f1e489e3b03be7126"}
        actual = database.get_matched_links("Evaluation", ["*", "*"], toplevel_only=True)
        assert expected == actual
        assert len(actual) == 1

    def test_get_all_nodes(self, database: RedisMongoDB):
        ret = database.get_all_nodes("Concept")
        assert len(ret) == 14
        ret = database.get_all_nodes("Concept", True)
        assert len(ret) == 14
        ret = database.get_all_nodes("ConceptFake")
        assert len(ret) == 0

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

    def test_get_matched_type_template_error(self, database: RedisMongoDB):
        with mock.patch(
            "hyperon_das_atomdb.adapters.redis_mongo_db.RedisMongoDB._build_named_type_hash_template",
            return_value=mock.MagicMock(side_effect=Exception("Test")),
        ):
            with pytest.raises(ValueError) as exc_info:
                database.get_matched_type_template(["Inheritance", "Concept", "Concept"])
            assert exc_info.type is ValueError

    def test_get_matched_type(self, database: RedisMongoDB):
        inheritance = database.get_matched_type("Inheritance")
        similarity = database.get_matched_type("Similarity")
        assert len(inheritance) == 12
        assert len(similarity) == 14

    def test_get_matched_type_toplevel_only(self, database: RedisMongoDB):
        ret = database.get_matched_type("Evaluation")
        assert len(ret) == 2

        ret = database.get_matched_type("Evaluation", toplevel_only=True)
        assert len(ret) == 1

    def test_get_node_name(self, database: RedisMongoDB):
        node_type = "Concept"
        node_name = "monkey"

        handle = database.get_node_handle(node_type, node_name)
        db_name = database.get_node_name(handle)

        assert db_name == node_name

    def test_get_node_name_value_error(self, database: RedisMongoDB):
        with mock.patch(
            "hyperon_das_atomdb.adapters.redis_mongo_db.RedisMongoDB._retrieve_name",
            return_value=None,
        ):
            with pytest.raises(ValueError) as exc_info:
                database.get_node_name("handle")
            assert exc_info.type is ValueError
            assert exc_info.value.args[0] == "Invalid handle: handle"

    def test_get_matched_node_name(self, database: RedisMongoDB):
        expected = sorted(
            [
                database.get_node_handle("Concept", "human"),
                database.get_node_handle("Concept", "mammal"),
                database.get_node_handle("Concept", "animal"),
            ]
        )
        actual = sorted(database.get_node_by_name("Concept", "ma"))

        assert expected == actual
        assert sorted(database.get_node_by_name("blah", "Concept")) == []
        assert sorted(database.get_node_by_name("Concept", "blah")) == []

    def test_get_startswith_node_name(self, database: RedisMongoDB):
        expected = [
            database.get_node_handle("Concept", "mammal"),
        ]
        actual = database.get_node_by_name_starting_with("Concept", "ma")

        assert expected == actual

    def test_get_node_by_field(self, database: RedisMongoDB):
        expected = [
            database.get_node_handle("Concept", "mammal"),
        ]
        actual = database.get_atoms_by_field([{"field": "name", "value": "mammal"}])

        assert expected == actual

    def test_get_atoms_by_index(self, database: RedisMongoDB):
        expected = [
            database.get_node_handle("Concept", "mammal"),
        ]

        result = database.create_field_index("node", fields=["name"])

        with mock.patch(
            "hyperon_das_atomdb.adapters.redis_mongo_db.RedisMongoDB._retrieve_custom_index",
            return_value={"conditionals": {}},
        ):
            cursor, actual = database.get_atoms_by_index(
                result, [{"field": "name", "value": "mammal"}]
            )
        assert cursor == 0
        assert expected[0] == actual[0].handle

    def test_get_node_by_text_field(self, database: RedisMongoDB):
        expected = [
            database.get_node_handle("Concept", "mammal"),
        ]
        actual = database.get_atoms_by_text_field("mammal", "name")

        assert expected == actual

    def test_get_node_type(self, database: RedisMongoDB):
        monkey = database.get_node_handle("Concept", "monkey")
        resp_node = database.get_node_type(monkey)
        assert "Concept" == resp_node

    def test_get_node_type_without_cache(self, database: RedisMongoDB):
        from hyperon_das_atomdb.adapters import redis_mongo_db

        redis_mongo_db.USE_CACHED_NODE_TYPES = False
        monkey = database.get_node_handle("Concept", "monkey")
        resp_node = database.get_node_type(monkey)
        assert "Concept" == resp_node

    def test_get_link_type(self, database: RedisMongoDB):
        human = database.get_node_handle("Concept", "human")
        chimp = database.get_node_handle("Concept", "chimp")
        link_handle = database.get_link_handle("Similarity", [human, chimp])
        resp_link = database.get_link_type(link_handle)
        assert "Similarity" == resp_link

    def test_get_link_type_without_cache(self, database: RedisMongoDB):
        from hyperon_das_atomdb.adapters import redis_mongo_db

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
        all_nodes_before = database.get_all_nodes("Concept")
        database.add_node(dict_to_node_params({"type": "Concept", "name": "lion"}))
        database.commit()
        all_nodes_after = database.get_all_nodes("Concept")
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

        all_nodes_before = database.get_all_nodes("Concept")
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
        all_nodes_after = database.get_all_nodes("Concept")
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

    def test_get_incoming_links(self, database: RedisMongoDB):
        h = database.get_node_handle("Concept", "human")
        m = database.get_node_handle("Concept", "monkey")
        s = database.get_link_handle("Similarity", [h, m])

        links = database.get_incoming_links_atoms(atom_handle=h)
        atom = database.get_atom(handle=s)
        assert atom.handle in [link.handle for link in links]

        links = database.get_incoming_links_atoms(atom_handle=h, targets_document=True)
        assert len(links) > 0
        assert all(isinstance(link, LinkT) for link in links)
        for link in links:
            for a, b in zip(link.targets, link.targets_documents):
                assert a == b.handle

        links = database.get_incoming_links_handles(atom_handle=h)
        assert len(links) > 0
        assert all(isinstance(link, str) for link in links)
        answer = database.redis.smembers(f"incoming_set:{h}")
        assert sorted(links) == sorted(answer)
        assert s in links

        links = database.get_incoming_links_handles(atom_handle=m)
        assert len(links) > 0
        assert all(isinstance(link, str) for link in links)
        answer = database.redis.smembers(f"incoming_set:{m}")
        assert sorted(links) == sorted(answer)

        links = database.get_incoming_links_handles(atom_handle=s)
        assert len(links) == 0
        assert links == []

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

    @pytest.mark.skip(reason="Maybe change the way to handle this test")
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
