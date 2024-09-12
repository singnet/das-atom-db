from unittest import mock
import pytest
from hyperon_das_atomdb.database import AtomDB
from .fixtures import in_memory_db, redis_mongo_db  # noqa: F401


def _check_handle(handle):
    return all((isinstance(handle, str), len(handle) == 32, int(handle, 16)))


class TestDatabase:
    @staticmethod
    def _add_node(db: AtomDB, node_name, node_type, adapter, extra_fields=None):
        node_dict = {"name": node_name, "type": node_type}
        node_dict.update(extra_fields or {})
        node = db.add_node(node_dict)
        if adapter == "redis_mongo_db":
            db.commit()
        return node

    @staticmethod
    def _add_link(db: AtomDB, link_type, dict_targets, adapter, is_top_level=True):
        link = db.add_link({"type": link_type, "targets": dict_targets}, toplevel=is_top_level)
        if adapter != "in_memory_db":
            db.commit()
        return link

    @pytest.mark.parametrize(
        "database,expected",
        [
            ("redis_mongo_db", "1fd600f0fd8a1fab79546a4fc3612df3"),  # Concept, Human
            ("in_memory_db", "1fd600f0fd8a1fab79546a4fc3612df3"),  # Concept, Human
        ],
    )
    def test_node_handle(self, database, expected, request):
        db: AtomDB = request.getfixturevalue(database)
        handle = db.node_handle("Concept", "Human")
        incorrect_handle = db.node_handle("Concept", "human")
        assert handle, incorrect_handle
        assert handle != incorrect_handle
        assert handle == expected
        assert _check_handle(handle)

    @pytest.mark.parametrize(
        "database,expected",
        [
            ("redis_mongo_db", "1fd600f0fd8a1fab79546a4fc3612df3"),  # Concept, Human
            ("in_memory_db", "1fd600f0fd8a1fab79546a4fc3612df3"),  # Concept, Human
        ],
    )
    def test_node_handle_exceptions(self, database, expected, request):
        db: AtomDB = request.getfixturevalue(database)
        # NOTE Should raise ValueError
        with pytest.raises(TypeError):
            db.node_handle([], [])

    @pytest.mark.parametrize(
        "database,expected",
        [
            ("redis_mongo_db", "a9dea78180588431ec64d6bc4872fdbc"),  # Similarity
            ("in_memory_db", "a9dea78180588431ec64d6bc4872fdbc"),  # Similarity
        ],
    )
    def test_link_handle(self, database, expected, request):
        db: AtomDB = request.getfixturevalue(database)
        handle = db.link_handle("Similarity", [])
        assert len(set([db.link_handle("Similarity", f) for f in [[], [], ""]])) == 1
        assert handle
        assert _check_handle(handle)

    @pytest.mark.parametrize(
        "database,expected",
        [
            ("redis_mongo_db", "a9dea78180588431ec64d6bc4872fdbc"),  # Similarity
            ("in_memory_db", "a9dea78180588431ec64d6bc4872fdbc"),  # Similarity
        ],
    )
    def test_link_handle_exceptions(self, database, expected, request):
        db: AtomDB = request.getfixturevalue(database)
        with pytest.raises(TypeError):
            db.link_handle("Similarity", None)
        # NOTE Unreachable
        # TODO: unreachable code must be deleted or fixed to become reachable
        # with pytest.raises(ValueError):
        #     db.link_handle("Similarity", set())

    @pytest.mark.parametrize(
        "database,kwlist",
        [
            ("redis_mongo_db", ["targets_document", "deep_representation"]),
            ("in_memory_db", ["targets_document", "deep_representation"]),
        ],
    )
    def test_reformat_document(self, database, kwlist, request):
        db: AtomDB = request.getfixturevalue(database)
        node_handle = db.add_node({"name": "A", "type": "Test"}).get("handle")
        if database != "in_memory_db":
            db.commit()
        link = {"name": "A", "targets": [node_handle]}
        for kw in kwlist:
            answer = db._reformat_document(link, **{kw: True})
            assert set(answer.keys()) == {"name", "targets", "targets_document"}
            assert len(answer["targets"]) == 1
            assert len(answer["targets_document"]) == 1
            assert answer["name"] == "A"
            assert isinstance(answer["targets"][0], (str if kw == "targets_document" else dict))

    @pytest.mark.parametrize(
        "database,kwlist",
        [
            ("redis_mongo_db", ["targets_document", "deep_representation"]),
            ("in_memory_db", ["targets_document", "deep_representation"]),
        ],
    )
    def test_reformat_document_exceptions(self, database, kwlist, request):
        db: AtomDB = request.getfixturevalue(database)
        link = {"name": "A", "targets": ["test"]}
        for kw in kwlist:
            with pytest.raises(Exception, match="Nonexistent atom"):
                db._reformat_document(link, **{kw: True})

    @pytest.mark.parametrize(
        "database,expected_fields, expected_handle",
        [
            (
                "redis_mongo_db",
                ["handle", "_id", "composite_type_hash", "name", "named_type"],
                "180fed764dbd593f1ea45b63b13d7e69",
            ),
            (
                "in_memory_db",
                ["handle", "_id", "composite_type_hash", "name", "named_type"],
                "180fed764dbd593f1ea45b63b13d7e69",
            ),
        ],
    )
    def test_build_node(self, database, expected_fields, expected_handle, request):
        db: AtomDB = request.getfixturevalue(database)
        handle, node = db._build_node({"type": "Test", "name": "test"})
        assert node
        assert handle == expected_handle
        assert all([k in node for k in expected_fields])
        assert isinstance(node, dict)
        assert _check_handle(handle)

        # Test exception
        with pytest.raises(Exception, match="The \"name\" and \"type\" fields must be sent"):
            db._build_node({})

    @pytest.mark.parametrize(
        "database,expected_fields, expected_handle,is_top_level",
        [
            (
                "redis_mongo_db",
                [
                    "handle",
                    "_id",
                    "composite_type_hash",
                    "named_type_hash",
                    "named_type",
                    "is_toplevel",
                    "targets",
                ],
                "180fed764dbd593f1ea45b63b13d7e69",
                True,
            ),
            (
                "redis_mongo_db",
                [
                    "handle",
                    "_id",
                    "composite_type_hash",
                    "named_type_hash",
                    "named_type",
                    "is_toplevel",
                    "targets",
                ],
                "180fed764dbd593f1ea45b63b13d7e69",
                False,
            ),
            (
                "in_memory_db",
                [
                    "handle",
                    "_id",
                    "composite_type_hash",
                    "named_type_hash",
                    "named_type",
                    "is_toplevel",
                    "targets",
                ],
                "180fed764dbd593f1ea45b63b13d7e69",
                True,
            ),
            (
                "in_memory_db",
                [
                    "handle",
                    "_id",
                    "composite_type_hash",
                    "named_type_hash",
                    "named_type",
                    "is_toplevel",
                    "targets",
                ],
                "180fed764dbd593f1ea45b63b13d7e69",
                False,
            ),
        ],
    )
    def test_build_link(self, database, expected_fields, expected_handle, is_top_level, request):
        db: AtomDB = request.getfixturevalue(database)
        handle, link, targets = db._build_link(
            {"type": "Test", "targets": [{"type": "Test", "name": "test"}]}, is_top_level
        )
        assert expected_handle in targets
        assert all([k in link for k in expected_fields])
        assert link["is_toplevel"] == is_top_level
        assert _check_handle(handle)
        assert isinstance(link, dict)
        assert isinstance(targets, list)

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_build_link_exceptions(self, database, request):
        db: AtomDB = request.getfixturevalue(database)
        with pytest.raises(ValueError, match="The target must be a dictionary"):
            db._build_link({"type": "Test", "targets": [""]})
        with pytest.raises(Exception, match="The \"type\" and \"targets\" fields must be sent"):
            db._build_link({"type": "Test", "targets": None})
        with pytest.raises(Exception, match="The \"type\" and \"targets\" fields must be sent"):
            db._build_link({"type": None, "targets": []})

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_node_exists(self, database, request):
        db: AtomDB = request.getfixturevalue(database)
        db.add_node({"name": "A", "type": "Test"})
        if database != "in_memory_db":
            db.commit()
        no_exists = db.node_exists("Test", "B")
        exists = db.node_exists("Test", "A")
        assert isinstance(no_exists, bool)
        assert isinstance(exists, bool)
        assert not no_exists
        assert exists

    @pytest.mark.parametrize(
        "database,targets",
        [
            ("redis_mongo_db", ["180fed764dbd593f1ea45b63b13d7e69"]),
            ("redis_mongo_db", []),
            ("in_memory_db", ["180fed764dbd593f1ea45b63b13d7e69"]),
            ("in_memory_db", []),
        ],
    )
    def test_link_exists(self, database, targets, request):
        db: AtomDB = request.getfixturevalue(database)
        dict_targets = [{"type": "Test", "name": "test"}] if targets else []
        link = {"type": "Test", "targets": dict_targets}
        db.add_link(link)
        if database != "in_memory_db":
            db.commit()
        no_exists = db.link_exists("Tes", [])
        exists = db.link_exists("Test", targets)

        assert isinstance(no_exists, bool)
        assert isinstance(exists, bool)
        assert not no_exists
        assert exists

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_get_node_handle(self, database, request):
        db: AtomDB = request.getfixturevalue(database)
        expected_node = self._add_node(db, "A", "Test", database)
        node = db.get_node_handle("Test", "A")
        assert node == expected_node["handle"]
        assert _check_handle(node)

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_get_node_handle_exceptions(self, database, request):
        db: AtomDB = request.getfixturevalue(database)
        with pytest.raises(Exception, match="Nonexistent atom"):
            db.get_node_handle("Test", "A")

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_get_node_name(self, database, request):
        db: AtomDB = request.getfixturevalue(database)
        expected_node = self._add_node(db, "A", "Test", database)
        name = db.get_node_name(expected_node["handle"])
        # NOTE all adapters must return the same type
        assert isinstance(name, str)
        assert name == expected_node["name"]

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_get_node_name_exceptions(self, database, request):
        if database == "redis_mongo_db":
            # TODO: fix this
            pytest.skip(
                "ERROR in_memory returns a AtomDoesNotExist exception, redis_mongo returns ValueError. "
                "See https://github.com/singnet/das-atom-db/issues/210"
            )
        db: AtomDB = request.getfixturevalue(database)
        # in memory returns a AtomDoesNotExist exception, redis_mongo returns ValueError
        # TODO: should this be fixed/synced? I mean, make both raise the same exception?
        with pytest.raises(Exception, match="Nonexistent atom"):
            db.get_node_name("error")

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_get_node_type(self, database, request):
        db: AtomDB = request.getfixturevalue(database)
        expected_node = self._add_node(db, "A", "Test", database)
        node_type = db.get_node_type(expected_node["handle"])
        assert isinstance(node_type, str)
        assert node_type == expected_node["named_type"]

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_get_node_type_exceptions(self, database, request):
        db: AtomDB = request.getfixturevalue(database)
        with pytest.raises(Exception, match="Nonexistent atom"):
            db.get_node_type("test")

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_get_node_by_name(self, database, request):
        db: AtomDB = request.getfixturevalue(database)
        expected_nodes = [self._add_node(db, n, "Test", database) for n in {"A", "Aa", "Ac"}]
        not_expected_nodes = [self._add_node(db, n, "Test", database) for n in {"B", "Ba", "Bc"}]
        nodes = db.get_node_by_name("Test", "A")
        not_nodes = db.get_node_by_name("Test", "C")
        assert not_nodes == []
        assert isinstance(nodes, list)
        assert len(nodes) == 3
        assert all(_check_handle(node) for node in nodes)
        assert all(n["handle"] in nodes for n in expected_nodes)
        assert not any(n["handle"] in nodes for n in not_expected_nodes)

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_get_atoms_by_field(self, database, request):
        if database == "in_memory_db":
            # TODO: fix this
            pytest.skip(
                "ERROR Not implemented. See https://github.com/singnet/das-atom-db/issues/210"
            )
        db: AtomDB = request.getfixturevalue(database)
        expected_node = self._add_node(db, "Ac", "Test", database)
        expected_link = self._add_link(db, "Ac", [expected_node], database)
        nodes = db.get_atoms_by_field([{"field": "name", "value": "Ac"}])
        links = db.get_atoms_by_field([{"field": "named_type", "value": "Ac"}])
        assert isinstance(nodes, list)
        assert isinstance(links, list)
        assert all(_check_handle(node) for node in nodes)
        assert all(_check_handle(link) for link in links)
        assert nodes[0] == expected_node["handle"]
        assert links[0] == expected_link["handle"]

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_get_atoms_by_index(self, database, request):
        if database == "in_memory_db":
            # TODO: fix this
            pytest.skip(
                "ERROR Not implemented. See https://github.com/singnet/das-atom-db/issues/210"
            )
        db: AtomDB = request.getfixturevalue(database)
        self._add_node(db, "A", "Test", database, {"value": 3})
        index_id = db.create_field_index(atom_type="node", fields=["value"], named_type="Test")
        cursor, atoms = db.get_atoms_by_index(index_id, [{"field": "value", "value": 3}])
        assert isinstance(cursor, int)
        assert isinstance(atoms, list)
        assert cursor == 0
        assert len(atoms) == 1
        assert all(isinstance(a, dict) for a in atoms)

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_get_atoms_by_index_exceptions(self, database, request):
        if database == "in_memory_db":
            # TODO: fix this
            pytest.skip(
                "ERROR Not implemented. See https://github.com/singnet/das-atom-db/issues/210"
            )
        db: AtomDB = request.getfixturevalue(database)
        with pytest.raises(Exception):
            db.get_atoms_by_index("", [])

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_get_atoms_by_text_field_regex(self, database, request):
        if database == "in_memory_db":
            # TODO: fix this
            pytest.skip(
                "ERROR Not implemented. See https://github.com/singnet/das-atom-db/issues/210"
            )
        db: AtomDB = request.getfixturevalue(database)
        self._add_node(db, "A", "Test", database, {"value": "Test sentence"})
        self._add_link(db, "Test", [], database)
        index_id = db.create_field_index(
            atom_type="node", fields=["value"], named_type="Test", index_type="token_inverted_list"
        )
        atoms = db.get_atoms_by_text_field("Test", "value", text_index_id=index_id)
        assert isinstance(atoms, list)
        assert all(_check_handle(a) for a in atoms)
        assert all(isinstance(a, str) for a in atoms)
        assert len(atoms) == 1

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_get_atoms_by_text_field_text(self, database, request):
        if database == "in_memory_db":
            # TODO: fix this
            pytest.skip(
                "ERROR Not implemented. See https://github.com/singnet/das-atom-db/issues/210"
            )
        db: AtomDB = request.getfixturevalue(database)
        self._add_node(db, "A", "Test", database, {"value": "Test sentence"})
        index_id = db.create_field_index(
            atom_type="node", fields=["value"], named_type="Test", index_type="token_inverted_list"
        )
        with mock.patch(
            "mongomock.filtering._Filterer.apply", return_value=["815212e3d7ac246e70c1744d14a8c402"]
        ):
            atoms = db.get_atoms_by_text_field("Test", text_index_id=index_id)
            assert isinstance(atoms, list)
            assert all(_check_handle(a) for a in atoms)
            assert all(isinstance(a, str) for a in atoms)
            assert len(atoms) == 1

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_get_node_by_name_starting_with(self, database, request):
        if database == "in_memory_db":
            # TODO: fix this
            pytest.skip(
                "ERROR Not implemented. See https://github.com/singnet/das-atom-db/issues/210"
            )
        db: AtomDB = request.getfixturevalue(database)
        node_a = self._add_node(db, "Aaa", "Test", database)
        node_b = self._add_node(db, "Abb", "Test", database)
        self._add_node(db, "Bbb", "Test", database)
        nodes = db.get_node_by_name_starting_with("Test", "A")
        assert isinstance(nodes, list)
        assert all(_check_handle(n) for n in nodes)
        assert all(isinstance(n, str) for n in nodes)
        assert all(handle in nodes for handle in [node_a["handle"], node_b["handle"]])
        assert len(nodes) == 2

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_get_all_nodes(self, database, request):
        db: AtomDB = request.getfixturevalue(database)
        self._add_node(db, "Aaa", "Test", database)
        self._add_node(db, "Abb", "Test", database)
        self._add_node(db, "Bbb", "Test", database)
        nodes = db.get_all_nodes("Test")
        assert isinstance(nodes, list)
        assert all(_check_handle(n) for n in nodes)
        assert all(isinstance(n, str) for n in nodes)
        assert len(nodes) == 3

    @pytest.mark.parametrize(
        "database,params,links_len,cursor_value",
        [  # TODO: differences here must be fixed if possible
            ("redis_mongo_db", {"link_type": "Ac"}, 3, 0),
            ("redis_mongo_db", {"link_type": "Ac", "names": True, "chunk_size": 1}, 3, 0),
            ("redis_mongo_db", {"link_type": "Ac", "names": True, "cursor": 0}, 3, 0),
            ("redis_mongo_db", {"link_type": "Z", "names": True, "cursor": 1}, 0, 0),
            ("redis_mongo_db", {"link_type": "Ac", "chunk_size": 1, "cursor": 0}, 1, 1),
            ("in_memory_db", {"link_type": "Ac"}, 3, None),
            # NOTE should return the same value for the cursor
            ("in_memory_db", {"link_type": "Ac", "names": True, "cursor": 0}, 3, 0),
            ("in_memory_db", {"link_type": "Ac", "names": True, "cursor": 0}, 3, 0),
            ("in_memory_db", {"link_type": "Z", "names": True, "cursor": 1}, 0, 1),
            ("in_memory_db", {"link_type": "Ac", "chunk_size": 1, "cursor": 0}, 3, 0),
        ],
    )
    def test_get_all_links(self, database, params, links_len, cursor_value, request):
        db: AtomDB = request.getfixturevalue(database)
        self._add_link(db, "Ac", [{"name": "A", "type": "A"}], database)
        self._add_link(db, "Ac", [{"name": "B", "type": "B"}], database)
        self._add_link(db, "Ac", [{"name": "C", "type": "C"}], database)
        cursor, links = db.get_all_links(**params)
        assert cursor == cursor_value
        assert isinstance(links, list)
        assert all(_check_handle(link) for link in links)
        assert all(isinstance(link, str) for link in links)
        assert len(links) == links_len

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_get_link_handle(self, database, request):
        db: AtomDB = request.getfixturevalue(database)
        link = self._add_link(db, "Ac", [{"name": "A", "type": "A"}], database)
        handle = db.get_link_handle(link["type"], link["targets"])
        assert _check_handle(handle)

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_get_link_handle_exceptions(self, database, request):
        db: AtomDB = request.getfixturevalue(database)
        with pytest.raises(Exception, match="Nonexistent atom"):
            db.get_link_handle("A", [])

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_get_link_type(self, database, request):
        db: AtomDB = request.getfixturevalue(database)
        link_a = self._add_link(db, "Ac", [{"name": "A", "type": "A"}], database)
        self._add_link(db, "Bc", [{"name": "A", "type": "A"}], database)
        link_type = db.get_link_type(link_a["handle"])
        assert link_type
        assert isinstance(link_type, str)
        assert link_type == link_a["type"]

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_get_link_type_exceptions(self, database, request):
        db: AtomDB = request.getfixturevalue(database)
        with pytest.raises(Exception, match="Nonexistent atom"):
            db.get_link_type("")

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_get_link_targets(self, database, request):
        db: AtomDB = request.getfixturevalue(database)
        link_a = self._add_link(db, "Ac", [{"name": "A", "type": "A"}], database)
        targets = db.get_link_targets(link_a["handle"])
        assert isinstance(targets, list)
        assert len(targets) == 1
        assert all(_check_handle(t) for t in targets)
        assert all(isinstance(t, str) for t in targets)
        assert targets == link_a["targets"]

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_is_ordered(self, database, request):
        db: AtomDB = request.getfixturevalue(database)
        link_a = self._add_link(db, "Ac", [{"name": "A", "type": "A"}], database)
        # NOTE just retrieves the link ...
        assert db.is_ordered(link_a["handle"])

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_is_ordered_no_handle(self, database, request):
        if database == "redis_mongo_db":
            # TODO: fix this
            pytest.skip(
                "ERROR redis_mongo_db is raising ValueError exception, should be AtomDoesNotExist. "
                "See https://github.com/singnet/das-atom-db/issues/210"
            )
        db: AtomDB = request.getfixturevalue(database)
        with pytest.raises(Exception, match="Nonexistent atom"):
            db.is_ordered("handle")

    @pytest.mark.parametrize(
        "database,params,links_len",
        [  # TODO: differences here must be fixed if possible
            ("redis_mongo_db", {}, 3),
            ("redis_mongo_db", {"handles_only": True}, 3),
            ("redis_mongo_db", {"no_target_format": True}, 3),
            # NOTE should return None on all cases
            ("in_memory_db", {}, 3),
            ("in_memory_db", {"handles_only": True}, 3),
            ("in_memory_db", {"no_target_format": True}, 3),
        ],
    )
    def test_get_incoming_links(self, database, params, links_len, request):
        db: AtomDB = request.getfixturevalue(database)
        node_a = self._add_node(db, "Aaa", "Test", database)
        self._add_link(db, "Aa", [node_a], database)
        self._add_link(db, "Ab", [node_a], database)
        self._add_link(db, "Ac", [node_a], database)
        links = db.get_incoming_links(node_a["handle"], **params)
        assert len(links) == links_len
        assert all(
            [
                _check_handle(link if params.get("handles_only") else link["handle"])
                for link in links
            ]
        )

    @pytest.mark.parametrize(
        "database,params,links_len",
        [  # TODO: differences here must be fixed if possible
            ("redis_mongo_db", {}, 1),
            ("redis_mongo_db", {"toplevel_only": True}, 1),
            # ("redis_mongo_db", {"link_type": "NoTopLevel" , "toplevel_only": True}, 0), # doesn"t work
            # Note returning different values
            # ("in_memory_db", {"link_type": "*", "toplevel_only": True}, 3),
            # ("redis_mongo_db", {"link_type": "*", "toplevel_only": True}, 0),
            # ("redis_mongo_db", {"link_type": "*"}, 3), # should return 3
            ("redis_mongo_db", {"target_handles": ["*"]}, 1),
            ("redis_mongo_db", {"handles_only": True}, 1),
            ("redis_mongo_db", {"no_target_format": True}, 1),
            ("in_memory_db", {}, 1),
            ("in_memory_db", {"toplevel_only": True}, 1),
            # ("in_memory_db", {"link_type": "NoTopLevel", "toplevel_only": True}, 0), # doesn"t work
            ("in_memory_db", {"link_type": "*"}, 3),
            ("in_memory_db", {"toplevel_only": True}, 1),
            ("in_memory_db", {"target_handles": ["*"]}, 1),
            ("in_memory_db", {"handles_only": True}, 1),
            ("in_memory_db", {"no_target_format": True}, 1),
        ],
    )
    def test_get_matched_links(self, database, params, links_len, request):
        db: AtomDB = request.getfixturevalue(database)
        node_a = self._add_node(db, "Aaa", "Test", database)
        link_a = self._add_link(db, "Aa", [node_a], database)
        _ = self._add_link(db, "NoTopLevel", [node_a], database, is_top_level=False)
        _ = self._add_link(db, "Ac", [node_a], database)
        params["link_type"] = link_a["type"] if not params.get("link_type") else params["link_type"]
        params["target_handles"] = (
            link_a["targets"] if not params.get("target_handles") else params["target_handles"]
        )
        links = db.get_matched_links(**params)
        assert len(links) == links_len
        if all(isinstance(link, tuple) for link in links):
            for link in links:
                while link:
                    assert _check_handle(link[0])
                    link = link[1] if len(link) > 1 else None
        else:
            assert all([_check_handle(link) for link in links])

    @pytest.mark.parametrize(
        "database,params,links_len",
        [  # TODO: differences here must be fixed if possible
            ("redis_mongo_db", {"link_type": "Z", "target_handles": []}, 0),
            # ("redis_mongo_db", {"link_type": "*", "target_handles": ["*", "*"], "toplevel_only": True}, 0),
            # ("in_memory_db", {"link_type": "*", "target_handles": ["*", "*"]}, 0),
            # ("in_memory_db", {"link_type": "*", "target_handles": ["*", "*"], "toplevel_only": True}, 0),
        ],
    )
    def test_get_matched_no_links(self, database, params, links_len, request):
        db: AtomDB = request.getfixturevalue(database)
        self._add_node(db, "Aaa", "Test", database)
        links = db.get_matched_links(**params)
        assert len(links) == links_len

    @pytest.mark.parametrize(
        "database,params,links_len,is_top_level",
        [  # TODO: differences here must be fixed if possible
            ("redis_mongo_db", {}, 1, True),
            ("redis_mongo_db", {}, 1, False),
            ("redis_mongo_db", {"toplevel_only": True}, 0, False),
            ("in_memory_db", {}, 1, True),
            ("in_memory_db", {}, 1, False),
            ("in_memory_db", {"toplevel_only": True}, 0, False),
            # NOTE should return None or same as redis_mongo
        ],
    )
    def test_get_matched_type_template(
        self, database, params, links_len, is_top_level, request
    ):
        db: AtomDB = request.getfixturevalue(database)
        node_a = self._add_node(db, "Aaa", "Test", database)
        node_b = self._add_node(db, "Bbb", "Test", database)
        link_a = self._add_link(db, "Aa", [node_a, node_b], database, is_top_level=is_top_level)
        links = db.get_matched_type_template(["Aa", "Test", "Test"], **params)
        assert len(links) == links_len
        if len(links) > 0:
            for link in links:
                assert _check_handle(link[0])
                assert link[0] == link_a["handle"]
                assert all(t in link[1] for t in link_a["targets"])
                assert all(_check_handle(t) for t in link[1])

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_get_matched_type(self, database, request):
        if database == "redis_mongo_db":
            # TODO: fix this
            pytest.skip(
                "ERROR redis_mongo_db is returning more values. "
                "See https://github.com/singnet/das-atom-db/issues/210"
            )
        db: AtomDB = request.getfixturevalue(database)
        link_a = self._add_link(db, "Aa", [], database)
        self._add_link(db, "Ab", [], database)
        links = db.get_matched_type(link_a["type"])
        assert len(links) == 1
        if len(links) > 0:
            for link in links:
                assert _check_handle(link[0])
                assert link[0] == link_a["handle"]
                assert all(t in link[1] for t in link_a["targets"])
                assert all(_check_handle(t) for t in link[1])

    @pytest.mark.parametrize(
        "database,params,top_level,n_links,n_nodes",
        [  # TODO: differences here must be fixed if possible
            ("redis_mongo_db", {}, True, 1, 1),
            ("redis_mongo_db", {"no_target_format": True}, False, 1, 1),
            # ("redis_mongo_db", {"targets_document": True}, False, 1, 1),# breaks when is a node
            ("redis_mongo_db", {"deep_representation": True}, False, 1, 1),
            ("in_memory_db", {}, True, 1, 1),
            ("in_memory_db", {"no_target_format": True}, False, 0, 1),
            # ("in_memory_db", {"targets_document": True}, False, 0, 1),# breaks when is a node
            ("in_memory_db", {"deep_representation": True}, False, 0, 1),
        ],
    )
    def test_get_atom_node(self, database, params, top_level, n_links, n_nodes, request):
        db: AtomDB = request.getfixturevalue(database)
        node_a = self._add_node(db, "Aaa", "Test", database)
        atom_n = db.get_atom(node_a["handle"], **params)
        assert atom_n
        assert atom_n["handle"] == node_a["handle"]
        assert _check_handle(atom_n["handle"])

    @pytest.mark.parametrize(
        "database,params,top_level,n_links,n_nodes",
        [
            ("redis_mongo_db", {}, True, 1, 1),
            ("redis_mongo_db", {"no_target_format": True}, False, 1, 1),
            ("redis_mongo_db", {"targets_document": True}, False, 1, 1),
            ("redis_mongo_db", {"deep_representation": True}, False, 1, 1),
            ("in_memory_db", {}, True, 1, 1),
            ("in_memory_db", {"no_target_format": True}, False, 0, 1),
            ("in_memory_db", {"targets_document": True}, False, 0, 1),
            ("in_memory_db", {"deep_representation": True}, False, 0, 1),
        ],
    )
    def test_get_atom_link(self, database, params, top_level, n_links, n_nodes, request):
        db: AtomDB = request.getfixturevalue(database)
        link_a = self._add_link(db, "Aa", [], database, is_top_level=top_level)
        atom_l = db.get_atom(link_a["handle"], **params)
        assert atom_l
        assert atom_l["handle"] == link_a["handle"]
        assert _check_handle(atom_l["handle"])

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test__get_atom(self, database, request):
        db: AtomDB = request.getfixturevalue(database)
        node_a = self._add_node(db, "Aaa", "Test", database)
        link_a = self._add_link(db, "Aa", [], database)
        node = db._get_atom(node_a["handle"])
        link = db._get_atom(link_a["handle"])
        assert node, link

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test__get_atom_none(self, database, request):
        db: AtomDB = request.getfixturevalue(database)
        node = db._get_atom("handle")
        link = db._get_atom("handle")
        assert node is None
        assert link is None

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_get_atom_type(self, database, request):
        db: AtomDB = request.getfixturevalue(database)
        node_a = self._add_node(db, "Aaa", "Test", database)
        link_a = self._add_link(db, "Test", [], database)
        atom_type_node = db.get_atom_type(node_a["handle"])
        atom_type_link = db.get_atom_type(link_a["handle"])
        assert isinstance(atom_type_node, str)
        assert isinstance(atom_type_link, str)
        assert atom_type_node == atom_type_link

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_get_atom_type_none(self, database, request):
        db: AtomDB = request.getfixturevalue(database)
        atom_type_node = db.get_atom_type("handle")
        atom_type_link = db.get_atom_type("handle")
        assert atom_type_node is None
        assert atom_type_link is None

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_get_atom_as_dict(self, database, request):
        db: AtomDB = request.getfixturevalue(database)
        node_a = self._add_node(db, "Aaa", "Test", database)
        link_a = self._add_link(db, "Test", [], database)
        atom_node = db.get_atom_as_dict(node_a["handle"])
        atom_link = db.get_atom_as_dict(link_a["handle"])
        assert isinstance(atom_node, dict)
        assert isinstance(atom_link, dict)

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_get_atom_as_dict_none(self, database, request):
        if database == "in_memory_db":
            # TODO: fix this
            pytest.skip(
                "ERROR in_memory raises exception, they should return the same result/exception. "
                "See https://github.com/singnet/das-atom-db/issues/210"
            )
        db: AtomDB = request.getfixturevalue(database)
        atom_node = db.get_atom_as_dict("handle")
        atom_link = db.get_atom_as_dict("handle")
        assert atom_node is None
        assert atom_link is None

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_get_atom_as_dict_exceptions(self, database, request):
        if database == "redis_mongo_db":
            # TODO: fix this
            pytest.skip(
                "ERROR redis_mongo_db doesn't raises exception, they should return the same result/exception. "
                "See https://github.com/singnet/das-atom-db/issues/210"
            )
        db: AtomDB = request.getfixturevalue(database)
        with pytest.raises(Exception, match="Nonexistent atom"):
            db.get_atom_as_dict("handle")
        with pytest.raises(Exception, match="Nonexistent atom"):
            db.get_atom_as_dict("handle")

    @pytest.mark.parametrize(
        "database,params",
        [  # TODO: differences here must be fixed if possible
            ("redis_mongo_db", {}),
            ("redis_mongo_db", {"precise": True}),
            ("redis_mongo_db", {"precise": False}),
            # NOTE should return the same value if the arg precise is set
            ("in_memory_db", {}),
            ("in_memory_db", {"precise": True}),
            ("in_memory_db", {"precise": False}),
        ],
    )
    def test_count_atoms(self, database, params, request):
        db: AtomDB = request.getfixturevalue(database)
        self._add_node(db, "Aaa", "Test", database)
        self._add_link(db, "Test", [], database)
        atoms_count = db.count_atoms(params)
        assert atoms_count
        assert isinstance(atoms_count, dict)
        assert isinstance(atoms_count["atom_count"], int)
        assert atoms_count["atom_count"] == 2
        if params.get("precise", False):
            assert isinstance(atoms_count["node_count"], int)
            assert isinstance(atoms_count["link_count"], int)
            assert atoms_count["node_count"] == 1
            assert atoms_count["link_count"] == 1

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_clear_database(self, database, request):
        db: AtomDB = request.getfixturevalue(database)
        self._add_node(db, "Aaa", "Test", database)
        self._add_link(db, "Test", [], database)
        assert db.count_atoms()["atom_count"] == 2
        db.clear_database()
        assert db.count_atoms()["atom_count"] == 0

    @pytest.mark.parametrize(
        "database,node",
        [
            ("redis_mongo_db", {"name": "A", "type": "A"}),
            ("in_memory_db", {"name": "A", "type": "A"}),
        ],
    )
    def test_add_node(self, database, node, request):
        db: AtomDB = request.getfixturevalue(database)
        if database == "redis_mongo_db":
            db.mongo_bulk_insertion_limit = 1
        node = db.add_node(node)
        count = db.count_atoms()
        assert node
        assert count["atom_count"] == 1
        assert isinstance(node, dict)

    @pytest.mark.parametrize(
        "database,node",
        [
            ("redis_mongo_db", {"name": "AAAA", "type": "A"}),
        ],
    )
    def test_add_node_discard(self, database, node, request):
        db: AtomDB = request.getfixturevalue(database)
        db.mongo_bulk_insertion_limit = 1
        db.max_mongo_db_document_size = 1
        node = db.add_node(node)
        count = db.count_atoms()
        assert node is None
        assert count["atom_count"] == 0

    @pytest.mark.parametrize(
        "database,node",
        [  # TODO: differences here must be fixed if possible
            ("redis_mongo_db", {}),
            # NOTE it"s not breaking, should break?
            # ("redis_mongo_db", {"name": "A", "type": "A", "handle": ""}),
            # ("redis_mongo_db", {"name": "A", "type": "A", "_id": ""}),
            # ("redis_mongo_db", {"name": "A", "type": "A", "composite_type_hash": ""}),
            # ("redis_mongo_db", {"name": "A", "type": "A", "named_type": ""}),
            ("in_memory_db", {}),
            # NOTE it"s not breaking, should break?
            # ("in_memory_db", {"name": "A", "type": "A", "handle": ""}),
            # ("in_memory_db", {"name": "A", "type": "A", "_id": ""}),
            # ("in_memory_db", {"name": "A", "type": "A", "composite_type_hash": ""}),
            # ("in_memory_db", {"name": "A", "type": "A", "named_type": ""}),
        ],
    )
    def test_add_node_exceptions(self, database, node, request):
        db: AtomDB = request.getfixturevalue(database)
        with pytest.raises(Exception):
            db.add_node(node)

    @pytest.mark.parametrize(
        "database,params,expected_count,top_level",
        [
            ("redis_mongo_db", {"type": "A", "targets": [{"name": "A", "type": "A"}]}, 2, True),
            ("redis_mongo_db", {"type": "A", "targets": []}, 1, True),
            ("in_memory_db", {"type": "A", "targets": [{"name": "A", "type": "A"}]}, 2, True),
            ("in_memory_db", {"type": "A", "targets": []}, 1, True),
        ],
    )
    def test_add_link(self, database, params, expected_count, top_level, request):
        db: AtomDB = request.getfixturevalue(database)
        if database == "redis_mongo_db":
            db.mongo_bulk_insertion_limit = 1
        link = db.add_link(params, top_level)
        count = db.count_atoms()
        assert link
        assert count["atom_count"] == expected_count
        assert isinstance(link, dict)

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_reindex(self, database, request):
        if database == "in_memory_db":
            # TODO: fix this
            pytest.skip(
                "ERROR Not implemented. See https://github.com/singnet/das-atom-db/issues/210"
            )
        db: AtomDB = request.getfixturevalue(database)
        self._add_node(db, "Aaa", "Test", database)
        self._add_link(db, "Test", [], database)
        db.reindex()

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_delete_atom(self, database, request):
        if database == "in_memory_db":
            # TODO: fix this
            pytest.skip(
                "ERROR Atom not in incoming_set. See https://github.com/singnet/das-atom-db/issues/210"
            )
        db: AtomDB = request.getfixturevalue(database)
        node_a = self._add_node(db, "Aaa", "Test", database)
        link_a = self._add_link(db, "Test", [], database)
        count = db.count_atoms({"precise": True})
        assert count["atom_count"] == 2
        assert count["node_count"] == 1
        assert count["link_count"] == 1
        db.delete_atom(node_a["handle"])
        count = db.count_atoms({"precise": True})
        assert count["atom_count"] == 1
        assert count["node_count"] == 0
        assert count["link_count"] == 1
        db.delete_atom(link_a["handle"])
        count = db.count_atoms({"precise": True})
        assert count["atom_count"] == 0
        assert count["node_count"] == 0
        assert count["link_count"] == 0

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_delete_atom_exceptions(self, database, request):
        if database == "in_memory_db":
            # TODO: fix this
            pytest.skip(
                "ERROR Atom not in incoming_set. See https://github.com/singnet/das-atom-db/issues/210"
            )
        db: AtomDB = request.getfixturevalue(database)
        with pytest.raises(Exception):
            db.delete_atom("handle")

    @pytest.mark.parametrize(
        "database,params",
        [
            ("redis_mongo_db", {"atom_type": "A", "fields": ["value"]}),
            ("redis_mongo_db", {"atom_type": "A", "fields": ["value"], "named_type": "A"}),
            (
                "redis_mongo_db",
                {"atom_type": "A", "fields": ["value"], "composite_type": ["value", "side"]},
            ),
            (
                "redis_mongo_db",
                {
                    "atom_type": "A",
                    "fields": ["value"],
                    "named_type": "A",
                    "index_type": "binary_tree",
                },
            ),
            (
                "redis_mongo_db",
                {
                    "atom_type": "A",
                    "fields": ["value", "side"],
                    "composite_type": ["value", "side"],
                    "index_type": "binary_tree",
                },
            ),
            (
                "redis_mongo_db",
                {
                    "atom_type": "A",
                    "fields": ["value", "side"],
                    "composite_type": ["value", "side"],
                    "index_type": "token_inverted_list",
                },
            ),
            (
                "redis_mongo_db",
                {
                    "atom_type": "A",
                    "fields": ["value"],
                    "named_type": "A",
                    "index_type": "token_inverted_list",
                },
            ),
            (
                "redis_mongo_db",
                {
                    "atom_type": "A",
                    "fields": ["value"],
                    "named_type": "A",
                    "index_type": "binary_tree",
                },
            ),
            (
                "redis_mongo_db",
                {
                    "atom_type": "A",
                    "fields": ["value"],
                    "composite_type": ["value", "side"],
                    "index_type": "token_inverted_list",
                },
            ),
            (
                "redis_mongo_db",
                {
                    "atom_type": "A",
                    "fields": ["value"],
                    "composite_type": ["value", "side"],
                    "index_type": "binary_tree",
                },
            ),
            ("in_memory_db", {}),
        ],
    )
    def test_create_field_index(self, database, params, request):
        if database == "in_memory_db":
            # TODO: fix this
            pytest.skip(
                "ERROR Not implemented on in_memory_db. See https://github.com/singnet/das-atom-db/issues/210"
            )
        db: AtomDB = request.getfixturevalue(database)
        index_id = db.create_field_index(**params)
        assert index_id
        assert isinstance(index_id, str)
        # check if creating a duplicated index breaks
        index_id2 = db.create_field_index(**params)
        assert isinstance(index_id2, str)
        assert index_id2 == index_id

    @pytest.mark.parametrize(
        "database,params",
        [
            ("redis_mongo_db", {"atom_type": "A", "fields": []}),
            (
                "redis_mongo_db",
                {
                    "atom_type": "A",
                    "fields": ["value"],
                    "named_type": "A",
                    "composite_type": ["value", "side"],
                },
            ),
            ("in_memory_db", {}),
        ],
    )
    def test_create_field_index_value_error(self, database, params, request):
        if database == "in_memory_db":
            # TODO: fix this
            pytest.skip(
                "ERROR Not implemented on in_memory_db. See https://github.com/singnet/das-atom-db/issues/210"
            )
        db: AtomDB = request.getfixturevalue(database)
        with pytest.raises(ValueError):
            db.create_field_index(**params)

    # TODO: fix this or delete
    # @pytest.mark.parametrize("database,params", [
    #     ("redis_mongo_db", {"atom_type": "A", "fields": ["side"], "index_type": "wrong_type"}),
    #     ("in_memory_db", {})
    # ])
    # def test_create_field_index_mongo_error(self, database, params, request):
    #     if database == "in_memory_db":
    #         # TODO: fix this
    #         pytest.skip("ERROR Not implemented on in_memory_db. See https://github.com/singnet/das-atom-db/issues/210")
    #
    #     db: AtomDB = request.getfixturevalue(database)
    #     # with pytest.raises(ValueError):
    #     db.create_field_index(**params)

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_bulk_insert(self, database, request):
        if database == "redis_mongo_db":
            # TODO: fix this
            pytest.skip(
                "ERROR redis_mongo_db is not updating targets. See https://github.com/singnet/das-atom-db/issues/210"
            )
        db: AtomDB = request.getfixturevalue(database)
        node_a = self._add_node(db, "Aaa", "Test", database)
        link_a = self._add_link(db, "Test", [{"name": "A", "type": "A"}], database)
        node_a["name"] = "B"
        link_a["targets"] = [node_a["handle"]]
        db.bulk_insert([node_a, link_a])
        count = db.count_atoms({"precise": True})
        node = db.get_atom(node_a["handle"])
        link = db.get_atom(link_a["handle"])
        assert count["atom_count"] == 3
        assert count["node_count"] == 2
        assert count["link_count"] == 1
        assert node["name"] == "B"
        assert link["targets"] == [node_a["handle"]]

    # Note no exception is raised if error
    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_bulk_insert_exceptions(self, database, request):
        db: AtomDB = request.getfixturevalue(database)
        node_a = db._build_node({"name": "A", "type": "A"})
        link_a = db._build_link({"targets": [], "type": "A"})
        with pytest.raises(Exception):
            db.bulk_insert([node_a, link_a])
            # TODO: fix this
            pytest.skip(
                "ERROR should raise an exception. See https://github.com/singnet/das-atom-db/issues/210"
            )

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_retrieve_all_atoms(self, database, request):
        db: AtomDB = request.getfixturevalue(database)
        node_a = self._add_node(db, "Aaa", "Test", database)
        link_a = self._add_link(db, "Test", [{"name": "A", "type": "A"}], database)
        node_b = db.get_atom(db.get_node_handle(node_type="A", node_name="A"))
        atoms = db.retrieve_all_atoms()
        assert isinstance(atoms, list)
        assert len(atoms) == 3
        assert all(a in atoms for a in [node_a, link_a, node_b])

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_commit(self, database, request):
        if database == "in_memory_db":
            # TODO: fix this
            pytest.skip(
                "ERROR Not implemented on in_memory_db. See https://github.com/singnet/das-atom-db/issues/210"
            )
        db: AtomDB = request.getfixturevalue(database)
        db.add_node({"name": "A", "type": "Test"})
        db.add_link({"type": "Test", "targets": []})
        db.commit()
        count = db.count_atoms({"precise": True})
        assert count["atom_count"] == 2
        assert count["node_count"] == 1
        assert count["link_count"] == 1

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test_commit_buffer(self, database, request):
        if database == "in_memory_db":
            # TODO: fix this
            pytest.skip(
                "ERROR Not implemented on in_memory_db. See https://github.com/singnet/das-atom-db/issues/210"
            )
        db: AtomDB = request.getfixturevalue(database)
        node_a = self._add_node(db, "Aaa", "Test", database)
        link_a = self._add_link(db, "Test", [{"name": "A", "type": "A"}], database)
        node_a["name"] = "B"
        link_a["targets"] = [node_a["handle"]]
        db.commit(buffer=[node_a, link_a])
        count = db.count_atoms({"precise": True})
        assert count["atom_count"] == 3
        assert count["node_count"] == 2
        assert count["link_count"] == 1

    @pytest.mark.parametrize("database", ["redis_mongo_db"])
    def test_commit_buffer_exception(self, database, request):
        db: AtomDB = request.getfixturevalue(database)
        with pytest.raises(Exception):
            db.commit(buffer=[{"name": "A", "type": "A"}])

        with pytest.raises(Exception, match="Failed to commit Atom Types"):
            db.mongo_bulk_insertion_buffer = {"atom_types": ("a", "a")}
            db.commit()
