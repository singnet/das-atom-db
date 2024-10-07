import pytest

from hyperon_das_atomdb.database import AtomDB

from .fixtures import in_memory_db, redis_mongo_db  # noqa: F401
from .test_database_public_methods import add_link, add_node, check_handle


class TestDatabasePrivateMethods:
    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test__get_atom(self, database, request):
        db: AtomDB = request.getfixturevalue(database)
        node_a = add_node(db, "Aaa", "Test", database)
        link_a = add_link(db, "Aa", [], database)
        node = db._get_atom(node_a["handle"])
        link = db._get_atom(link_a["handle"])
        assert node, link

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test__get_atom_none(self, database, request):
        db: AtomDB = request.getfixturevalue(database)
        node = db._get_atom("handle")
        assert node is None

    @pytest.mark.parametrize(
        "database,kwlist",
        [
            ("redis_mongo_db", ["targets_document", "deep_representation"]),
            ("in_memory_db", ["targets_document", "deep_representation"]),
        ],
    )
    def test__reformat_document(self, database, kwlist, request):
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
    def test__reformat_document_exceptions(self, database, kwlist, request):
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
    def test__build_node(self, database, expected_fields, expected_handle, request):
        db: AtomDB = request.getfixturevalue(database)
        handle, node = db._build_node({"type": "Test", "name": "test"})
        assert node
        assert handle == expected_handle
        assert all([k in node for k in expected_fields])
        assert isinstance(node, dict)
        assert check_handle(handle)

        # Test exception
        with pytest.raises(Exception, match="The \"name\" and \"type\" fields must be sent"):
            db._build_node({})

    @pytest.mark.parametrize(
        "database,expected_fields, expected_handle,is_toplevel",
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
    def test__build_link(self, database, expected_fields, expected_handle, is_toplevel, request):
        db: AtomDB = request.getfixturevalue(database)
        handle, link, targets = db._build_link(
            {"type": "Test", "targets": [{"type": "Test", "name": "test"}]}, is_toplevel
        )
        assert expected_handle in targets
        assert all([k in link for k in expected_fields])
        assert link["is_toplevel"] == is_toplevel
        assert check_handle(handle)
        assert isinstance(link, dict)
        assert isinstance(targets, list)

    @pytest.mark.parametrize("database", ["redis_mongo_db", "in_memory_db"])
    def test__build_link_exceptions(self, database, request):
        db: AtomDB = request.getfixturevalue(database)
        with pytest.raises(ValueError, match="The target must be a dictionary"):
            db._build_link({"type": "Test", "targets": [""]})
        with pytest.raises(Exception, match="The \"type\" and \"targets\" fields must be sent"):
            db._build_link({"type": "Test", "targets": None})
        with pytest.raises(Exception, match="The \"type\" and \"targets\" fields must be sent"):
            db._build_link({"type": None, "targets": []})
