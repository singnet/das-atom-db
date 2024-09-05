from tests.unit.fixtures import in_memory_db
from tests.unit.test_database import _check_handle
from hyperon_das_atomdb.adapters.ram_only import InMemoryDB

class TestRamOnlyExtra:

    def test__build_atom_type_key_hash(self, in_memory_db):
        db: InMemoryDB = in_memory_db
        hash = db._build_atom_type_key_hash("A")
        assert _check_handle(hash)
        assert hash == "2c832bdcd9d74bf961205676d861540a"

    def test__delete_atom_type(self, in_memory_db):
        db: InMemoryDB = in_memory_db
        node = db.add_node({"name": "A", "type": "A"})
        assert  len(db.all_named_types) == 1
        assert node["named_type"] in db.all_named_types
        db._delete_atom_type("A")
        assert len(db.all_named_types) == 0
        assert node["named_type"] not in db.all_named_types


    def test__update_atom_indexes(self, in_memory_db):
        db: InMemoryDB = in_memory_db
        node = db.add_node({"name": "A", "type": "A"})
        db._update_atom_indexes([node])
        assert len(db.all_named_types) == 1





