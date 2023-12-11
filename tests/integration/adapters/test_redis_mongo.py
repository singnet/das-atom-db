import subprocess
import pytest
import os
from hyperon_das_atomdb.adapters import RedisMongoDB

redis_port = "15926"
mongo_port = "15927"
scripts_path = "./tests/integration/scripts/"
devnull = open(os.devnull, 'w')

DAS_MONGODB_HOSTNAME = os.environ.get("DAS_MONGODB_HOSTNAME")
DAS_MONGODB_PORT = os.environ.get("DAS_MONGODB_PORT")
DAS_MONGODB_USERNAME = os.environ.get("DAS_MONGODB_USERNAME")
DAS_MONGODB_PASSWORD = os.environ.get("DAS_MONGODB_PASSWORD")
DAS_REDIS_HOSTNAME = os.environ.get("DAS_REDIS_HOSTNAME")
DAS_REDIS_PORT = os.environ.get("DAS_REDIS_PORT")
DAS_REDIS_USERNAME = os.environ.get("DAS_REDIS_USERNAME")
DAS_REDIS_PASSWORD = os.environ.get("DAS_REDIS_PASSWORD")
DAS_USE_REDIS_CLUSTER = os.environ.get("DAS_USE_REDIS_CLUSTER")
DAS_USE_REDIS_SSL = os.environ.get("DAS_USE_REDIS_SSL")

os.environ["DAS_MONGODB_HOSTNAME"] = "localhost"
os.environ["DAS_MONGODB_PORT"] = mongo_port
os.environ["DAS_MONGODB_USERNAME"] = "dbadmin"
os.environ["DAS_MONGODB_PASSWORD"] = "dassecret"
os.environ["DAS_REDIS_HOSTNAME"] = "localhost"
os.environ["DAS_REDIS_PORT"] = redis_port
os.environ["DAS_REDIS_USERNAME"] = ""
os.environ["DAS_REDIS_PASSWORD"] = ""
os.environ["DAS_USE_REDIS_CLUSTER"] = "false"
os.environ["DAS_USE_REDIS_SSL"] = "false"

def _db_up():
    subprocess.call(["bash", f"{scripts_path}/redis-up.sh", redis_port], stdout=devnull, stderr=devnull)
    subprocess.call(["bash", f"{scripts_path}/mongo-up.sh", mongo_port], stdout=devnull, stderr=devnull)

def _db_down():
    subprocess.call(["bash", f"{scripts_path}/redis-down.sh", redis_port], stdout=devnull, stderr=devnull)
    subprocess.call(["bash", f"{scripts_path}/mongo-down.sh", mongo_port], stdout=devnull, stderr=devnull)

@pytest.fixture(scope="session", autouse=True)
def cleanup(request):

    def restore_environment():
        if DAS_MONGODB_HOSTNAME:
            os.environ["DAS_MONGODB_HOSTNAME"] = DAS_MONGODB_HOSTNAME
        if DAS_MONGODB_PORT:
            os.environ["DAS_MONGODB_PORT"] = DAS_MONGODB_PORT
        if DAS_MONGODB_USERNAME:
            os.environ["DAS_MONGODB_USERNAME"] = DAS_MONGODB_USERNAME
        if DAS_MONGODB_PASSWORD:
            os.environ["DAS_MONGODB_PASSWORD"] = DAS_MONGODB_PASSWORD
        if DAS_REDIS_HOSTNAME:
            os.environ["DAS_REDIS_HOSTNAME"] = DAS_REDIS_HOSTNAME
        if DAS_REDIS_PORT:
            os.environ["DAS_REDIS_PORT"] = DAS_REDIS_PORT
        if DAS_REDIS_USERNAME:
            os.environ["DAS_REDIS_USERNAME"] = DAS_REDIS_USERNAME
        if DAS_REDIS_PASSWORD:
            os.environ["DAS_REDIS_PASSWORD"] = DAS_REDIS_PASSWORD
        if DAS_USE_REDIS_CLUSTER:
            os.environ["DAS_USE_REDIS_CLUSTER"] = DAS_USE_REDIS_CLUSTER
        if DAS_USE_REDIS_SSL:
            os.environ["DAS_USE_REDIS_SSL"] = DAS_USE_REDIS_SSL

    def enforce_containers_removal():
        _db_down()

    request.addfinalizer(restore_environment)
    request.addfinalizer(enforce_containers_removal)

class TestRedisMongo:

    def _add_atoms(self, db: RedisMongoDB):
        db.add_node({"type": "Concept", "name": "human"})
        db.add_node({"type": "Concept", "name": "monkey"})
        db.add_node({"type": "Concept", "name": "chimp"})
        db.add_node({"type": "Concept", "name": "mammal"})
        db.add_node({"type": "Concept", "name": "reptile"})
        db.add_node({"type": "Concept", "name": "snake"})
        db.add_node({"type": "Concept", "name": "dinosaur"})
        db.add_node({"type": "Concept", "name": "triceratops"})
        db.add_node({"type": "Concept", "name": "earthworm"})
        db.add_node({"type": "Concept", "name": "rhino"})
        db.add_node({"type": "Concept", "name": "vine"})
        db.add_node({"type": "Concept", "name": "ent"})
        db.add_node({"type": "Concept", "name": "animal"})
        db.add_node({"type": "Concept", "name": "plant"})

        db.add_link({ "type": "Similarity", "targets": [ {"type": "Concept", "name": "human"}, {"type": "Concept", "name": "monkey"},]}) 
        db.add_link({ "type": "Similarity", "targets": [ {"type": "Concept", "name": "human"}, {"type": "Concept", "name": "chimp"},]}) 
        db.add_link({ "type": "Similarity", "targets": [ {"type": "Concept", "name": "chimp"}, {"type": "Concept", "name": "monkey"},]}) 
        db.add_link({ "type": "Similarity", "targets": [ {"type": "Concept", "name": "snake"}, {"type": "Concept", "name": "earthworm"},]}) 
        db.add_link({ "type": "Similarity", "targets": [ {"type": "Concept", "name": "rhino"}, {"type": "Concept", "name": "triceratops"},]}) 
        db.add_link({ "type": "Similarity", "targets": [ {"type": "Concept", "name": "snake"}, {"type": "Concept", "name": "vine"},]}) 
        db.add_link({ "type": "Similarity", "targets": [ {"type": "Concept", "name": "human"}, {"type": "Concept", "name": "ent"},]}) 
        db.add_link({ "type": "Similarity", "targets": [ {"type": "Concept", "name": "monkey"}, {"type": "Concept", "name": "human"},]}) 
        db.add_link({ "type": "Similarity", "targets": [ {"type": "Concept", "name": "chimp"}, {"type": "Concept", "name": "human"},]}) 
        db.add_link({ "type": "Similarity", "targets": [ {"type": "Concept", "name": "monkey"}, {"type": "Concept", "name": "chimp"},]}) 
        db.add_link({ "type": "Similarity", "targets": [ {"type": "Concept", "name": "earthworm"}, {"type": "Concept", "name": "snake"},]}) 
        db.add_link({ "type": "Similarity", "targets": [ {"type": "Concept", "name": "triceratops"}, {"type": "Concept", "name": "rhino"},]}) 
        db.add_link({ "type": "Similarity", "targets": [ {"type": "Concept", "name": "vine"}, {"type": "Concept", "name": "snake"},]}) 
        db.add_link({ "type": "Similarity", "targets": [ {"type": "Concept", "name": "ent"}, {"type": "Concept", "name": "human"},]}) 
        db.add_link({ "type": "Inheritance", "targets": [ {"type": "Concept", "name": "human"}, {"type": "Concept", "name": "mammal"},]}) 
        db.add_link({ "type": "Inheritance", "targets": [ {"type": "Concept", "name": "monkey"}, {"type": "Concept", "name": "mammal"},]}) 
        db.add_link({ "type": "Inheritance", "targets": [ {"type": "Concept", "name": "chimp"}, {"type": "Concept", "name": "mammal"},]}) 
        db.add_link({ "type": "Inheritance", "targets": [ {"type": "Concept", "name": "mammal"}, {"type": "Concept", "name": "animal"},]}) 
        db.add_link({ "type": "Inheritance", "targets": [ {"type": "Concept", "name": "reptile"}, {"type": "Concept", "name": "animal"},]}) 
        db.add_link({ "type": "Inheritance", "targets": [ {"type": "Concept", "name": "snake"}, {"type": "Concept", "name": "reptile"},]}) 
        db.add_link({ "type": "Inheritance", "targets": [ {"type": "Concept", "name": "dinosaur"}, {"type": "Concept", "name": "reptile"},]}) 
        db.add_link({ "type": "Inheritance", "targets": [ {"type": "Concept", "name": "triceratops"}, {"type": "Concept", "name": "dinosaur"},]}) 
        db.add_link({ "type": "Inheritance", "targets": [ {"type": "Concept", "name": "earthworm"}, {"type": "Concept", "name": "animal"},]}) 
        db.add_link({ "type": "Inheritance", "targets": [ {"type": "Concept", "name": "rhino"}, {"type": "Concept", "name": "mammal"},]}) 
        db.add_link({ "type": "Inheritance", "targets": [ {"type": "Concept", "name": "vine"}, {"type": "Concept", "name": "plant"},]}) 
        db.add_link({ "type": "Inheritance", "targets": [ {"type": "Concept", "name": "ent"}, {"type": "Concept", "name": "plant"},]})

    def test_commit(self):
        _db_up()
        db = RedisMongoDB(
            mongo_port=mongo_port,
            mongo_username='dbadmin',
            mongo_password='dassecret',
            redis_port=redis_port,
            redis_cluster=False,
            redis_ssl=False
        )
        assert db.count_atoms() == (0, 0)
        self._add_atoms(db)
        assert db.count_atoms() == (0, 0)
        db.commit()
        assert db.count_atoms() == (14, 26)
        _db_down()
