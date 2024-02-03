import os
import subprocess

import pytest

from hyperon_das_atomdb.adapters import RedisMongoDB
from hyperon_das_atomdb.database import WILDCARD

from .animals_kb import (
    animal,
    chimp,
    ent,
    human,
    inheritance,
    inheritance_docs,
    mammal,
    monkey,
    node_docs,
    rhino,
    similarity_docs,
)

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
    subprocess.call(
        ["bash", f"{scripts_path}/redis-up.sh", redis_port], stdout=devnull, stderr=devnull
    )
    subprocess.call(
        ["bash", f"{scripts_path}/mongo-up.sh", mongo_port], stdout=devnull, stderr=devnull
    )


def _db_down():
    subprocess.call(
        ["bash", f"{scripts_path}/redis-down.sh", redis_port], stdout=devnull, stderr=devnull
    )
    subprocess.call(
        ["bash", f"{scripts_path}/mongo-down.sh", mongo_port], stdout=devnull, stderr=devnull
    )


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
        for node in node_docs.values():
            db.add_node(node)
        for link in inheritance_docs.values():
            db.add_link(link)
        for link in similarity_docs.values():
            db.add_link(link)

    def _connect_db(self):
        db = RedisMongoDB(
            mongo_port=mongo_port,
            mongo_username='dbadmin',
            mongo_password='dassecret',
            redis_port=redis_port,
            redis_cluster=False,
            redis_ssl=False,
        )
        return db

    def test_redis_retrieve(self):
        _db_up()
        db = self._connect_db()
        self._add_atoms(db)
        assert db.count_atoms() == (0, 0)
        db.commit()
        assert db.count_atoms() == (14, 26)

        assert db._retrieve_name(human) == "human"

        links = db._retrieve_incoming_set(mammal)
        assert len(links) == 5
        for link in links:
            outgoing = db._retrieve_outgoing_set(link)
            assert outgoing in [
                [human, mammal],
                [monkey, mammal],
                [chimp, mammal],
                [rhino, mammal],
                [mammal, animal],
            ]

        templates = db._retrieve_template("41c082428b28d7e9ea96160f7fd614ad")
        assert len(templates) == 12
        assert tuple(
            [
                "116df61c01859c710d178ba14a483509",
                tuple(["c1db9b517073e51eb7ef6fed608ec204", "b99ae727c787f1b13b452fd4c9ce1b9a"]),
            ]
        )
        assert tuple(
            [
                "1c3bf151ea200b2d9e088a1178d060cb",
                tuple(["bdfe4e7a431f73386f37c6448afe5840", "0a32b476852eeb954979b87f5f6cb7af"]),
            ]
        )
        assert tuple(
            [
                "4120e428ab0fa162a04328e5217912ff",
                tuple(["bb34ce95f161a6b37ff54b3d4c817857", "0a32b476852eeb954979b87f5f6cb7af"]),
            ]
        )
        assert tuple(
            [
                "75756335011dcedb71a0d9a7bd2da9e8",
                tuple(["5b34c54bee150c04f9fa584b899dc030", "bdfe4e7a431f73386f37c6448afe5840"]),
            ]
        )
        assert tuple(
            [
                "906fa505ae3bc6336d80a5f9aaa47b3b",
                tuple(["d03e59654221c1e8fcda404fd5c8d6cb", "08126b066d32ee37743e255a2558cccd"]),
            ]
        )
        assert tuple(
            [
                "959924e3aab197af80a84c1ab261fd65",
                tuple(["08126b066d32ee37743e255a2558cccd", "b99ae727c787f1b13b452fd4c9ce1b9a"]),
            ]
        )
        assert tuple(
            [
                "b0f428929706d1d991e4d712ad08f9ab",
                tuple(["b99ae727c787f1b13b452fd4c9ce1b9a", "0a32b476852eeb954979b87f5f6cb7af"]),
            ]
        )
        assert tuple(
            [
                "c93e1e758c53912638438e2a7d7f7b7f",
                tuple(["af12f10f9ae2002a1607ba0b47ba8407", "bdfe4e7a431f73386f37c6448afe5840"]),
            ]
        )
        assert tuple(
            [
                "e4685d56969398253b6f77efd21dc347",
                tuple(["b94941d8cd1c0ee4ad3dd3dcab52b964", "80aff30094874e75028033a38ce677bb"]),
            ]
        )
        assert tuple(
            [
                "ee1c03e6d1f104ccd811cfbba018451a",
                tuple(["4e8e26e3276af8a5c2ac2cc2dc95c6d2", "80aff30094874e75028033a38ce677bb"]),
            ]
        )
        assert tuple(
            [
                "f31dfe97db782e8cec26de18dddf8965",
                tuple(["1cdffc6b0b89ff41d68bec237481d1e1", "bdfe4e7a431f73386f37c6448afe5840"]),
            ]
        )
        assert tuple(
            [
                "fbf03d17d6a40feff828a3f2c6e86f05",
                tuple(["99d18c702e813b07260baf577c60c455", "bdfe4e7a431f73386f37c6448afe5840"]),
            ]
        )

        patterns = db._retrieve_pattern("112002ff70ea491aad735f978e9d95f5")
        assert len(patterns) == 4
        assert (
            tuple(
                [
                    "75756335011dcedb71a0d9a7bd2da9e8",
                    tuple(["5b34c54bee150c04f9fa584b899dc030", "bdfe4e7a431f73386f37c6448afe5840"]),
                ]
            )
            in patterns
        )
        assert (
            tuple(
                [
                    "fbf03d17d6a40feff828a3f2c6e86f05",
                    tuple(["99d18c702e813b07260baf577c60c455", "bdfe4e7a431f73386f37c6448afe5840"]),
                ]
            )
            in patterns
        )
        assert (
            tuple(
                [
                    "f31dfe97db782e8cec26de18dddf8965",
                    tuple(["1cdffc6b0b89ff41d68bec237481d1e1", "bdfe4e7a431f73386f37c6448afe5840"]),
                ]
            )
            in patterns
        )
        assert (
            tuple(
                [
                    "c93e1e758c53912638438e2a7d7f7b7f",
                    tuple(["af12f10f9ae2002a1607ba0b47ba8407", "bdfe4e7a431f73386f37c6448afe5840"]),
                ]
            )
            in patterns
        )

        _db_down()

    def _check_basic_patterns(self, db):
        assert sorted(
            [
                answer[1][0]
                for answer in db.get_matched_links(
                    "Inheritance", [WILDCARD, db.node_handle("Concept", "mammal")]
                )
            ]
        ) == sorted([human, monkey, chimp, rhino])
        assert sorted(
            [
                answer[1][1]
                for answer in db.get_matched_links(
                    "Inheritance", [db.node_handle("Concept", "mammal"), WILDCARD]
                )
            ]
        ) == sorted([animal])
        assert sorted(
            [
                answer[1][0]
                for answer in db.get_matched_links(
                    "Similarity", [WILDCARD, db.node_handle("Concept", "human")]
                )
            ]
        ) == sorted([monkey, chimp, ent])
        assert sorted(
            [
                answer[1][1]
                for answer in db.get_matched_links(
                    "Similarity", [db.node_handle("Concept", "human"), WILDCARD]
                )
            ]
        ) == sorted([monkey, chimp, ent])

    def test_patterns(self):
        _db_up()
        db = self._connect_db()
        self._add_atoms(db)
        db.commit()
        assert db.count_atoms() == (14, 26)
        self._check_basic_patterns(db)
        _db_down()

    def test_commit(self):
        _db_up()
        db = self._connect_db()
        assert db.count_atoms() == (0, 0)
        self._add_atoms(db)
        assert db.count_atoms() == (0, 0)
        db.commit()
        assert db.count_atoms() == (14, 26)
        assert sorted(
            [
                answer[1][0]
                for answer in db.get_matched_links(
                    "Inheritance", [WILDCARD, db.node_handle("Concept", "mammal")]
                )
            ]
        ) == sorted([human, monkey, chimp, rhino])
        assert db.get_atom(human)["name"] == node_docs[human]["name"]
        link_pre = db.get_atom(inheritance[human][mammal])
        assert "strength" not in link_pre
        assert link_pre["named_type"] == "Inheritance"
        assert link_pre["targets"] == [human, mammal]
        link_new = inheritance_docs[inheritance[human][mammal]].copy()
        link_new["strength"] = 1.0
        db.add_link(link_new)
        db.add_link(
            {
                "type": "Inheritance",
                "targets": [
                    {"type": "Concept", "name": "dog"},
                    {"type": "Concept", "name": "mammal"},
                ],
            }
        )
        db.commit()
        assert db.count_atoms() == (15, 27)
        link_pos = db.get_atom(inheritance[human][mammal])
        assert link_pos["named_type"] == "Inheritance"
        assert link_pos["targets"] == [human, mammal]
        assert "strength" in link_pos
        assert link_pos["strength"] == 1.0
        dog = db.node_handle("Concept", "dog")
        assert db.get_node_name(dog) == "dog"
        new_link_handle = db.get_link_handle("Inheritance", [dog, mammal])
        new_link = db.get_atom(new_link_handle)
        assert db.get_link_targets(new_link_handle) == new_link["targets"]
        assert sorted(
            [
                answer[1][0]
                for answer in db.get_matched_links(
                    "Inheritance", [WILDCARD, db.node_handle("Concept", "mammal")]
                )
            ]
        ) == sorted([human, monkey, chimp, rhino, dog])
        _db_down()

    def test_reindex(self):
        _db_up()
        db = self._connect_db()
        self._add_atoms(db)
        db.commit()
        db.reindex()
        assert db.count_atoms() == (14, 26)
        self._check_basic_patterns(db)
        _db_down()

    def test_delete_atom(self):
        from hyperon_das_atomdb.database import AtomDB

        _db_up()
        db = self._connect_db()
        # db.add_node({'type':'Concept','name':'human'})
        self._add_atoms(db)
        db.commit()
        try:
            # db.add_node({'type':'Concept','name':'monkey'})
            # db.delete_atom(handle=AtomDB.link_handle('Similarityx', [human, monkey]))
            db.delete_atom(handle=human)
        except Exception as e:
            _db_down()
        _db_down()
