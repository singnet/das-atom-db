import os
import random
import string
import subprocess

import pytest

from hyperon_das_atomdb.adapters import RedisMongoDB
from hyperon_das_atomdb.database import WILDCARD, AtomDB
from hyperon_das_atomdb.utils import ExpressionHasher

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

    def test_metta_mapping(self):
        _db_up()
        db = RedisMongoDB(
            mongo_port=mongo_port,
            mongo_username='dbadmin',
            mongo_password='dassecret',
            redis_port=redis_port,
            redis_cluster=False,
            redis_ssl=False,
            use_metta_mapping=True,
        )
        self._add_atoms(db)
        assert db.count_atoms() == (0, 0)
        db.commit()
        assert db.count_atoms() == (14, 26)
        self._check_basic_patterns(db, toplevel_only=True)
        db = RedisMongoDB(
            mongo_port=mongo_port,
            mongo_username='dbadmin',
            mongo_password='dassecret',
            redis_port=redis_port,
            redis_cluster=False,
            redis_ssl=False,
            use_metta_mapping=False,
        )
        assert db.count_atoms() == (14, 26)
        with pytest.raises(Exception):
            self._check_basic_patterns(db, toplevel_only=True)
        _db_down()

    def test_redis_retrieve(self):
        _db_up()
        db = self._connect_db()
        self._add_atoms(db)
        assert db.count_atoms() == (0, 0)
        db.commit()
        assert db.count_atoms() == (14, 26)

        assert db._retrieve_name(human) == "human"

        _, links = db._retrieve_incoming_set(mammal)
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

        _, templates = db._retrieve_template("41c082428b28d7e9ea96160f7fd614ad")
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

        _, patterns = db._retrieve_pattern("112002ff70ea491aad735f978e9d95f5")
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

    def _check_basic_patterns(self, db, toplevel_only=False):
        assert sorted(
            [
                answer[1][0]
                for answer in db.get_matched_links(
                    "Inheritance",
                    [WILDCARD, db.node_handle("Concept", "mammal")],
                    toplevel_only=toplevel_only,
                )
            ]
        ) == sorted([human, monkey, chimp, rhino])
        assert sorted(
            [
                answer[1][1]
                for answer in db.get_matched_links(
                    "Inheritance",
                    [db.node_handle("Concept", "mammal"), WILDCARD],
                    toplevel_only=toplevel_only,
                )
            ]
        ) == sorted([animal])
        assert sorted(
            [
                answer[1][0]
                for answer in db.get_matched_links(
                    "Similarity",
                    [WILDCARD, db.node_handle("Concept", "human")],
                    toplevel_only=toplevel_only,
                )
            ]
        ) == sorted([monkey, chimp, ent])
        assert sorted(
            [
                answer[1][1]
                for answer in db.get_matched_links(
                    "Similarity",
                    [db.node_handle("Concept", "human"), WILDCARD],
                    toplevel_only=toplevel_only,
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
        def _add_all_links():
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
            db.commit()

        def _add_nested_links():
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
            db.commit()

        def _check_asserts():
            assert db.count_atoms() == (3, 2)
            assert db._retrieve_name(cat_handle) == 'cat'
            assert db._retrieve_name(dog_handle) == 'dog'
            assert db._retrieve_name(mammal_handle) == 'mammal'
            assert db._retrieve_incoming_set(cat_handle)[1] == [inheritance_cat_mammal_handle]
            assert db._retrieve_incoming_set(dog_handle)[1] == [inheritance_dog_mammal_handle]
            assert sorted(db._retrieve_incoming_set(mammal_handle)[1]) == sorted(
                [inheritance_cat_mammal_handle, inheritance_dog_mammal_handle]
            )
            assert db._retrieve_incoming_set(inheritance_cat_mammal_handle)[1] == []
            assert db._retrieve_incoming_set(inheritance_dog_mammal_handle)[1] == []
            assert sorted(db._retrieve_outgoing_set(inheritance_cat_mammal_handle)) == sorted(
                [cat_handle, mammal_handle]
            )
            assert sorted(db._retrieve_outgoing_set(inheritance_dog_mammal_handle)) == sorted(
                [dog_handle, mammal_handle]
            )
            assert sorted(db._retrieve_template('e40489cd1e7102e35469c937e05c8bba')[1]) == sorted(
                [
                    (inheritance_dog_mammal_handle, (dog_handle, mammal_handle)),
                    (inheritance_cat_mammal_handle, (cat_handle, mammal_handle)),
                ]
            )
            assert sorted(db._retrieve_template('41c082428b28d7e9ea96160f7fd614ad')[1]) == sorted(
                [
                    (inheritance_dog_mammal_handle, (dog_handle, mammal_handle)),
                    (inheritance_cat_mammal_handle, (cat_handle, mammal_handle)),
                ]
            )

            links = [
                db.get_atom(inheritance_cat_mammal_handle),
                db.get_atom(inheritance_dog_mammal_handle),
            ]
            keys = set()
            for link in links:
                for template in db.default_pattern_index_templates:
                    key = db._apply_index_template(
                        template, link['named_type_hash'], link['targets'], len(link['targets'])
                    )
                    keys.add(key)
            assert set([p.decode() for p in db.redis.keys('patterns:*')]) == keys

            assert sorted(db._retrieve_pattern('112002ff70ea491aad735f978e9d95f5')[1]) == sorted(
                [
                    (inheritance_dog_mammal_handle, (dog_handle, mammal_handle)),
                    (inheritance_cat_mammal_handle, (cat_handle, mammal_handle)),
                ]
            )
            assert sorted(db._retrieve_pattern('6e644e70a9fe3145c88b5b6261af5754')[1]) == sorted(
                [
                    (inheritance_dog_mammal_handle, (dog_handle, mammal_handle)),
                    (inheritance_cat_mammal_handle, (cat_handle, mammal_handle)),
                ]
            )
            assert sorted(db._retrieve_pattern('5dd515aa7a451276feac4f8b9d84ae91')[1]) == sorted(
                [
                    (inheritance_dog_mammal_handle, (dog_handle, mammal_handle)),
                    (inheritance_cat_mammal_handle, (cat_handle, mammal_handle)),
                ]
            )
            assert sorted(db._retrieve_pattern('7ead6cfa03894c62761162b7603aa885')[1]) == sorted(
                [
                    (inheritance_dog_mammal_handle, (dog_handle, mammal_handle)),
                    (inheritance_cat_mammal_handle, (cat_handle, mammal_handle)),
                ]
            )
            assert db._retrieve_pattern('e55007a8477a4e6bf4fec76e4ffd7e10')[1] == [
                (inheritance_dog_mammal_handle, (dog_handle, mammal_handle))
            ]
            assert db._retrieve_pattern('23dc149b3218d166a14730db55249126')[1] == [
                (inheritance_dog_mammal_handle, (dog_handle, mammal_handle))
            ]
            assert db._retrieve_pattern('399751d7319f9061d97cd1d75728b66b')[1] == [
                (inheritance_dog_mammal_handle, (dog_handle, mammal_handle))
            ]
            assert db._retrieve_pattern('d0eaae6eaf750e821b26642cef32becf')[1] == [
                (inheritance_dog_mammal_handle, (dog_handle, mammal_handle))
            ]
            assert db._retrieve_pattern('f29daafee640d91aa7091e44551fc74a')[1] == [
                (inheritance_cat_mammal_handle, (cat_handle, mammal_handle))
            ]
            assert db._retrieve_pattern('a11d7cbf62bc544f75702b5fb6a514ff')[1] == [
                (inheritance_cat_mammal_handle, (cat_handle, mammal_handle))
            ]
            assert db._retrieve_pattern('3ba42d45a50c89600d92fb3f1a46c1b5')[1] == [
                (inheritance_cat_mammal_handle, (cat_handle, mammal_handle))
            ]
            assert db._retrieve_pattern('9fb71ffef74a1a98eb0bfce7aa3d54e3')[1] == [
                (inheritance_cat_mammal_handle, (cat_handle, mammal_handle))
            ]

        def _check_asserts_2():
            assert db.count_atoms() == (3, 0)
            assert db._retrieve_name(cat_handle) == 'cat'
            assert db._retrieve_name(dog_handle) == 'dog'
            assert db._retrieve_name(mammal_handle) == 'mammal'
            assert db._retrieve_incoming_set(cat_handle)[1] == []
            assert db._retrieve_incoming_set(dog_handle)[1] == []
            assert db._retrieve_incoming_set(mammal_handle)[1] == []
            assert db._retrieve_outgoing_set(inheritance_cat_mammal_handle) == []
            assert db._retrieve_outgoing_set(inheritance_dog_mammal_handle) == []
            assert db._retrieve_template('e40489cd1e7102e35469c937e05c8bba')[1] == []
            assert db._retrieve_template('41c082428b28d7e9ea96160f7fd614ad')[1] == []
            assert db._retrieve_pattern('112002ff70ea491aad735f978e9d95f5')[1] == []
            assert db._retrieve_pattern('6e644e70a9fe3145c88b5b6261af5754')[1] == []
            assert db._retrieve_pattern('5dd515aa7a451276feac4f8b9d84ae91')[1] == []
            assert db._retrieve_pattern('7ead6cfa03894c62761162b7603aa885')[1] == []
            assert db._retrieve_pattern('e55007a8477a4e6bf4fec76e4ffd7e10')[1] == []
            assert db._retrieve_pattern('23dc149b3218d166a14730db55249126')[1] == []
            assert db._retrieve_pattern('399751d7319f9061d97cd1d75728b66b')[1] == []
            assert db._retrieve_pattern('d0eaae6eaf750e821b26642cef32becf')[1] == []
            assert db._retrieve_pattern('f29daafee640d91aa7091e44551fc74a')[1] == []
            assert db._retrieve_pattern('a11d7cbf62bc544f75702b5fb6a514ff')[1] == []
            assert db._retrieve_pattern('3ba42d45a50c89600d92fb3f1a46c1b5')[1] == []
            assert db._retrieve_pattern('9fb71ffef74a1a98eb0bfce7aa3d54e3')[1] == []

        def _check_asserts_3():
            assert db.count_atoms() == (2, 0)
            assert db._retrieve_name(cat_handle) == 'cat'
            assert db._retrieve_name(dog_handle) == 'dog'
            assert db._retrieve_name(mammal_handle) is None
            assert db._retrieve_incoming_set(cat_handle)[1] == []
            assert db._retrieve_incoming_set(dog_handle)[1] == []
            assert db._retrieve_incoming_set(mammal_handle)[1] == []
            assert db._retrieve_outgoing_set(inheritance_cat_mammal_handle) == []
            assert db._retrieve_outgoing_set(inheritance_dog_mammal_handle) == []
            assert db._retrieve_template('e40489cd1e7102e35469c937e05c8bba')[1] == []
            assert db._retrieve_template('41c082428b28d7e9ea96160f7fd614ad')[1] == []
            assert db._retrieve_pattern('112002ff70ea491aad735f978e9d95f5')[1] == []
            assert db._retrieve_pattern('6e644e70a9fe3145c88b5b6261af5754')[1] == []
            assert db._retrieve_pattern('5dd515aa7a451276feac4f8b9d84ae91')[1] == []
            assert db._retrieve_pattern('7ead6cfa03894c62761162b7603aa885')[1] == []
            assert db._retrieve_pattern('e55007a8477a4e6bf4fec76e4ffd7e10')[1] == []
            assert db._retrieve_pattern('23dc149b3218d166a14730db55249126')[1] == []
            assert db._retrieve_pattern('399751d7319f9061d97cd1d75728b66b')[1] == []
            assert db._retrieve_pattern('d0eaae6eaf750e821b26642cef32becf')[1] == []
            assert db._retrieve_pattern('f29daafee640d91aa7091e44551fc74a')[1] == []
            assert db._retrieve_pattern('a11d7cbf62bc544f75702b5fb6a514ff')[1] == []
            assert db._retrieve_pattern('3ba42d45a50c89600d92fb3f1a46c1b5')[1] == []
            assert db._retrieve_pattern('9fb71ffef74a1a98eb0bfce7aa3d54e3')[1] == []

        def _check_asserts_4():
            assert db.count_atoms() == (2, 1)
            assert db._retrieve_name(cat_handle) is None
            assert db._retrieve_name(dog_handle) == 'dog'
            assert db._retrieve_name(mammal_handle) == 'mammal'
            assert db._retrieve_incoming_set(cat_handle)[1] == []
            assert db._retrieve_incoming_set(dog_handle)[1] == [inheritance_dog_mammal_handle]
            assert db._retrieve_incoming_set(mammal_handle)[1] == [inheritance_dog_mammal_handle]
            assert db._retrieve_outgoing_set(inheritance_cat_mammal_handle) == []
            assert sorted(db._retrieve_outgoing_set(inheritance_dog_mammal_handle)) == sorted(
                [dog_handle, mammal_handle]
            )
            assert db._retrieve_template('e40489cd1e7102e35469c937e05c8bba')[1] == [
                (inheritance_dog_mammal_handle, (dog_handle, mammal_handle))
            ]
            assert db._retrieve_template('41c082428b28d7e9ea96160f7fd614ad')[1] == [
                (inheritance_dog_mammal_handle, (dog_handle, mammal_handle))
            ]
            assert db._retrieve_pattern('112002ff70ea491aad735f978e9d95f5')[1] == [
                (inheritance_dog_mammal_handle, (dog_handle, mammal_handle)),
            ]
            assert db._retrieve_pattern('6e644e70a9fe3145c88b5b6261af5754')[1] == [
                (inheritance_dog_mammal_handle, (dog_handle, mammal_handle)),
            ]
            assert db._retrieve_pattern('5dd515aa7a451276feac4f8b9d84ae91')[1] == [
                (inheritance_dog_mammal_handle, (dog_handle, mammal_handle)),
            ]
            assert db._retrieve_pattern('7ead6cfa03894c62761162b7603aa885')[1] == [
                (inheritance_dog_mammal_handle, (dog_handle, mammal_handle)),
            ]
            assert db._retrieve_pattern('e55007a8477a4e6bf4fec76e4ffd7e10')[1] == [
                (inheritance_dog_mammal_handle, (dog_handle, mammal_handle))
            ]
            assert db._retrieve_pattern('23dc149b3218d166a14730db55249126')[1] == [
                (inheritance_dog_mammal_handle, (dog_handle, mammal_handle))
            ]
            assert db._retrieve_pattern('399751d7319f9061d97cd1d75728b66b')[1] == [
                (inheritance_dog_mammal_handle, (dog_handle, mammal_handle))
            ]
            assert db._retrieve_pattern('d0eaae6eaf750e821b26642cef32becf')[1] == [
                (inheritance_dog_mammal_handle, (dog_handle, mammal_handle))
            ]
            assert db._retrieve_pattern('f29daafee640d91aa7091e44551fc74a')[1] == []
            assert db._retrieve_pattern('a11d7cbf62bc544f75702b5fb6a514ff')[1] == []
            assert db._retrieve_pattern('3ba42d45a50c89600d92fb3f1a46c1b5')[1] == []
            assert db._retrieve_pattern('9fb71ffef74a1a98eb0bfce7aa3d54e3')[1] == []

        def _check_asserts_5():
            assert db.count_atoms() == (2, 1)
            assert db._retrieve_name(cat_handle) == 'cat'
            assert db._retrieve_name(dog_handle) is None
            assert db._retrieve_name(mammal_handle) == 'mammal'
            assert db._retrieve_incoming_set(cat_handle)[1] == [inheritance_cat_mammal_handle]
            assert db._retrieve_incoming_set(dog_handle)[1] == []
            assert db._retrieve_incoming_set(mammal_handle)[1] == [inheritance_cat_mammal_handle]
            assert sorted(db._retrieve_outgoing_set(inheritance_cat_mammal_handle)) == sorted(
                [cat_handle, mammal_handle]
            )
            assert db._retrieve_outgoing_set(inheritance_dog_mammal_handle) == []
            assert db._retrieve_template('e40489cd1e7102e35469c937e05c8bba')[1] == [
                (inheritance_cat_mammal_handle, (cat_handle, mammal_handle))
            ]
            assert db._retrieve_template('41c082428b28d7e9ea96160f7fd614ad')[1] == [
                (inheritance_cat_mammal_handle, (cat_handle, mammal_handle))
            ]
            assert db._retrieve_pattern('112002ff70ea491aad735f978e9d95f5')[1] == [
                (inheritance_cat_mammal_handle, (cat_handle, mammal_handle))
            ]
            assert db._retrieve_pattern('6e644e70a9fe3145c88b5b6261af5754')[1] == [
                (inheritance_cat_mammal_handle, (cat_handle, mammal_handle))
            ]
            assert db._retrieve_pattern('5dd515aa7a451276feac4f8b9d84ae91')[1] == [
                (inheritance_cat_mammal_handle, (cat_handle, mammal_handle))
            ]
            assert db._retrieve_pattern('7ead6cfa03894c62761162b7603aa885')[1] == [
                (inheritance_cat_mammal_handle, (cat_handle, mammal_handle))
            ]
            assert db._retrieve_pattern('e55007a8477a4e6bf4fec76e4ffd7e10')[1] == []
            assert db._retrieve_pattern('23dc149b3218d166a14730db55249126')[1] == []
            assert db._retrieve_pattern('399751d7319f9061d97cd1d75728b66b')[1] == []
            assert db._retrieve_pattern('d0eaae6eaf750e821b26642cef32becf')[1] == []
            assert db._retrieve_pattern('f29daafee640d91aa7091e44551fc74a')[1] == [
                (inheritance_cat_mammal_handle, (cat_handle, mammal_handle))
            ]
            assert db._retrieve_pattern('a11d7cbf62bc544f75702b5fb6a514ff')[1] == [
                (inheritance_cat_mammal_handle, (cat_handle, mammal_handle))
            ]
            assert db._retrieve_pattern('3ba42d45a50c89600d92fb3f1a46c1b5')[1] == [
                (inheritance_cat_mammal_handle, (cat_handle, mammal_handle))
            ]
            assert db._retrieve_pattern('9fb71ffef74a1a98eb0bfce7aa3d54e3')[1] == [
                (inheritance_cat_mammal_handle, (cat_handle, mammal_handle))
            ]

        _db_up()

        db = self._connect_db()

        cat_handle = AtomDB.node_handle('Concept', 'cat')
        dog_handle = AtomDB.node_handle('Concept', 'dog')
        mammal_handle = AtomDB.node_handle('Concept', 'mammal')
        inheritance_cat_mammal_handle = AtomDB.link_handle(
            'Inheritance', [cat_handle, mammal_handle]
        )
        inheritance_dog_mammal_handle = AtomDB.link_handle(
            'Inheritance', [dog_handle, mammal_handle]
        )

        assert db.count_atoms() == (0, 0)

        _add_all_links()
        _check_asserts()

        db.delete_atom(inheritance_cat_mammal_handle)
        db.delete_atom(inheritance_dog_mammal_handle)
        _check_asserts_2()

        _add_all_links()
        db.delete_atom(mammal_handle)
        _check_asserts_3()

        _add_all_links()
        db.delete_atom(cat_handle)
        _check_asserts_4()

        db.add_link(
            {
                'type': 'Inheritance',
                'targets': [
                    {'type': 'Concept', 'name': 'cat'},
                    {'type': 'Concept', 'name': 'mammal'},
                ],
            }
        )
        db.commit()

        db.delete_atom(dog_handle)
        _check_asserts_5()

        db.clear_database()

        _add_nested_links()
        db.delete_atom(inheritance_cat_mammal_handle)
        _check_asserts_2()

        _db_down()

    def test_retrieve_members_with_pagination(self):
        _db_up()
        db = self._connect_db()

        def _add_links():
            for name in [
                ''.join([random.choice(string.ascii_lowercase) for i in range(5)])
                for j in range(1000)
            ]:
                db.add_link(
                    {
                        'type': 'Inheritance',
                        'targets': [
                            {'type': 'Concept', 'name': 'snet'},
                            {'type': 'Concept', 'name': name},
                        ],
                    }
                )
            db.commit()

        def _asserts_incoming_set():
            _, all_links_1 = db._retrieve_incoming_set(snet_handle)

            cursor = '0'
            all_links_2 = []
            while cursor != 0:
                cursor, links = db._retrieve_incoming_set(
                    snet_handle, cursor=cursor, chunk_size=100
                )
                all_links_2.extend(links)

            assert sorted(all_links_1) == sorted(all_links_2)

        def _asserts_templates():
            _, all_templates_1 = db._retrieve_template(type_hash)

            cursor = '0'
            all_templates_2 = []
            while cursor != 0:
                cursor, templates = db._retrieve_template(type_hash, cursor=cursor, chunk_size=100)
                all_templates_2.extend(templates)

            assert sorted(all_templates_1) == sorted(all_templates_2)

        def _asserts_patterns():
            pattern_hash = ExpressionHasher.composite_hash([type_hash, *[snet_handle, '*']])
            _, all_patterns_1 = db._retrieve_pattern(pattern_hash)

            cursor = '0'
            all_patterns_2 = []
            while cursor != 0:
                cursor, templates = db._retrieve_pattern(
                    pattern_hash, cursor=cursor, chunk_size=100
                )
                all_patterns_2.extend(templates)

            assert sorted(all_patterns_1) == sorted(all_patterns_2)

        snet_handle = AtomDB.node_handle('Concept', 'snet')
        type_hash = db._get_atom_type_hash('Inheritance')

        _add_links()
        _asserts_incoming_set()
        _asserts_templates()
        _asserts_patterns()

        _db_down()

    def test_get_matched_with_pagination(self):
        _db_up()
        db = self._connect_db()
        self._add_atoms(db)
        db.commit()

        response = db.get_matched_links('Similarity', [human, monkey], cursor=0)
        assert response == (None, [AtomDB.link_handle('Similarity', [human, monkey])])

        response = db.get_matched_links('Fake', [human, monkey], cursor=0)
        assert response == (None, [])

        response = db.get_matched_links('Similarity', [human, '*'], cursor=0)
        assert (response[0], sorted(response[1])) == (
            0,
            [
                (
                    '16f7e407087bfa0b35b13d13a1aadcae',
                    ('af12f10f9ae2002a1607ba0b47ba8407', '4e8e26e3276af8a5c2ac2cc2dc95c6d2'),
                ),
                (
                    'b5459e299a5c5e8662c427f7e01b3bf1',
                    ('af12f10f9ae2002a1607ba0b47ba8407', '5b34c54bee150c04f9fa584b899dc030'),
                ),
                (
                    'bad7472f41a0e7d601ca294eb4607c3a',
                    ('af12f10f9ae2002a1607ba0b47ba8407', '1cdffc6b0b89ff41d68bec237481d1e1'),
                ),
            ],
        )

        template = ['Inheritance', 'Concept', 'Concept']

        response = db.get_matched_type_template(template, cursor=0)
        assert (response[0], sorted(response[1])) == (
            0,
            [
                (
                    '116df61c01859c710d178ba14a483509',
                    ('c1db9b517073e51eb7ef6fed608ec204', 'b99ae727c787f1b13b452fd4c9ce1b9a'),
                ),
                (
                    '1c3bf151ea200b2d9e088a1178d060cb',
                    ('bdfe4e7a431f73386f37c6448afe5840', '0a32b476852eeb954979b87f5f6cb7af'),
                ),
                (
                    '4120e428ab0fa162a04328e5217912ff',
                    ('bb34ce95f161a6b37ff54b3d4c817857', '0a32b476852eeb954979b87f5f6cb7af'),
                ),
                (
                    '75756335011dcedb71a0d9a7bd2da9e8',
                    ('5b34c54bee150c04f9fa584b899dc030', 'bdfe4e7a431f73386f37c6448afe5840'),
                ),
                (
                    '906fa505ae3bc6336d80a5f9aaa47b3b',
                    ('d03e59654221c1e8fcda404fd5c8d6cb', '08126b066d32ee37743e255a2558cccd'),
                ),
                (
                    '959924e3aab197af80a84c1ab261fd65',
                    ('08126b066d32ee37743e255a2558cccd', 'b99ae727c787f1b13b452fd4c9ce1b9a'),
                ),
                (
                    'b0f428929706d1d991e4d712ad08f9ab',
                    ('b99ae727c787f1b13b452fd4c9ce1b9a', '0a32b476852eeb954979b87f5f6cb7af'),
                ),
                (
                    'c93e1e758c53912638438e2a7d7f7b7f',
                    ('af12f10f9ae2002a1607ba0b47ba8407', 'bdfe4e7a431f73386f37c6448afe5840'),
                ),
                (
                    'e4685d56969398253b6f77efd21dc347',
                    ('b94941d8cd1c0ee4ad3dd3dcab52b964', '80aff30094874e75028033a38ce677bb'),
                ),
                (
                    'ee1c03e6d1f104ccd811cfbba018451a',
                    ('4e8e26e3276af8a5c2ac2cc2dc95c6d2', '80aff30094874e75028033a38ce677bb'),
                ),
                (
                    'f31dfe97db782e8cec26de18dddf8965',
                    ('1cdffc6b0b89ff41d68bec237481d1e1', 'bdfe4e7a431f73386f37c6448afe5840'),
                ),
                (
                    'fbf03d17d6a40feff828a3f2c6e86f05',
                    ('99d18c702e813b07260baf577c60c455', 'bdfe4e7a431f73386f37c6448afe5840'),
                ),
            ],
        )

        response = db.get_matched_type('Inheritance', cursor=0)
        assert (response[0], sorted(response[1])) == (
            0,
            [
                (
                    '116df61c01859c710d178ba14a483509',
                    ('c1db9b517073e51eb7ef6fed608ec204', 'b99ae727c787f1b13b452fd4c9ce1b9a'),
                ),
                (
                    '1c3bf151ea200b2d9e088a1178d060cb',
                    ('bdfe4e7a431f73386f37c6448afe5840', '0a32b476852eeb954979b87f5f6cb7af'),
                ),
                (
                    '4120e428ab0fa162a04328e5217912ff',
                    ('bb34ce95f161a6b37ff54b3d4c817857', '0a32b476852eeb954979b87f5f6cb7af'),
                ),
                (
                    '75756335011dcedb71a0d9a7bd2da9e8',
                    ('5b34c54bee150c04f9fa584b899dc030', 'bdfe4e7a431f73386f37c6448afe5840'),
                ),
                (
                    '906fa505ae3bc6336d80a5f9aaa47b3b',
                    ('d03e59654221c1e8fcda404fd5c8d6cb', '08126b066d32ee37743e255a2558cccd'),
                ),
                (
                    '959924e3aab197af80a84c1ab261fd65',
                    ('08126b066d32ee37743e255a2558cccd', 'b99ae727c787f1b13b452fd4c9ce1b9a'),
                ),
                (
                    'b0f428929706d1d991e4d712ad08f9ab',
                    ('b99ae727c787f1b13b452fd4c9ce1b9a', '0a32b476852eeb954979b87f5f6cb7af'),
                ),
                (
                    'c93e1e758c53912638438e2a7d7f7b7f',
                    ('af12f10f9ae2002a1607ba0b47ba8407', 'bdfe4e7a431f73386f37c6448afe5840'),
                ),
                (
                    'e4685d56969398253b6f77efd21dc347',
                    ('b94941d8cd1c0ee4ad3dd3dcab52b964', '80aff30094874e75028033a38ce677bb'),
                ),
                (
                    'ee1c03e6d1f104ccd811cfbba018451a',
                    ('4e8e26e3276af8a5c2ac2cc2dc95c6d2', '80aff30094874e75028033a38ce677bb'),
                ),
                (
                    'f31dfe97db782e8cec26de18dddf8965',
                    ('1cdffc6b0b89ff41d68bec237481d1e1', 'bdfe4e7a431f73386f37c6448afe5840'),
                ),
                (
                    'fbf03d17d6a40feff828a3f2c6e86f05',
                    ('99d18c702e813b07260baf577c60c455', 'bdfe4e7a431f73386f37c6448afe5840'),
                ),
            ],
        )
        _db_down()
