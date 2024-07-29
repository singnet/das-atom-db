import random
import string

import pytest

from hyperon_das_atomdb.adapters import RedisMongoDB
from hyperon_das_atomdb.database import WILDCARD, AtomDB, FieldIndexType
from hyperon_das_atomdb.utils.expression_hasher import ExpressionHasher

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
from .helpers import Database, PyMongoFindExplain, _db_down, _db_up, cleanup, mongo_port, redis_port


class TestRedisMongo:
    @pytest.fixture(scope="session", autouse=True)
    def _cleanup(self, request):
        return cleanup(request)

    @pytest.fixture(autouse=True)
    def _db(self):
        _db_up(Database.REDIS, Database.MONGO)
        yield self._connect_db()
        _db_down()

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

    def _check_basic_patterns(self, db, toplevel_only=False):
        assert sorted(
            [
                answer[1]
                for answer in db.get_matched_links(
                    "Inheritance",
                    [WILDCARD, db.node_handle("Concept", "mammal")],
                    toplevel_only=toplevel_only,
                )
            ]
        ) == sorted([human, monkey, chimp, rhino])
        assert sorted(
            [
                answer[2]
                for answer in db.get_matched_links(
                    "Inheritance",
                    [db.node_handle("Concept", "mammal"), WILDCARD],
                    toplevel_only=toplevel_only,
                )
            ]
        ) == sorted([animal])
        assert sorted(
            [
                answer[1]
                for answer in db.get_matched_links(
                    "Similarity",
                    [WILDCARD, db.node_handle("Concept", "human")],
                    toplevel_only=toplevel_only,
                )
            ]
        ) == sorted([monkey, chimp, ent])
        assert sorted(
            [
                answer[2]
                for answer in db.get_matched_links(
                    "Similarity",
                    [db.node_handle("Concept", "human"), WILDCARD],
                    toplevel_only=toplevel_only,
                )
            ]
        ) == sorted([monkey, chimp, ent])

    def test_redis_retrieve(self, _cleanup, _db: RedisMongoDB):
        db = _db
        self._add_atoms(db)
        assert db.count_atoms() == {'atom_count': 0}
        db.commit()
        assert db.count_atoms() == {'atom_count': 40}
        assert db.count_atoms({'precise': True}) == {
            'atom_count': 40,
            'node_count': 14,
            'link_count': 26,
        }
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
        assert [
            "75756335011dcedb71a0d9a7bd2da9e8",
            "5b34c54bee150c04f9fa584b899dc030",
            "bdfe4e7a431f73386f37c6448afe5840",
        ] in patterns
        assert [
            "fbf03d17d6a40feff828a3f2c6e86f05",
            "99d18c702e813b07260baf577c60c455",
            "bdfe4e7a431f73386f37c6448afe5840",
        ] in patterns
        assert [
            "f31dfe97db782e8cec26de18dddf8965",
            "1cdffc6b0b89ff41d68bec237481d1e1",
            "bdfe4e7a431f73386f37c6448afe5840",
        ] in patterns
        assert [
            "c93e1e758c53912638438e2a7d7f7b7f",
            "af12f10f9ae2002a1607ba0b47ba8407",
            "bdfe4e7a431f73386f37c6448afe5840",
        ] in patterns

    def test_patterns(self, _cleanup, _db: RedisMongoDB):
        db = _db
        self._add_atoms(db)
        db.commit()
        assert db.count_atoms() == {'atom_count': 40}
        self._check_basic_patterns(db)

    def test_commit(self, _cleanup, _db: RedisMongoDB):
        db = _db
        assert db.count_atoms() == {'atom_count': 0}
        self._add_atoms(db)
        assert db.count_atoms() == {'atom_count': 0}
        db.commit()
        assert db.count_atoms() == {'atom_count': 40}
        assert sorted(
            [
                answer[1]
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
        assert db.count_atoms({'precise': True}) == {
            'atom_count': 42,
            'node_count': 15,
            'link_count': 27,
        }
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
                answer[1]
                for answer in db.get_matched_links(
                    "Inheritance", [WILDCARD, db.node_handle("Concept", "mammal")]
                )
            ]
        ) == sorted([human, monkey, chimp, rhino, dog])

    def test_reindex(self, _cleanup, _db: RedisMongoDB):
        db = _db
        self._add_atoms(db)
        db.commit()
        db.reindex()
        assert db.count_atoms() == {'atom_count': 40}
        self._check_basic_patterns(db)
        _db_down()

    def test_delete_atom(self, _cleanup, _db: RedisMongoDB):
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
            assert db.count_atoms({'precise': True}) == {
                'atom_count': 5,
                'node_count': 3,
                'link_count': 2,
            }
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
                    [inheritance_dog_mammal_handle, dog_handle, mammal_handle],
                    [inheritance_cat_mammal_handle, cat_handle, mammal_handle],
                ]
            )
            assert sorted(db._retrieve_template('41c082428b28d7e9ea96160f7fd614ad')[1]) == sorted(
                [
                    [inheritance_dog_mammal_handle, dog_handle, mammal_handle],
                    [inheritance_cat_mammal_handle, cat_handle, mammal_handle],
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
            assert set([p for p in db.redis.keys('patterns:*')]) == keys

            assert sorted(db._retrieve_pattern('112002ff70ea491aad735f978e9d95f5')[1]) == sorted(
                [
                    [inheritance_dog_mammal_handle, dog_handle, mammal_handle],
                    [inheritance_cat_mammal_handle, cat_handle, mammal_handle],
                ]
            )
            assert sorted(db._retrieve_pattern('6e644e70a9fe3145c88b5b6261af5754')[1]) == sorted(
                [
                    [inheritance_dog_mammal_handle, dog_handle, mammal_handle],
                    [inheritance_cat_mammal_handle, cat_handle, mammal_handle],
                ]
            )
            assert sorted(db._retrieve_pattern('5dd515aa7a451276feac4f8b9d84ae91')[1]) == sorted(
                [
                    [inheritance_dog_mammal_handle, dog_handle, mammal_handle],
                    [inheritance_cat_mammal_handle, cat_handle, mammal_handle],
                ]
            )
            assert sorted(db._retrieve_pattern('7ead6cfa03894c62761162b7603aa885')[1]) == sorted(
                [
                    [inheritance_dog_mammal_handle, dog_handle, mammal_handle],
                    [inheritance_cat_mammal_handle, cat_handle, mammal_handle],
                ]
            )
            assert db._retrieve_pattern('e55007a8477a4e6bf4fec76e4ffd7e10')[1] == [
                [inheritance_dog_mammal_handle, dog_handle, mammal_handle]
            ]
            assert db._retrieve_pattern('23dc149b3218d166a14730db55249126')[1] == [
                [inheritance_dog_mammal_handle, dog_handle, mammal_handle]
            ]
            assert db._retrieve_pattern('399751d7319f9061d97cd1d75728b66b')[1] == [
                [inheritance_dog_mammal_handle, dog_handle, mammal_handle]
            ]
            assert db._retrieve_pattern('d0eaae6eaf750e821b26642cef32becf')[1] == [
                [inheritance_dog_mammal_handle, dog_handle, mammal_handle]
            ]
            assert db._retrieve_pattern('f29daafee640d91aa7091e44551fc74a')[1] == [
                [inheritance_cat_mammal_handle, cat_handle, mammal_handle]
            ]
            assert db._retrieve_pattern('a11d7cbf62bc544f75702b5fb6a514ff')[1] == [
                [inheritance_cat_mammal_handle, cat_handle, mammal_handle]
            ]
            assert db._retrieve_pattern('3ba42d45a50c89600d92fb3f1a46c1b5')[1] == [
                [inheritance_cat_mammal_handle, cat_handle, mammal_handle]
            ]
            assert db._retrieve_pattern('9fb71ffef74a1a98eb0bfce7aa3d54e3')[1] == [
                [inheritance_cat_mammal_handle, cat_handle, mammal_handle]
            ]

        def _check_asserts_2():
            assert db.count_atoms() == {'atom_count': 3}
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
            assert db.count_atoms() == {'atom_count': 2}
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
            assert db.count_atoms({'precise': True}) == {
                'atom_count': 3,
                'node_count': 2,
                'link_count': 1,
            }
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
                [inheritance_dog_mammal_handle, dog_handle, mammal_handle]
            ]
            assert db._retrieve_template('41c082428b28d7e9ea96160f7fd614ad')[1] == [
                [inheritance_dog_mammal_handle, dog_handle, mammal_handle]
            ]
            assert db._retrieve_pattern('112002ff70ea491aad735f978e9d95f5')[1] == [
                [inheritance_dog_mammal_handle, dog_handle, mammal_handle],
            ]
            assert db._retrieve_pattern('6e644e70a9fe3145c88b5b6261af5754')[1] == [
                [inheritance_dog_mammal_handle, dog_handle, mammal_handle],
            ]
            assert db._retrieve_pattern('5dd515aa7a451276feac4f8b9d84ae91')[1] == [
                [inheritance_dog_mammal_handle, dog_handle, mammal_handle],
            ]
            assert db._retrieve_pattern('7ead6cfa03894c62761162b7603aa885')[1] == [
                [inheritance_dog_mammal_handle, dog_handle, mammal_handle],
            ]
            assert db._retrieve_pattern('e55007a8477a4e6bf4fec76e4ffd7e10')[1] == [
                [inheritance_dog_mammal_handle, dog_handle, mammal_handle]
            ]
            assert db._retrieve_pattern('23dc149b3218d166a14730db55249126')[1] == [
                [inheritance_dog_mammal_handle, dog_handle, mammal_handle]
            ]
            assert db._retrieve_pattern('399751d7319f9061d97cd1d75728b66b')[1] == [
                [inheritance_dog_mammal_handle, dog_handle, mammal_handle]
            ]
            assert db._retrieve_pattern('d0eaae6eaf750e821b26642cef32becf')[1] == [
                [inheritance_dog_mammal_handle, dog_handle, mammal_handle]
            ]
            assert db._retrieve_pattern('f29daafee640d91aa7091e44551fc74a')[1] == []
            assert db._retrieve_pattern('a11d7cbf62bc544f75702b5fb6a514ff')[1] == []
            assert db._retrieve_pattern('3ba42d45a50c89600d92fb3f1a46c1b5')[1] == []
            assert db._retrieve_pattern('9fb71ffef74a1a98eb0bfce7aa3d54e3')[1] == []

        def _check_asserts_5():
            assert db.count_atoms({'precise': True}) == {
                'atom_count': 3,
                'node_count': 2,
                'link_count': 1,
            }
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
                [inheritance_cat_mammal_handle, cat_handle, mammal_handle]
            ]
            assert db._retrieve_template('41c082428b28d7e9ea96160f7fd614ad')[1] == [
                [inheritance_cat_mammal_handle, cat_handle, mammal_handle]
            ]
            assert db._retrieve_pattern('112002ff70ea491aad735f978e9d95f5')[1] == [
                [inheritance_cat_mammal_handle, cat_handle, mammal_handle]
            ]
            assert db._retrieve_pattern('6e644e70a9fe3145c88b5b6261af5754')[1] == [
                [inheritance_cat_mammal_handle, cat_handle, mammal_handle]
            ]
            assert db._retrieve_pattern('5dd515aa7a451276feac4f8b9d84ae91')[1] == [
                [inheritance_cat_mammal_handle, cat_handle, mammal_handle]
            ]
            assert db._retrieve_pattern('7ead6cfa03894c62761162b7603aa885')[1] == [
                [inheritance_cat_mammal_handle, cat_handle, mammal_handle]
            ]
            assert db._retrieve_pattern('e55007a8477a4e6bf4fec76e4ffd7e10')[1] == []
            assert db._retrieve_pattern('23dc149b3218d166a14730db55249126')[1] == []
            assert db._retrieve_pattern('399751d7319f9061d97cd1d75728b66b')[1] == []
            assert db._retrieve_pattern('d0eaae6eaf750e821b26642cef32becf')[1] == []
            assert db._retrieve_pattern('f29daafee640d91aa7091e44551fc74a')[1] == [
                [inheritance_cat_mammal_handle, cat_handle, mammal_handle]
            ]
            assert db._retrieve_pattern('a11d7cbf62bc544f75702b5fb6a514ff')[1] == [
                [inheritance_cat_mammal_handle, cat_handle, mammal_handle]
            ]
            assert db._retrieve_pattern('3ba42d45a50c89600d92fb3f1a46c1b5')[1] == [
                [inheritance_cat_mammal_handle, cat_handle, mammal_handle]
            ]
            assert db._retrieve_pattern('9fb71ffef74a1a98eb0bfce7aa3d54e3')[1] == [
                [inheritance_cat_mammal_handle, cat_handle, mammal_handle]
            ]

        db = _db

        cat_handle = AtomDB.node_handle('Concept', 'cat')
        dog_handle = AtomDB.node_handle('Concept', 'dog')
        mammal_handle = AtomDB.node_handle('Concept', 'mammal')
        inheritance_cat_mammal_handle = AtomDB.link_handle(
            'Inheritance', [cat_handle, mammal_handle]
        )
        inheritance_dog_mammal_handle = AtomDB.link_handle(
            'Inheritance', [dog_handle, mammal_handle]
        )

        assert db.count_atoms() == {'atom_count': 0}

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

    def test_retrieve_members_with_pagination(self, _cleanup, _db: RedisMongoDB):
        db = _db

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

    def test_get_matched_with_pagination(self, _cleanup, _db: RedisMongoDB):
        db = _db
        self._add_atoms(db)
        db.commit()

        response = db.get_matched_links('Similarity', [human, monkey], cursor=0)
        assert response == (0, [AtomDB.link_handle('Similarity', [human, monkey])])

        response = db.get_matched_links('Fake', [human, monkey], cursor=0)
        assert response == (0, [])

        response = db.get_matched_links('Similarity', [human, '*'], cursor=0)
        assert (response[0], sorted(response[1])) == (
            0,
            [
                [
                    '16f7e407087bfa0b35b13d13a1aadcae',
                    'af12f10f9ae2002a1607ba0b47ba8407',
                    '4e8e26e3276af8a5c2ac2cc2dc95c6d2',
                ],
                [
                    'b5459e299a5c5e8662c427f7e01b3bf1',
                    'af12f10f9ae2002a1607ba0b47ba8407',
                    '5b34c54bee150c04f9fa584b899dc030',
                ],
                [
                    'bad7472f41a0e7d601ca294eb4607c3a',
                    'af12f10f9ae2002a1607ba0b47ba8407',
                    '1cdffc6b0b89ff41d68bec237481d1e1',
                ],
            ],
        )

        template = ['Inheritance', 'Concept', 'Concept']

        response = db.get_matched_type_template(template, cursor=0)
        assert (response[0], sorted(response[1])) == (
            0,
            [
                [
                    '116df61c01859c710d178ba14a483509',
                    'c1db9b517073e51eb7ef6fed608ec204',
                    'b99ae727c787f1b13b452fd4c9ce1b9a',
                ],
                [
                    '1c3bf151ea200b2d9e088a1178d060cb',
                    'bdfe4e7a431f73386f37c6448afe5840',
                    '0a32b476852eeb954979b87f5f6cb7af',
                ],
                [
                    '4120e428ab0fa162a04328e5217912ff',
                    'bb34ce95f161a6b37ff54b3d4c817857',
                    '0a32b476852eeb954979b87f5f6cb7af',
                ],
                [
                    '75756335011dcedb71a0d9a7bd2da9e8',
                    '5b34c54bee150c04f9fa584b899dc030',
                    'bdfe4e7a431f73386f37c6448afe5840',
                ],
                [
                    '906fa505ae3bc6336d80a5f9aaa47b3b',
                    'd03e59654221c1e8fcda404fd5c8d6cb',
                    '08126b066d32ee37743e255a2558cccd',
                ],
                [
                    '959924e3aab197af80a84c1ab261fd65',
                    '08126b066d32ee37743e255a2558cccd',
                    'b99ae727c787f1b13b452fd4c9ce1b9a',
                ],
                [
                    'b0f428929706d1d991e4d712ad08f9ab',
                    'b99ae727c787f1b13b452fd4c9ce1b9a',
                    '0a32b476852eeb954979b87f5f6cb7af',
                ],
                [
                    'c93e1e758c53912638438e2a7d7f7b7f',
                    'af12f10f9ae2002a1607ba0b47ba8407',
                    'bdfe4e7a431f73386f37c6448afe5840',
                ],
                [
                    'e4685d56969398253b6f77efd21dc347',
                    'b94941d8cd1c0ee4ad3dd3dcab52b964',
                    '80aff30094874e75028033a38ce677bb',
                ],
                [
                    'ee1c03e6d1f104ccd811cfbba018451a',
                    '4e8e26e3276af8a5c2ac2cc2dc95c6d2',
                    '80aff30094874e75028033a38ce677bb',
                ],
                [
                    'f31dfe97db782e8cec26de18dddf8965',
                    '1cdffc6b0b89ff41d68bec237481d1e1',
                    'bdfe4e7a431f73386f37c6448afe5840',
                ],
                [
                    'fbf03d17d6a40feff828a3f2c6e86f05',
                    '99d18c702e813b07260baf577c60c455',
                    'bdfe4e7a431f73386f37c6448afe5840',
                ],
            ],
        )

        response = db.get_matched_type('Inheritance', cursor=0)
        assert (response[0], sorted(response[1])) == (
            0,
            [
                [
                    '116df61c01859c710d178ba14a483509',
                    'c1db9b517073e51eb7ef6fed608ec204',
                    'b99ae727c787f1b13b452fd4c9ce1b9a',
                ],
                [
                    '1c3bf151ea200b2d9e088a1178d060cb',
                    'bdfe4e7a431f73386f37c6448afe5840',
                    '0a32b476852eeb954979b87f5f6cb7af',
                ],
                [
                    '4120e428ab0fa162a04328e5217912ff',
                    'bb34ce95f161a6b37ff54b3d4c817857',
                    '0a32b476852eeb954979b87f5f6cb7af',
                ],
                [
                    '75756335011dcedb71a0d9a7bd2da9e8',
                    '5b34c54bee150c04f9fa584b899dc030',
                    'bdfe4e7a431f73386f37c6448afe5840',
                ],
                [
                    '906fa505ae3bc6336d80a5f9aaa47b3b',
                    'd03e59654221c1e8fcda404fd5c8d6cb',
                    '08126b066d32ee37743e255a2558cccd',
                ],
                [
                    '959924e3aab197af80a84c1ab261fd65',
                    '08126b066d32ee37743e255a2558cccd',
                    'b99ae727c787f1b13b452fd4c9ce1b9a',
                ],
                [
                    'b0f428929706d1d991e4d712ad08f9ab',
                    'b99ae727c787f1b13b452fd4c9ce1b9a',
                    '0a32b476852eeb954979b87f5f6cb7af',
                ],
                [
                    'c93e1e758c53912638438e2a7d7f7b7f',
                    'af12f10f9ae2002a1607ba0b47ba8407',
                    'bdfe4e7a431f73386f37c6448afe5840',
                ],
                [
                    'e4685d56969398253b6f77efd21dc347',
                    'b94941d8cd1c0ee4ad3dd3dcab52b964',
                    '80aff30094874e75028033a38ce677bb',
                ],
                [
                    'ee1c03e6d1f104ccd811cfbba018451a',
                    '4e8e26e3276af8a5c2ac2cc2dc95c6d2',
                    '80aff30094874e75028033a38ce677bb',
                ],
                [
                    'f31dfe97db782e8cec26de18dddf8965',
                    '1cdffc6b0b89ff41d68bec237481d1e1',
                    'bdfe4e7a431f73386f37c6448afe5840',
                ],
                [
                    'fbf03d17d6a40feff828a3f2c6e86f05',
                    '99d18c702e813b07260baf577c60c455',
                    'bdfe4e7a431f73386f37c6448afe5840',
                ],
            ],
        )

    def test_create_field_index(self, _cleanup, _db: RedisMongoDB):
        db = _db
        self._add_atoms(db)
        db.add_link(
            {
                "type": "Similarity",
                "targets": [
                    {"type": "Concept", "name": 'human'},
                    {"type": "Concept", "name": 'monkey'},
                ],
                "tag": 'DAS',
            }
        )
        db.commit()

        collection = db.mongo_atoms_collection

        response = collection.find({'named_type': 'Similarity', 'tag': 'DAS'}).explain()

        with pytest.raises(KeyError):
            response['queryPlanner']['winningPlan']['inputStage']['indexName']

        # Create the index
        my_index = db.create_field_index(atom_type='link', fields=['tag'], named_type='Similarity')

        collection_index_names = [idx.get('name') for idx in collection.list_indexes()]
        #
        assert my_index in collection_index_names

        # # Using the index
        response = collection.find({'named_type': 'Similarity', 'tag': 'DAS'}).explain()

        assert my_index == response['queryPlanner']['winningPlan']['inputStage']['indexName']

        with PyMongoFindExplain(db.mongo_atoms_collection) as explain:
            _, doc = db.get_atoms_by_index(my_index, [{'field': 'tag', 'value': 'DAS'}])
            assert doc[0]['handle'] == ExpressionHasher.expression_hash(
                ExpressionHasher.named_type_hash("Similarity"), [human, monkey]
            )
            assert doc[0]['targets'] == [human, monkey]
            assert explain[0]['executionStats']['executionSuccess']
            assert explain[0]['executionStats']['executionStages']['docsExamined'] == 1
            assert explain[0]['executionStats']['executionStages']['stage'] == 'FETCH'
            assert (
                explain[0]['executionStats']['executionStages']['inputStage']['stage'] == 'IXSCAN'
            )
            assert (
                explain[0]['executionStats']['executionStages']['inputStage']['keysExamined'] == 1
            )
            assert (
                explain[0]['executionStats']['executionStages']['inputStage']['indexName']
                == my_index
            )

    def test_create_text_index(self, _cleanup, _db: RedisMongoDB):
        db: RedisMongoDB = _db
        self._add_atoms(db)
        db.add_link(
            {
                "type": "Similarity",
                "targets": [
                    {"type": "Concept", "name": 'human'},
                    {"type": "Concept", "name": 'monkey'},
                ],
                "tag": 'DAS',
            }
        )
        db.commit()

        collection = db.mongo_atoms_collection

        # Create the index
        my_index = db.create_field_index(
            atom_type='link',
            fields=['tag'],
            named_type='Similarity',
            index_type=FieldIndexType.TOKEN_INVERTED_LIST,
        )

        collection_index_names = [idx.get('name') for idx in collection.list_indexes()]
        #
        assert my_index in collection_index_names

    def test_create_compound_index(self, _cleanup, _db: RedisMongoDB):
        db: RedisMongoDB = _db
        self._add_atoms(db)
        db.add_link(
            {
                "type": "Similarity",
                "targets": [
                    {"type": "Concept", "name": 'human'},
                    {"type": "Concept", "name": 'monkey'},
                ],
                "tag": 'DAS',
            }
        )
        db.commit()
        collection = db.mongo_atoms_collection
        # Create the index
        my_index = db.create_field_index(
            atom_type='link',
            fields=['type', 'tag'],
            named_type='Similarity',
            index_type=FieldIndexType.BINARY_TREE,
        )
        collection_index_names = [idx.get('name') for idx in collection.list_indexes()]
        assert my_index in collection_index_names

    def test_get_atoms_by_field_no_index(self, _cleanup, _db: RedisMongoDB):
        db: RedisMongoDB = _db
        self._add_atoms(db)
        db.add_link(
            {
                "type": "Similarity",
                "targets": [
                    {"type": "Concept", "name": 'human'},
                    {"type": "Concept", "name": 'monkey'},
                ],
                "tag": 'DAS',
            }
        )
        db.commit()

        with PyMongoFindExplain(db.mongo_atoms_collection) as explain:
            result = db.get_atoms_by_field([{'field': 'tag', 'value': 'DAS'}])
            assert len(result) == 1
            assert explain[0]['executionStats']['executionSuccess']
            assert explain[0]['queryPlanner']['winningPlan']['stage'] == 'COLLSCAN'
            assert explain[0]['executionStats']['totalKeysExamined'] == 0

    def test_get_atoms_by_field_with_index(self, _cleanup, _db: RedisMongoDB):
        db: RedisMongoDB = _db
        self._add_atoms(db)
        db.add_link(
            {
                "type": "Similarity",
                "targets": [
                    {"type": "Concept", "name": 'human'},
                    {"type": "Concept", "name": 'monkey'},
                ],
                "tag": 'DAS',
            }
        )
        db.commit()
        my_index = db.create_field_index(atom_type='link', fields=['tag'])

        with PyMongoFindExplain(db.mongo_atoms_collection) as explain:
            result = db.get_atoms_by_field([{'field': 'tag', 'value': 'DAS'}])
            assert len(result) == 1
            assert explain[0]['executionStats']['executionSuccess']
            assert explain[0]['executionStats']['nReturned'] == 1
            assert explain[0]['executionStats']['executionStages']['stage'] == 'FETCH'
            assert (
                explain[0]['executionStats']['executionStages']['inputStage']['stage'] == 'IXSCAN'
            )
            assert (
                explain[0]['executionStats']['executionStages']['inputStage']['keysExamined'] == 1
            )
            assert (
                explain[0]['executionStats']['executionStages']['inputStage']['indexName']
                == my_index
            )

    def test_get_atoms_by_index(self, _cleanup, _db: RedisMongoDB):
        db: RedisMongoDB = _db
        db.add_link(
            {
                "type": "Similarity",
                "targets": [
                    {"type": "Concept", "name": 'human'},
                    {"type": "Concept", "name": 'monkey'},
                ],
                "tag": 'DAS',
            }
        )
        db.add_link(
            {
                "type": "Similarity",
                "targets": [
                    {"type": "Concept", "name": 'mammal'},
                    {"type": "Concept", "name": 'monkey'},
                ],
                "tag": 'DAS2',
            }
        )
        db.commit()

        my_index = db.create_field_index(atom_type='link', fields=['tag'], named_type='Similarity')

        with PyMongoFindExplain(db.mongo_atoms_collection) as explain:
            _, doc = db.get_atoms_by_index(my_index, [{'field': 'tag', 'value': 'DAS2'}])
            assert doc[0]['handle'] == ExpressionHasher.expression_hash(
                ExpressionHasher.named_type_hash("Similarity"), [mammal, monkey]
            )
            assert doc[0]['targets'] == [mammal, monkey]
            assert explain[0]['executionStats']['executionSuccess']
            assert explain[0]['executionStats']['nReturned'] == 1
            assert explain[0]['executionStats']['executionStages']['stage'] == 'FETCH'
            assert (
                explain[0]['executionStats']['executionStages']['inputStage']['stage'] == 'IXSCAN'
            )
            assert (
                explain[0]['executionStats']['executionStages']['inputStage']['keysExamined'] == 1
            )
            assert (
                explain[0]['executionStats']['executionStages']['inputStage']['indexName']
                == my_index
            )

    def test_get_atoms_by_text_field_regex(self, _cleanup, _db: RedisMongoDB):
        db: RedisMongoDB = _db
        self._add_atoms(db)
        db.commit()

        with PyMongoFindExplain(db.mongo_atoms_collection) as explain:
            result = db.get_atoms_by_text_field('mammal', 'name')
            assert len(result) == 1
            assert result[0] == db.get_node_handle('Concept', 'mammal')
            assert explain[0]['executionStats']['executionSuccess']
            assert (
                explain[0]['executionStats']['executionStages']['inputStage']['stage'] == 'IXSCAN'
            )
            assert explain[0]['executionStats']['totalKeysExamined'] == 14

    def test_get_atoms_by_text_field_with_index(self, _cleanup, _db: RedisMongoDB):
        db: RedisMongoDB = _db
        self._add_atoms(db)
        db.commit()
        # Create index

        db.create_field_index(
            atom_type='node', fields=['name'], index_type=FieldIndexType.TOKEN_INVERTED_LIST
        )

        with PyMongoFindExplain(db.mongo_atoms_collection) as explain:
            result = db.get_atoms_by_text_field('mammal')
            assert len(result) == 1
            assert result[0] == db.get_node_handle('Concept', 'mammal')
            assert explain[0]['executionStats']['executionSuccess']
            assert explain[0]['executionStats']['executionStages']['stage'] == 'TEXT_MATCH'
            assert explain[0]['executionStats']['totalKeysExamined'] == 1

    def test_get_node_starting_name(self, _cleanup, _db: RedisMongoDB):
        db = _db
        self._add_atoms(db)
        db.commit()
        with PyMongoFindExplain(db.mongo_atoms_collection) as explain:
            result = db.get_node_by_name_starting_with('Concept', 'mamm')
            assert len(result) == 1
            assert result[0] == db.get_node_handle('Concept', 'mammal')
            assert explain[0]['executionStats']['executionSuccess']
            assert (
                explain[0]['executionStats']['executionStages']['inputStage']['stage'] == 'IXSCAN'
            )
            assert explain[0]['executionStats']['totalKeysExamined'] == 2
            assert explain[0]['executionStats']['totalDocsExamined'] == 1

    def test_bulk_insert(self, _cleanup, _db: RedisMongoDB):
        db = _db
        assert db.count_atoms() == {'atom_count': 0}

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
                '_id': db.link_handle('Similarity', ['node1', 'node2']),
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

        assert db.count_atoms() == {'atom_count': 3}
        assert db.get_matched_links('Similarity', ['node1', 'node2']) == [
            db.link_handle('Similarity', ['node1', 'node2'])
        ]
        _, similarity = db.get_all_links('Similarity')
        assert similarity == [db.link_handle('Similarity', ['node1', 'node2'])]
        assert db.get_all_nodes('Concept') == ['node1', 'node2']

    def test_retrieve_all_atoms(self, _cleanup, _db: RedisMongoDB):
        db = _db
        self._add_atoms(db)
        db.commit()
        response = db.retrieve_all_atoms()
        _, inheritance = db.get_all_links('Inheritance')
        _, similarity = db.get_all_links('Similarity')
        links = inheritance + similarity
        nodes = db.get_all_nodes('Concept')
        assert len(response) == len(links) + len(nodes)

    def test_add_fields_to_atoms(self, _cleanup, _db: RedisMongoDB):
        db = _db
        self._add_atoms(db)
        db.commit()
        human = db.node_handle('Concept', 'human')
        monkey = db.node_handle('Concept', 'monkey')
        link_handle = db.link_handle('Similarity', [human, monkey])

        node_human = db.get_atom(human)

        assert node_human['handle'] == human
        assert node_human['name'] == 'human'
        assert node_human['named_type'] == 'Concept'

        node_human['score'] = 0.5

        db.add_node(node_human)
        db.commit()

        assert db.get_atom(human)['score'] == 0.5

        link_similarity = db.get_atom(link_handle, deep_representation=True)

        assert link_similarity['handle'] == link_handle
        assert link_similarity['type'] == 'Similarity'
        assert link_similarity['targets'] == [db.get_atom(human), db.get_atom(monkey)]

        link_similarity['score'] = 0.5

        db.add_link(link_similarity)
        db.commit()

        assert db.get_atom(link_handle)['score'] == 0.5

    def test_commit_with_buffer(self, _cleanup, _db: RedisMongoDB):
        db = _db
        assert db.count_atoms() == {'atom_count': 0}
        buffer = [
            {
                '_id': '26d35e45817f4270f2b7cff971b04138',
                'composite_type_hash': 'd99a604c79ce3c2e76a2f43488d5d4c3',
                'name': 'dog',
                'named_type': 'Concept',
            },
            {
                '_id': 'b7db6a9ed2191eb77ee54479570db9a4',
                'composite_type_hash': 'd99a604c79ce3c2e76a2f43488d5d4c3',
                'name': 'cat',
                'named_type': 'Concept',
            },
            {
                '_id': '3dab102938606f4549d68405ec9f4f61',
                'composite_type_hash': 'ed73ea081d170e1d89fc950820ce1cee',
                'is_toplevel': True,
                'composite_type': [
                    'a9dea78180588431ec64d6bc4872fdbc',
                    'd99a604c79ce3c2e76a2f43488d5d4c3',
                    'd99a604c79ce3c2e76a2f43488d5d4c3',
                ],
                'named_type': 'Similarity',
                'named_type_hash': 'a9dea78180588431ec64d6bc4872fdbc',
                'key_0': '26d35e45817f4270f2b7cff971b04138',
                'key_1': 'b7db6a9ed2191eb77ee54479570db9a4',
            },
        ]
        db.commit(buffer=buffer)
        assert db.count_atoms({'precise': True}) == {
            'atom_count': 3,
            'node_count': 2,
            'link_count': 1,
        }
        assert db.get_atom('26d35e45817f4270f2b7cff971b04138')['name'] == 'dog'
        assert db.get_atom('b7db6a9ed2191eb77ee54479570db9a4')['name'] == 'cat'
        assert db.get_atom('3dab102938606f4549d68405ec9f4f61')['targets'] == [
            '26d35e45817f4270f2b7cff971b04138',
            'b7db6a9ed2191eb77ee54479570db9a4',
        ]
