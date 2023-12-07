import os
import pickle
import re
from typing import Any, Dict, List, Optional
from unittest import mock

import pytest
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from redis import Redis

from hyperon_das_atomdb.adapters import RedisMongoDB
from hyperon_das_atomdb.constants.redis_mongo_db import MongoCollectionNames, MongoFieldNames
from hyperon_das_atomdb.constants.redis_mongo_db import RedisCollectionNames as KeyPrefix
from hyperon_das_atomdb.exceptions import LinkDoesNotExist, NodeDoesNotExist
from hyperon_das_atomdb.utils.expression_hasher import ExpressionHasher

node_collection_mock_data = [
    {
        '_id': 'af12f10f9ae2002a1607ba0b47ba8407',
        'composite_type_hash': 'd99a604c79ce3c2e76a2f43488d5d4c3',
        'name': 'human',
        'named_type': 'Concept',
    },
    {
        '_id': '1cdffc6b0b89ff41d68bec237481d1e1',
        'composite_type_hash': 'd99a604c79ce3c2e76a2f43488d5d4c3',
        'name': 'monkey',
        'named_type': 'Concept',
    },
    {
        '_id': '5b34c54bee150c04f9fa584b899dc030',
        'composite_type_hash': 'd99a604c79ce3c2e76a2f43488d5d4c3',
        'name': 'chimp',
        'named_type': 'Concept',
    },
    {
        '_id': 'c1db9b517073e51eb7ef6fed608ec204',
        'composite_type_hash': 'd99a604c79ce3c2e76a2f43488d5d4c3',
        'name': 'snake',
        'named_type': 'Concept',
    },
    {
        '_id': 'bb34ce95f161a6b37ff54b3d4c817857',
        'composite_type_hash': 'd99a604c79ce3c2e76a2f43488d5d4c3',
        'name': 'earthworm',
        'named_type': 'Concept',
    },
    {
        '_id': '99d18c702e813b07260baf577c60c455',
        'composite_type_hash': 'd99a604c79ce3c2e76a2f43488d5d4c3',
        'name': 'rhino',
        'named_type': 'Concept',
    },
    {
        '_id': 'd03e59654221c1e8fcda404fd5c8d6cb',
        'composite_type_hash': 'd99a604c79ce3c2e76a2f43488d5d4c3',
        'name': 'triceratops',
        'named_type': 'Concept',
    },
    {
        '_id': 'b94941d8cd1c0ee4ad3dd3dcab52b964',
        'composite_type_hash': 'd99a604c79ce3c2e76a2f43488d5d4c3',
        'name': 'vine',
        'named_type': 'Concept',
    },
    {
        '_id': '4e8e26e3276af8a5c2ac2cc2dc95c6d2',
        'composite_type_hash': 'd99a604c79ce3c2e76a2f43488d5d4c3',
        'name': 'ent',
        'named_type': 'Concept',
    },
    {
        '_id': 'bdfe4e7a431f73386f37c6448afe5840',
        'composite_type_hash': 'd99a604c79ce3c2e76a2f43488d5d4c3',
        'name': 'mammal',
        'named_type': 'Concept',
    },
    {
        '_id': '0a32b476852eeb954979b87f5f6cb7af',
        'composite_type_hash': 'd99a604c79ce3c2e76a2f43488d5d4c3',
        'name': 'animal',
        'named_type': 'Concept',
    },
    {
        '_id': 'b99ae727c787f1b13b452fd4c9ce1b9a',
        'composite_type_hash': 'd99a604c79ce3c2e76a2f43488d5d4c3',
        'name': 'reptile',
        'named_type': 'Concept',
    },
    {
        '_id': '08126b066d32ee37743e255a2558cccd',
        'composite_type_hash': 'd99a604c79ce3c2e76a2f43488d5d4c3',
        'name': 'dinosaur',
        'named_type': 'Concept',
    },
    {
        '_id': '80aff30094874e75028033a38ce677bb',
        'composite_type_hash': 'd99a604c79ce3c2e76a2f43488d5d4c3',
        'name': 'plant',
        'named_type': 'Concept',
    },
]
added_nodes = []

type_collection_mock_data = [
    {
        "_id": "3e419e4d468bdac682103ea2615d0902",
        "composite_type_hash": "26a3ccbc2d2c83e0e39a01e0eccc4fd1",
        "named_type": "Inheritance",
        "named_type_hash": "e40489cd1e7102e35469c937e05c8bba",
    },
    {
        "_id": "fb34c2def109acdd4c7b3c39bba142b2",
        "composite_type_hash": "01959fba970d810fdc38bf0c6b4884db",
        "named_type": "snake",
        "named_type_hash": "de1b2a7baf7850243db71c4abd4e5a39",
    },
    {
        "_id": "93052a62616e8afd99a29c2ea7a84eb2",
        "composite_type_hash": "01959fba970d810fdc38bf0c6b4884db",
        "named_type": "earthworm",
        "named_type_hash": "35fa0b1f2fca4be6ed0fab8c315494c6",
    },
    {
        "_id": "70d5310bc38a2f3014921b5a03b69782",
        "composite_type_hash": "01959fba970d810fdc38bf0c6b4884db",
        "named_type": "triceratops",
        "named_type_hash": "e6c008b012a13755d9bcb47f182e53d5",
    },
    {
        "_id": "bd06bda419f3ae607812e944ef635baa",
        "composite_type_hash": "01959fba970d810fdc38bf0c6b4884db",
        "named_type": "human",
        "named_type_hash": "99e9bae675b12967251c175696f00a70",
    },
    {
        "_id": "3028445ae4a8df2f220fe95b69c423f5",
        "composite_type_hash": "01959fba970d810fdc38bf0c6b4884db",
        "named_type": "monkey",
        "named_type_hash": "d0763edaa9d9bd2a9516280e9044d885",
    },
    {
        "_id": "ad9b605df4ec82cb559192729324f7fb",
        "composite_type_hash": "01959fba970d810fdc38bf0c6b4884db",
        "named_type": "mammal",
        "named_type_hash": "f82c38962ba45f35aba678ee51d4797f",
    },
    {
        "_id": "29cc3f7b7277215d002bd3c4c0753e14",
        "composite_type_hash": "01959fba970d810fdc38bf0c6b4884db",
        "named_type": "dinosaur",
        "named_type_hash": "03318769a5ee1354f7479acc69755e7c",
    },
    {
        "_id": "77acd011f17414d823cebe9920a9cd27",
        "composite_type_hash": "01959fba970d810fdc38bf0c6b4884db",
        "named_type": "plant",
        "named_type_hash": "9ea0a36b3a20901fafe834eb519a595c",
    },
    {
        "_id": "19e229d0e1acddfa8210b263fd94eb8c",
        "composite_type_hash": "01959fba970d810fdc38bf0c6b4884db",
        "named_type": "reptile",
        "named_type_hash": "74790f436b9dc6ae4d47bfb6c924d3ad",
    },
    {
        "_id": "d3d7fcbfbb2581e62d56b5d600d6cc23",
        "composite_type_hash": "01959fba970d810fdc38bf0c6b4884db",
        "named_type": "vine",
        "named_type_hash": "a77b51765620b8b0c50236e9ea8907ab",
    },
    {
        "_id": "80db0b8181369fe636f2c82b7eb163b9",
        "composite_type_hash": "01959fba970d810fdc38bf0c6b4884db",
        "named_type": "chimp",
        "named_type_hash": "46bdd2ce40657500dcb21ca51d22b29f",
    },
    {
        "_id": "845005addea22e3272ba56edac5aa311",
        "composite_type_hash": "01959fba970d810fdc38bf0c6b4884db",
        "named_type": "ent",
        "named_type_hash": "645ec79f22bec5efe970061d395cf7c4",
    },
    {
        "_id": "13159c17892af91350009b407b170b44",
        "composite_type_hash": "01959fba970d810fdc38bf0c6b4884db",
        "named_type": "animal",
        "named_type_hash": "1e4483e833025ac10e6184e75cb2d19d",
    },
    {
        "_id": "0cc896470995bd4187a844163b6f1012",
        "composite_type_hash": "26a3ccbc2d2c83e0e39a01e0eccc4fd1",
        "named_type": "Similarity",
        "named_type_hash": "a9dea78180588431ec64d6bc4872fdbc",
    },
    {
        "_id": "01959fba970d810fdc38bf0c6b4884db",
        "composite_type_hash": "26a3ccbc2d2c83e0e39a01e0eccc4fd1",
        "named_type": "Concept",
        "named_type_hash": "d99a604c79ce3c2e76a2f43488d5d4c3",
    },
    {
        "_id": "26a3ccbc2d2c83e0e39a01e0eccc4fd1",
        "composite_type_hash": "26a3ccbc2d2c83e0e39a01e0eccc4fd1",
        "named_type": "Type",
        "named_type_hash": "a1fa27779242b4902f7ae3bdd5c6d508",
    },
    {
        "_id": "8b1109d7321c28cb68d4814573d41ad3",
        "composite_type_hash": "01959fba970d810fdc38bf0c6b4884db",
        "named_type": "rhino",
        "named_type_hash": "c6318323cc5693ce1f8d220cc9a5030e",
    },
]

arity_2_collection_mock_data = [
    {
        '_id': '2d7abd27644a9c08a7ca2c8d68338579',
        'composite_type_hash': 'ed73ea081d170e1d89fc950820ce1cee',
        'is_toplevel': True,
        'composite_type': [
            'a9dea78180588431ec64d6bc4872fdbc',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
        ],
        'named_type': 'Similarity',
        'named_type_hash': 'a9dea78180588431ec64d6bc4872fdbc',
        'key_0': 'bb34ce95f161a6b37ff54b3d4c817857',
        'key_1': 'c1db9b517073e51eb7ef6fed608ec204',
    },
    {
        '_id': 'fbf03d17d6a40feff828a3f2c6e86f05',
        'composite_type_hash': '41c082428b28d7e9ea96160f7fd614ad',
        'is_toplevel': True,
        'composite_type': [
            'e40489cd1e7102e35469c937e05c8bba',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
        ],
        'named_type': 'Inheritance',
        'named_type_hash': 'e40489cd1e7102e35469c937e05c8bba',
        'key_0': '99d18c702e813b07260baf577c60c455',
        'key_1': 'bdfe4e7a431f73386f37c6448afe5840',
    },
    {
        '_id': '31535ddf214f5b239d3b517823cb8144',
        'composite_type_hash': 'ed73ea081d170e1d89fc950820ce1cee',
        'is_toplevel': True,
        'composite_type': [
            'a9dea78180588431ec64d6bc4872fdbc',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
        ],
        'named_type': 'Similarity',
        'named_type_hash': 'a9dea78180588431ec64d6bc4872fdbc',
        'key_0': '1cdffc6b0b89ff41d68bec237481d1e1',
        'key_1': '5b34c54bee150c04f9fa584b899dc030',
    },
    {
        '_id': '2c927fdc6c0f1272ee439ceb76a6d1a4',
        'composite_type_hash': 'ed73ea081d170e1d89fc950820ce1cee',
        'is_toplevel': True,
        'composite_type': [
            'a9dea78180588431ec64d6bc4872fdbc',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
        ],
        'named_type': 'Similarity',
        'named_type_hash': 'a9dea78180588431ec64d6bc4872fdbc',
        'key_0': '5b34c54bee150c04f9fa584b899dc030',
        'key_1': 'af12f10f9ae2002a1607ba0b47ba8407',
    },
    {
        '_id': '75756335011dcedb71a0d9a7bd2da9e8',
        'composite_type_hash': '41c082428b28d7e9ea96160f7fd614ad',
        'is_toplevel': True,
        'composite_type': [
            'e40489cd1e7102e35469c937e05c8bba',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
        ],
        'named_type': 'Inheritance',
        'named_type_hash': 'e40489cd1e7102e35469c937e05c8bba',
        'key_0': '5b34c54bee150c04f9fa584b899dc030',
        'key_1': 'bdfe4e7a431f73386f37c6448afe5840',
    },
    {
        '_id': '7ee00a03f67b39f620bd3d0f6ed0c3e6',
        'composite_type_hash': 'ed73ea081d170e1d89fc950820ce1cee',
        'is_toplevel': True,
        'composite_type': [
            'a9dea78180588431ec64d6bc4872fdbc',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
        ],
        'named_type': 'Similarity',
        'named_type_hash': 'a9dea78180588431ec64d6bc4872fdbc',
        'key_0': 'c1db9b517073e51eb7ef6fed608ec204',
        'key_1': 'bb34ce95f161a6b37ff54b3d4c817857',
    },
    {
        '_id': '4120e428ab0fa162a04328e5217912ff',
        'composite_type_hash': '41c082428b28d7e9ea96160f7fd614ad',
        'is_toplevel': True,
        'composite_type': [
            'e40489cd1e7102e35469c937e05c8bba',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
        ],
        'named_type': 'Inheritance',
        'named_type_hash': 'e40489cd1e7102e35469c937e05c8bba',
        'key_0': 'bb34ce95f161a6b37ff54b3d4c817857',
        'key_1': '0a32b476852eeb954979b87f5f6cb7af',
    },
    {
        '_id': 'b0f428929706d1d991e4d712ad08f9ab',
        'composite_type_hash': '41c082428b28d7e9ea96160f7fd614ad',
        'is_toplevel': True,
        'composite_type': [
            'e40489cd1e7102e35469c937e05c8bba',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
        ],
        'named_type': 'Inheritance',
        'named_type_hash': 'e40489cd1e7102e35469c937e05c8bba',
        'key_0': 'b99ae727c787f1b13b452fd4c9ce1b9a',
        'key_1': '0a32b476852eeb954979b87f5f6cb7af',
    },
    {
        '_id': 'c93e1e758c53912638438e2a7d7f7b7f',
        'composite_type_hash': '41c082428b28d7e9ea96160f7fd614ad',
        'is_toplevel': True,
        'composite_type': [
            'e40489cd1e7102e35469c937e05c8bba',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
        ],
        'named_type': 'Inheritance',
        'named_type_hash': 'e40489cd1e7102e35469c937e05c8bba',
        'key_0': 'af12f10f9ae2002a1607ba0b47ba8407',
        'key_1': 'bdfe4e7a431f73386f37c6448afe5840',
    },
    {
        '_id': 'e431a2eda773adf06ef3f9268f93deaf',
        'composite_type_hash': 'ed73ea081d170e1d89fc950820ce1cee',
        'is_toplevel': True,
        'composite_type': [
            'a9dea78180588431ec64d6bc4872fdbc',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
        ],
        'named_type': 'Similarity',
        'named_type_hash': 'a9dea78180588431ec64d6bc4872fdbc',
        'key_0': 'c1db9b517073e51eb7ef6fed608ec204',
        'key_1': 'b94941d8cd1c0ee4ad3dd3dcab52b964',
    },
    {
        '_id': 'e4685d56969398253b6f77efd21dc347',
        'composite_type_hash': '41c082428b28d7e9ea96160f7fd614ad',
        'is_toplevel': True,
        'composite_type': [
            'e40489cd1e7102e35469c937e05c8bba',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
        ],
        'named_type': 'Inheritance',
        'named_type_hash': 'e40489cd1e7102e35469c937e05c8bba',
        'key_0': 'b94941d8cd1c0ee4ad3dd3dcab52b964',
        'key_1': '80aff30094874e75028033a38ce677bb',
    },
    {
        '_id': 'ee1c03e6d1f104ccd811cfbba018451a',
        'composite_type_hash': '41c082428b28d7e9ea96160f7fd614ad',
        'is_toplevel': True,
        'composite_type': [
            'e40489cd1e7102e35469c937e05c8bba',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
        ],
        'named_type': 'Inheritance',
        'named_type_hash': 'e40489cd1e7102e35469c937e05c8bba',
        'key_0': '4e8e26e3276af8a5c2ac2cc2dc95c6d2',
        'key_1': '80aff30094874e75028033a38ce677bb',
    },
    {
        '_id': '1c3bf151ea200b2d9e088a1178d060cb',
        'composite_type_hash': '41c082428b28d7e9ea96160f7fd614ad',
        'is_toplevel': True,
        'composite_type': [
            'e40489cd1e7102e35469c937e05c8bba',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
        ],
        'named_type': 'Inheritance',
        'named_type_hash': 'e40489cd1e7102e35469c937e05c8bba',
        'key_0': 'bdfe4e7a431f73386f37c6448afe5840',
        'key_1': '0a32b476852eeb954979b87f5f6cb7af',
    },
    {
        '_id': '2a8a69c01305563932b957de4b3a9ba6',
        'composite_type_hash': 'ed73ea081d170e1d89fc950820ce1cee',
        'is_toplevel': True,
        'composite_type': [
            'a9dea78180588431ec64d6bc4872fdbc',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
        ],
        'named_type': 'Similarity',
        'named_type_hash': 'a9dea78180588431ec64d6bc4872fdbc',
        'key_0': '1cdffc6b0b89ff41d68bec237481d1e1',
        'key_1': 'af12f10f9ae2002a1607ba0b47ba8407',
    },
    {
        '_id': 'abe6ad743fc81bd1c55ece2e1307a178',
        'composite_type_hash': 'ed73ea081d170e1d89fc950820ce1cee',
        'is_toplevel': True,
        'composite_type': [
            'a9dea78180588431ec64d6bc4872fdbc',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
        ],
        'named_type': 'Similarity',
        'named_type_hash': 'a9dea78180588431ec64d6bc4872fdbc',
        'key_0': '5b34c54bee150c04f9fa584b899dc030',
        'key_1': '1cdffc6b0b89ff41d68bec237481d1e1',
    },
    {
        '_id': '116df61c01859c710d178ba14a483509',
        'composite_type_hash': '41c082428b28d7e9ea96160f7fd614ad',
        'is_toplevel': True,
        'composite_type': [
            'e40489cd1e7102e35469c937e05c8bba',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
        ],
        'named_type': 'Inheritance',
        'named_type_hash': 'e40489cd1e7102e35469c937e05c8bba',
        'key_0': 'c1db9b517073e51eb7ef6fed608ec204',
        'key_1': 'b99ae727c787f1b13b452fd4c9ce1b9a',
    },
    {
        '_id': '906fa505ae3bc6336d80a5f9aaa47b3b',
        'composite_type_hash': '41c082428b28d7e9ea96160f7fd614ad',
        'is_toplevel': True,
        'composite_type': [
            'e40489cd1e7102e35469c937e05c8bba',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
        ],
        'named_type': 'Inheritance',
        'named_type_hash': 'e40489cd1e7102e35469c937e05c8bba',
        'key_0': 'd03e59654221c1e8fcda404fd5c8d6cb',
        'key_1': '08126b066d32ee37743e255a2558cccd',
    },
    {
        '_id': 'f31dfe97db782e8cec26de18dddf8965',
        'composite_type_hash': '41c082428b28d7e9ea96160f7fd614ad',
        'is_toplevel': True,
        'composite_type': [
            'e40489cd1e7102e35469c937e05c8bba',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
        ],
        'named_type': 'Inheritance',
        'named_type_hash': 'e40489cd1e7102e35469c937e05c8bba',
        'key_0': '1cdffc6b0b89ff41d68bec237481d1e1',
        'key_1': 'bdfe4e7a431f73386f37c6448afe5840',
    },
    {
        '_id': '16f7e407087bfa0b35b13d13a1aadcae',
        'composite_type_hash': 'ed73ea081d170e1d89fc950820ce1cee',
        'is_toplevel': True,
        'composite_type': [
            'a9dea78180588431ec64d6bc4872fdbc',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
        ],
        'named_type': 'Similarity',
        'named_type_hash': 'a9dea78180588431ec64d6bc4872fdbc',
        'key_0': 'af12f10f9ae2002a1607ba0b47ba8407',
        'key_1': '4e8e26e3276af8a5c2ac2cc2dc95c6d2',
    },
    {
        '_id': 'b5459e299a5c5e8662c427f7e01b3bf1',
        'composite_type_hash': 'ed73ea081d170e1d89fc950820ce1cee',
        'is_toplevel': True,
        'composite_type': [
            'a9dea78180588431ec64d6bc4872fdbc',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
        ],
        'named_type': 'Similarity',
        'named_type_hash': 'a9dea78180588431ec64d6bc4872fdbc',
        'key_0': 'af12f10f9ae2002a1607ba0b47ba8407',
        'key_1': '5b34c54bee150c04f9fa584b899dc030',
    },
    {
        '_id': '9923fc3e46d779c925d26ac4cf2d9e3b',
        'composite_type_hash': 'ed73ea081d170e1d89fc950820ce1cee',
        'is_toplevel': True,
        'composite_type': [
            'a9dea78180588431ec64d6bc4872fdbc',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
        ],
        'named_type': 'Similarity',
        'named_type_hash': 'a9dea78180588431ec64d6bc4872fdbc',
        'key_0': 'b94941d8cd1c0ee4ad3dd3dcab52b964',
        'key_1': 'c1db9b517073e51eb7ef6fed608ec204',
    },
    {
        '_id': 'bad7472f41a0e7d601ca294eb4607c3a',
        'composite_type_hash': 'ed73ea081d170e1d89fc950820ce1cee',
        'is_toplevel': True,
        'composite_type': [
            'a9dea78180588431ec64d6bc4872fdbc',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
        ],
        'named_type': 'Similarity',
        'named_type_hash': 'a9dea78180588431ec64d6bc4872fdbc',
        'key_0': 'af12f10f9ae2002a1607ba0b47ba8407',
        'key_1': '1cdffc6b0b89ff41d68bec237481d1e1',
    },
    {
        '_id': '959924e3aab197af80a84c1ab261fd65',
        'composite_type_hash': '41c082428b28d7e9ea96160f7fd614ad',
        'is_toplevel': True,
        'composite_type': [
            'e40489cd1e7102e35469c937e05c8bba',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
        ],
        'named_type': 'Inheritance',
        'named_type_hash': 'e40489cd1e7102e35469c937e05c8bba',
        'key_0': '08126b066d32ee37743e255a2558cccd',
        'key_1': 'b99ae727c787f1b13b452fd4c9ce1b9a',
    },
    {
        '_id': 'a45af31b43ee5ea271214338a5a5bd61',
        'composite_type_hash': 'ed73ea081d170e1d89fc950820ce1cee',
        'is_toplevel': True,
        'composite_type': [
            'a9dea78180588431ec64d6bc4872fdbc',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
        ],
        'named_type': 'Similarity',
        'named_type_hash': 'a9dea78180588431ec64d6bc4872fdbc',
        'key_0': '4e8e26e3276af8a5c2ac2cc2dc95c6d2',
        'key_1': 'af12f10f9ae2002a1607ba0b47ba8407',
    },
    {
        '_id': '72d0f9904bda2f89f9b68a1010ac61b5',
        'composite_type_hash': 'ed73ea081d170e1d89fc950820ce1cee',
        'is_toplevel': True,
        'composite_type': [
            'a9dea78180588431ec64d6bc4872fdbc',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
        ],
        'named_type': 'Similarity',
        'named_type_hash': 'a9dea78180588431ec64d6bc4872fdbc',
        'key_0': '99d18c702e813b07260baf577c60c455',
        'key_1': 'd03e59654221c1e8fcda404fd5c8d6cb',
    },
    {
        '_id': 'aef4d3da2565a640e15a52fd98d24d15',
        'composite_type_hash': 'ed73ea081d170e1d89fc950820ce1cee',
        'is_toplevel': True,
        'composite_type': [
            'a9dea78180588431ec64d6bc4872fdbc',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
        ],
        'named_type': 'Similarity',
        'named_type_hash': 'a9dea78180588431ec64d6bc4872fdbc',
        'key_0': 'd03e59654221c1e8fcda404fd5c8d6cb',
        'key_1': '99d18c702e813b07260baf577c60c455',
    },
    {
        '_id': '1e8ba9639663105e6c735ba83174f789',
        'composite_type': [
            'b74a43dbb36287ea86eb5b0c7b86e8e8',
            '79a5be2004199066acb26e7c1c963c29',
            'd99a604c79ce3c2e76a2f43488d5d4c3',
        ],
        'composite_type_hash': '158e76774ba76f59ef774871252cfb7e',
        'is_toplevel': False,
        'key_0': '07508083e73bbc1e9ad513dd10a968ae',
        'key_1': '24bc29bd87ecc3b3bc6c16c646506438',
        'named_type': 'Evaluation',
        'named_type_hash': 'b74a43dbb36287ea86eb5b0c7b86e8e8',
    },
    {
        '_id': 'd542caa94b57219f1e489e3b03be7126',
        'composite_type': [
            'b74a43dbb36287ea86eb5b0c7b86e8e8',
            '3b1b1a93a9b97ec3c8f2636fc6d54d0f',
            [
                'b74a43dbb36287ea86eb5b0c7b86e8e8',
                '79a5be2004199066acb26e7c1c963c29',
                'd99a604c79ce3c2e76a2f43488d5d4c3',
            ],
        ],
        'composite_type_hash': '0ef46597d9234ad94b014af4a1997545',
        'is_toplevel': True,
        'key_0': 'a912032ece1826e55fa583dcaacdc4a9',
        'key_1': '1e8ba9639663105e6c735ba83174f789',
        'named_type': 'Evaluation',
        'named_type_hash': 'b74a43dbb36287ea86eb5b0c7b86e8e8',
    },
]
added_links_arity_2 = []

outgoing_set_redis_mock_data = [
    {
        'outgoing_set:31535ddf214f5b239d3b517823cb8144': [
            b'1cdffc6b0b89ff41d68bec237481d1e1',
            b'5b34c54bee150c04f9fa584b899dc030',
        ]
    },
    {
        'outgoing_set:72d0f9904bda2f89f9b68a1010ac61b5': [
            b'99d18c702e813b07260baf577c60c455',
            b'd03e59654221c1e8fcda404fd5c8d6cb',
        ]
    },
    {
        'outgoing_set:2c927fdc6c0f1272ee439ceb76a6d1a4': [
            b'af12f10f9ae2002a1607ba0b47ba8407',
            b'5b34c54bee150c04f9fa584b899dc030',
        ]
    },
    {
        'outgoing_set:b0f428929706d1d991e4d712ad08f9ab': [
            b'b99ae727c787f1b13b452fd4c9ce1b9a',
            b'0a32b476852eeb954979b87f5f6cb7af',
        ]
    },
    {
        'outgoing_set:959924e3aab197af80a84c1ab261fd65': [
            b'08126b066d32ee37743e255a2558cccd',
            b'b99ae727c787f1b13b452fd4c9ce1b9a',
        ]
    },
    {
        'outgoing_set:e431a2eda773adf06ef3f9268f93deaf': [
            b'c1db9b517073e51eb7ef6fed608ec204',
            b'b94941d8cd1c0ee4ad3dd3dcab52b964',
        ]
    },
    {
        'outgoing_set:ee1c03e6d1f104ccd811cfbba018451a': [
            b'80aff30094874e75028033a38ce677bb',
            b'4e8e26e3276af8a5c2ac2cc2dc95c6d2',
        ]
    },
    {
        'outgoing_set:9923fc3e46d779c925d26ac4cf2d9e3b': [
            b'c1db9b517073e51eb7ef6fed608ec204',
            b'b94941d8cd1c0ee4ad3dd3dcab52b964',
        ]
    },
    {
        'outgoing_set:1c3bf151ea200b2d9e088a1178d060cb': [
            b'bdfe4e7a431f73386f37c6448afe5840',
            b'0a32b476852eeb954979b87f5f6cb7af',
        ]
    },
    {
        'outgoing_set:16f7e407087bfa0b35b13d13a1aadcae': [
            b'af12f10f9ae2002a1607ba0b47ba8407',
            b'4e8e26e3276af8a5c2ac2cc2dc95c6d2',
        ]
    },
    {
        'outgoing_set:f31dfe97db782e8cec26de18dddf8965': [
            b'bdfe4e7a431f73386f37c6448afe5840',
            b'1cdffc6b0b89ff41d68bec237481d1e1',
        ]
    },
    {
        'outgoing_set:c93e1e758c53912638438e2a7d7f7b7f': [
            b'af12f10f9ae2002a1607ba0b47ba8407',
            b'bdfe4e7a431f73386f37c6448afe5840',
        ]
    },
    {
        'outgoing_set:75756335011dcedb71a0d9a7bd2da9e8': [
            b'bdfe4e7a431f73386f37c6448afe5840',
            b'5b34c54bee150c04f9fa584b899dc030',
        ]
    },
    {
        'outgoing_set:2d7abd27644a9c08a7ca2c8d68338579': [
            b'c1db9b517073e51eb7ef6fed608ec204',
            b'bb34ce95f161a6b37ff54b3d4c817857',
        ]
    },
    {
        'outgoing_set:a45af31b43ee5ea271214338a5a5bd61': [
            b'af12f10f9ae2002a1607ba0b47ba8407',
            b'4e8e26e3276af8a5c2ac2cc2dc95c6d2',
        ]
    },
    {
        'outgoing_set:b5459e299a5c5e8662c427f7e01b3bf1': [
            b'af12f10f9ae2002a1607ba0b47ba8407',
            b'5b34c54bee150c04f9fa584b899dc030',
        ]
    },
    {
        'outgoing_set:bad7472f41a0e7d601ca294eb4607c3a': [
            b'af12f10f9ae2002a1607ba0b47ba8407',
            b'1cdffc6b0b89ff41d68bec237481d1e1',
        ]
    },
    {
        'outgoing_set:116df61c01859c710d178ba14a483509': [
            b'c1db9b517073e51eb7ef6fed608ec204',
            b'b99ae727c787f1b13b452fd4c9ce1b9a',
        ]
    },
    {
        'outgoing_set:2a8a69c01305563932b957de4b3a9ba6': [
            b'af12f10f9ae2002a1607ba0b47ba8407',
            b'1cdffc6b0b89ff41d68bec237481d1e1',
        ]
    },
    {
        'outgoing_set:7ee00a03f67b39f620bd3d0f6ed0c3e6': [
            b'c1db9b517073e51eb7ef6fed608ec204',
            b'bb34ce95f161a6b37ff54b3d4c817857',
        ]
    },
    {
        'outgoing_set:abe6ad743fc81bd1c55ece2e1307a178': [
            b'1cdffc6b0b89ff41d68bec237481d1e1',
            b'5b34c54bee150c04f9fa584b899dc030',
        ]
    },
    {
        'outgoing_set:aef4d3da2565a640e15a52fd98d24d15': [
            b'99d18c702e813b07260baf577c60c455',
            b'd03e59654221c1e8fcda404fd5c8d6cb',
        ]
    },
    {
        'outgoing_set:4120e428ab0fa162a04328e5217912ff': [
            b'0a32b476852eeb954979b87f5f6cb7af',
            b'bb34ce95f161a6b37ff54b3d4c817857',
        ]
    },
    {
        'outgoing_set:e4685d56969398253b6f77efd21dc347': [
            b'80aff30094874e75028033a38ce677bb',
            b'b94941d8cd1c0ee4ad3dd3dcab52b964',
        ]
    },
    {
        'outgoing_set:fbf03d17d6a40feff828a3f2c6e86f05': [
            b'bdfe4e7a431f73386f37c6448afe5840',
            b'99d18c702e813b07260baf577c60c455',
        ]
    },
    {
        'outgoing_set:906fa505ae3bc6336d80a5f9aaa47b3b': [
            b'08126b066d32ee37743e255a2558cccd',
            b'd03e59654221c1e8fcda404fd5c8d6cb',
        ]
    },
    {
        'outgoing_set:dc2891a1e8cb273c1c87b4b539615511': [
            b'8a224e9b499baf68bf02a3f72335806c',
            b'0ddefbdb97e354f36b694dfb5ae33922',
        ]
    },
    {
        'outgoing_set:ee8aa90f2f1b6eba761359fbf65ac39d': [
            b'da8db3df47c6d03b44ed4d357715aeff',
            b'dc2891a1e8cb273c1c87b4b539615511',
        ]
    },
    {
        'outgoing_set:b5c9e71594b40cd532b34baf0be29e11': [
            b'da8db3df47c6d03b44ed4d357715aeff',
            b'ee8aa90f2f1b6eba761359fbf65ac39d',
        ]
    },
]

incomming_set_redis_mock_data = [
    {
        'incomming_set:5b34c54bee150c04f9fa584b899dc030': [
            '2c927fdc6c0f1272ee439ceb76a6d1a4',
            'abe6ad743fc81bd1c55ece2e1307a178',
            '31535ddf214f5b239d3b517823cb8144',
            'b5459e299a5c5e8662c427f7e01b3bf1',
            '75756335011dcedb71a0d9a7bd2da9e8',
        ]
    },
    {
        'incomming_set:bdfe4e7a431f73386f37c6448afe5840': [
            'c93e1e758c53912638438e2a7d7f7b7f',
            'fbf03d17d6a40feff828a3f2c6e86f05',
            '1c3bf151ea200b2d9e088a1178d060cb',
            'f31dfe97db782e8cec26de18dddf8965',
            '75756335011dcedb71a0d9a7bd2da9e8',
        ]
    },
    {
        'incomming_set:99d18c702e813b07260baf577c60c455': [
            'aef4d3da2565a640e15a52fd98d24d15',
            'fbf03d17d6a40feff828a3f2c6e86f05',
            '72d0f9904bda2f89f9b68a1010ac61b5',
        ]
    },
    {
        'incomming_set:c1db9b517073e51eb7ef6fed608ec204': [
            '2d7abd27644a9c08a7ca2c8d68338579',
            '9923fc3e46d779c925d26ac4cf2d9e3b',
            'e431a2eda773adf06ef3f9268f93deaf',
            '116df61c01859c710d178ba14a483509',
            '7ee00a03f67b39f620bd3d0f6ed0c3e6',
        ]
    },
    {
        'incomming_set:4e8e26e3276af8a5c2ac2cc2dc95c6d2': [
            '16f7e407087bfa0b35b13d13a1aadcae',
            'ee1c03e6d1f104ccd811cfbba018451a',
            'a45af31b43ee5ea271214338a5a5bd61',
        ]
    },
    {
        'incomming_set:08126b066d32ee37743e255a2558cccd': [
            '959924e3aab197af80a84c1ab261fd65',
            '906fa505ae3bc6336d80a5f9aaa47b3b',
        ]
    },
    {
        'incomming_set:bb34ce95f161a6b37ff54b3d4c817857': [
            '4120e428ab0fa162a04328e5217912ff',
            '2d7abd27644a9c08a7ca2c8d68338579',
            '7ee00a03f67b39f620bd3d0f6ed0c3e6',
        ]
    },
    {
        'incomming_set:0a32b476852eeb954979b87f5f6cb7af': [
            '1c3bf151ea200b2d9e088a1178d060cb',
            '4120e428ab0fa162a04328e5217912ff',
            'b0f428929706d1d991e4d712ad08f9ab',
        ]
    },
    {
        'incomming_set:b94941d8cd1c0ee4ad3dd3dcab52b964': [
            '9923fc3e46d779c925d26ac4cf2d9e3b',
            'e4685d56969398253b6f77efd21dc347',
            'e431a2eda773adf06ef3f9268f93deaf',
        ]
    },
    {
        'incomming_set:1cdffc6b0b89ff41d68bec237481d1e1': [
            'abe6ad743fc81bd1c55ece2e1307a178',
            '31535ddf214f5b239d3b517823cb8144',
            '2a8a69c01305563932b957de4b3a9ba6',
            'bad7472f41a0e7d601ca294eb4607c3a',
            'f31dfe97db782e8cec26de18dddf8965',
        ]
    },
    {
        'incomming_set:af12f10f9ae2002a1607ba0b47ba8407': [
            'c93e1e758c53912638438e2a7d7f7b7f',
            'a45af31b43ee5ea271214338a5a5bd61',
            '2c927fdc6c0f1272ee439ceb76a6d1a4',
            '2a8a69c01305563932b957de4b3a9ba6',
            'b5459e299a5c5e8662c427f7e01b3bf1',
            'bad7472f41a0e7d601ca294eb4607c3a',
            '16f7e407087bfa0b35b13d13a1aadcae',
        ]
    },
    {
        'incomming_set:d03e59654221c1e8fcda404fd5c8d6cb': [
            'aef4d3da2565a640e15a52fd98d24d15',
            '906fa505ae3bc6336d80a5f9aaa47b3b',
            '72d0f9904bda2f89f9b68a1010ac61b5',
        ]
    },
    {
        'incomming_set:80aff30094874e75028033a38ce677bb': [
            'ee1c03e6d1f104ccd811cfbba018451a',
            'e4685d56969398253b6f77efd21dc347',
        ]
    },
    {
        'incomming_set:b99ae727c787f1b13b452fd4c9ce1b9a': [
            '959924e3aab197af80a84c1ab261fd65',
            '116df61c01859c710d178ba14a483509',
            'b0f428929706d1d991e4d712ad08f9ab',
        ]
    },
    {'incomming_set:8a224e9b499baf68bf02a3f72335806c': ['dc2891a1e8cb273c1c87b4b539615511']},
    {'incomming_set:0ddefbdb97e354f36b694dfb5ae33922': ['dc2891a1e8cb273c1c87b4b539615511']},
    {
        'incomming_set:da8db3df47c6d03b44ed4d357715aeff': [
            'ee8aa90f2f1b6eba761359fbf65ac39d',
            'b5c9e71594b40cd532b34baf0be29e11',
        ]
    },
    {'incomming_set:dc2891a1e8cb273c1c87b4b539615511': ['ee8aa90f2f1b6eba761359fbf65ac39d']},
    {'incomming_set:ee8aa90f2f1b6eba761359fbf65ac39d': ['b5c9e71594b40cd532b34baf0be29e11']},
]

patterns_redis_mock_data = {
    "patterns:33ece2fbf2de88041254699ce03e5d12": [
        (
            "fbf03d17d6a40feff828a3f2c6e86f05",
            (
                "99d18c702e813b07260baf577c60c455",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        )
    ],
    "patterns:bc715adab19a81146b0f05f298138d66": [
        (
            "959924e3aab197af80a84c1ab261fd65",
            (
                "08126b066d32ee37743e255a2558cccd",
                "b99ae727c787f1b13b452fd4c9ce1b9a",
            ),
        )
    ],
    "patterns:599b247a48a77f14348df0ff9711e615": [
        (
            "16f7e407087bfa0b35b13d13a1aadcae",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "4e8e26e3276af8a5c2ac2cc2dc95c6d2",
            ),
        ),
        (
            "bad7472f41a0e7d601ca294eb4607c3a",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "1cdffc6b0b89ff41d68bec237481d1e1",
            ),
        ),
        (
            "b5459e299a5c5e8662c427f7e01b3bf1",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "5b34c54bee150c04f9fa584b899dc030",
            ),
        ),
    ],
    "patterns:e43eba1ad6978dda624397c44f6fadd1": [
        (
            "959924e3aab197af80a84c1ab261fd65",
            (
                "08126b066d32ee37743e255a2558cccd",
                "b99ae727c787f1b13b452fd4c9ce1b9a",
            ),
        )
    ],
    "patterns:36b1e657e2b7d0170f656c175ea5e7d9": [
        (
            "31535ddf214f5b239d3b517823cb8144",
            (
                "1cdffc6b0b89ff41d68bec237481d1e1",
                "5b34c54bee150c04f9fa584b899dc030",
            ),
        ),
        (
            "b5459e299a5c5e8662c427f7e01b3bf1",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "5b34c54bee150c04f9fa584b899dc030",
            ),
        ),
    ],
    "patterns:d4a7666708c529cbc0f0144da31eb414": [
        (
            "ee1c03e6d1f104ccd811cfbba018451a",
            (
                "4e8e26e3276af8a5c2ac2cc2dc95c6d2",
                "80aff30094874e75028033a38ce677bb",
            ),
        ),
        (
            "e4685d56969398253b6f77efd21dc347",
            (
                "b94941d8cd1c0ee4ad3dd3dcab52b964",
                "80aff30094874e75028033a38ce677bb",
            ),
        ),
    ],
    "patterns:a2e5f55c0885bdabe057e32c1d929174": [
        (
            "72d0f9904bda2f89f9b68a1010ac61b5",
            (
                "99d18c702e813b07260baf577c60c455",
                "d03e59654221c1e8fcda404fd5c8d6cb",
            ),
        )
    ],
    "patterns:c48b5236102ae75ba3e71729a6bfa2e5": [
        (
            "7ee00a03f67b39f620bd3d0f6ed0c3e6",
            (
                "c1db9b517073e51eb7ef6fed608ec204",
                "bb34ce95f161a6b37ff54b3d4c817857",
            ),
        ),
        (
            "abe6ad743fc81bd1c55ece2e1307a178",
            (
                "5b34c54bee150c04f9fa584b899dc030",
                "1cdffc6b0b89ff41d68bec237481d1e1",
            ),
        ),
        (
            "31535ddf214f5b239d3b517823cb8144",
            (
                "1cdffc6b0b89ff41d68bec237481d1e1",
                "5b34c54bee150c04f9fa584b899dc030",
            ),
        ),
        (
            "2d7abd27644a9c08a7ca2c8d68338579",
            (
                "bb34ce95f161a6b37ff54b3d4c817857",
                "c1db9b517073e51eb7ef6fed608ec204",
            ),
        ),
        (
            "16f7e407087bfa0b35b13d13a1aadcae",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "4e8e26e3276af8a5c2ac2cc2dc95c6d2",
            ),
        ),
        (
            "bad7472f41a0e7d601ca294eb4607c3a",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "1cdffc6b0b89ff41d68bec237481d1e1",
            ),
        ),
        (
            "9923fc3e46d779c925d26ac4cf2d9e3b",
            (
                "b94941d8cd1c0ee4ad3dd3dcab52b964",
                "c1db9b517073e51eb7ef6fed608ec204",
            ),
        ),
        (
            "2c927fdc6c0f1272ee439ceb76a6d1a4",
            (
                "5b34c54bee150c04f9fa584b899dc030",
                "af12f10f9ae2002a1607ba0b47ba8407",
            ),
        ),
        (
            "e431a2eda773adf06ef3f9268f93deaf",
            (
                "c1db9b517073e51eb7ef6fed608ec204",
                "b94941d8cd1c0ee4ad3dd3dcab52b964",
            ),
        ),
        (
            "a45af31b43ee5ea271214338a5a5bd61",
            (
                "4e8e26e3276af8a5c2ac2cc2dc95c6d2",
                "af12f10f9ae2002a1607ba0b47ba8407",
            ),
        ),
        (
            "aef4d3da2565a640e15a52fd98d24d15",
            (
                "d03e59654221c1e8fcda404fd5c8d6cb",
                "99d18c702e813b07260baf577c60c455",
            ),
        ),
        (
            "2a8a69c01305563932b957de4b3a9ba6",
            (
                "1cdffc6b0b89ff41d68bec237481d1e1",
                "af12f10f9ae2002a1607ba0b47ba8407",
            ),
        ),
        (
            "b5459e299a5c5e8662c427f7e01b3bf1",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "5b34c54bee150c04f9fa584b899dc030",
            ),
        ),
        (
            "72d0f9904bda2f89f9b68a1010ac61b5",
            (
                "99d18c702e813b07260baf577c60c455",
                "d03e59654221c1e8fcda404fd5c8d6cb",
            ),
        ),
    ],
    "patterns:d024789956d6b49fdfdcdd32f25c0770": [
        (
            "116df61c01859c710d178ba14a483509",
            (
                "c1db9b517073e51eb7ef6fed608ec204",
                "b99ae727c787f1b13b452fd4c9ce1b9a",
            ),
        )
    ],
    "patterns:3ac5e7cda6d75569e64aa31bc9408a95": [
        (
            "16f7e407087bfa0b35b13d13a1aadcae",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "4e8e26e3276af8a5c2ac2cc2dc95c6d2",
            ),
        )
    ],
    "patterns:6066388a9e29f972424275ea2f39715d": [
        (
            "16f7e407087bfa0b35b13d13a1aadcae",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "4e8e26e3276af8a5c2ac2cc2dc95c6d2",
            ),
        ),
        (
            "bad7472f41a0e7d601ca294eb4607c3a",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "1cdffc6b0b89ff41d68bec237481d1e1",
            ),
        ),
        (
            "c93e1e758c53912638438e2a7d7f7b7f",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        ),
        (
            "b5459e299a5c5e8662c427f7e01b3bf1",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "5b34c54bee150c04f9fa584b899dc030",
            ),
        ),
    ],
    "patterns:e45b923c049c6fc2826059e291a3d161": [
        (
            "7ee00a03f67b39f620bd3d0f6ed0c3e6",
            (
                "c1db9b517073e51eb7ef6fed608ec204",
                "bb34ce95f161a6b37ff54b3d4c817857",
            ),
        )
    ],
    "patterns:f5269cbecbad862d32ffe6d599cc8c65": [
        (
            "2c927fdc6c0f1272ee439ceb76a6d1a4",
            (
                "5b34c54bee150c04f9fa584b899dc030",
                "af12f10f9ae2002a1607ba0b47ba8407",
            ),
        ),
        (
            "abe6ad743fc81bd1c55ece2e1307a178",
            (
                "5b34c54bee150c04f9fa584b899dc030",
                "1cdffc6b0b89ff41d68bec237481d1e1",
            ),
        ),
    ],
    "patterns:b3036a9a22fc020e3600445bd9585537": [
        (
            "72d0f9904bda2f89f9b68a1010ac61b5",
            (
                "99d18c702e813b07260baf577c60c455",
                "d03e59654221c1e8fcda404fd5c8d6cb",
            ),
        )
    ],
    "patterns:86b4631eb9e7495a0a492f6888b351c3": [
        (
            "9923fc3e46d779c925d26ac4cf2d9e3b",
            (
                "b94941d8cd1c0ee4ad3dd3dcab52b964",
                "c1db9b517073e51eb7ef6fed608ec204",
            ),
        )
    ],
    "patterns:60b0c956050d10392d422fe4fb616c74": [
        (
            "f31dfe97db782e8cec26de18dddf8965",
            (
                "1cdffc6b0b89ff41d68bec237481d1e1",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        )
    ],
    "patterns:7ead6cfa03894c62761162b7603aa885": [
        (
            "ee1c03e6d1f104ccd811cfbba018451a",
            (
                "4e8e26e3276af8a5c2ac2cc2dc95c6d2",
                "80aff30094874e75028033a38ce677bb",
            ),
        ),
        (
            "e4685d56969398253b6f77efd21dc347",
            (
                "b94941d8cd1c0ee4ad3dd3dcab52b964",
                "80aff30094874e75028033a38ce677bb",
            ),
        ),
        (
            "906fa505ae3bc6336d80a5f9aaa47b3b",
            (
                "d03e59654221c1e8fcda404fd5c8d6cb",
                "08126b066d32ee37743e255a2558cccd",
            ),
        ),
        (
            "75756335011dcedb71a0d9a7bd2da9e8",
            (
                "5b34c54bee150c04f9fa584b899dc030",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        ),
        (
            "fbf03d17d6a40feff828a3f2c6e86f05",
            (
                "99d18c702e813b07260baf577c60c455",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        ),
        (
            "c93e1e758c53912638438e2a7d7f7b7f",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        ),
        (
            "116df61c01859c710d178ba14a483509",
            (
                "c1db9b517073e51eb7ef6fed608ec204",
                "b99ae727c787f1b13b452fd4c9ce1b9a",
            ),
        ),
        (
            "4120e428ab0fa162a04328e5217912ff",
            (
                "bb34ce95f161a6b37ff54b3d4c817857",
                "0a32b476852eeb954979b87f5f6cb7af",
            ),
        ),
        (
            "b0f428929706d1d991e4d712ad08f9ab",
            (
                "b99ae727c787f1b13b452fd4c9ce1b9a",
                "0a32b476852eeb954979b87f5f6cb7af",
            ),
        ),
        (
            "1c3bf151ea200b2d9e088a1178d060cb",
            (
                "bdfe4e7a431f73386f37c6448afe5840",
                "0a32b476852eeb954979b87f5f6cb7af",
            ),
        ),
        (
            "959924e3aab197af80a84c1ab261fd65",
            (
                "08126b066d32ee37743e255a2558cccd",
                "b99ae727c787f1b13b452fd4c9ce1b9a",
            ),
        ),
        (
            "f31dfe97db782e8cec26de18dddf8965",
            (
                "1cdffc6b0b89ff41d68bec237481d1e1",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        ),
    ],
    "patterns:23b1805ee2eee8b47b1336ed5eb67a63": [
        (
            "31535ddf214f5b239d3b517823cb8144",
            (
                "1cdffc6b0b89ff41d68bec237481d1e1",
                "5b34c54bee150c04f9fa584b899dc030",
            ),
        )
    ],
    "patterns:112e50d4310273ba9f99403948623d11": [
        (
            "b0f428929706d1d991e4d712ad08f9ab",
            (
                "b99ae727c787f1b13b452fd4c9ce1b9a",
                "0a32b476852eeb954979b87f5f6cb7af",
            ),
        )
    ],
    "patterns:e3c1b9073620a2d776110a2f867115e1": [
        (
            "c93e1e758c53912638438e2a7d7f7b7f",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        )
    ],
    "patterns:02715345a1c12653f16f99ef38d2dc74": [
        (
            "abe6ad743fc81bd1c55ece2e1307a178",
            (
                "5b34c54bee150c04f9fa584b899dc030",
                "1cdffc6b0b89ff41d68bec237481d1e1",
            ),
        )
    ],
    "patterns:207553a15eca937359e6940b2bce1147": [
        (
            "4120e428ab0fa162a04328e5217912ff",
            (
                "bb34ce95f161a6b37ff54b3d4c817857",
                "0a32b476852eeb954979b87f5f6cb7af",
            ),
        )
    ],
    "patterns:be73cdab95fb7e83d4a93b65c4dd60c4": [
        (
            "9923fc3e46d779c925d26ac4cf2d9e3b",
            (
                "b94941d8cd1c0ee4ad3dd3dcab52b964",
                "c1db9b517073e51eb7ef6fed608ec204",
            ),
        ),
        (
            "2d7abd27644a9c08a7ca2c8d68338579",
            (
                "bb34ce95f161a6b37ff54b3d4c817857",
                "c1db9b517073e51eb7ef6fed608ec204",
            ),
        ),
    ],
    "patterns:2586aef3dfb28f6d476188aa60846ac2": [
        (
            "116df61c01859c710d178ba14a483509",
            (
                "c1db9b517073e51eb7ef6fed608ec204",
                "b99ae727c787f1b13b452fd4c9ce1b9a",
            ),
        ),
        (
            "959924e3aab197af80a84c1ab261fd65",
            (
                "08126b066d32ee37743e255a2558cccd",
                "b99ae727c787f1b13b452fd4c9ce1b9a",
            ),
        ),
    ],
    "patterns:ee04dd921bcb34ddc9e7407961e2b171": [
        (
            "b5459e299a5c5e8662c427f7e01b3bf1",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "5b34c54bee150c04f9fa584b899dc030",
            ),
        )
    ],
    "patterns:9072a1bb302c235a29862699eec32433": [
        (
            "2c927fdc6c0f1272ee439ceb76a6d1a4",
            (
                "5b34c54bee150c04f9fa584b899dc030",
                "af12f10f9ae2002a1607ba0b47ba8407",
            ),
        )
    ],
    "patterns:a98f2b763cbf7350e2c01bbbd605709e": [
        (
            "a45af31b43ee5ea271214338a5a5bd61",
            (
                "4e8e26e3276af8a5c2ac2cc2dc95c6d2",
                "af12f10f9ae2002a1607ba0b47ba8407",
            ),
        )
    ],
    "patterns:3d5c095a8fced20a9c32369563fc51fb": [
        (
            "4120e428ab0fa162a04328e5217912ff",
            (
                "bb34ce95f161a6b37ff54b3d4c817857",
                "0a32b476852eeb954979b87f5f6cb7af",
            ),
        )
    ],
    "patterns:c811a103e6a4a924f35f9ec79b6f9bea": [
        (
            "c93e1e758c53912638438e2a7d7f7b7f",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        )
    ],
    "patterns:93392b12a01496f48e6ae816fa5423c2": [
        (
            "b0f428929706d1d991e4d712ad08f9ab",
            (
                "b99ae727c787f1b13b452fd4c9ce1b9a",
                "0a32b476852eeb954979b87f5f6cb7af",
            ),
        )
    ],
    "patterns:17f12e8041c9d3d14029bce4b3436bc5": [
        (
            "aef4d3da2565a640e15a52fd98d24d15",
            (
                "d03e59654221c1e8fcda404fd5c8d6cb",
                "99d18c702e813b07260baf577c60c455",
            ),
        )
    ],
    "patterns:4f038728598597ba7c1d9b7db83c6624": [
        (
            "16f7e407087bfa0b35b13d13a1aadcae",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "4e8e26e3276af8a5c2ac2cc2dc95c6d2",
            ),
        )
    ],
    "patterns:71eae19734970016ca8060eb69576c2a": [
        (
            "e431a2eda773adf06ef3f9268f93deaf",
            (
                "c1db9b517073e51eb7ef6fed608ec204",
                "b94941d8cd1c0ee4ad3dd3dcab52b964",
            ),
        )
    ],
    "patterns:38db3cf156a77e7666f5d84c00d2308d": [
        (
            "9923fc3e46d779c925d26ac4cf2d9e3b",
            (
                "b94941d8cd1c0ee4ad3dd3dcab52b964",
                "c1db9b517073e51eb7ef6fed608ec204",
            ),
        ),
        (
            "2d7abd27644a9c08a7ca2c8d68338579",
            (
                "bb34ce95f161a6b37ff54b3d4c817857",
                "c1db9b517073e51eb7ef6fed608ec204",
            ),
        ),
    ],
    "patterns:e0d9464a089b6183115b46dbdf7fcf31": [
        (
            "4120e428ab0fa162a04328e5217912ff",
            (
                "bb34ce95f161a6b37ff54b3d4c817857",
                "0a32b476852eeb954979b87f5f6cb7af",
            ),
        ),
        (
            "2d7abd27644a9c08a7ca2c8d68338579",
            (
                "bb34ce95f161a6b37ff54b3d4c817857",
                "c1db9b517073e51eb7ef6fed608ec204",
            ),
        ),
    ],
    "patterns:f3ec599397695b620e12fafc2e65c3dc": [
        (
            "75756335011dcedb71a0d9a7bd2da9e8",
            (
                "5b34c54bee150c04f9fa584b899dc030",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        )
    ],
    "patterns:e050d7c9ebfca41b026f80bf6f8ff75b": [
        (
            "7ee00a03f67b39f620bd3d0f6ed0c3e6",
            (
                "c1db9b517073e51eb7ef6fed608ec204",
                "bb34ce95f161a6b37ff54b3d4c817857",
            ),
        )
    ],
    "patterns:6ad6f251e177ffb0a4a69ba848ea5951": [
        (
            "116df61c01859c710d178ba14a483509",
            (
                "c1db9b517073e51eb7ef6fed608ec204",
                "b99ae727c787f1b13b452fd4c9ce1b9a",
            ),
        ),
        (
            "959924e3aab197af80a84c1ab261fd65",
            (
                "08126b066d32ee37743e255a2558cccd",
                "b99ae727c787f1b13b452fd4c9ce1b9a",
            ),
        ),
    ],
    "patterns:ca793f4d22e39f3623e37b64796372fe": [
        (
            "2c927fdc6c0f1272ee439ceb76a6d1a4",
            (
                "5b34c54bee150c04f9fa584b899dc030",
                "af12f10f9ae2002a1607ba0b47ba8407",
            ),
        ),
        (
            "a45af31b43ee5ea271214338a5a5bd61",
            (
                "4e8e26e3276af8a5c2ac2cc2dc95c6d2",
                "af12f10f9ae2002a1607ba0b47ba8407",
            ),
        ),
        (
            "2a8a69c01305563932b957de4b3a9ba6",
            (
                "1cdffc6b0b89ff41d68bec237481d1e1",
                "af12f10f9ae2002a1607ba0b47ba8407",
            ),
        ),
    ],
    "patterns:191cac7d4a23f69ac0dd5fc7364eb04a": [
        (
            "72d0f9904bda2f89f9b68a1010ac61b5",
            (
                "99d18c702e813b07260baf577c60c455",
                "d03e59654221c1e8fcda404fd5c8d6cb",
            ),
        )
    ],
    "patterns:a3fdfcd50ac65616075382d2fefd11fd": [
        (
            "fbf03d17d6a40feff828a3f2c6e86f05",
            (
                "99d18c702e813b07260baf577c60c455",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        )
    ],
    "patterns:8f097beb9b347c6044b6b2b639da5985": [
        (
            "ee1c03e6d1f104ccd811cfbba018451a",
            (
                "4e8e26e3276af8a5c2ac2cc2dc95c6d2",
                "80aff30094874e75028033a38ce677bb",
            ),
        )
    ],
    "patterns:22529d575a1670c3bbbaa3eda547d60e": [
        (
            "75756335011dcedb71a0d9a7bd2da9e8",
            (
                "5b34c54bee150c04f9fa584b899dc030",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        )
    ],
    "patterns:8e8fe0e9ff68f6ecb3bdfbac82ff6d35": [
        (
            "906fa505ae3bc6336d80a5f9aaa47b3b",
            (
                "d03e59654221c1e8fcda404fd5c8d6cb",
                "08126b066d32ee37743e255a2558cccd",
            ),
        )
    ],
    "patterns:bccfc7700d7fbfe77977afb9a9b7bc1a": [
        (
            "aef4d3da2565a640e15a52fd98d24d15",
            (
                "d03e59654221c1e8fcda404fd5c8d6cb",
                "99d18c702e813b07260baf577c60c455",
            ),
        )
    ],
    "patterns:3a724cad494e3c682a29058b2a9e3266": [
        (
            "906fa505ae3bc6336d80a5f9aaa47b3b",
            (
                "d03e59654221c1e8fcda404fd5c8d6cb",
                "08126b066d32ee37743e255a2558cccd",
            ),
        )
    ],
    "patterns:8f09c760225cf45b8f050de3a538e663": [
        (
            "e4685d56969398253b6f77efd21dc347",
            (
                "b94941d8cd1c0ee4ad3dd3dcab52b964",
                "80aff30094874e75028033a38ce677bb",
            ),
        ),
        (
            "9923fc3e46d779c925d26ac4cf2d9e3b",
            (
                "b94941d8cd1c0ee4ad3dd3dcab52b964",
                "c1db9b517073e51eb7ef6fed608ec204",
            ),
        ),
    ],
    "patterns:cce0902b2c15647f3312d55aebe71338": [
        (
            "31535ddf214f5b239d3b517823cb8144",
            (
                "1cdffc6b0b89ff41d68bec237481d1e1",
                "5b34c54bee150c04f9fa584b899dc030",
            ),
        ),
        (
            "2a8a69c01305563932b957de4b3a9ba6",
            (
                "1cdffc6b0b89ff41d68bec237481d1e1",
                "af12f10f9ae2002a1607ba0b47ba8407",
            ),
        ),
    ],
    "patterns:575e010a4bb0c25d964ddd35cbb9cb58": [
        (
            "2d7abd27644a9c08a7ca2c8d68338579",
            (
                "bb34ce95f161a6b37ff54b3d4c817857",
                "c1db9b517073e51eb7ef6fed608ec204",
            ),
        )
    ],
    "patterns:ab91d5ffc5a3ecb186ccadfc54aef0eb": [
        (
            "31535ddf214f5b239d3b517823cb8144",
            (
                "1cdffc6b0b89ff41d68bec237481d1e1",
                "5b34c54bee150c04f9fa584b899dc030",
            ),
        ),
        (
            "b5459e299a5c5e8662c427f7e01b3bf1",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "5b34c54bee150c04f9fa584b899dc030",
            ),
        ),
    ],
    "patterns:177ce9159d7af38dfa2720a65f547cec": [
        (
            "4120e428ab0fa162a04328e5217912ff",
            (
                "bb34ce95f161a6b37ff54b3d4c817857",
                "0a32b476852eeb954979b87f5f6cb7af",
            ),
        ),
        (
            "b0f428929706d1d991e4d712ad08f9ab",
            (
                "b99ae727c787f1b13b452fd4c9ce1b9a",
                "0a32b476852eeb954979b87f5f6cb7af",
            ),
        ),
        (
            "1c3bf151ea200b2d9e088a1178d060cb",
            (
                "bdfe4e7a431f73386f37c6448afe5840",
                "0a32b476852eeb954979b87f5f6cb7af",
            ),
        ),
    ],
    "patterns:5dd515aa7a451276feac4f8b9d84ae91": [
        (
            "75756335011dcedb71a0d9a7bd2da9e8",
            (
                "5b34c54bee150c04f9fa584b899dc030",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        ),
        (
            "fbf03d17d6a40feff828a3f2c6e86f05",
            (
                "99d18c702e813b07260baf577c60c455",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        ),
        (
            "c93e1e758c53912638438e2a7d7f7b7f",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        ),
        (
            "f31dfe97db782e8cec26de18dddf8965",
            (
                "1cdffc6b0b89ff41d68bec237481d1e1",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        ),
    ],
    "patterns:2073045fa2ad12eba1af55a257fd34f5": [
        (
            "116df61c01859c710d178ba14a483509",
            (
                "c1db9b517073e51eb7ef6fed608ec204",
                "b99ae727c787f1b13b452fd4c9ce1b9a",
            ),
        )
    ],
    "patterns:a240d2b25c0082b015763984c5d57a82": [
        (
            "e431a2eda773adf06ef3f9268f93deaf",
            (
                "c1db9b517073e51eb7ef6fed608ec204",
                "b94941d8cd1c0ee4ad3dd3dcab52b964",
            ),
        )
    ],
    "patterns:1f063c5794592bfa341bf854deab42f2": [
        (
            "906fa505ae3bc6336d80a5f9aaa47b3b",
            (
                "d03e59654221c1e8fcda404fd5c8d6cb",
                "08126b066d32ee37743e255a2558cccd",
            ),
        )
    ],
    "patterns:9bf5182cf1cc2d1607a2e892e85637e5": [
        (
            "bad7472f41a0e7d601ca294eb4607c3a",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "1cdffc6b0b89ff41d68bec237481d1e1",
            ),
        ),
        (
            "abe6ad743fc81bd1c55ece2e1307a178",
            (
                "5b34c54bee150c04f9fa584b899dc030",
                "1cdffc6b0b89ff41d68bec237481d1e1",
            ),
        ),
    ],
    "patterns:10ffc1e4bee3bf1940a343e2ce791210": [
        (
            "e4685d56969398253b6f77efd21dc347",
            (
                "b94941d8cd1c0ee4ad3dd3dcab52b964",
                "80aff30094874e75028033a38ce677bb",
            ),
        )
    ],
    "patterns:112002ff70ea491aad735f978e9d95f5": [
        (
            "75756335011dcedb71a0d9a7bd2da9e8",
            (
                "5b34c54bee150c04f9fa584b899dc030",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        ),
        (
            "fbf03d17d6a40feff828a3f2c6e86f05",
            (
                "99d18c702e813b07260baf577c60c455",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        ),
        (
            "c93e1e758c53912638438e2a7d7f7b7f",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        ),
        (
            "f31dfe97db782e8cec26de18dddf8965",
            (
                "1cdffc6b0b89ff41d68bec237481d1e1",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        ),
    ],
    "patterns:5a3bffe6a97084f0e267ff972303e5a6": [
        (
            "e4685d56969398253b6f77efd21dc347",
            (
                "b94941d8cd1c0ee4ad3dd3dcab52b964",
                "80aff30094874e75028033a38ce677bb",
            ),
        )
    ],
    "patterns:b0dc87d838daa18f735de1b9e1b08d49": [
        (
            "b0f428929706d1d991e4d712ad08f9ab",
            (
                "b99ae727c787f1b13b452fd4c9ce1b9a",
                "0a32b476852eeb954979b87f5f6cb7af",
            ),
        )
    ],
    "patterns:a2383971ffc7f41b3bdd74a849134087": [
        (
            "906fa505ae3bc6336d80a5f9aaa47b3b",
            (
                "d03e59654221c1e8fcda404fd5c8d6cb",
                "08126b066d32ee37743e255a2558cccd",
            ),
        )
    ],
    "patterns:5a2d7f958f944e7cbe1eb980d5718fdc": [
        (
            "e431a2eda773adf06ef3f9268f93deaf",
            (
                "c1db9b517073e51eb7ef6fed608ec204",
                "b94941d8cd1c0ee4ad3dd3dcab52b964",
            ),
        ),
        (
            "7ee00a03f67b39f620bd3d0f6ed0c3e6",
            (
                "c1db9b517073e51eb7ef6fed608ec204",
                "bb34ce95f161a6b37ff54b3d4c817857",
            ),
        ),
    ],
    "patterns:45467511cdeea84ad6971277ba74e074": [
        (
            "7ee00a03f67b39f620bd3d0f6ed0c3e6",
            (
                "c1db9b517073e51eb7ef6fed608ec204",
                "bb34ce95f161a6b37ff54b3d4c817857",
            ),
        )
    ],
    "patterns:6e644e70a9fe3145c88b5b6261af5754": [
        (
            "e4685d56969398253b6f77efd21dc347",
            (
                "b94941d8cd1c0ee4ad3dd3dcab52b964",
                "80aff30094874e75028033a38ce677bb",
            ),
        ),
        (
            "abe6ad743fc81bd1c55ece2e1307a178",
            (
                "5b34c54bee150c04f9fa584b899dc030",
                "1cdffc6b0b89ff41d68bec237481d1e1",
            ),
        ),
        (
            "16f7e407087bfa0b35b13d13a1aadcae",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "4e8e26e3276af8a5c2ac2cc2dc95c6d2",
            ),
        ),
        (
            "9923fc3e46d779c925d26ac4cf2d9e3b",
            (
                "b94941d8cd1c0ee4ad3dd3dcab52b964",
                "c1db9b517073e51eb7ef6fed608ec204",
            ),
        ),
        (
            "a45af31b43ee5ea271214338a5a5bd61",
            (
                "4e8e26e3276af8a5c2ac2cc2dc95c6d2",
                "af12f10f9ae2002a1607ba0b47ba8407",
            ),
        ),
        (
            "2a8a69c01305563932b957de4b3a9ba6",
            (
                "1cdffc6b0b89ff41d68bec237481d1e1",
                "af12f10f9ae2002a1607ba0b47ba8407",
            ),
        ),
        (
            "f31dfe97db782e8cec26de18dddf8965",
            (
                "1cdffc6b0b89ff41d68bec237481d1e1",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        ),
        (
            "7ee00a03f67b39f620bd3d0f6ed0c3e6",
            (
                "c1db9b517073e51eb7ef6fed608ec204",
                "bb34ce95f161a6b37ff54b3d4c817857",
            ),
        ),
        (
            "906fa505ae3bc6336d80a5f9aaa47b3b",
            (
                "d03e59654221c1e8fcda404fd5c8d6cb",
                "08126b066d32ee37743e255a2558cccd",
            ),
        ),
        (
            "fbf03d17d6a40feff828a3f2c6e86f05",
            (
                "99d18c702e813b07260baf577c60c455",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        ),
        (
            "c93e1e758c53912638438e2a7d7f7b7f",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        ),
        (
            "bad7472f41a0e7d601ca294eb4607c3a",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "1cdffc6b0b89ff41d68bec237481d1e1",
            ),
        ),
        (
            "116df61c01859c710d178ba14a483509",
            (
                "c1db9b517073e51eb7ef6fed608ec204",
                "b99ae727c787f1b13b452fd4c9ce1b9a",
            ),
        ),
        (
            "1c3bf151ea200b2d9e088a1178d060cb",
            (
                "bdfe4e7a431f73386f37c6448afe5840",
                "0a32b476852eeb954979b87f5f6cb7af",
            ),
        ),
        (
            "2c927fdc6c0f1272ee439ceb76a6d1a4",
            (
                "5b34c54bee150c04f9fa584b899dc030",
                "af12f10f9ae2002a1607ba0b47ba8407",
            ),
        ),
        (
            "e431a2eda773adf06ef3f9268f93deaf",
            (
                "c1db9b517073e51eb7ef6fed608ec204",
                "b94941d8cd1c0ee4ad3dd3dcab52b964",
            ),
        ),
        (
            "959924e3aab197af80a84c1ab261fd65",
            (
                "08126b066d32ee37743e255a2558cccd",
                "b99ae727c787f1b13b452fd4c9ce1b9a",
            ),
        ),
        (
            "b5459e299a5c5e8662c427f7e01b3bf1",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "5b34c54bee150c04f9fa584b899dc030",
            ),
        ),
        (
            "2d7abd27644a9c08a7ca2c8d68338579",
            (
                "bb34ce95f161a6b37ff54b3d4c817857",
                "c1db9b517073e51eb7ef6fed608ec204",
            ),
        ),
        (
            "4120e428ab0fa162a04328e5217912ff",
            (
                "bb34ce95f161a6b37ff54b3d4c817857",
                "0a32b476852eeb954979b87f5f6cb7af",
            ),
        ),
        (
            "b0f428929706d1d991e4d712ad08f9ab",
            (
                "b99ae727c787f1b13b452fd4c9ce1b9a",
                "0a32b476852eeb954979b87f5f6cb7af",
            ),
        ),
        (
            "aef4d3da2565a640e15a52fd98d24d15",
            (
                "d03e59654221c1e8fcda404fd5c8d6cb",
                "99d18c702e813b07260baf577c60c455",
            ),
        ),
        (
            "ee1c03e6d1f104ccd811cfbba018451a",
            (
                "4e8e26e3276af8a5c2ac2cc2dc95c6d2",
                "80aff30094874e75028033a38ce677bb",
            ),
        ),
        (
            "75756335011dcedb71a0d9a7bd2da9e8",
            (
                "5b34c54bee150c04f9fa584b899dc030",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        ),
        (
            "31535ddf214f5b239d3b517823cb8144",
            (
                "1cdffc6b0b89ff41d68bec237481d1e1",
                "5b34c54bee150c04f9fa584b899dc030",
            ),
        ),
        (
            "72d0f9904bda2f89f9b68a1010ac61b5",
            (
                "99d18c702e813b07260baf577c60c455",
                "d03e59654221c1e8fcda404fd5c8d6cb",
            ),
        ),
    ],
    "patterns:77b4608883110386871480c9b4b93429": [
        (
            "ee1c03e6d1f104ccd811cfbba018451a",
            (
                "4e8e26e3276af8a5c2ac2cc2dc95c6d2",
                "80aff30094874e75028033a38ce677bb",
            ),
        ),
        (
            "e4685d56969398253b6f77efd21dc347",
            (
                "b94941d8cd1c0ee4ad3dd3dcab52b964",
                "80aff30094874e75028033a38ce677bb",
            ),
        ),
    ],
    "patterns:b8ed8c96ed9cbc145ed3a891bd16924f": [
        (
            "f31dfe97db782e8cec26de18dddf8965",
            (
                "1cdffc6b0b89ff41d68bec237481d1e1",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        )
    ],
    "patterns:53dd550f94ec5c12d8c021550f77f3a6": [
        (
            "aef4d3da2565a640e15a52fd98d24d15",
            (
                "d03e59654221c1e8fcda404fd5c8d6cb",
                "99d18c702e813b07260baf577c60c455",
            ),
        )
    ],
    "patterns:d77f08e15edf2a7192b43686d2b2c993": [
        (
            "1c3bf151ea200b2d9e088a1178d060cb",
            (
                "bdfe4e7a431f73386f37c6448afe5840",
                "0a32b476852eeb954979b87f5f6cb7af",
            ),
        )
    ],
    "patterns:89a4e7313f510e044202bd5682fbf55f": [
        (
            "31535ddf214f5b239d3b517823cb8144",
            (
                "1cdffc6b0b89ff41d68bec237481d1e1",
                "5b34c54bee150c04f9fa584b899dc030",
            ),
        ),
        (
            "2a8a69c01305563932b957de4b3a9ba6",
            (
                "1cdffc6b0b89ff41d68bec237481d1e1",
                "af12f10f9ae2002a1607ba0b47ba8407",
            ),
        ),
        (
            "f31dfe97db782e8cec26de18dddf8965",
            (
                "1cdffc6b0b89ff41d68bec237481d1e1",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        ),
    ],
    "patterns:219f8f719231d340934db0e913dfc821": [
        (
            "1c3bf151ea200b2d9e088a1178d060cb",
            (
                "bdfe4e7a431f73386f37c6448afe5840",
                "0a32b476852eeb954979b87f5f6cb7af",
            ),
        )
    ],
    "patterns:ca06a74888b763517838cf4155794aed": [
        (
            "2c927fdc6c0f1272ee439ceb76a6d1a4",
            (
                "5b34c54bee150c04f9fa584b899dc030",
                "af12f10f9ae2002a1607ba0b47ba8407",
            ),
        ),
        (
            "75756335011dcedb71a0d9a7bd2da9e8",
            (
                "5b34c54bee150c04f9fa584b899dc030",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        ),
        (
            "abe6ad743fc81bd1c55ece2e1307a178",
            (
                "5b34c54bee150c04f9fa584b899dc030",
                "1cdffc6b0b89ff41d68bec237481d1e1",
            ),
        ),
    ],
    "patterns:b6b420517271ae8c181537e01e06b0e0": [
        (
            "bad7472f41a0e7d601ca294eb4607c3a",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "1cdffc6b0b89ff41d68bec237481d1e1",
            ),
        ),
        (
            "abe6ad743fc81bd1c55ece2e1307a178",
            (
                "5b34c54bee150c04f9fa584b899dc030",
                "1cdffc6b0b89ff41d68bec237481d1e1",
            ),
        ),
    ],
    "patterns:b9c90deb7e496fa84922a162ed036f1d": [
        (
            "fbf03d17d6a40feff828a3f2c6e86f05",
            (
                "99d18c702e813b07260baf577c60c455",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        ),
        (
            "72d0f9904bda2f89f9b68a1010ac61b5",
            (
                "99d18c702e813b07260baf577c60c455",
                "d03e59654221c1e8fcda404fd5c8d6cb",
            ),
        ),
    ],
    "patterns:fe49c75321cfbea324513ea6be03047e": [
        (
            "959924e3aab197af80a84c1ab261fd65",
            (
                "08126b066d32ee37743e255a2558cccd",
                "b99ae727c787f1b13b452fd4c9ce1b9a",
            ),
        )
    ],
    "patterns:53e8d3304d5ca1582e9cabd009695c74": [
        (
            "bad7472f41a0e7d601ca294eb4607c3a",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "1cdffc6b0b89ff41d68bec237481d1e1",
            ),
        )
    ],
    "patterns:796cd25b5d0a24532b7de470414ddd43": [
        (
            "ee1c03e6d1f104ccd811cfbba018451a",
            (
                "4e8e26e3276af8a5c2ac2cc2dc95c6d2",
                "80aff30094874e75028033a38ce677bb",
            ),
        ),
        (
            "a45af31b43ee5ea271214338a5a5bd61",
            (
                "4e8e26e3276af8a5c2ac2cc2dc95c6d2",
                "af12f10f9ae2002a1607ba0b47ba8407",
            ),
        ),
    ],
    "patterns:0dae65f7537787a242d67f4db5c4fbbf": [
        (
            "2d7abd27644a9c08a7ca2c8d68338579",
            (
                "bb34ce95f161a6b37ff54b3d4c817857",
                "c1db9b517073e51eb7ef6fed608ec204",
            ),
        )
    ],
    "patterns:35c13ecf02c032d7afa0a335f58bc966": [
        (
            "aef4d3da2565a640e15a52fd98d24d15",
            (
                "d03e59654221c1e8fcda404fd5c8d6cb",
                "99d18c702e813b07260baf577c60c455",
            ),
        ),
        (
            "906fa505ae3bc6336d80a5f9aaa47b3b",
            (
                "d03e59654221c1e8fcda404fd5c8d6cb",
                "08126b066d32ee37743e255a2558cccd",
            ),
        ),
    ],
    "patterns:42cf50554e4eab75dcd3ae1fedb05bfb": [
        (
            "9923fc3e46d779c925d26ac4cf2d9e3b",
            (
                "b94941d8cd1c0ee4ad3dd3dcab52b964",
                "c1db9b517073e51eb7ef6fed608ec204",
            ),
        )
    ],
    "patterns:6e87db346db89c2f7441dd29c934f37b": [
        (
            "aef4d3da2565a640e15a52fd98d24d15",
            (
                "d03e59654221c1e8fcda404fd5c8d6cb",
                "99d18c702e813b07260baf577c60c455",
            ),
        )
    ],
    "patterns:b5b52cf4f3d592e2ebe66afd9f8d707e": [
        (
            "4120e428ab0fa162a04328e5217912ff",
            (
                "bb34ce95f161a6b37ff54b3d4c817857",
                "0a32b476852eeb954979b87f5f6cb7af",
            ),
        ),
        (
            "b0f428929706d1d991e4d712ad08f9ab",
            (
                "b99ae727c787f1b13b452fd4c9ce1b9a",
                "0a32b476852eeb954979b87f5f6cb7af",
            ),
        ),
        (
            "1c3bf151ea200b2d9e088a1178d060cb",
            (
                "bdfe4e7a431f73386f37c6448afe5840",
                "0a32b476852eeb954979b87f5f6cb7af",
            ),
        ),
    ],
    "patterns:9bc459ad6a9d086aac843296a3a23774": [
        (
            "2c927fdc6c0f1272ee439ceb76a6d1a4",
            (
                "5b34c54bee150c04f9fa584b899dc030",
                "af12f10f9ae2002a1607ba0b47ba8407",
            ),
        ),
        (
            "a45af31b43ee5ea271214338a5a5bd61",
            (
                "4e8e26e3276af8a5c2ac2cc2dc95c6d2",
                "af12f10f9ae2002a1607ba0b47ba8407",
            ),
        ),
        (
            "2a8a69c01305563932b957de4b3a9ba6",
            (
                "1cdffc6b0b89ff41d68bec237481d1e1",
                "af12f10f9ae2002a1607ba0b47ba8407",
            ),
        ),
    ],
    "patterns:f31534e0246ef8b6a2f78e409e2d685c": [
        (
            "16f7e407087bfa0b35b13d13a1aadcae",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "4e8e26e3276af8a5c2ac2cc2dc95c6d2",
            ),
        )
    ],
    "patterns:229f1cf9864fc0b648f9379da70f716b": [
        (
            "e431a2eda773adf06ef3f9268f93deaf",
            (
                "c1db9b517073e51eb7ef6fed608ec204",
                "b94941d8cd1c0ee4ad3dd3dcab52b964",
            ),
        )
    ],
    "patterns:1bd7c2f037df05925e9d6a3898860d6d": [
        (
            "116df61c01859c710d178ba14a483509",
            (
                "c1db9b517073e51eb7ef6fed608ec204",
                "b99ae727c787f1b13b452fd4c9ce1b9a",
            ),
        ),
        (
            "e431a2eda773adf06ef3f9268f93deaf",
            (
                "c1db9b517073e51eb7ef6fed608ec204",
                "b94941d8cd1c0ee4ad3dd3dcab52b964",
            ),
        ),
        (
            "7ee00a03f67b39f620bd3d0f6ed0c3e6",
            (
                "c1db9b517073e51eb7ef6fed608ec204",
                "bb34ce95f161a6b37ff54b3d4c817857",
            ),
        ),
    ],
    "patterns:ea7a31dad0e3567905520206fa22dd67": [
        (
            "a45af31b43ee5ea271214338a5a5bd61",
            (
                "4e8e26e3276af8a5c2ac2cc2dc95c6d2",
                "af12f10f9ae2002a1607ba0b47ba8407",
            ),
        )
    ],
    "patterns:11ab43defbe2154243e368ecb96cf382": [
        (
            "1c3bf151ea200b2d9e088a1178d060cb",
            (
                "bdfe4e7a431f73386f37c6448afe5840",
                "0a32b476852eeb954979b87f5f6cb7af",
            ),
        )
    ],
    "patterns:6a69435164e0bb3405767f941159a5d6": [
        (
            "ee1c03e6d1f104ccd811cfbba018451a",
            (
                "4e8e26e3276af8a5c2ac2cc2dc95c6d2",
                "80aff30094874e75028033a38ce677bb",
            ),
        )
    ],
    "patterns:dcbceb9cdec4e97c239d6b7f6fb12ade": [
        (
            "2a8a69c01305563932b957de4b3a9ba6",
            (
                "1cdffc6b0b89ff41d68bec237481d1e1",
                "af12f10f9ae2002a1607ba0b47ba8407",
            ),
        )
    ],
    "patterns:8f3c3ee0b825d8e2face2ea7600e5605": [
        (
            "72d0f9904bda2f89f9b68a1010ac61b5",
            (
                "99d18c702e813b07260baf577c60c455",
                "d03e59654221c1e8fcda404fd5c8d6cb",
            ),
        )
    ],
    "patterns:09d62c43e5ac4738fb2e38035d88cf79": [
        (
            'd542caa94b57219f1e489e3b03be7126',
            (
                'a912032ece1826e55fa583dcaacdc4a9',
                '1e8ba9639663105e6c735ba83174f789',
            ),
        )
    ],
    'patterns:22b109cd4b54e8bc27cd3c399436bc8f': [
        (
            '1e8ba9639663105e6c735ba83174f789',
            (
                '07508083e73bbc1e9ad513dd10a968ae',
                '24bc29bd87ecc3b3bc6c16c646506438',
            ),
        )
    ],
    'patterns:410fbee4b1683893342e748372cc0674': [
        (
            '1e8ba9639663105e6c735ba83174f789',
            (
                '07508083e73bbc1e9ad513dd10a968ae',
                '24bc29bd87ecc3b3bc6c16c646506438',
            ),
        )
    ],
    'patterns:55bf38e5e6ae7091f87c6f540bfc1896': [
        (
            '1e8ba9639663105e6c735ba83174f789',
            (
                '07508083e73bbc1e9ad513dd10a968ae',
                '24bc29bd87ecc3b3bc6c16c646506438',
            ),
        )
    ],
    'patterns:61e20b9d946843c0e391818f1a4e4fac': [
        (
            '1e8ba9639663105e6c735ba83174f789',
            (
                '07508083e73bbc1e9ad513dd10a968ae',
                '24bc29bd87ecc3b3bc6c16c646506438',
            ),
        )
    ],
    'patterns:6e644e70a9fe3145c88b5b6261af5754': [
        (
            '1e8ba9639663105e6c735ba83174f789',
            (
                '07508083e73bbc1e9ad513dd10a968ae',
                '24bc29bd87ecc3b3bc6c16c646506438',
            ),
        ),
        (
            'd542caa94b57219f1e489e3b03be7126',
            (
                'a912032ece1826e55fa583dcaacdc4a9',
                '1e8ba9639663105e6c735ba83174f789',
            ),
        ),
    ],
    'patterns:7e4071137a69f147dde49f892cb8e61d': [
        (
            '1e8ba9639663105e6c735ba83174f789',
            (
                '07508083e73bbc1e9ad513dd10a968ae',
                '24bc29bd87ecc3b3bc6c16c646506438',
            ),
        )
    ],
    'patterns:8721a229b5cad6403828924cbdd726a4': [
        (
            'd542caa94b57219f1e489e3b03be7126',
            (
                'a912032ece1826e55fa583dcaacdc4a9',
                '1e8ba9639663105e6c735ba83174f789',
            ),
        )
    ],
    'patterns:bcc291dd0778be127bec52ee2e28ac84': [
        (
            'd542caa94b57219f1e489e3b03be7126',
            (
                'a912032ece1826e55fa583dcaacdc4a9',
                '1e8ba9639663105e6c735ba83174f789',
            ),
        )
    ],
    'patterns:d23673920e8289897273316a1331048e': [
        (
            'd542caa94b57219f1e489e3b03be7126',
            (
                'a912032ece1826e55fa583dcaacdc4a9',
                '1e8ba9639663105e6c735ba83174f789',
            ),
        )
    ],
    'patterns:e48c0b4d17a514cb58a75c789eb8bb14': [
        (
            'd542caa94b57219f1e489e3b03be7126',
            (
                'a912032ece1826e55fa583dcaacdc4a9',
                '1e8ba9639663105e6c735ba83174f789',
            ),
        )
    ],
    'patterns:fc55eaf3fe4af40321a3bec94e50fd5b': [
        (
            '1e8ba9639663105e6c735ba83174f789',
            (
                '07508083e73bbc1e9ad513dd10a968ae',
                '24bc29bd87ecc3b3bc6c16c646506438',
            ),
        ),
        (
            'd542caa94b57219f1e489e3b03be7126',
            (
                'a912032ece1826e55fa583dcaacdc4a9',
                '1e8ba9639663105e6c735ba83174f789',
            ),
        ),
    ],
}

templates_redis_mock_data = {
    "templates:e40489cd1e7102e35469c937e05c8bba": [
        (
            "ee1c03e6d1f104ccd811cfbba018451a",
            (
                "4e8e26e3276af8a5c2ac2cc2dc95c6d2",
                "80aff30094874e75028033a38ce677bb",
            ),
        ),
        (
            "e4685d56969398253b6f77efd21dc347",
            (
                "b94941d8cd1c0ee4ad3dd3dcab52b964",
                "80aff30094874e75028033a38ce677bb",
            ),
        ),
        (
            "906fa505ae3bc6336d80a5f9aaa47b3b",
            (
                "d03e59654221c1e8fcda404fd5c8d6cb",
                "08126b066d32ee37743e255a2558cccd",
            ),
        ),
        (
            "75756335011dcedb71a0d9a7bd2da9e8",
            (
                "5b34c54bee150c04f9fa584b899dc030",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        ),
        (
            "fbf03d17d6a40feff828a3f2c6e86f05",
            (
                "99d18c702e813b07260baf577c60c455",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        ),
        (
            "c93e1e758c53912638438e2a7d7f7b7f",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        ),
        (
            "116df61c01859c710d178ba14a483509",
            (
                "c1db9b517073e51eb7ef6fed608ec204",
                "b99ae727c787f1b13b452fd4c9ce1b9a",
            ),
        ),
        (
            "4120e428ab0fa162a04328e5217912ff",
            (
                "bb34ce95f161a6b37ff54b3d4c817857",
                "0a32b476852eeb954979b87f5f6cb7af",
            ),
        ),
        (
            "b0f428929706d1d991e4d712ad08f9ab",
            (
                "b99ae727c787f1b13b452fd4c9ce1b9a",
                "0a32b476852eeb954979b87f5f6cb7af",
            ),
        ),
        (
            "1c3bf151ea200b2d9e088a1178d060cb",
            (
                "bdfe4e7a431f73386f37c6448afe5840",
                "0a32b476852eeb954979b87f5f6cb7af",
            ),
        ),
        (
            "959924e3aab197af80a84c1ab261fd65",
            (
                "08126b066d32ee37743e255a2558cccd",
                "b99ae727c787f1b13b452fd4c9ce1b9a",
            ),
        ),
        (
            "f31dfe97db782e8cec26de18dddf8965",
            (
                "1cdffc6b0b89ff41d68bec237481d1e1",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        ),
    ],
    "templates:ed73ea081d170e1d89fc950820ce1cee": [
        (
            "7ee00a03f67b39f620bd3d0f6ed0c3e6",
            (
                "c1db9b517073e51eb7ef6fed608ec204",
                "bb34ce95f161a6b37ff54b3d4c817857",
            ),
        ),
        (
            "abe6ad743fc81bd1c55ece2e1307a178",
            (
                "5b34c54bee150c04f9fa584b899dc030",
                "1cdffc6b0b89ff41d68bec237481d1e1",
            ),
        ),
        (
            "31535ddf214f5b239d3b517823cb8144",
            (
                "1cdffc6b0b89ff41d68bec237481d1e1",
                "5b34c54bee150c04f9fa584b899dc030",
            ),
        ),
        (
            "2d7abd27644a9c08a7ca2c8d68338579",
            (
                "bb34ce95f161a6b37ff54b3d4c817857",
                "c1db9b517073e51eb7ef6fed608ec204",
            ),
        ),
        (
            "16f7e407087bfa0b35b13d13a1aadcae",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "4e8e26e3276af8a5c2ac2cc2dc95c6d2",
            ),
        ),
        (
            "bad7472f41a0e7d601ca294eb4607c3a",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "1cdffc6b0b89ff41d68bec237481d1e1",
            ),
        ),
        (
            "9923fc3e46d779c925d26ac4cf2d9e3b",
            (
                "b94941d8cd1c0ee4ad3dd3dcab52b964",
                "c1db9b517073e51eb7ef6fed608ec204",
            ),
        ),
        (
            "2c927fdc6c0f1272ee439ceb76a6d1a4",
            (
                "5b34c54bee150c04f9fa584b899dc030",
                "af12f10f9ae2002a1607ba0b47ba8407",
            ),
        ),
        (
            "e431a2eda773adf06ef3f9268f93deaf",
            (
                "c1db9b517073e51eb7ef6fed608ec204",
                "b94941d8cd1c0ee4ad3dd3dcab52b964",
            ),
        ),
        (
            "a45af31b43ee5ea271214338a5a5bd61",
            (
                "4e8e26e3276af8a5c2ac2cc2dc95c6d2",
                "af12f10f9ae2002a1607ba0b47ba8407",
            ),
        ),
        (
            "aef4d3da2565a640e15a52fd98d24d15",
            (
                "d03e59654221c1e8fcda404fd5c8d6cb",
                "99d18c702e813b07260baf577c60c455",
            ),
        ),
        (
            "2a8a69c01305563932b957de4b3a9ba6",
            (
                "1cdffc6b0b89ff41d68bec237481d1e1",
                "af12f10f9ae2002a1607ba0b47ba8407",
            ),
        ),
        (
            "b5459e299a5c5e8662c427f7e01b3bf1",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "5b34c54bee150c04f9fa584b899dc030",
            ),
        ),
        (
            "72d0f9904bda2f89f9b68a1010ac61b5",
            (
                "99d18c702e813b07260baf577c60c455",
                "d03e59654221c1e8fcda404fd5c8d6cb",
            ),
        ),
    ],
    "templates:a9dea78180588431ec64d6bc4872fdbc": [
        (
            "7ee00a03f67b39f620bd3d0f6ed0c3e6",
            (
                "c1db9b517073e51eb7ef6fed608ec204",
                "bb34ce95f161a6b37ff54b3d4c817857",
            ),
        ),
        (
            "abe6ad743fc81bd1c55ece2e1307a178",
            (
                "5b34c54bee150c04f9fa584b899dc030",
                "1cdffc6b0b89ff41d68bec237481d1e1",
            ),
        ),
        (
            "31535ddf214f5b239d3b517823cb8144",
            (
                "1cdffc6b0b89ff41d68bec237481d1e1",
                "5b34c54bee150c04f9fa584b899dc030",
            ),
        ),
        (
            "2d7abd27644a9c08a7ca2c8d68338579",
            (
                "bb34ce95f161a6b37ff54b3d4c817857",
                "c1db9b517073e51eb7ef6fed608ec204",
            ),
        ),
        (
            "16f7e407087bfa0b35b13d13a1aadcae",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "4e8e26e3276af8a5c2ac2cc2dc95c6d2",
            ),
        ),
        (
            "bad7472f41a0e7d601ca294eb4607c3a",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "1cdffc6b0b89ff41d68bec237481d1e1",
            ),
        ),
        (
            "9923fc3e46d779c925d26ac4cf2d9e3b",
            (
                "b94941d8cd1c0ee4ad3dd3dcab52b964",
                "c1db9b517073e51eb7ef6fed608ec204",
            ),
        ),
        (
            "2c927fdc6c0f1272ee439ceb76a6d1a4",
            (
                "5b34c54bee150c04f9fa584b899dc030",
                "af12f10f9ae2002a1607ba0b47ba8407",
            ),
        ),
        (
            "e431a2eda773adf06ef3f9268f93deaf",
            (
                "c1db9b517073e51eb7ef6fed608ec204",
                "b94941d8cd1c0ee4ad3dd3dcab52b964",
            ),
        ),
        (
            "a45af31b43ee5ea271214338a5a5bd61",
            (
                "4e8e26e3276af8a5c2ac2cc2dc95c6d2",
                "af12f10f9ae2002a1607ba0b47ba8407",
            ),
        ),
        (
            "aef4d3da2565a640e15a52fd98d24d15",
            (
                "d03e59654221c1e8fcda404fd5c8d6cb",
                "99d18c702e813b07260baf577c60c455",
            ),
        ),
        (
            "2a8a69c01305563932b957de4b3a9ba6",
            (
                "1cdffc6b0b89ff41d68bec237481d1e1",
                "af12f10f9ae2002a1607ba0b47ba8407",
            ),
        ),
        (
            "b5459e299a5c5e8662c427f7e01b3bf1",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "5b34c54bee150c04f9fa584b899dc030",
            ),
        ),
        (
            "72d0f9904bda2f89f9b68a1010ac61b5",
            (
                "99d18c702e813b07260baf577c60c455",
                "d03e59654221c1e8fcda404fd5c8d6cb",
            ),
        ),
    ],
    "templates:41c082428b28d7e9ea96160f7fd614ad": [
        (
            "ee1c03e6d1f104ccd811cfbba018451a",
            (
                "4e8e26e3276af8a5c2ac2cc2dc95c6d2",
                "80aff30094874e75028033a38ce677bb",
            ),
        ),
        (
            "e4685d56969398253b6f77efd21dc347",
            (
                "b94941d8cd1c0ee4ad3dd3dcab52b964",
                "80aff30094874e75028033a38ce677bb",
            ),
        ),
        (
            "906fa505ae3bc6336d80a5f9aaa47b3b",
            (
                "d03e59654221c1e8fcda404fd5c8d6cb",
                "08126b066d32ee37743e255a2558cccd",
            ),
        ),
        (
            "75756335011dcedb71a0d9a7bd2da9e8",
            (
                "5b34c54bee150c04f9fa584b899dc030",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        ),
        (
            "fbf03d17d6a40feff828a3f2c6e86f05",
            (
                "99d18c702e813b07260baf577c60c455",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        ),
        (
            "c93e1e758c53912638438e2a7d7f7b7f",
            (
                "af12f10f9ae2002a1607ba0b47ba8407",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        ),
        (
            "116df61c01859c710d178ba14a483509",
            (
                "c1db9b517073e51eb7ef6fed608ec204",
                "b99ae727c787f1b13b452fd4c9ce1b9a",
            ),
        ),
        (
            "4120e428ab0fa162a04328e5217912ff",
            (
                "bb34ce95f161a6b37ff54b3d4c817857",
                "0a32b476852eeb954979b87f5f6cb7af",
            ),
        ),
        (
            "b0f428929706d1d991e4d712ad08f9ab",
            (
                "b99ae727c787f1b13b452fd4c9ce1b9a",
                "0a32b476852eeb954979b87f5f6cb7af",
            ),
        ),
        (
            "1c3bf151ea200b2d9e088a1178d060cb",
            (
                "bdfe4e7a431f73386f37c6448afe5840",
                "0a32b476852eeb954979b87f5f6cb7af",
            ),
        ),
        (
            "959924e3aab197af80a84c1ab261fd65",
            (
                "08126b066d32ee37743e255a2558cccd",
                "b99ae727c787f1b13b452fd4c9ce1b9a",
            ),
        ),
        (
            "f31dfe97db782e8cec26de18dddf8965",
            (
                "1cdffc6b0b89ff41d68bec237481d1e1",
                "bdfe4e7a431f73386f37c6448afe5840",
            ),
        ),
    ],
    'templates:158e76774ba76f59ef774871252cfb7e': [
        (
            '1e8ba9639663105e6c735ba83174f789',
            (
                '07508083e73bbc1e9ad513dd10a968ae',
                '24bc29bd87ecc3b3bc6c16c646506438',
            ),
        )
    ],
    "templates:b74a43dbb36287ea86eb5b0c7b86e8e8": [
        (
            '1e8ba9639663105e6c735ba83174f789',
            (
                '07508083e73bbc1e9ad513dd10a968ae',
                '24bc29bd87ecc3b3bc6c16c646506438',
            ),
        ),
        (
            'd542caa94b57219f1e489e3b03be7126',
            (
                'a912032ece1826e55fa583dcaacdc4a9',
                '1e8ba9639663105e6c735ba83174f789',
            ),
        ),
    ],
    'templates:0ef46597d9234ad94b014af4a1997545': [
        (
            'd542caa94b57219f1e489e3b03be7126',
            (
                'a912032ece1826e55fa583dcaacdc4a9',
                '1e8ba9639663105e6c735ba83174f789',
            ),
        )
    ],
}

names_redis_mock_data = {
    'names:bdfe4e7a431f73386f37c6448afe5840': 'mammal',
    'names:c1db9b517073e51eb7ef6fed608ec204': 'snake',
    'names:1cdffc6b0b89ff41d68bec237481d1e1': 'monkey',
    'names:08126b066d32ee37743e255a2558cccd': 'dinosaur',
    'names:4e8e26e3276af8a5c2ac2cc2dc95c6d2': 'ent',
    'names:99d18c702e813b07260baf577c60c455': 'rhino',
    'names:d03e59654221c1e8fcda404fd5c8d6cb': 'triceratops',
    'names:5b34c54bee150c04f9fa584b899dc030': 'chimp',
    'names:bb34ce95f161a6b37ff54b3d4c817857': 'earthworm',
    'names:b94941d8cd1c0ee4ad3dd3dcab52b964': 'vine',
    'names:b99ae727c787f1b13b452fd4c9ce1b9a': 'reptile',
    'names:0a32b476852eeb954979b87f5f6cb7af': 'animal',
    'names:af12f10f9ae2002a1607ba0b47ba8407': 'human',
    'names:80aff30094874e75028033a38ce677bb': 'plant',
}


class TestRedisMongoDB:
    @pytest.fixture()
    def mongo_db(self):
        mongo_db = mock.MagicMock(spec=Database, client=mock.Mock(spec=MongoClient), name='db-test')
        return mongo_db

    @pytest.fixture()
    def redis_db(self):
        redis_db = mock.MagicMock(spec=Redis)

        def smembers(key: str):
            if 'outgoing_set' in key:
                for data in outgoing_set_redis_mock_data:
                    if list(data.keys())[0] == key:
                        return list(data.values())[0]
                return []
            if 'patterns' in key:
                value = patterns_redis_mock_data.get(key)
                if value:
                    custom_set = set()
                    custom_set.add(pickle.dumps(value))
                    return custom_set
                else:
                    return []
            if 'templates' in key:
                value = templates_redis_mock_data.get(key)
                if value:
                    custom_set = set()
                    custom_set.add(pickle.dumps(value))
                    return custom_set
                else:
                    return []
            if 'names' in key:
                value = names_redis_mock_data.get(key)
                if value:
                    custom_set = set()
                    custom_set.add(value.encode())
                    return custom_set
                else:
                    return []

        redis_db.smembers = mock.Mock(side_effect=smembers)
        return redis_db

    @pytest.fixture()
    def mongo_nodes_collection(self, mongo_db):
        collection = mock.MagicMock(
            spec=Collection, database=mongo_db, name=MongoCollectionNames.NODES
        )

        def insert_many(documents: List[Dict[str, Any]], ordered: bool):
            added_nodes.extend(documents)

        def find_one(handle: dict):
            for data in node_collection_mock_data + added_nodes:
                if data['_id'] == handle['_id']:
                    return data

        def find(_filter: Optional[Any] = None):
            if _filter is None:
                return node_collection_mock_data + added_nodes
            else:
                ret = []
                for node in node_collection_mock_data + added_nodes:
                    if (
                        _filter[MongoFieldNames.TYPE] == node[MongoFieldNames.TYPE]
                        and _filter[MongoFieldNames.NODE_NAME]['$regex']
                        in node[MongoFieldNames.NODE_NAME]
                    ):
                        ret.append(node)
                return ret

        def estimated_document_count():
            return len(node_collection_mock_data) + len(added_nodes)

        collection.insert_many = mock.Mock(side_effect=insert_many)
        collection.find_one = mock.Mock(side_effect=find_one)
        collection.find = mock.Mock(side_effect=find)
        collection.estimated_document_count = mock.Mock(side_effect=estimated_document_count)
        return collection

    @pytest.fixture()
    def mongo_types_collection(self, mongo_db):
        collection = mock.MagicMock(
            spec=Collection,
            database=mongo_db,
            name=MongoCollectionNames.ATOM_TYPES,
        )

        def find(_filter: Optional[Any] = None):
            if _filter is None:
                return type_collection_mock_data
            return []

        collection.find = mock.Mock(side_effect=find)
        return collection

    @pytest.fixture()
    def mongo_arity_1_collection(self, mongo_db):
        collection = mock.MagicMock(
            spec=Collection,
            database=mongo_db,
            name=MongoCollectionNames.LINKS_ARITY_1,
        )

        def find(_filter: Optional[Any] = None):
            if _filter is None:
                return []
            return []

        def estimated_document_count():
            return len([])

        collection.find = mock.Mock(side_effect=find)
        collection.estimated_document_count = mock.Mock(side_effect=estimated_document_count)
        return collection

    @pytest.fixture()
    def mongo_arity_2_collection(self, mongo_db):
        collection = mock.MagicMock(
            spec=Collection,
            database=mongo_db,
            name=MongoCollectionNames.LINKS_ARITY_2,
        )

        def find_one(_filter: dict):
            for data in arity_2_collection_mock_data:
                if data['_id'] == _filter['_id']:
                    return data

        def find(_filter: Optional[Any] = None):
            if _filter is None:
                return arity_2_collection_mock_data
            return []

        def insert_many(documents: List[Dict[str, Any]], ordered: bool):
            added_links_arity_2.extend(documents)

        def estimated_document_count():
            return len(arity_2_collection_mock_data) + len(added_links_arity_2)

        collection.find_one = mock.Mock(side_effect=find_one)
        collection.find = mock.Mock(side_effect=find)
        collection.estimated_document_count = mock.Mock(side_effect=estimated_document_count)

        return collection

    @pytest.fixture()
    def mongo_arity_n_collection(self, mongo_db):
        collection = mock.MagicMock(
            spec=Collection,
            database=mongo_db,
            name=MongoCollectionNames.LINKS_ARITY_N,
        )

        def find(_filter: Optional[Any] = None):
            if _filter is None:
                return []
            return []

        def estimated_document_count():
            return len([])

        collection.find = mock.Mock(side_effect=find)
        collection.estimated_document_count = mock.Mock(side_effect=estimated_document_count)
        return collection

    @pytest.fixture()
    def database(
        self,
        mongo_db,
        redis_db,
        mongo_arity_1_collection,
        mongo_arity_2_collection,
        mongo_arity_n_collection,
        mongo_nodes_collection,
        mongo_types_collection,
    ):
        with mock.patch(
            'hyperon_das_atomdb.adapters.redis_mongo_db.RedisMongoDB._connection_mongo_db',
            return_value=mongo_db,
        ), mock.patch(
            'hyperon_das_atomdb.adapters.redis_mongo_db.RedisMongoDB._connection_redis',
            return_value=redis_db,
        ):
            db = RedisMongoDB(
                mongo_hostname='0.0.0.0',
                mongo_port=0,
                mongo_username='test',
                mongo_password='test',
                redis_hostname='0.0.0.0',
                redis_port=0,
            )
            db.mongo_link_collection = {
                '1': mongo_arity_1_collection,
                '2': mongo_arity_2_collection,
                'N': mongo_arity_n_collection,
            }
            db.mongo_nodes_collection = mongo_nodes_collection
            db.mongo_types_collection = mongo_types_collection
            db.all_mongo_collections = [
                (
                    MongoCollectionNames.LINKS_ARITY_1,
                    db.mongo_link_collection['1'],
                ),
                (
                    MongoCollectionNames.LINKS_ARITY_2,
                    db.mongo_link_collection['2'],
                ),
                (
                    MongoCollectionNames.LINKS_ARITY_N,
                    db.mongo_link_collection['N'],
                ),
                (MongoCollectionNames.NODES, db.mongo_nodes_collection),
                (MongoCollectionNames.ATOM_TYPES, db.mongo_types_collection),
            ]
            db.mongo_bulk_insertion_buffer = {
                MongoCollectionNames.LINKS_ARITY_1: tuple([db.mongo_link_collection['1'], set()]),
                MongoCollectionNames.LINKS_ARITY_2: tuple([db.mongo_link_collection['2'], set()]),
                MongoCollectionNames.LINKS_ARITY_N: tuple([db.mongo_link_collection['N'], set()]),
                MongoCollectionNames.NODES: tuple([db.mongo_nodes_collection, set()]),
                MongoCollectionNames.ATOM_TYPES: tuple([db.mongo_types_collection, set()]),
            }
            db.prefetch()
        return db

    def test_node_exists(self, database):
        node_type = 'Concept'
        node_name = 'monkey'

        resp = database.node_exists(node_type, node_name)

        assert resp is True

    def test_node_exists_false(self, database):
        node_type = 'Concept'
        node_name = 'human-fake'

        resp = database.node_exists(node_type, node_name)

        assert resp is False

    def test_link_exists(self, database):
        human = ExpressionHasher.terminal_hash('Concept', 'human')
        monkey = ExpressionHasher.terminal_hash('Concept', 'monkey')

        resp = database.link_exists('Similarity', [human, monkey])

        assert resp is True

    def test_link_exists_false(self, database):
        human = ExpressionHasher.terminal_hash('Concept', 'fake')
        monkey = ExpressionHasher.terminal_hash('Concept', 'monkey')

        resp = database.link_exists('Similarity', [human, monkey])

        assert resp is False

    def test_get_node_handle(self, database):
        node_type = 'Concept'
        node_name = 'human'

        resp = database.get_node_handle(node_type, node_name)

        assert resp == ExpressionHasher.terminal_hash('Concept', 'human')

    def test_get_node_handle_node_does_not_exist(self, database):
        node_type = 'Fake'
        node_name = 'Fake2'

        with pytest.raises(NodeDoesNotExist) as exc_info:
            database.get_node_handle(node_type, node_name)
        assert exc_info.type is NodeDoesNotExist
        assert exc_info.value.args[0] == "This node does not exist"

    def test_get_link_handle(self, database):
        human = ExpressionHasher.terminal_hash('Concept', 'human')
        chimp = ExpressionHasher.terminal_hash('Concept', 'chimp')

        resp = database.get_link_handle(link_type='Similarity', target_handles=[human, chimp])

        assert resp is not None

    def test_get_link_handle_link_does_not_exist(self, database):
        brazil = ExpressionHasher.terminal_hash('Concept', 'brazil')
        travel = ExpressionHasher.terminal_hash('Concept', 'travel')

        with pytest.raises(LinkDoesNotExist) as exc_info:
            database.get_link_handle(link_type='Similarity', target_handles=[brazil, travel])
        assert exc_info.type is LinkDoesNotExist
        assert exc_info.value.args[0] == "This link does not exist"

    def test_get_link_targets(self, database):
        human = database.get_node_handle('Concept', 'human')
        mammal = database.get_node_handle('Concept', 'mammal')
        handle = database.get_link_handle('Inheritance', [human, mammal])
        assert database.get_link_targets(handle)

    def test_get_link_targets_invalid(self, database):
        human = database.get_node_handle('Concept', 'human')
        mammal = database.get_node_handle('Concept', 'mammal')
        handle = database.get_link_handle('Inheritance', [human, mammal])

        with pytest.raises(ValueError) as exc_info:
            database.get_link_targets(f'{handle}-Fake')
        assert exc_info.type is ValueError
        assert exc_info.value.args[0] == f"Invalid handle: {handle}-Fake"

    def test_is_ordered(self, database):
        human = database.get_node_handle('Concept', 'human')
        monkey = database.get_node_handle('Concept', 'monkey')
        mammal = database.get_node_handle('Concept', 'mammal')
        link_1 = database.get_link_handle('Inheritance', [human, mammal])
        link_2 = database.get_link_handle('Similarity', [human, monkey])
        assert database.is_ordered(link_1)
        assert database.is_ordered(link_2)

    def test_is_ordered_invalid(self, database):
        human = database.get_node_handle('Concept', 'human')
        mammal = database.get_node_handle('Concept', 'mammal')
        link = database.get_link_handle('Inheritance', [human, mammal])
        with pytest.raises(ValueError) as exc_info:
            database.get_link_targets(f'{link}-Fake')
        assert exc_info.type is ValueError
        assert exc_info.value.args[0] == f"Invalid handle: {link}-Fake"

    def test_get_matched_links_without_wildcard(self, database):
        link_type = 'Similarity'
        human = ExpressionHasher.terminal_hash('Concept', 'human')
        monkey = ExpressionHasher.terminal_hash('Concept', 'monkey')
        link_handle = database.get_link_handle(link_type, [human, monkey])
        expected = [link_handle]
        actual = database.get_matched_links(link_type, [human, monkey])

        assert expected == actual

    def test_get_matched_links_link_equal_wildcard(self, database):
        link_type = '*'
        human = ExpressionHasher.terminal_hash('Concept', 'human')
        chimp = ExpressionHasher.terminal_hash('Concept', 'chimp')
        expected = [
            (
                "b5459e299a5c5e8662c427f7e01b3bf1",
                (
                    "af12f10f9ae2002a1607ba0b47ba8407",
                    "5b34c54bee150c04f9fa584b899dc030",
                ),
            )
        ]
        actual = database.get_matched_links(link_type, [human, chimp])

        assert expected == actual[0]

    def test_get_matched_links_link_diff_wildcard(self, database):
        link_type = 'Similarity'
        chimp = ExpressionHasher.terminal_hash('Concept', 'chimp')
        expected = [
            (
                '31535ddf214f5b239d3b517823cb8144',
                (
                    '1cdffc6b0b89ff41d68bec237481d1e1',
                    '5b34c54bee150c04f9fa584b899dc030',
                ),
            ),
            (
                'b5459e299a5c5e8662c427f7e01b3bf1',
                (
                    'af12f10f9ae2002a1607ba0b47ba8407',
                    '5b34c54bee150c04f9fa584b899dc030',
                ),
            ),
        ]
        actual = database.get_matched_links(link_type, ['*', chimp])

        assert expected == actual[0]

    def test_get_matched_links_toplevel_only(self, database):
        expected = [
            (
                'd542caa94b57219f1e489e3b03be7126',
                (
                    'a912032ece1826e55fa583dcaacdc4a9',
                    '1e8ba9639663105e6c735ba83174f789',
                ),
            )
        ]
        actual = database.get_matched_links('Evaluation', ['*', '*'], {'toplevel_only': True})

        assert expected == actual
        assert len(actual) == 1

    def test_get_all_nodes(self, database):
        ret = database.get_all_nodes('Concept')
        assert len(ret) == 14
        ret = database.get_all_nodes('Concept', True)
        assert len(ret) == 14
        ret = database.get_all_nodes('ConceptFake')
        assert len(ret) == 0

    def test_get_all_nodes_error(self, database):
        with mock.patch(
            'hyperon_das_atomdb.adapters.redis_mongo_db.RedisMongoDB._get_atom_type_hash',
            return_value=None,
        ):
            with pytest.raises(ValueError) as exc_info:
                database.get_all_nodes('Concept-Fake')
            assert exc_info.type is ValueError
            assert exc_info.value.args[0] == f"Invalid node type: Concept-Fake"

    def test_get_matched_type_template(self, database):
        v1 = database.get_matched_type_template(['Inheritance', 'Concept', 'Concept'])
        v2 = database.get_matched_type_template(['Similarity', 'Concept', 'Concept'])
        v3 = database.get_matched_type_template(['Inheritance', 'Concept', 'blah'])
        v4 = database.get_matched_type_template(['Similarity', 'blah', 'Concept'])
        v5 = database.get_matched_links('Inheritance', ['*', '*'])
        v6 = database.get_matched_links('Similarity', ['*', '*'])
        assert len(v1[0]) == 12
        assert len(v2[0]) == 14
        assert len(v3) == 0
        assert len(v4) == 0
        assert v1 == v5
        assert v2 == v6

    def test_get_matched_type_template_error(self, database):
        with mock.patch(
            'hyperon_das_atomdb.adapters.redis_mongo_db.RedisMongoDB._build_named_type_hash_template',
            return_value=mock.MagicMock(side_effect=Exception("Test")),
        ):
            with pytest.raises(ValueError) as exc_info:
                database.get_matched_type_template(['Inheritance', 'Concept', 'Concept'])
            assert exc_info.type is ValueError

    def test_get_matched_type(self, database):
        inheritance = database.get_matched_type('Inheritance')
        similarity = database.get_matched_type('Similarity')
        assert len(inheritance[0]) == 12
        assert len(similarity[0]) == 14

    def test_get_matched_type_toplevel_only(self, database):
        ret = database.get_matched_type('Evaluation')
        assert len(ret[0]) == 2

        ret = database.get_matched_type('Evaluation', {'toplevel_only': True})

        assert len(ret) == 1

    def test_get_node_name(self, database):
        node_type = 'Concept'
        node_name = 'monkey'

        handle = database.get_node_handle(node_type, node_name)
        db_name = database.get_node_name(handle)

        assert db_name == node_name

    def test_get_node_name_value_error(self, database):
        with mock.patch(
            'hyperon_das_atomdb.adapters.redis_mongo_db.RedisMongoDB._retrieve_key_value',
            return_value=None,
        ):
            with pytest.raises(ValueError) as exc_info:
                database.get_node_name('handle')
            assert exc_info.type is ValueError
            assert exc_info.value.args[0] == f"Invalid handle: handle"

    def test_get_matched_node_name(self, database):
        expected = sorted(
            [
                database.get_node_handle('Concept', 'human'),
                database.get_node_handle('Concept', 'mammal'),
                database.get_node_handle('Concept', 'animal'),
            ]
        )
        actual = sorted(database.get_matched_node_name('Concept', 'ma'))

        assert expected == actual
        assert sorted(database.get_matched_node_name('blah', 'Concept')) == []
        assert sorted(database.get_matched_node_name('Concept', 'blah')) == []

    def test_get_node_type(self, database):
        monkey = database.get_node_handle('Concept', 'monkey')
        resp_node = database.get_node_type(monkey)
        assert 'Concept' == resp_node

    def test_get_node_type_without_cache(self, database):
        from hyperon_das_atomdb.adapters import redis_mongo_db

        redis_mongo_db.USE_CACHED_NODE_TYPES = False
        monkey = database.get_node_handle('Concept', 'monkey')
        resp_node = database.get_node_type(monkey)
        assert 'Concept' == resp_node

    def test_get_link_type(self, database):
        human = database.get_node_handle('Concept', 'human')
        chimp = database.get_node_handle('Concept', 'chimp')
        link_handle = database.get_link_handle('Similarity', [human, chimp])
        resp_link = database.get_link_type(link_handle)
        assert 'Similarity' == resp_link

    def test_get_link_type_without_cache(self, database):
        from hyperon_das_atomdb.adapters import redis_mongo_db

        redis_mongo_db.USE_CACHED_LINK_TYPES = False
        human = database.get_node_handle('Concept', 'human')
        chimp = database.get_node_handle('Concept', 'chimp')
        link_handle = database.get_link_handle('Similarity', [human, chimp])
        resp_link = database.get_link_type(link_handle)
        assert 'Similarity' == resp_link

    def test_atom_count(self, database):
        node_count, link_count = database.count_atoms()
        assert node_count == 14
        assert link_count == 28

    def test_add_node(self, database):
        added_nodes.clear()
        assert (14, 28) == database.count_atoms()
        all_nodes_before = database.get_all_nodes('Concept')
        database.add_node(
            {
                'type': 'Concept',
                'name': 'lion',
            }
        )
        database.commit()
        all_nodes_after = database.get_all_nodes('Concept')
        assert len(all_nodes_before) == 14
        assert len(all_nodes_after) == 15
        assert (15, 28) == database.count_atoms()
        new_node_handle = database.get_node_handle('Concept', 'lion')
        assert new_node_handle == ExpressionHasher.terminal_hash('Concept', 'lion')
        assert new_node_handle not in all_nodes_before
        assert new_node_handle in all_nodes_after
        new_node = database.get_atom(new_node_handle)
        assert new_node['handle'] == new_node_handle
        assert new_node['named_type'] == 'Concept'
        assert new_node['name'] == 'lion'
        added_nodes.clear()

    def test_add_link(self, database):
        added_nodes.clear()
        added_links_arity_2.clear()
        assert (14, 28) == database.count_atoms()

        all_nodes_before = database.get_all_nodes('Concept')
        all_links_before = database.get_matched_type('Similarity')
        database.add_link(
            {
                'type': 'Similarity',
                'targets': [
                    {'type': 'Concept', 'name': 'lion'},
                    {'type': 'Concept', 'name': 'cat'},
                ],
            }
        )
        database.commit()
        all_nodes_after = database.get_all_nodes('Concept')
        all_links_after = database.get_matched_type('Similarity')

        assert len(all_nodes_before) == 14
        assert len(all_nodes_after) == 16
        # assert len(all_links_before) == 28
        # assert len(all_links_after) == 29
        # assert (16, 29) == database.count_atoms()

        new_node_handle = database.get_node_handle('Concept', 'lion')
        assert new_node_handle == ExpressionHasher.terminal_hash('Concept', 'lion')
        assert new_node_handle not in all_nodes_before
        assert new_node_handle in all_nodes_after
        new_node = database.get_atom(new_node_handle)
        assert new_node['handle'] == new_node_handle
        assert new_node['named_type'] == 'Concept'
        assert new_node['name'] == 'lion'

        new_node_handle = database.get_node_handle('Concept', 'cat')
        assert new_node_handle == ExpressionHasher.terminal_hash('Concept', 'cat')
        assert new_node_handle not in all_nodes_before
        assert new_node_handle in all_nodes_after
        new_node = database.get_atom(new_node_handle)
        assert new_node['handle'] == new_node_handle
        assert new_node['named_type'] == 'Concept'
        assert new_node['name'] == 'cat'

        added_nodes.clear()
        added_links_arity_2.clear()
