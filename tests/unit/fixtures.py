from unittest import mock

import mongomock
import pytest

from hyperon_das_atomdb.adapters.ram_only import InMemoryDB
from hyperon_das_atomdb.adapters.redis_mongo_db import MongoCollectionNames, RedisMongoDB


class MockRedis:
    def __init__(self, cache=dict()):
        self.cache = cache

    def get(self, key):
        if key in self.cache:
            return self.cache[key]
        return None  # return nil

    def set(self, key, value, *args, **kwargs):
        if self.cache:
            self.cache[key] = value
            return "OK"
        return None  # return nil in case of some issue

    def hget(self, hash, key):
        if hash in self.cache:
            if key in self.cache[hash]:
                return self.cache[hash][key]
        return None  # return nil

    def hset(self, hash, key, value, *args, **kwargs):
        if self.cache:
            self.cache[hash][key] = value
            return 1
        return None  # return nil in case of some issue

    def exists(self, key):
        if key in self.cache:
            return 1
        return 0

    def cache_overwrite(self, cache=dict()):
        self.cache = cache

    def sadd(self, key, *members):
        # Simulate the sadd method of Redis
        if key not in self.cache:
            self.cache[key] = set()
        before_count = len(self.cache[key])
        self.cache[key].update(members)
        after_count = len(self.cache[key])
        return after_count - before_count

    def smembers(self, key):
        # Simulate the smembers method of Redis
        if key in self.cache:
            return self.cache[key]
        return set()

    def flushall(self):
        # Simulate the flushall method of Redis
        self.cache.clear()

    def delete(self, *keys):
        # Simulate the delete method of Redis
        deleted_count = 0
        for key in keys:
            if key in self.cache:
                del self.cache[key]
                deleted_count += 1
        return deleted_count

    def getdel(self, key):
        # Simulate the getdel method of Redis
        value = self.cache.get(key)
        if key in self.cache:
            del self.cache[key]
        return value

    def srem(self, key, *members):
        # Simulate the srem method of Redis
        if key not in self.cache:
            return 0
        initial_count = len(self.cache[key])
        self.cache[key].difference_update(members)
        removed_count = initial_count - len(self.cache[key])
        return removed_count

    def sscan(self, name, cursor=0, match=None, count=None):
        # Simulate the sscan method of Redis
        key = name
        if key not in self.cache:
            return (0, [])

        elements = list(self.cache[key])
        if match:
            elements = [e for e in elements if match in e]

        # Cursor-based iteration simulation
        start = cursor
        end = min(start + (count if count else len(elements)), len(elements))
        new_cursor = end if end < len(elements) else 0

        return (new_cursor, elements[start:end])


@pytest.fixture
def redis_mongo_db():
    # import fakeredis
    mongo_db = mongomock.MongoClient().db
    # redis_db = fakeredis.FakeRedis()
    redis_db = MockRedis()
    with mock.patch(
        "hyperon_das_atomdb.adapters.redis_mongo_db.RedisMongoDB._connection_mongo_db",
        return_value=mongo_db,
    ), mock.patch(
        "hyperon_das_atomdb.adapters.redis_mongo_db.RedisMongoDB._connection_redis",
        # return_value=fakeredis.FakeRedis(),
        return_value=redis_db,
    ):
        db = RedisMongoDB()
        db.mongo_atoms_collection = mongo_db.collection
        db.mongo_types_collection = mongo_db.collection

        # db.mongo_atoms_collection.insert_many(loader("atom_collection_data.json"))
        db.all_mongo_collections = [
            (MongoCollectionNames.ATOMS, db.mongo_atoms_collection),
            (MongoCollectionNames.ATOM_TYPES, db.mongo_types_collection),
        ]
        db.mongo_bulk_insertion_buffer = {
            MongoCollectionNames.ATOMS: tuple([db.mongo_atoms_collection, set()]),
            MongoCollectionNames.ATOM_TYPES: tuple([db.mongo_types_collection, set()]),
        }

    yield db


@pytest.fixture
def in_memory_db():
    db = InMemoryDB()
    yield db
