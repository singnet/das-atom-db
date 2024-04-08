import psycopg2
import pytest

from hyperon_das_atomdb.adapters import RedisPostgreSQLLobeDB
from hyperon_das_atomdb.database import WILDCARD

from .helpers import Database, _db_down, _db_up, cleanup, postgresql_port, redis_port


class TestRedisPostgreSQLLobeDB:
    @pytest.fixture(scope="session", autouse=True)
    def _cleanup(self, request):
        return cleanup(request)

    def _populate_db(self):
        conn = psycopg2.connect(
            host='localhost',
            database='postgres',
            port=postgresql_port,
            user='dbadmin',
            password='dassecret',
        )
        with conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    ID SERIAL PRIMARY KEY,
                    name VARCHAR(100),
                    age INTEGER
                );
                """
            )
            cursor.execute("INSERT INTO users (name, age) VALUES ('Adam', 30);")
            cursor.execute("INSERT INTO users (name, age) VALUES ('Eve', 31);")
        conn.commit()
        conn.close()

    def test_initialization(self, _cleanup):
        _db_up(Database.REDIS, Database.POSTGRESQL)

        self._populate_db()

        db = RedisPostgreSQLLobeDB(
            postgresql_port=postgresql_port,
            postgresql_username='dbadmin',
            postgresql_password='dassecret',
            redis_port=redis_port,
            redis_cluster=False,
            redis_ssl=False,
        )

        assert db.count_atoms() == (9, 6)

        node_users = db.node_handle('Symbol', 'users')
        node_users_name = db.node_handle('Symbol', 'users.name')
        node_users_age = db.node_handle('Symbol', 'users.age')
        node_1 = db.node_handle('Symbol', '"1"')
        node_2 = db.node_handle('Symbol', '"2"')
        node_Adam = db.node_handle('Symbol', '"Adam"')
        node_30 = db.node_handle('Symbol', '"30"')
        node_Eve = db.node_handle('Symbol', '"Eve"')
        node_31 = db.node_handle('Symbol', '"31"')

        link1 = db.link_handle('Expression', [node_users, node_1])
        link2 = db.link_handle('Expression', [node_users, node_2])

        matched_links = db.get_matched_links('Expression', [node_users_name, WILDCARD, WILDCARD])
        assert sorted([matched_link[3] for matched_link in matched_links]) == sorted(
            [node_Adam, node_Eve]
        )

        matched_links = db.get_matched_links('Expression', [WILDCARD, link1, WILDCARD])
        assert sorted([matched_link[3] for matched_link in matched_links]) == sorted(
            [node_Adam, node_30]
        )

        matched_links = db.get_matched_links('Expression', [WILDCARD, WILDCARD, node_30])
        assert matched_links[0][1:] == [node_users_age, link1, node_30]

        matched_links = db.get_matched_links('Expression', [node_users_age, WILDCARD, WILDCARD])
        assert sorted([matched_link[3] for matched_link in matched_links]) == sorted(
            [node_30, node_31]
        )

        matched_links = db.get_matched_links('Expression', [WILDCARD, link2, WILDCARD])
        assert sorted([matched_link[3] for matched_link in matched_links]) == sorted(
            [node_Eve, node_31]
        )

        matched_links = db.get_matched_links('Expression', [WILDCARD, WILDCARD, node_31])
        assert matched_links[0][1:] == [node_users_age, link2, node_31]

        _db_down()
