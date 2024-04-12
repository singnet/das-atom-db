from hyperon_das_atomdb.adapters import RedisPostgresLobeDB
import time


def run():
    print('\n ==== Start script ====')
    start = time.time()
    db = RedisPostgresLobeDB(
        postgres_database_name='postgres',
        postgres_hostname='149.28.214.107',
        postgres_port=30500,
        postgres_username='dbadmin',
        postgres_password='dassecret',
        redis_hostname='149.28.214.107',
        redis_port=29500,
        redis_cluster=False,
        redis_ssl=False,
    )
    print(f"\n==> Total time to instantiate the class: {time.time() - start}")
    print("(nodes, links) = ", db.count_atoms())


if __name__ == '__main__':
    run()