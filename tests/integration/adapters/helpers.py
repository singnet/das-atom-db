import os
import subprocess
from enum import Enum
from typing import List

redis_port = "15926"
mongo_port = "15927"
postgres_port = "15928"
scripts_path = "./tests/integration/scripts"
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
DAS_POSTGRES_HOSTNAME = os.environ.get("DAS_POSTGRES_HOSTNAME")
DAS_POSTGRES_PORT = os.environ.get("DAS_POSTGRES_PORT")
DAS_POSTGRES_USERNAME = os.environ.get("DAS_POSTGRES_USERNAME")
DAS_POSTGRES_PASSWORD = os.environ.get("DAS_POSTGRES_PASSWORD")
DAS_POSTGRES_DATABASE = os.environ.get("DAS_POSTGRES_DATABASE")

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
os.environ["DAS_POSTGRES_HOSTNAME"] = "localhost"
os.environ["DAS_POSTGRES_PORT"] = postgres_port
os.environ["DAS_POSTGRES_USERNAME"] = "dbadmin"
os.environ["DAS_POSTGRES_PASSWORD"] = "dassecret"
os.environ["DAS_POSTGRES_DATABASE"] = "das"


class Database(Enum):
    REDIS = "redis"
    MONGO = "mongo"
    POSTGRES = "postgres"


def _db_up(*database_names: List[Database]):
    if database_names:
        for database_name in database_names:
            subprocess.call(
                [
                    "bash",
                    f"{scripts_path}/{database_name.value}-up.sh",
                    eval(f"{database_name.value}_port"),
                ],
                stdout=devnull,
                stderr=devnull,
            )
    else:
        subprocess.call(
            ["bash", f"{scripts_path}/redis-up.sh", redis_port], stdout=devnull, stderr=devnull
        )
        subprocess.call(
            ["bash", f"{scripts_path}/mongo-up.sh", mongo_port], stdout=devnull, stderr=devnull
        )
        subprocess.call(
            ["bash", f"{scripts_path}/postgres-up.sh", postgres_port],
            stdout=devnull,
            stderr=devnull,
        )


def _db_down():
    subprocess.call(
        ["bash", f"{scripts_path}/redis-down.sh", redis_port], stdout=devnull, stderr=devnull
    )
    subprocess.call(
        ["bash", f"{scripts_path}/mongo-down.sh", mongo_port], stdout=devnull, stderr=devnull
    )
    subprocess.call(
        ["bash", f"{scripts_path}/postgres-down.sh", postgres_port],
        stdout=devnull,
        stderr=devnull,
    )


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
        if DAS_POSTGRES_HOSTNAME:
            os.environ["DAS_POSTGRES_HOSTNAME"] = DAS_POSTGRES_HOSTNAME
        if DAS_POSTGRES_PORT:
            os.environ["DAS_POSTGRES_PORT"] = DAS_POSTGRES_PORT
        if DAS_POSTGRES_USERNAME:
            os.environ["DAS_POSTGRES_USERNAME"] = DAS_POSTGRES_USERNAME
        if DAS_POSTGRES_PASSWORD:
            os.environ["DAS_POSTGRES_PASSWORD"] = DAS_POSTGRES_PASSWORD
        if DAS_POSTGRES_DATABASE:
            os.environ["DAS_POSTGRES_DATABASE"] = DAS_POSTGRES_DATABASE

    def enforce_containers_removal():
        _db_down()

    request.addfinalizer(restore_environment)
    request.addfinalizer(enforce_containers_removal)
