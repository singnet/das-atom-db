# Hyperon DAS AtomDB
Persistence layer for Distributed AtomSpace

## Installation

This package requires:
[python](https://www.python.org/) >= 3.8.5 to run.

#### This package is deployed on [Pypi](https://pypi.org/project/hyperon-das/). If you want, you can install using the pip command

```
pip install hyperon-das-atomdb
```

#### If you want to run it without installing it by pip, you can follow the following approach

We use the [Poetry](https://python-poetry.org/) package to manage project dependencies and other things. So, if you have Poetry on your machine, you can run the commands below to prepare your environment

**1. poetry install**

**2. poetry shell** (activate virtual environment)

### Prepare environment

**1 - Redis and MongoDB**
You must have Redis and MongoDB running in your environment

**2.1 - Environments Variables**
You must have the following variables set in your environment with their respective values:
```
DAS_MONGODB_HOSTNAME=172.17.0.2
DAS_MONGODB_PORT=27017
DAS_MONGODB_USERNAME=mongo
DAS_MONGODB_PASSWORD=mongo
DAS_REDIS_HOSTNAME=127.0.0.1
DAS_REDIS_PORT=6379
```
**2.2 or you can export necessary environment using the enviroment file**
source environment

## Usage

#### Use adapters

```python
from hyperon_das_atomdb.adapters import RedisMongoDB, InMemoryDB

redis_mongo_db = RedisMongoDB()
in_memory_db = InMemoryDB()
```

## Tests

You can ran the command below to execute the unittests

```bash
make test-coverage
```