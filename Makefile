isort:
	@isort --settings-path .isort.cfg ./hyperon_das ./tests

black:
	@black --config .black.cfg ./hyperon_das ./tests

flake8:
	@flake8 --config .flake8.cfg ./hyperon_das ./tests --exclude ./hyperon_das/grpc/

pylint:
	@pylint ./hyperon_das_atomdb --rcfile=.pylintrc

mypy:
	@unbuffer mypy --color-output --config-file mypy.ini ./hyperon_das_atomdb

lint: isort black flake8

unit-tests:
	@py.test -sx -vv ./tests/unit

unit-tests-coverage:
	@py.test -sx -vv ./tests/unit --cov=./hyperon_das_atomdb/ --cov-report=term-missing --cov-fail-under=70

integration-tests:
	@py.test -sx -vv ./tests/integration

pre-commit: lint unit-tests-coverage unit-tests integration-tests
