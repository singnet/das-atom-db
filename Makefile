isort:
	@isort --settings-path .isort.cfg ./hyperon_das_atomdb ./tests

black:
	@black --config .black.cfg ./hyperon_das_atomdb ./tests

flake8:
	@flake8 --config .flake8.cfg ./hyperon_das_atomdb ./tests

pylint:
	@pylint ./hyperon_das_atomdb --rcfile=.pylintrc

mypy:
	@mypy --color-output --config-file mypy.ini ./hyperon_das_atomdb

lint: isort black flake8 pylint mypy

unit-tests:
	@py.test -sx -vv ./tests/unit

unit-tests-coverage:
	@py.test -sx -vv ./tests/unit --cov=./hyperon_das_atomdb/ --cov-report=term-missing --cov-fail-under=70

integration-tests:
	@py.test -sx -vv ./tests/integration

pre-commit: lint unit-tests-coverage unit-tests integration-tests

