export PYTHONPATH=$(shell pwd)/hyperon_das_atomdb/
export PYTHONDONTWRITEBYTECODE=1

test:
	@pytest -v

test-coverage:
	@py.test -sx ./tests --cov=./hyperon_das_atomdb/ --cov-report=term-missing --cov-fail-under=90

isort:
	@isort ./hyperon_das_atomdb --multi-line=3 --trailing-comma --force-grid-wrap=0 --use-parentheses --line-width=79

black:
	@black ./hyperon_das_atomdb --line-length 79 -t py37 --skip-string-normalization

flake8:
	@flake8 --show-source ./hyperon_das_atomdb

lint: isort black flake8