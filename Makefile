isort:
	@isort ./hyperon_das_atomdb ./tests --multi-line=3 --trailing-comma --force-grid-wrap=0 --use-parentheses --line-width=100

black:
	@black ./hyperon_das_atomdb ./tests --line-length 100 -t py37 --skip-string-normalization

flake8:
	@flake8 ./hyperon_das_atomdb ./tests --show-source --extend-ignore E501

lint: isort black flake8

unit-tests:
	@py.test -sx -vv ./tests/unit

unit-tests-coverage:
	@py.test -sx -vv ./tests/unit --cov=./hyperon_das_atomdb/ --cov-report=term-missing --cov-fail-under=70

integration-tests:
	@py.test -sx -vv ./tests/integration

performance-tests:
	@bash ./tests/performance/run_perf_tests.sh

pre-commit: unit-tests-coverage lint