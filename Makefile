test:
	@pytest -v

isort:
	@isort ./src --multi-line=3 --trailing-comma --force-grid-wrap=0 --use-parentheses --line-width=79

black:
	@black ./src --line-length 79 -t py37 --skip-string-normalization

flake8:
	@flake8 --show-source ./src

lint: isort black flake8