test:
	@pytest -v

isort:
	@isort ./das_atom_db --multi-line=3 --trailing-comma --force-grid-wrap=0 --use-parentheses --line-width=79

black:
	@black ./das_atom_db --line-length 79 -t py37 --skip-string-normalization

flake8:
	@flake8 --show-source ./das_atom_db

lint: isort black flake8