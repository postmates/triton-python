.PHONY: all pep8 pyflakes clean dev

GITIGNORES=$(shell cat .gitignore |tr "\\n" ",")

all: pep8

pep8: .gitignore env
	@bin/virtual-env-exec pep8 . --exclude=$(GITIGNORES)

pyflakes: env
	@bin/virtual-env-exec pyflakes triton tests

pylint: env
	@bin/virtual-env-exec pylint triton 2>&1 |less

yapf:
	find triton -name "*.py" | xargs env/bin/yapf -i --style=google
	find bin | xargs env/bin/yapf -i --style=google

dev: env env/.pip

env:
	@virtualenv --distribute env

env/.pip: env cfg/requirements.txt
	@bin/virtual-env-exec pip install -r cfg/requirements.txt
	@bin/virtual-env-exec pip install -e .
	@touch env/.pip

test: env/.pip
	@bin/virtual-env-exec testify tests

shell:
	@bin/virtual-env-exec ipython

devclean:
	@rm -rf env

clean:
	@rm -rf build dist env
