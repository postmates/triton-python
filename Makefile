.PHONY: all pep8 pyflakes clean dev test-drone

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
	find tests -name "*.py" | xargs env/bin/yapf -i --style=google
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

test-drone:
	DRONE_BRANCH="$(shell git rev-parse --abbrev-ref HEAD)" \
	DRONE_COMMIT_SHA="$(shell git rev-parse HEAD)" \
	drone exec
