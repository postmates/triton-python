.PHONY: all pep8 pyflakes clean dev

GITIGNORES=$(shell cat .gitignore |tr "\\n" ",")
VERSION=$(shell grep version triton/__init__.py |cut -d' ' -f3 |sed "s/\\'//g")

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
	@rm -rf build dist env pystatsd-2.0.1-py2-none-any.whl

docker-build-deps:
	aws s3 cp s3://artifacts.postmates.com/postal/simple2/pystatsd/pystatsd-2.0.1-py2-none-any.whl .

docker-build: docker-build-deps
	@sed -i.bak '/pystatsd/d' cfg/requirements.txt
	docker build -t quay.io/postmates/triton:$(VERSION) .
	@mv cfg/requirements.txt.bak cfg/requirements.txt

docker-push:
	docker push quay.io/postmates/triton:$(VERSION)
