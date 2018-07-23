#-*- mode: makefile-gmake -*-
-include ./Makefile.common
-include ./Makefile.svc

PROJECT 	= taxi
AUTHOR 		= gmoben
TAG 		= $(call mktag)
IMAGE 		= $(ORG)/$(PROJECT):$(TAG)

.PHONY: clean build run test docs

clean:
	find $(CURDIR) -name "__pycache__" | xargs sudo rm -rf
	find $(CURDIR) -name ".cache"      | xargs sudo rm -rf

build:
	echo $(shell echo $(BASE_DIR))
	docker build -t $(PROJECT) -t $(IMAGE) .

nocache:
	docker build --no-cache -t $(PROJECT) -t $(IMAGE) .

test: nats
	docker run --rm -t \
		--name $(PROJECT)-test \
		--network $(NATS_NETWORK) \
		-v $(PWD)/test:/test:ro \
		-v $(PWD)/coverage:/coverage \
		-e LOG_LEVEL=debug \
		-e TAXI_CONFIG=/test/configs/test.yaml \
		--entrypoint=py.test \
		$(IMAGE) \
		-p no:cacheprovider \
		-vvv \
		--cov $(PROJECT) \
		--cov-report html:/coverage/lcov-report \
		--cov-report xml:/coverage/coverage.xml \
		/test/unit

logger: nats
	docker run --rm -t \
		--name $(PROJECT)-logger \
		--network $(NATS_NETWORK) \
		$(IMAGE) \
		taxi.tools.logging MessageLogger

test_driver:
	docker run --rm -t \
		--name $(PROJECT)-driver \
		--network $(NATS_NETWORK) \
		--entrypoint='taxi' \
		$(IMAGE) \
		test 100 test

upload:
	python3 -m pip install --user --upgrade setuptools wheel twine
	python3 setup.py sdist bdist_wheel --universal
	twine upload dist/*

bash:
	docker run -it --rm \
		--name $(PROJECT) \
		--network $(NATS_NETWORK) \
		-v $(PWD)/test:/test \
		-e LOG_LEVEL=debug \
		--entrypoint=sh \
		$(IMAGE)

apidoc:
	@docker run --rm -t \
		--user $$(id -u):$$(id -g) \
		-v ${PWD}:/workdir \
		-w /workdir \
		--entrypoint=sphinx-apidoc \
		$(IMAGE) \
		-o docs taxi -f -d 6 \
		-H $(PROJECT) -A $(AUTHOR)


docs:
	@docker run --rm -t \
		--user $$(id -u):$$(id -g) \
		-v ${PWD}:/workdir \
		-w /workdir \
		--entrypoint=sphinx-build \
		$(IMAGE) \
		docs docs/_build \
		-b html
