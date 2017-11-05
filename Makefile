#-*- mode: makefile-gmake -*-
-include ./Makefile.common
-include ./Makefile.svc

PROJECT = taxi
TAG = $(call mktag)
IMAGE = ${ORG}/${PROJECT}:${TAG}

.PHONY: clean build run test

clean:
	find $(CURDIR) -name "__pycache__" | xargs sudo rm -rf
	find $(CURDIR) -name ".cache"      | xargs sudo rm -rf

build:
	echo $(shell echo $(BASE_DIR))
	docker build -t ${PROJECT} -t ${IMAGE} .

nocache:
	docker build --no-cache -t ${PROJECT} -t ${IMAGE} .

test: nats
	docker run --rm -t \
		--name ${PROJECT}-test \
		--link nats-main:nats \
		-v ${PWD}/test:/test:ro \
		-v ${PWD}/coverage:/coverage \
		-e LOG_LEVEL=debug \
		-e TAXI_CONFIG=/test/configs/test.yaml \
		--entrypoint=py.test \
		${IMAGE} \
		-p no:cacheprovider \
		-vvv \
		--cov ${PROJECT} \
		--cov-report html:/coverage/lcov-report \
		--cov-report xml:/coverage/coverage.xml \
		/test/unit

bash:
	docker run -it --rm \
		--name ${PROJECT} \
		--link nats-main \
		-v ${PWD}/test:/test \
		-e LOG_LEVEL=debug \
		--entrypoint=sh \
		${IMAGE}
