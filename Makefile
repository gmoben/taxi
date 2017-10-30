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

test: nats
	docker run --rm -t \
		--name ${PROJECT}-test \
		--link nats-main \
		-v ${PWD}/test:/test \
		-e LOG_LEVEL=debug \
		--entrypoint=py.test \
		${IMAGE} \
		-vvv \
		/test

bash:
	docker run -it --rm \
		--name ${PROJECT} \
		--link nats-main \
		-v ${PWD}/test:/test \
		-e LOG_LEVEL=debug \
		--entrypoint=sh \
		${IMAGE}
