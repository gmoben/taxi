PROJECT_NAME=taxi

docker_nocache_%:
	docker build --no-cache -t fds/${PROJECT_NAME}-$*:latest -f docker/$* .

docker_build_%:
	docker build -t fds/${PROJECT_NAME}-$*:latest -f docker/$* .

docker_bash_%: docker_run_%
	docker exec -it ${PROJECT_NAME}-$* /bin/bash

docker_stop_%:
	# Stop and remove the container
	$(eval WORKER_NAME := $(shell echo $(PROJECT_NAME)-$* | head --bytes=-1))
	docker stop $(WORKER_NAME) || true
	docker rm $(WORKER_NAME) || true

docker_run_%: docker_stop_% docker_build_%
	$(eval WORKER_NAME := $(shell echo $(PROJECT_NAME)-$* | head --bytes=-1))
	docker run -dt --name=$(WORKER_NAME) fds/$(WORKER_NAME)
