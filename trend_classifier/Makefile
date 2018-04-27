all: build
build: build-server
push: push-server
.PHONY: push build


TAG = 1.0.15
PROJECT_NAME = tweet_classifier

build-server:
	docker build -t graphy/$(PROJECT_NAME) .
	docker tag graphy/$(PROJECT_NAME) graphy/$(PROJECT_NAME):$(TAG)

push-server: build-server
	docker push graphy/$(PROJECT_NAME)
	docker push graphy/$(PROJECT_NAME):$(TAG)

clean:
	docker rmi graphy/$(PROJECT_NAME):$(TAG) || :
	docker rmi graphy/$(PROJECT_NAME) || :