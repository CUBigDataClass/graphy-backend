all: build
build: build-server
push: push-server
.PHONY: push build


TAG = 1.0.2
PROJECT_NAME = trend_puller

target/trend-puller-1.0-SNAPSHOT-jar-with-dependencies.jar: src/main/java/TwitterTrendProducer.java
	mvn package

build-server: target/trend-puller-1.0-SNAPSHOT-jar-with-dependencies.jar
	docker build -t graphy/$(PROJECT_NAME) .
	docker tag graphy/$(PROJECT_NAME) graphy/$(PROJECT_NAME):$(TAG)

push-server: build-server
	docker push graphy/$(PROJECT_NAME)
	docker push graphy/$(PROJECT_NAME):$(TAG)

clean:
	docker rmi graphy/$(PROJECT_NAME):$(TAG) || :
	docker rmi graphy/$(PROJECT_NAME) || :