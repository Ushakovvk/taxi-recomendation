SERVICE_NAME = taxi_demo
STACK_FILE = './docker-compose.yml'

all: deploy

build:
	docker-compose build

deploy:
	docker stack deploy -c $(STACK_FILE) $(SERVICE_NAME)

remove:
	docker stack rm $(SERVICE_NAME)

confirm:
	docker stack ps $(SERVICE_NAME) --no-trunc
