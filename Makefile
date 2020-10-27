SERVICE_NAME = jupyter
STACK_FILE = './docker-compose.yml'

all: deploy

deploy:
	docker stack deploy -c $(STACK_FILE) $(SERVICE_NAME)

remove:
	docker stack rm $(SERVICE_NAME)

logs:
	docker logs $$(docker ps | grep $(SERVICE_NAME)_spark | awk '{print $$NF}')

confirm:
	docker stack ps $(SERVICE_NAME) --no-trunc
