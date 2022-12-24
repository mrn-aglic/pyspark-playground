down:
	docker-compose down --volumes

run:
	make down && docker-compose up


run-scaled:
	make down && docker-compose up --scale spark-worker=3
