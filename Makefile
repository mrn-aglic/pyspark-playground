build:
	docker-compose build

build-yarn:
	docker-compose -f docker-compose.yarn.yml build

build-yarn-nc:
	docker-compose -f docker-compose.yarn.yml build --no-cache

build-nc:
	docker-compose build --no-cache

build-progress:
	docker-compose build --no-cache --progress=plain

down:
	docker-compose down --volumes --remove-orphans

down-yarn:
	docker-compose -f docker-compose.yarn.yml down --volumes --remove-orphans

run:
	make down && docker-compose up

run-scaled:
	make down && docker-compose up --scale spark-worker=3

run-d:
	make down && docker-compose up -d

run-yarn:
	make down-yarn && docker-compose -f docker-compose.yarn.yml up

run-yarn-scaled:
	make down-yarn && docker-compose -f docker-compose.yarn.yml up --scale spark-yarn-worker=$(n)

stop:
	docker-compose stop

stop-yarn:
	docker-compose -f docker-compose.yarn.yml stop


submit:
	docker exec da-spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client ./apps/$(app)

submit-da-book:
	make submit app=data_analysis_book/$(app)

submit-yarn-test:
	docker exec da-spark-yarn-master spark-submit --master yarn --deploy-mode cluster ./examples/src/main/python/pi.py

submit-yarn-cluster:
	docker exec da-spark-yarn-master spark-submit --master yarn --deploy-mode cluster ./apps/$(app)

rm-results:
	rm -r book_data/results/*


# Yarn cluster auto-generated docker-compose
run-ag:
	make down-ag && sh ./generate-docker-compose.sh $(n) && docker compose -f docker-compose.generated.yml up

down-ag:
	docker compose -f docker-compose.generated.yml down


# Scripts to handle /etc/hosts
#args=`n="$(filter-out $@,$(MAKECMDGOALS))" && echo $${n:-${1}}`
n=3

dns-modify-h:
	sh ./dns_scripts/add_docker_hosts.sh -h || true

dns-modify:
	sh ./dns_scripts/add_docker_hosts.sh -o $(o) -n $(n)

dns-restore:
	sh ./dns_scripts/restore_hosts_from_backup.sh
