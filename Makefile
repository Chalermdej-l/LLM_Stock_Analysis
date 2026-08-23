get-screen-data:
	python stock_analysis/dataroma.py
	python stock_analysis/finviz.py
	python stock_analysis/magic.py
	python stock_analysis/sec_13f.py
	sleep 1
	python stock_analysis/llm.py

llm-process:
	python stock_analysis/llm.py

infra-init:
	terraform -chdir=infra init
	terraform -chdir=infra plan

infra-up:
	terraform -chdir=infra apply

infra-down:
	terraform -chdir=infra state rm google_sql_user.users
	terraform -chdir=infra destroy

docker-build:
	docker build --rm -t llm-chainlit -f docker/llm-chainlit.dockerfile .

docker-up:
	docker-compose up