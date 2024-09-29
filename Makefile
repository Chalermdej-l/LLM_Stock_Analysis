
start:
	clear
	python main.py --message "Hi this is a test"

get-screen-data:
	python code/dataroma.py	
	python code/finviz.py
	python code/magic.py	
	python code/sec_13f.py
	sleep 1
	python code/llm.py

llm-process:
	python code/llm.py

infra-init:
	terraform -chdir=infra init

infra-plan:
	terraform -chdir=infra plan

infra-up:
	terraform -chdir=infra apply
	sleep 1
	terraform -chdir=infra output -raw service_account_key | python code/decode_key.py --encode_key="$(cat)"

infra-down:
	terraform -chdir=infra state rm google_sql_user.users
	terraform -chdir=infra destroy

docker-build:
	docker build --rm -t llm-chainlit -f docker/llm-chainlit.dockerfile .

docker-up:
	docker-compose up