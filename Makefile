
start:
	clear
	python main.py --message "Hi this is a test"

get_screen_data:
	python code/dataroma.py	
	python code/finviz.py
	python code/magic.py	
	python code/sec_13f.py
	sleep 1
	python code/llm.py

llm_process:
	python code/llm.py

infra_init:
	terraform -chdir=infra init

infra_plan:
	terraform -chdir=infra plan

infra_up:
	terraform -chdir=infra apply

infra_down:
	terraform -chdir=infra destroy