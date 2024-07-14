
start:
	clear
	python main.py --message "Hi this is a test"

get_screen_data:
	python code/dataroma.py	
	python code/finviz.py
	python code/magic.py	
	python code/sec_13f.py
	# python code/yahoofinance.py

llm_process:
	python code/llm.py