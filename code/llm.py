import logging
from typing import Dict
import os
import json
from dotenv import load_dotenv
from helper.sql_processor import CloudSQLDatabase
from helper.llm_processor import LLMProcessor
from yahoofinance import main as process_llm
import pandas as pd

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_environment_variables() -> Dict[str, str]:
    """
    Load and validate required environment variables.
    """
    load_dotenv('./.env')
    required_vars = ['SQL_DATABASE', 'SQL_USER', 'SQL_PASSWORD', 'SQL_PORT', 'SQL_HOST', 'MODEL', 'API_KEY']
    env_vars = {var: os.getenv(var) for var in required_vars}
    
    missing_vars = [var for var, value in env_vars.items() if value is None]
    if (missing_vars):
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    return env_vars

def save_to_file(filename: str, content: str) -> None:
    """
    Save content to a text file.
    """
    file_path =os.path.join('data', filename)
    with open(file_path, 'w+') as f:
        f.write(content)

def main():
    """
    Main function to execute the logic of loading environment variables,
    fetching stock screening data, inserting it into a Cloud SQL database,
    and processing reports using LLMProcessor.
    """
    try:
        # Load environment variables
        env_vars = load_environment_variables()

        # Initialize SQL helper
        sql_helper = CloudSQLDatabase(
            env_vars['SQL_USER'],
            env_vars['SQL_PASSWORD'],
            env_vars['SQL_HOST'],
            env_vars['SQL_PORT'],
            env_vars['SQL_DATABASE'],
            logger=logger
        )
        
        # Initialize LLM helper
        llm_helper = LLMProcessor(env_vars['API_KEY'], env_vars['MODEL'])

        # Load query configurations
        with open('./data/QUERY.json', 'r') as f:
            QUERY = json.load(f)

        # Fetch data using SQL helper
        data_frames = {
            'insider_buying_activity': sql_helper.fetch_data(QUERY['insider_buying_activity']),
            'insider_buying_activity_with_superinvestor': sql_helper.fetch_data(QUERY['insider_buying_activity_with_superinvestor']),
            'custom_insider': sql_helper.fetch_data(QUERY['custom_insider']),
            '52week_lows': sql_helper.fetch_data(QUERY['52week_lows']),
            '13f_filing': sql_helper.fetch_data(QUERY['13f_filing']),
            'custom_screen': sql_helper.fetch_data(QUERY['custom_screen']),
            'screen_magic': sql_helper.fetch_data(QUERY['screen_magic'])
        }

        # Process unique funds
        filing_13f = data_frames['13f_filing']
        unique_funds = filing_13f['fund_name'].unique()
        fund_mapping = {fund: i for i, fund in enumerate(unique_funds, start=1)}
        filing_13f['fund_name'] = filing_13f['fund_name'].map(fund_mapping)
        mapping_fund = pd.json_normalize(fund_mapping).T.reset_index()
        mapping_fund.columns = ['fund_name', 'index']

        # Generate prompts and process reports using LLM helper
        insider_prompt = llm_helper.get_prompt_insider(
            data_frames['insider_buying_activity'], 
            data_frames['insider_buying_activity_with_superinvestor'], 
            data_frames['custom_insider']
        )
        respond_insider = llm_helper.process_insider_report(insider_prompt)

        weeklow_prompt = llm_helper.get_prompt_52week_low(
            data_frames['52week_lows'], 
            filing_13f, 
            mapping_fund, 
            respond_insider
        )
        respond_low = llm_helper.process_52week_low_report(weeklow_prompt)

        custom_prompt = llm_helper.get_prompt_custom_screener(
            data_frames['custom_screen'], 
            data_frames['insider_buying_activity'], 
            data_frames['insider_buying_activity_with_superinvestor'], 
            data_frames['screen_magic']
        )
        respond_screen = llm_helper.process_custom_screener(custom_prompt)

        combined_prompt = llm_helper.get_prompt_combined_screener(
            respond_screen, 
            data_frames['custom_screen'], 
            filing_13f
        )
        respond_combine = llm_helper.process_combined_screener(combined_prompt)

        senior_prompt = llm_helper.get_prompt_senior_report(
            respond_insider, 
            respond_low, 
            respond_screen, 
            respond_combine
        )
        respond_senior = llm_helper.process_senior_report(senior_prompt)

        extract_prompt = llm_helper.get_promt_extract_list(respond_senior)
        respond_list = llm_helper.process_exctract_list(extract_prompt)
        
        # Save responses to text files
        save_to_file('respond_insider.txt', respond_insider)
        save_to_file('respond_low.txt', respond_low)
        save_to_file('respond_screen.txt', respond_screen)
        save_to_file('respond_combine.txt', respond_combine)
        save_to_file('respond_senior.txt', respond_senior)
        save_to_file('respond_list.txt', respond_list)

        # Process the final response list using the main LLM function
        process_llm(respond_list.split(','))
        
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()
