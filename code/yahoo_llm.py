import logging
from yahoofinance import main as process_llm

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    """
    Main function to execute the logic of loading environment variables,
    fetching stock screening data, inserting it into a Cloud SQL database,
    and processing reports using LLMProcessor.
    """
    try:
   
        with open('./data/respond_list.txt','r') as f:
            respond_list = f.read()
        # Process the final response list using the main LLM function
        process_llm(respond_list.split(','))
        
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()
