import logging
import os
from stock_analysis.settings import load_env
from stock_analysis.helper.pipeline_processor import PipelineProcessor
from stock_analysis.yahoofinance import main as process_yahoo_finance

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def save_to_file(filename: str, content: str) -> None:
    """Save content to a text file."""
    file_path = os.path.join("data", filename)
    with open(file_path, "w+") as f:
        f.write(content)


def main():
    """
    Run the LLM report pipeline, save the six report files, and fetch
    Yahoo Finance data for the extracted tickers.
    """
    try:
        env_vars = load_env(["SQL_DATABASE", "SQL_USER", "SQL_PASSWORD", "SQL_PORT", "SQL_HOST", "MODEL", "API_KEY"])
        processor = PipelineProcessor(env_vars=env_vars, logger=logger)
        reports = processor.run_llm_pipelines()
        if reports is None:
            return

        for name, content in reports.items():
            if name == "respond_list":
                content = ",".join(content)
            save_to_file(f"{name}.txt", content)

        process_yahoo_finance(reports["respond_list"])

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}", exc_info=True)


if __name__ == "__main__":
    main()
