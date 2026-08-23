import json
import re
from typing import Dict
import pandas as pd
from stock_analysis.constants import (
    SEC_13F_TABLE,
    DATAROMA_INSIDER_BUY_TABLE,
    DATAROMA_SCREEN_INSIDER_TABLE,
    DATAROMA_BIGBETS_TABLE,
    DATAROMA_LOW_TABLE,
    DATAROMA_INSIDER_SUPER_TABLE,
    FINVIZ_SCREEN_TABLE,
    MAGIC_SCREEN_TABLE,
    FINVIZ_SCREENER_URL,
    DATAROMA_INSIDER_BUY_PATH,
)
from stock_analysis.helper.sec_processor import SecProcessor
from stock_analysis.helper.dataroma_processor import DataromaScraper
from stock_analysis.helper.finviz_processor import FinvizScraper
from stock_analysis.helper.magic_processor import MagicFormulaInvesting
from stock_analysis.helper.sql_processor import CloudSQLDatabase
from stock_analysis.helper.llm_processor import LLMProcessor


class PipelineProcessor:
    _READONLY_QUERY = re.compile(r"^\s*(?:select|with)\b", re.IGNORECASE)

    def __init__(self, env_vars: Dict[str, str], logger, chat_env: Dict[str, str] = None):
        self.logger = logger
        self.env_vars = env_vars
        self.sql_helper = CloudSQLDatabase(
            self.env_vars["SQL_USER"],
            self.env_vars["SQL_PASSWORD"],
            self.env_vars["SQL_HOST"],
            self.env_vars["SQL_PORT"],
            self.env_vars["SQL_DATABASE"],
            big_flag=True,
            logger=logger,
        )
        if chat_env is not None:
            self.chat_sql_helper = CloudSQLDatabase(
                chat_env["SQL_READONLY_USER"],
                chat_env["SQL_READONLY_PASSWORD"],
                chat_env["SQL_HOST"],
                chat_env["SQL_PORT"],
                chat_env["SQL_DATABASE"],
                big_flag=False,
                logger=logger,
            )
        else:
            self.chat_sql_helper = self.sql_helper
        self.llm_helper = LLMProcessor(
            self.env_vars["API_KEY"], self.env_vars["MODEL"], self.env_vars.get("MODEL_TOOL")
        )

    def load_cik_list(self, file_path: str) -> Dict:
        try:
            with open(file_path, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            self.logger.error(f"CIK list file not found: {file_path}")
            raise
        except json.JSONDecodeError:
            self.logger.error(f"Invalid JSON in CIK list file: {file_path}")
            raise

    def process_sec_data(self, cik_list: Dict) -> pd.DataFrame:
        processor = SecProcessor(cik_list, self.env_vars["SEC_USER_AGENT"])
        df = processor.process_all_funds()

        numeric_columns = ["value", "voting_sole", "voting_shared", "voting_none", "prn_amt"]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        df = df.drop_duplicates()
        return df

    def insert_data_to_sql(self, df: pd.DataFrame, table_name: str) -> None:
        try:
            self.logger.info(f"Inserting data into SQL table {table_name}")
            self.sql_helper.create_table(table_name, df.dtypes)
            self.sql_helper.insert_data(table_name, df)
        except Exception as e:
            self.logger.error(f"Error inserting data into SQL: {str(e)}")
            raise

    def process_sec_pipeline(self):
        try:
            cik_list = self.load_cik_list("./data/CIK_LIST.json")
            df_sec = self.process_sec_data(cik_list)
            self.insert_data_to_sql(df_sec, SEC_13F_TABLE)
            self.logger.info("SEC data processing and insertion completed successfully.")
        except Exception as e:
            self.logger.error(f"An error occurred in SEC pipeline: {str(e)}")

    def process_dataroma_pipeline(self):
        try:
            scraper = DataromaScraper()

            df_insider_buy = scraper.scrape_insider_buy_data(DATAROMA_INSIDER_BUY_PATH)
            df_insider_buy_home, df_bigbets, df_low, df_insider_super = scraper.scrape_home_data()

            tables_to_update = [
                (DATAROMA_SCREEN_INSIDER_TABLE, df_insider_buy_home),
                (DATAROMA_INSIDER_BUY_TABLE, df_insider_buy),
                (DATAROMA_BIGBETS_TABLE, df_bigbets),
                (DATAROMA_LOW_TABLE, df_low),
                (DATAROMA_INSIDER_SUPER_TABLE, df_insider_super),
            ]

            for table_name, df in tables_to_update:
                self.insert_data_to_sql(df, table_name)

            self.logger.info("Dataroma pipeline completed successfully")
        except Exception as e:
            self.logger.error(f"An error occurred in Dataroma pipeline: {str(e)}")

    def process_finviz_pipeline(self):
        try:
            scraper = FinvizScraper(FINVIZ_SCREENER_URL)
            scraper.fetch_data()

            self.insert_data_to_sql(scraper.df, FINVIZ_SCREEN_TABLE)
            self.logger.info("Finviz pipeline completed successfully")
        except Exception as e:
            self.logger.error(f"An error occurred in Finviz pipeline: {str(e)}")

    def process_magic_formula_pipeline(self):
        try:
            email = self.env_vars["MAGIC_USER"]
            password = self.env_vars["MAGIC_PW"]
            mfi = MagicFormulaInvesting(email, password)
            stock_df = mfi.get_stock_screening()

            self.insert_data_to_sql(stock_df, MAGIC_SCREEN_TABLE)
            self.logger.info("Magic Formula pipeline completed successfully")
        except Exception as e:
            self.logger.error(f"An error occurred in Magic Formula pipeline: {str(e)}")

    def process_llm_pipeline(self):
        try:
            # Load query configurations
            with open("./data/QUERY.json", "r") as f:
                QUERY = json.load(f)

            # Fetch data using SQL helper
            data_frames = {
                "insider_buying_activity": self.sql_helper.fetch_data(QUERY["insider_buying_activity"]),
                "insider_buying_activity_with_superinvestor": self.sql_helper.fetch_data(
                    QUERY["insider_buying_activity_with_superinvestor"]
                ),
                "custom_insider": self.sql_helper.fetch_data(QUERY["custom_insider"]),
                "52week_lows": self.sql_helper.fetch_data(QUERY["52week_lows"]),
                "13f_filing": self.sql_helper.fetch_data(QUERY["13f_filing"]),
                "custom_screen": self.sql_helper.fetch_data(QUERY["custom_screen"]),
                "screen_magic": self.sql_helper.fetch_data(QUERY["screen_magic"]),
            }

            # Process unique funds
            filing_13f = data_frames["13f_filing"]
            unique_funds = filing_13f["fund_name"].unique()
            fund_mapping = {fund: i for i, fund in enumerate(unique_funds, start=1)}
            filing_13f["fund_name"] = filing_13f["fund_name"].map(fund_mapping)
            mapping_fund = pd.json_normalize(fund_mapping).T.reset_index()
            mapping_fund.columns = ["fund_name", "index"]

            # Generate prompts and process reports using LLM helper
            insider_prompt = self.llm_helper.get_prompt_insider(
                data_frames["insider_buying_activity"],
                data_frames["insider_buying_activity_with_superinvestor"],
                data_frames["custom_insider"],
            )
            respond_insider = self.llm_helper.process_insider_report(insider_prompt)

            weeklow_prompt = self.llm_helper.get_prompt_52week_low(
                data_frames["52week_lows"], filing_13f, mapping_fund, respond_insider
            )
            respond_low = self.llm_helper.process_52week_low_report(weeklow_prompt)

            custom_prompt = self.llm_helper.get_prompt_custom_screener(
                data_frames["custom_screen"],
                data_frames["insider_buying_activity"],
                data_frames["insider_buying_activity_with_superinvestor"],
                data_frames["screen_magic"],
            )
            respond_screen = self.llm_helper.process_custom_screener(custom_prompt)

            combined_prompt = self.llm_helper.get_prompt_combined_screener(
                respond_screen, data_frames["custom_screen"], filing_13f
            )
            respond_combine = self.llm_helper.process_combined_screener(combined_prompt)

            senior_prompt = self.llm_helper.get_prompt_senior_report(
                respond_insider, respond_low, respond_screen, respond_combine
            )
            respond_senior = self.llm_helper.process_senior_report(senior_prompt)

            extract_prompt = self.llm_helper.get_promt_extract_list(respond_senior)
            respond_list = self.llm_helper.process_exctract_list(extract_prompt).split(",")

            self.logger.info("LLM pipeline completed successfully")

            return {
                "respond_insider": respond_insider,
                "respond_low": respond_low,
                "respond_screen": respond_screen,
                "respond_combine": respond_combine,
                "respond_senior": respond_senior,
                "respond_list": respond_list,
            }
        except Exception as e:
            self.logger.error(f"An error occurred in LLM pipeline: {str(e)}")

    def run_all_pipelines(self):
        try:
            self.process_sec_pipeline()
            self.process_dataroma_pipeline()
            self.process_finviz_pipeline()
            self.process_magic_formula_pipeline()
            self.logger.info("All pipelines completed successfully")

        except Exception as e:
            self.logger.error(f"An error occurred while running pipelines: {str(e)}")
        finally:
            self.sql_helper.close_connection()

    def run_llm_pipelines(self):
        try:
            return self.process_llm_pipeline()

        except Exception as e:
            self.logger.error(f"An error occurred while running pipelines: {str(e)}")

    def sql_query_executor(self, sql_query):
        """Accept a read-only PostgreSQL query and execute it with the chat (read-only) connection."""
        if not self._READONLY_QUERY.match(sql_query):
            self.logger.warning("Rejected non-SELECT query from LLM: %.200s", sql_query)
            return json.dumps({"error": "Only SELECT and WITH queries are allowed."})
        try:
            result = self.chat_sql_helper.fetch_data(sql_query)
            if result is None:
                return json.dumps({"error": "Query failed. See server logs."})
            return json.dumps({"result": result.to_dict(orient="records")})
        except Exception:
            self.logger.exception("SQL tool query failed")
            return json.dumps({"error": "Query failed. See server logs."})

    def route_prompt(self, prompt_object: list):

        last_respond = prompt_object[-1]["content"]
        prompt_object_route = [
            {"role": "system", "content": self.llm_helper.get_system_route()},
            {"role": "user", "content": last_respond},
        ]
        route_result = self.llm_helper.chat_generate_open_ai(prompt_object=prompt_object_route)

        if route_result.choices[0].message.content == "toolbot":
            self.logger.info("Routing to tool agent")
            return self.llm_helper.chat_generate_with_tool(
                prompt_object=prompt_object, tool_function=self.sql_query_executor
            )
        else:
            self.logger.info("Routing to chat model")
            return self.llm_helper.chat_generate_open_ai(prompt_object=prompt_object)
