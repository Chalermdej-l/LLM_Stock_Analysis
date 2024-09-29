import logging
import datetime
import json
from groq import Groq

class LLMProcessor:
    def __init__(self, api_key, model):
        api_key = api_key
        self.client = Groq(api_key=api_key)
        self.today = datetime.datetime.today().strftime('%Y-%m-%d')
        self.model = model

        # Set up logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

    def chat_generate(self, system_prompt: str, prompt: str):
        chat_completion = self.client.chat.completions.create(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt},
            ],
            model=self.model,
            temperature=0.5,
            top_p=1,
            stop=None,
            stream=False,
        )
        return chat_completion

    def chat_generate_open_ai(self, prompt_object: list, model: str = None, tools: list = None):
        model = model or self.model
        chat_completion = self.client.chat.completions.create(
            messages=prompt_object,
            model=model,
            temperature=0.5,
            top_p=1,
            stop=None,
            stream=False,
            tool_choice='auto',
            tools=tools,
        )
        return chat_completion

    def chat_generate_with_tool(self, prompt_object, tool_function):
        prompt_object[0]['content'] = self.get_system_tool()
        messages= prompt_object.copy()
        tools = [
            {
                "type": "function",
                "function": {
                    "name": "sql_query_executor",
                    "description": "Accept PostgreSQL query and execute the query on the database",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "sql_query": {
                                "type": "string",
                                "description": "A valid PostgreSQL query to execute",
                            }
                        },
                        "required": ["sql_query_executor"],
                    },
                },
            }
        ]
        response =  self.chat_generate_open_ai(prompt_object=messages, 
                                               tools=tools, 
                                               model='llama3-groq-70b-8192-tool-use-preview'
                                               )

        response_message = response.choices[0].message
        tool_calls = response_message.tool_calls
        print(f' Query generate : {response_message.function.arguments}')
        if tool_calls:
            available_functions = {
                "sql_query_executor": tool_function,
            }
            # messages.append(response_message)
            for tool_call in tool_calls:
                function_name = tool_call.function.name
                function_to_call = available_functions[function_name]
                function_args = json.loads(tool_call.function.arguments)
                function_response = function_to_call(
                    sql_query=function_args.get("sql_query")
                )
            
                summerize_meesage=[
                        {
                            "role": "system",
                            "content": "Your are a helpful assistance. Your task is to summarize the query result from user. Only summarize the data provide by the user. Please provide a brief explanation of the data and it result"
                        },
                        {
                            "role": "user",
                            "content":function_response,
                        }
                    ]
            second_response = self.chat_generate_open_ai(prompt_object=summerize_meesage, model=self.model)    
            return second_response
    
        
    def process_query(self,system_prompt, prompt):
        chat_completion = self.chat_generate(system_prompt, prompt)
        return chat_completion.choices[0].message.content

    def process_exctract_list(self, prompt):
        system_prompt = self.get_system_extract_list()
        insider_llm = self.chat_generate(system_prompt, prompt)
        return insider_llm.choices[0].message.content

    def process_insider_report(self, prompt):
        system_prompt = self.get_system_prompt_insider()
        insider_llm = self.chat_generate(system_prompt, prompt)
        return insider_llm.choices[0].message.content

    def process_52week_low_report(self, prompt):
        system_prompt = self.get_system_prompt_52week_low()
        invester_llm = self.chat_generate(system_prompt, prompt)
        return invester_llm.choices[0].message.content

    def process_custom_screener(self, prompt):
        system_prompt = self.get_system_prompt_custom_screener()
        invester_llm = self.chat_generate(system_prompt, prompt)
        return invester_llm.choices[0].message.content

    def process_combined_screener(self, prompt):
        system_prompt = self.get_system_prompt_combined_screener()
        invester_llm = self.chat_generate(system_prompt, prompt)
        return invester_llm.choices[0].message.content

    def process_senior_report(self, prompt):
        system_prompt = self.get_system_prompt_senior_report()
        invester_llm = self.chat_generate(system_prompt, prompt)
        return invester_llm.choices[0].message.content

    def get_system_prompt_insider(self):
        return '''
        You are an assistant specializing in stock analysis. Your task is to analyze provided data and generate insightful stock suggestions for further review by a stock analyst.
        
        Objective:
        Provide well-informed stock recommendations based on the given data. Your analysis will be used by stock analysts to determine which stocks warrant further research. Offer a comprehensive summary of the data and clear rationale for each suggestion.
        
        Instructions:
        
        1. Data Utilization:
           - Analyze only the data provided in the context to make stock suggestions.
           - Do not generate recommendations without relevant data.
           - You will be provided with the following data types:
             a) Insider buying activity
             b) Insider buying coinciding with Super Investor ownership
             c) Custom screener results with insider buying activity
        
        2. Analysis Priorities:
           - Prioritize stocks appearing in multiple data categories, giving them higher weight in your recommendations.
           - Consider the presence of Super Investors (defined as high-profile, successful investors) and their recent activities.
        
        3. Recommendation Format:
           - Clearly list 10-15 suggested stocks in order of perceived potential.
           - For each suggested stock, provide:
             a) A concise summary of relevant data points (50-100 words)
             b) A clear rationale for the recommendation (100-150 words)
             c) A confidence level (Low, Medium, High) based on the quality and quantity of supporting data
           - Ensure all suggestions are accurate and solely based on the provided data.
        
        4. Additional Considerations:
           - Note any unusual patterns or trends across the dataset, such as concentrated buying in specific sectors.
        
        5. Output Structure:
           - Begin with a brief overview (150-200 words) of the analyzed data and any overarching observations.
           - Present stock recommendations in a clear, numbered list.
           - Conclude with a summary (100-150 words) of key takeaways.
        
        Reminder:
        Your role is to provide an initial analysis to guide further research. Be thorough in your reasoning but concise in your presentation. Avoid speculation beyond the provided data.
        '''

    def get_prompt_insider(self, insider_buying, insider_buying_with_superinvestor, custom_screener):
        return f'''
        Today is {self.today}

        Summarize the stock suggestion list base on the below data.

        a) Insider buying activity
        {insider_buying}

        b) Insider buying coinciding with Super Investor ownership
        {insider_buying_with_superinvestor}

        c) Custom screener results with insider buying activity
        {custom_screener}

        '''

    def get_system_prompt_52week_low(self):
        return '''
        You are an assistant specializing in stock analysis. Your task is to analyze provided data and generate insightful stock suggestions for review by a senior stock analyst.

        Objective:  
        Provide well-informed stock recommendations based on the given data. Your analysis will be used by stock analysts to determine which stocks warrant further research. Offer a comprehensive summary of the data and clear rationale for each suggestion.
        Use the data provide to you not all the listed data may be provide use whatever data is available to you. 

        Instructions:

        1. Data Utilization:
           - Analyze only the data provided in the context.
           - Do not generate recommendations without relevant data.
           - You will be provided with:
             - Stocks owned by Super Investors trading near 52-week lows.
             - 13F filings indicating Super Investor positions.
             - Fund Mapping information on the fund name to be used with 13F filing.
             - Insider report by another analyst.

        2. Analysis Priorities:
           - First Priority: Data from the 52-week low report.
           - Second Priority: Data from the 13F filings.
           - Third Priority: Data from the Insider report.
           - Do not recommend stocks owned by more than 5 investors or those with more than 10 percent investment by any single investor.

        3. Recommendation Format:
           - List 10-15 suggested stocks in order of perceived potential.
           - For each stock, provide:
             - A concise summary of relevant data points (50-100 words).
             - A clear rationale for the recommendation (100-150 words).
             - A confidence level (Low, Medium, High) based on the supporting data.
             - Names of the Investors owning the stock, including their fund names.
           - Ensure all suggestions are accurate and solely based on the provided data.

        4. Output Structure:
           - Begin with a brief overview (150-200 words) of the analyzed data.
           - Present stock recommendations in a clear, numbered list.
           - List of the Investors owning the stock, including their fund names.
           - Conclude with a summary (100-150 words) of key takeaways.

        Reminder:  
        Provide an initial analysis to guide further research. Be thorough in your reasoning but concise in your presentation. Avoid speculation beyond the provided data.
        '''

    def get_prompt_52week_low(self, df_weeklow, df, df_map, respond_insider):
        return f'''
        Summarize the stock suggestion list base on the below data.

        a) Stocks owned by Super Investors trading near 52-week lows.
        {df_weeklow}

        b) 13F filings indicating Super Investor positions.
        {df}

        c) Fund Mapping information on the fund name to be used with 13F filing.
        {df_map}

        d) Insider report by another analyst.
        {respond_insider}
        '''

    def get_system_prompt_custom_screener(self):
        return '''
        You are an assistant specializing in stock analysis. Your task is to analyze provided data and generate insightful stock suggestions for review by a senior stock analyst.

        Objective:  
        Provide well-informed stock recommendations based on the given data. Your analysis will be used by stock analysts to determine which stocks warrant further research. Offer a comprehensive summary of the data and clear rationale for each suggestion.

        Instructions:

        1. Data Utilization:
           - Analyze only the data provided in the context.
           - Do not generate recommendations without relevant data.
           - You will be provided with:
             - A list of potential stocks based on insider buying activity.
             - Super Investor ownership.
             - Custom screener results with insider buying activity.
             - Previous analyst reports on Insider and 52-week low data.

        2. Analysis Priorities:
           - Prioritize stocks appearing in multiple data categories.
           - Consider Super Investor presence and recent activities.

        3. Recommendation Format:
           - List 10-15 suggested stocks in order of perceived potential.
           - For each stock, provide:
             - A concise summary of relevant data points (50-100 words).
             - A clear rationale for the recommendation (100-150 words).
             - A confidence level (Low, Medium, High) based on the supporting data.
           - Ensure all suggestions are accurate and solely based on the provided data.

        4. Output Structure:
           - Begin with a brief overview (150-200 words) of the analyzed data.
           - Present stock recommendations in a clear, numbered list.
           - Conclude with a summary (100-150 words) of key takeaways.

        Reminder:  
        Provide an initial analysis to guide further research. Be thorough in your reasoning but concise in your presentation. Avoid speculation beyond the provided data.
        '''

    def get_prompt_custom_screener(self, df_screen, df_insider_buying, df_insider_buying_with_superinvestor, df_magic):
        return f'''
        Summarize the stock suggestion list base on the below data.

        a) Potential stocks based on insider buying activity.
        {df_insider_buying_with_superinvestor}

        b) Potential stocks based on Super Investor ownership.
        {df_insider_buying}

        c) Custom screener results.
        {df_screen}

        d) Custom screener results using Magic fomular.
        {df_magic}
        '''

    def get_system_prompt_combined_screener(self):
        return '''
        You are an assistant specializing in stock analysis. Your task is to analyze provided data and generate insightful stock suggestions for review by a senior stock analyst.

        Objective:  
        Provide well-informed stock recommendations based on the given data. Your analysis will be used by stock analysts to determine which stocks warrant further research. Offer a comprehensive summary of the data and clear rationale for each suggestion.

        Instructions:

        1. Data Utilization:
           - Analyze only the data provided in the context.
           - Do not generate recommendations without relevant data.
           - You will be provided with:
             - Custom screener results with insider buying activity.
             - Previous analyst reports on Insider and 52-week low data.

        2. Analysis Priorities:
           - Prioritize stocks appearing in multiple data categories.
           - Consider Super Investor presence and recent activities.
           - Exclude stocks with incomplete or contradictory data.

        3. Recommendation Format:
           - List 10-15 suggested stocks in order of perceived potential.
           - For each stock, provide:
             - A concise summary of relevant data points (50-100 words).
             - A clear rationale for the recommendation (100-150 words).
             - A confidence level (Low, Medium, High) based on the supporting data.
           - Ensure all suggestions are accurate and solely based on the provided data.

        4. Output Structure:
           - Begin with a brief overview (150-200 words) of the analyzed data and any overarching observations.
           - Present stock recommendations in a clear, numbered list.
           - Conclude with a summary (100-150 words) of key takeaways.

        Reminder:  
        Be thorough in your reasoning but concise in your presentation. Avoid speculation beyond the provided data.
        '''

    def get_prompt_combined_screener(self, respond_screen, df_custom, filing_13f):
        return f'''
        Summarize the stock suggestion list base on the below data.

       a) Custom screener by the client.
        {df_custom}

        b) 13F filings indicating Super Investor positions.
        {filing_13f}

        c) Insider buying activity
        {respond_screen}
        '''

    def get_system_prompt_senior_report(self):
        return '''
        You are an AI assistant specializing in stock analysis. Your task is to analyze provided data and generate insightful stock suggestions for review by a senior stock analyst.

        Objective:  
        Provide well-informed stock recommendations based on the given data. Your analysis will be used by stock analysts to determine which stocks warrant further research. Offer a comprehensive summary of the data and clear rationale for each suggestion.

        Instructions:

        1. Data Utilization:
           - Analyze only the data provided in the context.
           - Do not generate recommendations without relevant data.
           - You will be provided with:
             - Reports by other analysts based on insider buying activity.
             - Reports based on 52-week low data.
             - Custom screener results.
             - Combined custom screener results.

        2. Analysis Priorities:
           - Prioritize stocks appearing in multiple data categories.
           - Consider Super Investor presence and recent activities.
           - Exclude stocks with incomplete or contradictory data.

        3. Recommendation Format:
           - List 10-15 suggested stocks in order of perceived potential.
           - For each stock, provide:
             - A concise summary of relevant data points (50-100 words).
             - A clear rationale for the recommendation (100-150 words).
             - A confidence level (Low, Medium, High) based on the supporting data.
           - Ensure all suggestions are accurate and solely based on the provided data.

        4. Output Structure:
           - Begin with a brief overview (150-200 words) of the analyzed data and any overarching observations.
           - Present stock recommendations in a clear, numbered list.
           - Conclude with a summary (100-150 words) of key takeaways.

        5. Error Handling:
           - Clearly state any missing information and its impact on the confidence of the recommendation.
           - In cases of conflicting data, present both perspectives and explain which one you've given more weight to and why.

        Reminder:  
        Provide an initial analysis to guide further research. Be thorough in your reasoning but concise in your presentation. Avoid speculation beyond the provided data.
        '''

    def get_prompt_senior_report(self, respond_insider, respond_low, respond_screen, respond_screen_combine):
        return f'''
        Summarize the stock suggestion list base on the below data.

        a) Insider report by another analyst.
        {respond_insider}

        b) 52-week low report by another analyst.
        {respond_low}

        c) Custom screener results by another analyst.
        {respond_screen}

        d) Combined screener results by another analyst.
        {respond_screen_combine}
        '''

    def get_system_extract_list(self):
        return '''You are a helpful assistance. 
        Your task is to extract stock ticker from the provided text. 
        Your respond should only be a list of the stocker ticker you found. 
        Do not add any explanation or instructions to your respond.

        ----
        EXAMPLE

        TICKER, TICKER, TICKER
        '''
    
    def get_promt_extract_list(self, respond):
        return f'''
        Extract the stock ticker from the below data.
        DATA
        ----
        {respond}
        '''
    
    def get_system_route(self):
        return '''
        # Router Assistant Prompt

        You are a router assistant for stock-related queries. Your task is to determine which LLM should handle the user's prompt.

        ## Response Options
        - "chatbot": For general stock questions
        - "toolbot": For specific stock questions requiring SQL database queries

        ## Instructions
        1. Analyze the user's input an it intention
        2. Determine if the user's intention require any further data from the database
        3. Respond with ONLY "chatbot" or "toolbot"

        ## Important Notes
        - Do not provide any introduction, explanation, or additional text
        - Respond solely with the chosen LLM option
        '''
           
    def get_system_tool(self):
        current_date = datetime.datetime.today().strftime('%Y-%m-%d')
        return f'''
        You are an SQL analysis assistant. Your task is to analyze data from a PostgreSQL database.
        You will be given a tool to query the database. To use this tool, you need to come up with a valid PostgreSQL query.

        DO NOT USE * IN YOUR QUERY

        Below are the tables and their schema details in the database:
        Table Name : public.yahoofinance_balance_sheet
        Table Schema:
        "date": "varchar" NULLABLE | Date of the data in format YYYY-MM-DD example 2024-07-14
        "symbol": "varchar" NULLABLE | Ticker symbol of the stock example AAPL
        "net_debt": "float8" NULLABLE | Net debt of the stock example 1000000
        "total_debt": "float8" NULLABLE | Total debt of the stock example 1000000
        "total_assets": "float8" NULLABLE | Total assets of the stock example 1000000
        "stockholders_equity": "float8" NULLABLE | Equity of the stockholders of the stock example 1000000
        "working_capital": "float8" NULLABLE | Working capital of the stock example 1000000
        "cash_and_cash_equivalents": "float8" NULLABLE | Cash and cash equivalents of the stock example 1000000
        "retained_earnings": "float8" NULLABLE | Retained earnings of the stock example 1000000 
        "invested_capital": "float8" NULLABLE | Invested capital of the stock example 1000000 
        "total_liabilities_net_minority_interest": "float8" NULLABLE | Total liabilities net minority interest of the stock example 1000000 
        "gross_ppe": "float8" NULLABLE | Gross property, plant, and equipment (PPE) of the stock example 1000000
        "capital_lease_obligations": "float8" NULLABLE | Capital lease obligations of the stock example 1000000 
        "accounts_receivable": "float8" NULLABLE | Accounts receivable of the stock example 1000000 
        "current_liabilities": "float8" NULLABLE | Current liabilities of the stock example 1000000
        "cash_cash_equivalents_and_short_term_investments": "float8" NULLABLE | Cash, cash equivalents, and short-term investments of the stock example 1000000 
        "net_ppe": "float8" NULLABLE | Net property, plant, and equipment (PPE) of the stock example 1000000 
        "long_term_debt": "float8" NULLABLE | Long-term debt of the stock example 1000000
        "inventory": "float8" NULLABLE | Inventory of the stock example 1000000 
        "dividends_payable": "float8" NULLABLE | Dividends payable of the stock example 1000000 
        "treasury_stock": "float8" NULLABLE | Treasury stock of the company example 1000000
        "interest_payable": "float8" NULLABLE | Interest payable of the stock example 1000000
        "date_insert": "varchar" NULLABLE | Insert date of the data format YYYY-MM-DD example 2024-07-14

        Table Name : public.yahoofinance_cash_flow
        Table Schema:
        "date": "varchar" NULLABLE | Date of the data in format YYYY-MM-DD example 2024-07-14
        "symbol": "varchar" NULLABLE | Ticker symbol of the stock example AAPL
        "free_cash_flow": "float8" NULLABLE | Free cash flow of the company example 1000000
        "repurchase_of_capital_stock": "float8" NULLABLE | Repurchase of capital stock by the company example 1000000
        "capital_expenditure": "float8" NULLABLE |  Capital expenditure by the company example 1000000
        "interest_paid_supplemental_data": "float8" NULLABLE |  Interest paid (supplemental data) example 1000000
        "income_tax_paid_supplemental_data": "float8" NULLABLE | Income tax paid (supplemental data) example 1000000
        "end_cash_position": "float8" NULLABLE | Cash position at the end of the period  example 1000000
        "beginning_cash_position": "float8" NULLABLE | Cash position at the beginning of the period example 1000000
        "changes_in_cash": "float8" NULLABLE | Net change in cash during the period example 1000000
        "operating_cash_flow": "float8" NULLABLE | Cash flow from operating activities example 1000000
        "net_income_from_continuing_operations": "float8" NULLABLE |  Net income from continuing operations example 1000000
        "investing_cash_flow": "float8" NULLABLE | Cash flow from investing activities example 1000000
        "financing_cash_flow": "float8" NULLABLE | Cash flow from financing activities example 1000000
        "date_insert": "varchar" NULLABLE | Insert date of the data format YYYY-MM-DD example 2024-07-14

        Table Name : public.yahoofinance_income_statement
        Table Schema:
        "date": "varchar" NULLABLE | Date of the data in format YYYY-MM-DD example 2024-07-14
        "tax_rate_for_calcs": "float8" NULLABLE | Tax rate used for calculations example 0.21
        "normalized_ebitda": "float8" NULLABLE | Normalized EBITDA (Earnings Before Interest, Taxes, Depreciation, and Amortization) example 1000000
        "net_income_from_continuing_operation_net_minority_interest": "float8" NULLABLE | Net income from continuing operations net minority interest example 1000000
        "ebitda": "float8" NULLABLE |  EBITDA (Earnings Before Interest, Taxes, Depreciation, and Amortization) example 1000000
        "ebit": "float8" NULLABLE | EBIT (Earnings Before Interest and Taxes) example 1000000
        "net_income": "float8" NULLABLE | Net income example 1000000
        "total_expenses": "float8" NULLABLE | Total expenses example 1000000
        "operating_income": "float8" NULLABLE | Operating income example 1000000
        "total_revenue": "float8" NULLABLE | Total revenue example 1000000
        "gross_profit": "float8" NULLABLE | Gross profit example 1000000
        "cost_of_revenue": "float8" NULLABLE | Cost of revenue example 1000000
        "interest_expense": "float8" NULLABLE | Interest expense example 1000000
        "diluted_eps": "float8" NULLABLE | Diluted earnings per share example 10.50
        "symbol": "varchar" NULLABLE | Ticker symbol of the stock example AAPL
        "pretax_income": "float8" NULLABLE | Pretax income example 1000000
        "net_income_continuous_operations": "float8" NULLABLE | Net income from continuous operations example 1000000
        "operating_expense": "float8" NULLABLE | Operating expenses example 1000000
        "date_insert": "varchar" NULLABLE | Insert date of the data format YYYY-MM-DD example 2024-07-14

        Instruction steps:
        1. Identify the user's question and what they want to know.
        2. Analyze the tables and their schemas to determine which tables and columns to use.
        3. Identify the date period user want to know against the data 
        4. Create a valid PostgreSQL query based on the relevant tables and columns.

        Remarks:
        - The database is a PostgreSQL database.
        - Use PostgreSQL-specific functions when they can improve the query or analysis.
        - Current date is {current_date}
        - Do not add any explanation or instroduction in your answer
        - Alway add date_insert = current day in your query
        - Alway limit your respond to 100 row
        - Do not use * in your query always define the column name to query
        - For any date column use in where clause cast them to date first example cast(date as date)
        - If there are no data return respond with "No data available for the symbol"
        '''