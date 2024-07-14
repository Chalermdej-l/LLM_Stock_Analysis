import logging
import datetime
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
            max_tokens=32768,
            top_p=1,
            stop=None,
            stream=False,
        )
        return chat_completion

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
        You are an AI assistant specializing in stock analysis. Your task is to analyze provided data and generate insightful stock suggestions for further review by a stock analyst.
        
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
           - Conclude with a summary (100-150 words) of key takeaways and any suggestions for additional data that could enhance the analysis.
        
        6. Error Handling:
           - If data for a particular stock is incomplete, clearly state what information is missing and how it impacts the confidence of the recommendation.
           - In cases of conflicting data, present both perspectives and explain which you've given more weight to and why.
        
        Reminder:
        Your role is to provide an initial analysis to guide further research. Be thorough in your reasoning but concise in your presentation. Avoid speculation beyond the provided data.
        '''

    def get_prompt_insider(self, insider_buying, insider_buying_with_superinvestor, custom_screener):
        return f'''
        Today is {self.today}

        Summarize the stock suggestion list base on the below data.

        a) Insider buying activity
        {insider_buying.to_markdown(index=False)}

        b) Insider buying coinciding with Super Investor ownership
        {insider_buying_with_superinvestor.to_markdown(index=False)}

        c) Custom screener results with insider buying activity
        {custom_screener.to_markdown(index=False)}

        '''

    def get_system_prompt_52week_low(self):
        return '''
        You are an AI assistant specializing in stock analysis. Your task is to analyze provided data and generate insightful stock suggestions for review by a senior stock analyst.

        Objective:  
        Provide well-informed stock recommendations based on the given data. Your analysis will be used by stock analysts to determine which stocks warrant further research. Offer a comprehensive summary of the data and clear rationale for each suggestion.

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
           - Begin with a brief overview (150-200 words) of the analyzed data and any overarching observations.
           - Present stock recommendations in a clear, numbered list.
           - Conclude with a summary (100-150 words) of key takeaways.

        5. Error Handling:
           - Clearly state any missing information and its impact on the confidence of the recommendation.
           - In cases of conflicting data, present both perspectives and explain which one you've given more weight to and why.

        Reminder:  
        Provide an initial analysis to guide further research. Be thorough in your reasoning but concise in your presentation. Avoid speculation beyond the provided data.
        '''

    def get_prompt_52week_low(self, df_weeklow, df, df_map, respond_insider):
        return f'''
        Summarize the stock suggestion list base on the below data.

        a) Stocks owned by Super Investors trading near 52-week lows.
        {df_weeklow.to_markdown(index=False)}

        b) 13F filings indicating Super Investor positions.
        {df.to_markdown(index=False)}

        c) Fund Mapping information on the fund name to be used with 13F filing.
        {df_map.to_markdown(index=False)}

        d) Insider report by another analyst.
        {respond_insider}
        '''

    def get_system_prompt_custom_screener(self):
        return '''
        You are an AI assistant specializing in stock analysis. Your task is to analyze provided data and generate insightful stock suggestions for review by a senior stock analyst.

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

    def get_prompt_custom_screener(self, df_screen, df_insider_buying, df_insider_buying_with_superinvestor, df_magic):
        return f'''
        Summarize the stock suggestion list base on the below data.

        a) Potential stocks based on insider buying activity.
        {df_insider_buying_with_superinvestor.to_markdown(index=False)}

        b) Potential stocks based on Super Investor ownership.
        {df_insider_buying.to_markdown(index=False)}

        c) Custom screener results.
        {df_screen.to_markdown(index=False)}

        d) Custom screener results using Magic fomular.
        {df_magic}
        '''

    def get_system_prompt_combined_screener(self):
        return '''
        You are an AI assistant specializing in stock analysis. Your task is to analyze provided data and generate insightful stock suggestions for review by a senior stock analyst.

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

        5. Error Handling:
           - Clearly state any missing information and its impact on the confidence of the recommendation.
           - In cases of conflicting data, present both perspectives and explain which one you've given more weight to and why.

        Reminder:  
        Provide an initial analysis to guide further research. Be thorough in your reasoning but concise in your presentation. Avoid speculation beyond the provided data.
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
        '''
    
    def get_promt_extract_list(self, respond):
        return f'''
        Extract the stock ticker from the below data.
        DATA
        ----
        {respond}
        '''