from dotenv import load_dotenv
import os
load_dotenv('../.env')

# LLM
api_key = os.getenv("API_KEY")
config_list =  [
        {
            "model": "llama3-70b-8192", 
            "api_key": api_key, 
            "base_url":"https://api.groq.com/openai/v1", 
            "tags": ["llama"]     
        }
    ]
