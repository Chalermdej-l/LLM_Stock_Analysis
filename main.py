from autogen import ConversableAgent,UserProxyAgent
from config.config_file import config_list
import argparse

def main(meesage):

    llm_config = {
    "temperature": 1,
    "config_list": config_list,
    }

    number_agent = ConversableAgent(
        name="Chatbot_agent",
        system_message=
        "You are an expert in financial advice and financial planning."
        "The principle you are using is base on the stock value not their price"
        "Don't intorduce yourself. or add any extra explanation that user don't ask"
        "Don't advice any stock unless you have an evidence to support it"
        "Return 'TERMINATE' when the task is done.",
        llm_config=llm_config,
        human_input_mode="NEVER",
        default_auto_reply='That is all. Thank you',
        description=""
    )

    user_agent = UserProxyAgent(
        name="User",
        llm_config=llm_config,
        human_input_mode="ALWAYS",
        code_execution_config ={'use_docker':False},
        default_auto_reply='That is all. Thank you',
        is_termination_msg=lambda msg: msg.get("content") is not None and "TERMINATE" in msg["content"],
        description=""
    )
    user_agent.initiate_chat(recipient=number_agent,message=meesage)

if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument('--message', type=str)
    args = argparser.parse_args()
    main(args.message)