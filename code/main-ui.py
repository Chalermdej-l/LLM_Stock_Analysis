import chainlit as cl
import chainlit.data as cl_data
from chainlit.data.sql_alchemy import SQLAlchemyDataLayer
import logging
import os
from dotenv import load_dotenv
from helper.pipeline_processor import PipelineProcessor
from helper.stock_detail import StockDetail
from chainlit.types import ThreadDict

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv('./.env')
required_vars = ['SQL_DATABASE', 'SQL_USER', 'SQL_PASSWORD', 'SQL_PORT', 'SQL_HOST', 'MAGIC_USER', 'MAGIC_PW', 'MODEL', 'API_KEY']
env_vars = {var: os.getenv(var) for var in required_vars}

# Initialize processors
pipeline_processor = PipelineProcessor(env_vars=env_vars, logger=logger)
stock_detail_processor = StockDetail(logger=logger, env_vars=env_vars)

# Set up connection string for SQLAlchemy
con_string = f'postgresql+asyncpg://{env_vars["SQL_USER"]}:{env_vars["SQL_PASSWORD"]}@{env_vars["SQL_HOST"]}:{env_vars["SQL_PORT"]}/{env_vars["SQL_DATABASE"]}'
cl_data._data_layer = SQLAlchemyDataLayer(conninfo=con_string)

# Define a step to process the LLM request
@cl.step(type="llm")
async def process_llm_request():
    try:
        content = cl.chat_context.to_openai()
        if content:
            respond = await cl.make_async(pipeline_processor.route_prompt)(prompt_object=content)
            return respond.choices[0].message.content
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}", exc_info=True)
        return f"Apologize , We can't process your requests. {str(e)} . Please try again."

# Action callback to run the pipeline
@cl.action_callback("Run Pipeline")
async def on_action(action: cl.Action):
    logger.info("The user clicked on the action button!")
    await cl.make_async(pipeline_processor.run_all_pipelines)()
    msg = cl.Message(content='Pipeline finish running!')
    await msg.send()
    return 'Pipeline has run successfully'

# Action callback to summarize the pipeline results
@cl.action_callback("Summarize Pipeline")
async def on_action(action: cl.Action):
    logger.info("The user clicked on the action button!")
    respond_senior, _ = await cl.make_async(pipeline_processor.run_llm_pipelines)()
    stock_detail_processor.process_yahoo_finance_pipeline(_)
    await cl.Message(content=respond_senior).send()
    return 'Pipeline has run successfully'

# Resume chat context
@cl.on_chat_resume
async def on_chat_resume(thread: ThreadDict):
    result = await process_llm_request()
    if result:
        response = await cl.Message(content=result.choices[0].message.content).send()
        await response.update()

# Authentication callback
@cl.password_auth_callback
def auth_callback(username: str, password: str):
    # Authenticate user
    if (username, password) == ("admin", "admin"):
        return cl.User(identifier="admin", metadata={"role": "admin", "provider": "credentials"})
    else:
        return None

# Start a new chat session
@cl.on_chat_start
async def on_chat_start():
    actions = [
        cl.Action(name="Run Pipeline", value="Run Pipeline", description="Select to run the pipeline to fetch data."),
        cl.Action(name="Summarize Pipeline", value="Summarize Pipeline", description="Select to summarize the data.")
    ]
    await cl.Message(content="A new chat session has started!", actions=actions).send()

# Handle incoming messages
@cl.on_message
async def main(message: cl.Message):
    result = await process_llm_request()
    if result:
        response = await cl.Message(content=result).send()
        await response.update()
    else:
        response = await cl.Message(content='Aplogieze ').send()
        await response.update()

# Run the application
if __name__ == "__main__":
    cl.run()
