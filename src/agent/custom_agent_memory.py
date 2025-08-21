from strands import Agent
from langfuse import Langfuse,observe
from config.config import bedrock_model
from tools.custom_tool_execute_query import execute

from prompt.basic_prompt import prompt

from config.config import conversaion_manager,summarization_manager


agent = Agent(model=bedrock_model,tools=[execute] ,system_prompt=prompt,conversation_manager=summarization_manager)


@observe
def sql_agent(user_query):

   

   return agent(user_query)