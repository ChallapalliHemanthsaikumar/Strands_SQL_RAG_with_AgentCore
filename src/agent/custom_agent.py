



from strands import Agent
from langfuse import Langfuse,observe
from config.config import bedrock_model
from tools.custom_tool_execute_query import execute

from prompt.basic_prompt import prompt



@observe
def sql_agent(user_query):

   agent = Agent(model=bedrock_model,tools=[execute] ,system_prompt=prompt)

   return agent(user_query)