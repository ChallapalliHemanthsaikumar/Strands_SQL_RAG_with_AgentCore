import boto3
from strands.models import BedrockModel
from dotenv import load_dotenv
import os 
import logging
from strands.agent.conversation_manager import SlidingWindowConversationManager,SummarizingConversationManager


load_dotenv()




# Create a custom boto3 session
session = boto3.Session(
     aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION', 'us-west-2')  ,# If using temporary credential
    aws_session_token=os.getenv('AWS_SESSION_TOKEN')
    
     # Optional: Use a specific profile
)

# Create a Bedrock model with the custom session
bedrock_model = BedrockModel(
    model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
    boto_session=session
)


# Configure logging
logging.basicConfig(
    filename='./logs/tools_execution.log',  # Log file name
    filemode='a',  # 'a' for append mode (use 'w' to overwrite each time)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO  # Set logging level (change to DEBUG for more details)
)
logger = logging.getLogger(__name__)



conversaion_manager = SlidingWindowConversationManager(window_size=20,should_truncate_results=True)
summarization_manager = SummarizingConversationManager(summary_ratio=0.3,preserve_recent_messages=3)


