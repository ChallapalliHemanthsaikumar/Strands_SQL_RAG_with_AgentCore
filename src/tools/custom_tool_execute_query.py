"""
Custom Tool for Executing Redshift SQL Queries
This tool provides secure and efficient SQL query execution against Amazon Redshift
with built-in error handling, query validation, and observability through Langfuse.
"""

import os
from config.config import logger
import pandas as pd
import psycopg2
import sqlparse
import boto3
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
import json
import socket

# Environment variables
from dotenv import load_dotenv
from strands import tool

load_dotenv()



# Langfuse imports for observability (optional)
try:
    from langfuse import Langfuse
    from langfuse import observe
    
    langfuse = Langfuse(
        secret_key=os.getenv('LANGFUSE_SECRET_KEY'),
        public_key=os.getenv('LANGFUSE_PUBLIC_KEY'),
        host=os.getenv('LANGFUSE_HOST', 'https://cloud.langfuse.com')
    )
    LANGFUSE_ENABLED = True
    logger.info("Langfuse observability enabled")
except Exception as e:
    logger.warning(f"Langfuse initialization failed: {e}. Continuing without observability.")
    langfuse = None
    LANGFUSE_ENABLED = False


def check_network_connectivity(host: str, port: int, timeout: int = 10) -> Dict[str, Any]:
    """
    Check if we can reach the Redshift endpoint
    """
    try:
        logger.info(f"Testing network connectivity to {host}:{port}")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        
        if result == 0:
            return {'reachable': True, 'message': 'Host is reachable'}
        else:
            return {'reachable': False, 'message': f'Connection failed with error code: {result}'}
    except socket.gaierror as e:
        return {'reachable': False, 'message': f'DNS resolution failed: {e}'}
    except Exception as e:
        return {'reachable': False, 'message': f'Network test failed: {e}'}


def check_aws_credentials() -> Dict[str, Any]:
    """
    Check AWS credentials and permissions
    """
    try:
        # Try to get AWS credentials from environment or session
        session = boto3.Session(
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION', 'us-west-2')
        )
        
        # Test credentials by calling STS get-caller-identity
        sts = session.client('sts')
        identity = sts.get_caller_identity()
        
        return {
            'valid': True,
            'account': identity.get('Account'),
            'user_id': identity.get('UserId'),
            'arn': identity.get('Arn')
        }
    except Exception as e:
        return {
            'valid': False,
            'error': str(e),
            'message': 'AWS credentials are invalid or not configured'
        }


class RedshiftQueryExecutorTool:
    """
    A high-standard custom tool for executing SQL queries against Amazon Redshift.
    
    Features:
    - Secure connection management
    - Query validation and sanitization
    - Result formatting and error handling
    - Observability with Langfuse
    - Support for both read and write operations (with safety checks)
    """
    
    def __init__(self):
        # Tool metadata
        self.name = "redshift_query_executor"
        self.description = "Execute SQL queries against Amazon Redshift database with built-in safety checks and observability"
        
        # Database connection parameters
        self.connection_params = {
            'host': os.getenv('REDSHIFT_HOST'),
            'port': int(os.getenv('REDSHIFT_PORT', 5439)),
            'database': os.getenv('REDSHIFT_DATABASE'),
            'user': os.getenv('REDSHIFT_USER'),
            'password': os.getenv('REDSHIFT_PASSWORD'),
            'connect_timeout': 10,  # Connection timeout in seconds
            'sslmode': 'require'    # Use SSL for security
        }
        
        # Validate required environment variables
        self._validate_config()
        logger.info(f"Initialized {self.name} tool")
    
    def _validate_config(self):
        """Validate that all required configuration is present"""
        required_vars = ['REDSHIFT_HOST', 'REDSHIFT_DATABASE', 'REDSHIFT_USER', 'REDSHIFT_PASSWORD']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        logger.info("Configuration validation passed")
    
    def _get_connection(self):
        """Create and return a database connection with proper error handling"""
        try:
            logger.info(f"Connecting to Redshift at {self.connection_params['host']}...")
            connection = psycopg2.connect(**self.connection_params)
            connection.autocommit = False
            logger.info("Successfully connected to Redshift")
            return connection
        except psycopg2.OperationalError as e:
            logger.error(f"Failed to connect to Redshift: {str(e)}")
            if "timeout" in str(e).lower():
                raise ConnectionError("Connection timeout - check network connectivity and Redshift cluster status")
            elif "authentication" in str(e).lower():
                raise ConnectionError("Authentication failed - check username and password")
            else:
                raise ConnectionError(f"Database connection failed: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected connection error: {str(e)}")
            raise
    
    def _validate_query(self, query: str, query_type: str) -> bool:
        """
        Validate SQL query for basic safety checks
        """
        try:
            # Parse the query
            parsed = sqlparse.parse(query)
            if not parsed:
                logger.warning("Query parsing failed")
                return False
            
            # Basic safety checks
            query_upper = query.upper().strip()
            
            # Check for dangerous operations
            dangerous_keywords = ['DROP DATABASE', 'DROP SCHEMA', 'TRUNCATE', 'DELETE FROM']
            if any(keyword in query_upper for keyword in dangerous_keywords):
                if query_type not in ['DELETE', 'DROP', 'ALTER']:
                    logger.warning(f"Potentially dangerous operation detected: {query}")
                    return False
            
            # Ensure query type matches actual query
            if query_type == 'SELECT' and not query_upper.startswith('SELECT'):
                logger.warning("Query type mismatch")
                return False
            
            logger.info("Query validation passed")
            return True
        except Exception as e:
            logger.error(f"Query validation failed: {str(e)}")
            return False
    
    def execute_query(self, query: str, limit: int = 1000, timeout: int = 30) -> Dict[str, Any]:
        """
        Execute the SQL query with proper error handling and observability
        """
        connection = None
        cursor = None
        
        try:
            logger.info(f"Executing query: {query[:100]}{'...' if len(query) > 100 else ''}")
            
            # Get database connection
            connection = self._get_connection()
            cursor = connection.cursor()
            
            # Set query timeout
            cursor.execute(f"SET statement_timeout = {timeout * 1000}")  # Convert to milliseconds
            
            # Add LIMIT clause for SELECT queries if not already present
            if query.upper().strip().startswith('SELECT') and 'LIMIT' not in query.upper():
                query = f"{query.rstrip(';')} LIMIT {limit}"
                logger.info(f"Added LIMIT clause: {limit}")
            
            # Execute the query
            start_time = datetime.now()
            cursor.execute(query)
            execution_time = (datetime.now() - start_time).total_seconds()
            
            # Handle different types of queries
            if query.upper().strip().startswith('SELECT'):
                # Fetch results for SELECT queries
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                rows = cursor.fetchall()
                
                # Convert to pandas DataFrame for better handling
                if rows and columns:
                    df = pd.DataFrame(rows, columns=columns)
                    result = {
                        'success': True,
                        'data': df.to_dict('records'),
                        'columns': columns,
                        'row_count': len(rows),
                        'execution_time_seconds': execution_time,
                        'query': query
                    }
                    logger.info(f"Query returned {len(rows)} rows in {execution_time:.2f} seconds")
                else:
                    result = {
                        'success': True,
                        'data': [],
                        'columns': [],
                        'row_count': 0,
                        'execution_time_seconds': execution_time,
                        'query': query
                    }
                    logger.info(f"Query completed with no results in {execution_time:.2f} seconds")
            else:
                # For non-SELECT queries, commit the transaction
                connection.commit()
                affected_rows = cursor.rowcount
                
                result = {
                    'success': True,
                    'message': f"Query executed successfully. Affected rows: {affected_rows}",
                    'affected_rows': affected_rows,
                    'execution_time_seconds': execution_time,
                    'query': query
                }
                logger.info(f"Query modified {affected_rows} rows in {execution_time:.2f} seconds")
            
            # Log to Langfuse (if enabled)
          
            
            return result
            
        except psycopg2.Error as e:
            # Database-specific errors
            error_result = {
                'success': False,
                'error': f"Database error: {str(e)}",
                'error_code': e.pgcode if hasattr(e, 'pgcode') else None,
                'query': query
            }
            
            if connection:
                connection.rollback()
            
            logger.error(f"Database error executing query: {str(e)}")
            return error_result
            
        except Exception as e:
            # General errors
            error_result = {
                'success': False,
                'error': f"Execution error: {str(e)}",
                'query': query
            }
            
            if connection:
                connection.rollback()
            
            logger.error(f"Error executing query: {str(e)}")
            return error_result
            
        finally:
            # Clean up resources
            if cursor:
                cursor.close()
            if connection:
                connection.close()
            logger.info("Database connection closed")
    
    def execute(self, tool_input: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main execution method for the tool (AgentCore compatible)
        """
        try:
            # Extract parameters
            query = tool_input.get('query', '').strip()
            query_type = tool_input.get('query_type', 'SELECT').upper()
            limit = tool_input.get('limit', 1000)
            timeout = tool_input.get('timeout', 30)
            
            # Validate inputs
            if not query:
                return {
                    'success': False,
                    'error': "Query parameter is required and cannot be empty"
                }
            
            # Validate query
            if not self._validate_query(query, query_type):
                return {
                    'success': False,
                    'error': "Query validation failed. Please check your SQL syntax and query type."
                }
            
            # Execute query
            result = self.execute_query(query, limit, timeout)
            
            return result
                
        except Exception as e:
            logger.error(f"Tool execution failed: {str(e)}")
            return {
                'success': False,
                'error': f"Tool execution failed: {str(e)}"
            }


# Simple execution function for testing
@tool
def execute(query: str, limit: int = 1000) -> Any:
    """
    Simple function to execute queries in redshift
    """
    tool = RedshiftQueryExecutorTool()
    
    tool_input = {
        'query': query,
        'query_type': 'SELECT',
        'limit': limit,
        'timeout': 30
    }
    
    result = tool.execute(tool_input)
    
    if result['success']:
        return result['data'] if 'data' in result else result
    else:
        print(f"❌ Error: {result['error']}")
        return None
