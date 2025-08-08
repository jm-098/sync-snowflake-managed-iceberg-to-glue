/* ==============================================
Procedure: update_glue_metadata_location_poc
Description:  These procedures update the metadata location of an Iceberg table in AWS Glue
              to create the procedure:
                 - replace aws_glue_access_int_with_token with your own external access integration
                 - replace aws_glue_creds_secret_token with your own secret token 
                 - replace us-west-2 with your own region
                 - add addtional data types to the type_mapping dictionary if needed


 Sample Call:
 -----------------------------------------------
 -- Update glue-athena table with snowflake iceberg table data and structure change: 
 CALL update_glue_metadata_location(
     'my_athena_db',
     'my_athena_table',
     get_ddl('table', 'my_snow_db.my_snow_schema.my_snow_iceberg_table'),
     CAST(GET(PARSE_JSON(SYSTEM$GET_ICEBERG_TABLE_INFORMATION('my_snow_db.my_snow_schema.my_snow_iceberg_table')), 'metadataLocation') AS VARCHAR) ,
     'my_snowflake_stream_name'  
 );
----------------------------------------------- 
===============================================
 Change History
===============================================
 Date        | Author        | Description
-------------|---------------|------------------------------------------------------
2025-06-18   | J. Hughes     | Initial version: created update function
2025-07-10   | J. Ma         | convert update function to procedure, so we can create temporary table to clear stream
             |               | updated glue client to use cloud provider token
             |               | added function to clear stream after updating glue metadata
             |               | leverage logging
2025-07-25   | J. Ma         | updated update procedure to automatically sync schema changes from Snowflake to Athena
             |               | Combined create table logic and update logic into one procedure
             |               | For data type conversion, timezone is not converted, so it is assumed that the data in Snowflake and Athena are in the same timezone.
             |               | For data type conversion, geometry types are not supported in Athena, it is currently not mappped. 
2025-07-31   | J. Ma         | Fixed bug with clear stream.       
2025-08-04   | J. Ma         | Clear stream for create table as well, so that the stream is always clear before the task runs.    
2025-08-07   | J. Ma         | Propagate error msg from  individual functions to the main procedure return message.     
             |               | Added logic to check if database exists, if not, create it,  before creating or updating the table.
===============================================
*/

CREATE OR REPLACE PROCEDURE update_glue_metadata_location(
    athena_database_name string, 
    athena_table_name string, 
    snowflake_table_ddl string,
    snow_metadata_location string, 
    snow_stream_name string)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('boto3','botocore', 'snowflake-snowpark-python')
HANDLER = 'check_and_update'
EXTERNAL_ACCESS_INTEGRATIONS = (aws_glue_access_int_with_token)
SECRETS = ('cred'=aws_glue_creds_secret_token)
EXECUTE AS CALLER
AS 
$$
import _snowflake 
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import logging
from datetime import datetime
import re


logger = logging.getLogger("python_logger")
logging.basicConfig(level=logging.INFO) 
logger.info(f"Logging from [{__name__}].")


cloud_provider_object = _snowflake.get_cloud_provider_token('cred')

config = Config(
    retries=dict(total_max_attempts=9),
    connect_timeout=30,
    read_timeout=30,
    max_pool_connections=50
)

glue_client = boto3.client(
    service_name='glue',
    aws_access_key_id = cloud_provider_object.access_key_id,
    aws_secret_access_key = cloud_provider_object.secret_access_key,
    aws_session_token=cloud_provider_object.token,
    region_name='us-west-2'
)

def check_database_exists (athena_database_name):
    try:
        glue_client.get_database(Name=athena_database_name)
        logger.info(f"Database '{athena_database_name}' exists.")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            logger.info(f"Database '{athena_database_name}' does not exist.")
            return False
        else:
            logger.exception(f"Error checking if database '{athena_database_name}' exists.")
    except Exception as e:
        logger.exception("An unhandled exception occurred in check_database_exists.")

def create_database (athena_database_name): 
    try:
        glue_client.create_database(
            DatabaseInput={
                'Name': athena_database_name
            }
        )
        logger.info(f"Database '{athena_database_name}' created successfully.")
        return f"Database '{athena_database_name}' created successfully."
    except ClientError as e:
        logger.exception(f"Error creating database '{athena_database_name}'.")
        return f"ERROR: Failed to create database '{athena_database_name}'. Error: {e}"
    except Exception as e:
        logger.exception("An unhandled exception occurred. Error: {e}")
        return f"ERROR: An unhandled exception occurred. Error: {e}"
    
def check_table_exists(athena_database_name, athena_table_name):
    try:
        response = glue_client.get_table(
            DatabaseName=athena_database_name,
            Name=athena_table_name
        )
        return True  # Table exists
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            return False  # Table does not exist
        else:
            logger.exception(f"Sth wrong when checking if {table_name} exists in athena. ")
    except Exception as e:
        logger.exception("An unhandled exception occurred in check_table_exists.")
        
def extract_base_s3_path(full_s3_path: str) -> str:
    base_path = full_s3_path.rsplit('/', 1)[0] + '/'
    return base_path

def create_table(database_name, table_name, col_definition_str, metadata_location):
    column_def = parse_columns_to_glue_format (col_definition_str)
    output_location = extract_base_s3_path (metadata_location)

    table_input = {
            'Name': table_name,
            'TableType': 'EXTERNAL_TABLE',
            'StorageDescriptor': {
                'Columns': column_def, # Use the parsed columns
                'Location': output_location,
            },
            'Parameters': {
                'metadata_location': metadata_location,
                'table_type': 'ICEBERG'
            }
    }
     
    # Start query execution for table creation
    try: 
        response =   response = glue_client.create_table(
            DatabaseName=database_name,
            TableInput=table_input
        )
        return 'successful created. '
    except Exception as e:
        logger.exception(f" Failed to create {table_name} in database {database_name}:{str(e)} ")
        return 'failed to create: ' + str(e)


def get_table_details(athena_database_name, athena_table_name):
    response = glue_client.get_table(
        DatabaseName=athena_database_name, 
        Name=athena_table_name
    )

    return response

def extract_snow_columns(table_ddl: str) -> str:
    start_idx = table_ddl.find('(')
    end_idx = table_ddl.rfind(')')

    if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
        # Extract the content between the first '(' and the last ')'
        columns_block = table_ddl[start_idx + 1:end_idx].strip()
        logger.debug(f"extract_columns_handler: Extracted columns_block: '{columns_block}'")
        
        # Clean up any extra whitespace around commas
        final_columns_string = re.sub(r'\s*,\s*', ', ', columns_block).strip()
        
        logger.debug(f"extract_columns_handler: Final processed columns string: '{final_columns_string}'")
        return final_columns_string
    else:
        logger.error(f"Could not extract column block from DDL: {table_ddl}")
        return ""

def parse_columns_to_glue_format(columns_str: str) -> list:
    """
    Parses a string like 'col1 INT, col2 STRING' into a list of Glue column dictionaries.
    Handles basic types and maps them to Glue compatible types using a dictionary for cleaner mapping.
    """
    logger.debug(f"parse_columns_to_glue_format: Input columns_str: '{columns_str}'")
    type_mapping = {
        'number': 'int',
        'int': 'int',
        'integer': 'int',
        'string': 'string',
        'varchar': 'string',
        'text': 'string',
        'float': 'double',
        'double': 'double',
        'boolean': 'boolean',
        'date': 'date' 
    }

    parsed_columns = []
    column_defs = columns_str.split(',')
    logger.debug(f"parse_columns_to_glue_format: Split column_defs: {column_defs}")

    for i, col_def in enumerate(column_defs):
        col_def = col_def.strip()
        logger.debug(f"parse_columns_to_glue_format: Processing col_def[{i}]: '{col_def}'")
        if not col_def:
            logger.debug(f"parse_columns_to_glue_format: Skipping empty col_def at index {i}.")
            continue

        parts = col_def.split(' ', 1)
        logger.debug(f"parse_columns_to_glue_format: col_def[{i}] split parts: {parts}")

        if len(parts) >= 2:
            col_name = parts[0].strip()
            raw_col_type = parts[1].strip().lower()

            glue_type = raw_col_type # Default to raw type

            if raw_col_type.startswith('timestamp'):
                glue_type = 'timestamp'
            elif raw_col_type.startswith('decimal') or raw_col_type.startswith('numeric'):
                # Keep decimal/numeric types as they are (e.g., decimal(10,2))
                glue_type = raw_col_type
            elif raw_col_type in type_mapping:
                glue_type = type_mapping[raw_col_type]
            else:
                glue_type = 'string' # Fallback for unknown types
                logger.warning(f"Unknown column type '{raw_col_type}' for column '{col_name}'. Defaulting to 'string'.")

            parsed_columns.append({'Name': col_name, 'Type': glue_type})
            logger.debug(f"parse_columns_to_glue_format: Added column: {{'Name': '{col_name}', 'Type': '{glue_type}'}}")
        else:
            logger.warning(f"Malformed column definition at index {i}: '{col_def}'. Skipping.")
    
    logger.debug(f"parse_columns_to_glue_format: Final parsed_columns: {parsed_columns}")
    return parsed_columns
    
def clear_stream(session, stream_name):
    try:
        tmp_table_name = f"{stream_name}_tmp"
        query = f'CREATE OR REPLACE TEMPORARY TABLE {tmp_table_name} AS SELECT 1 cnt FROM {stream_name} WHERE 1=2'
        session.sql(query).collect()
        logger.info(f"Successfully create tmp table to consume stream")

        return "Temporary table created successfully."
    except Exception as e:
        logger.exception(f"Error in clear_stream: {str(e)}")
        return f"Error: {str(e)}"
        
def update_table( session, database_name, table_name, snow_table_def, new_metadata_location, snow_stream_name):
    existing_table = get_table_details(database_name, table_name)
    snow_column_def = extract_snow_columns (snow_table_def)
    glue_column_list = parse_columns_to_glue_format (snow_column_def)
    
    update_table_dict = {
        'DatabaseName': database_name,
        'TableInput': existing_table['Table']
    }

    update_table_dict['TableInput']['Parameters']['previous_metadata_location'] = update_table_dict['TableInput']['Parameters']['metadata_location']
    update_table_dict['TableInput']['Parameters']['metadata_location'] = new_metadata_location

    if 'Parameters' not in update_table_dict['TableInput']:
            update_table_dict['TableInput']['Parameters'] = {}
            
    # if 'StorageDescriptor' not in update_table_dict['TableInput']:
    #    update_table_dict['TableInput']['StorageDescriptor'] = {}
    update_table_dict['TableInput']['StorageDescriptor']['Columns'] = glue_column_list
        
    keys_to_remove = ['DatabaseName', 'CreatedBy', 'IsRegisteredWithLakeFormation', 'CatalogId', 'VersionId', 'IsMultiDialectView', 'CreateTime', 'UpdateTime']
    
    for key in keys_to_remove:
        if key in update_table_dict['TableInput']:
            del update_table_dict['TableInput'][key]
    
    try:
        # Update the table in Glue catalog
        response = glue_client.update_table(**update_table_dict)
        logger.info(f"Successfully updated for {table_name} at {datetime.now()}")

              
        return 'successful update. '
    except Exception as e:
        logger.exception("An error occurred during updated for {table_name} at {datetime.now()}")
        return 'failed to update: ' + str(e)


def check_and_update(session, athena_database_name, athena_table_name, snow_table_def, snow_metadata_location, snow_stream_name):

    try:
        if not check_database_exists (athena_database_name):
            return_msg = create_database(athena_database_name)

            if 'success' in return_msg:
                logger.info( f"create database  '{athena_database_name}' .")
            else:  
                logger.exception(f"create database {athena_database_name} failed. Message: {return_msg}")
                raise Exception(f"create database {athena_database_name} failed. Message: {return_msg}")

            
        if not check_table_exists(athena_database_name, athena_table_name):
            logger.info(f"Table {athena_table_name} does not exist. Creating table...")
            return_msg = create_table(athena_database_name, athena_table_name, snow_table_def, snow_metadata_location)
            logger.info(f"Table {athena_table_name} created with this msg: {return_msg}.")
        else:
            logger.info(f"Table {athena_table_name} exists. Perform update...")
            return_msg = update_table(session, athena_database_name, athena_table_name, snow_table_def, snow_metadata_location, snow_stream_name)
            logger.info(f"Table {athena_table_name} updated with this msg: {return_msg}.")
    
            
        if 'success' in return_msg:
            stream_clear_msg = clear_stream (session, snow_stream_name)
            if 'success' in stream_clear_msg:
                logger.info( f"SUCCESS: Table '{athena_table_name}' processed. Stream cleared.")
            else:
                logger.exception(f"Failed to clear stream. Message: {stream_clear_msg}")
                raise Exception(f"Failed to clear stream. Message: {stream_clear_msg}")
        else:  
            logger.exception(f"Main operation for {athena_table_name} failed. Message: {return_msg}")
            raise Exception(f"Main operation for {athena_table_name} failed. Message: {return_msg}")

        return f"Successfull processed table '{athena_table_name}' "
    except Exception as e:
        logger.exception("An unhandled exception occurred.Error: {e}")
        return f"ERROR: Failed to process table '{athena_table_name}'. Error: {e}"
$$
;
