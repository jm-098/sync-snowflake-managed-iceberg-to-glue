-- this procedure is used to update the metadata location of an Iceberg table in AWS Glue
-- changes from function:
---- convert update function to procedure, so we can create temporary table to clear stream 
---- updated glue client to use cloud provider token
---- added function to clear stream after updating glue metadata
---- leverage logging  


CREATE OR REPLACE PROCEDURE update_glue_metadata_location_poc(athena_database_name string, athena_table_name string, new_metadata_location string, snow_stream_name string)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('boto3','botocore', 'snowflake-snowpark-python')
HANDLER = 'update_table'
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


def get_table_details(database_name, table_name):
    response = glue_client.get_table(
        DatabaseName=database_name, 
        Name=table_name
    )

    return response


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
        
def update_table(session, database_name, table_name, new_metadata_location, snow_stream_name):
    existing_table = get_table_details(database_name, table_name)

    update_table_dict = {
        'DatabaseName': database_name,
        'TableInput': existing_table['Table']
    }

    update_table_dict['TableInput']['Parameters']['previous_metadata_location'] = update_table_dict['TableInput']['Parameters']['metadata_location']
    update_table_dict['TableInput']['Parameters']['metadata_location'] = new_metadata_location


    keys_to_remove = ['DatabaseName', 'CreatedBy', 'IsRegisteredWithLakeFormation', 'CatalogId', 'VersionId', 'IsMultiDialectView', 'CreateTime', 'UpdateTime']
    
    for key in keys_to_remove:
        if key in update_table_dict['TableInput']:
            del update_table_dict['TableInput'][key]
    
    try:
        # Update the table in Glue catalog
        response = glue_client.update_table(**update_table_dict)
        logger.info(f"Successfully updated for {table_name} at {datetime.now()}")

        str_clear_response = clear_stream (session, snow_stream_name)
        return 'successful update'
    except Exception as e:
        logger.exception("An error occurred during updated for {table_name} at {datetime.now()}")
        return 'failed to update: ' + str(e)

$$
;

-- this procedure is used to recreate the table in Athena if it does not exist or if the schema is different
-- it expects the table definition in Snowflake format from get_ddl function
-- sample call to run the procedure: 
--    call recreate_table_in_athena_poc('my_athena-db',   'my_athena_table', 
--          get_ddl('table', 'my_snow_db.my_snow_schema.my_snow_iceberg_table'),
 --         REGEXP_SUBSTR(CAST(GET(PARSE_JSON(SYSTEM$GET_ICEBERG_TABLE_INFORMATION('my_snow_iceberg_table')), 'metadataLocation') AS VARCHAR), 's3://[^/]+/[^/]+/[^/]+/TESTSC/')  
--          );           
-- it creates table in Athena if it does not exist or if the schema is different
-- it drops and recreates the table if the table structure is different (to be enhanced)
--  Notes for the function: compare_schema, which compares the structure of an Athena table with a Snowflake-managed table.
---- It primarily identifies columns that have been added or dropped between the two tables.
---- Column order is ignored in the comparison; differences in column order will be considered as changes.
---- For column types, all Snowflake timestamp_* columns are converted to the 'timestamp' type in Athena.
---- No timezone conversions are applied during the column type transformation.


CREATE OR REPLACE PROCEDURE recreate_table_in_athena_poc(database_name string, table_name string, table_def string, output_location string)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.11
HANDLER = 'check_and_create_table'
EXTERNAL_ACCESS_INTEGRATIONS = (aws_glue_access_int_with_token)
PACKAGES = ('boto3','botocore', 'snowflake-snowpark-python')
SECRETS = ('cred' = aws_glue_creds_secret_token)
AS
$$

import _snowflake
import boto3
import time
from botocore.config import Config
from botocore.exceptions import ClientError
import logging
from datetime import datetime
import re

logger = logging.getLogger("python_logger")
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

athena_client = boto3.client(
    service_name='athena',
    aws_access_key_id = cloud_provider_object.access_key_id,
    aws_secret_access_key = cloud_provider_object.secret_access_key,
    aws_session_token=cloud_provider_object.token,
    region_name='us-west-2'
)

def check_table_exists(database_name, table_name):
    try:
        response = glue_client.get_table(
            DatabaseName=database_name,
            Name=table_name
        )
        return True  # Table exists
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            return False  # Table does not exist
        else:
            logger.exception(f"Sth wrong when checking if {table_name} exists in athena. ")
            raise e  # Re-raise if it's another error

def columns_to_string(existing_columns):
    return ', '.join([
        f"{col['Name'].upper()} {col['Type'].upper()}"
        for col in existing_columns
    ])


def compare_schema (database_name, table_name , column_list_str ):
    try:
        response = glue_client.get_table(
            DatabaseName=database_name,
            Name=table_name
        )
        existing_columns = response['Table']['StorageDescriptor']['Columns']
        existing_columns_str = columns_to_string (existing_columns)

        def compare_strings_ignore_space_case(str1, str2):
            normalize = lambda s: ''.join(s.split()).lower()
            return 'same' if normalize(str1) == normalize(str2) else 'diff'
           
        result = compare_strings_ignore_space_case(column_list_str, existing_columns_str)
        return result
    except Exception as e:
        logger.exception(f"Sth wrong in compare_schema. ")
        return 'error'
       
def convert_to_athena_schema(def_str):
    return ', '.join([
        col.strip().replace(':', ' ').upper()
        for col in def_str.split(',')
    ])

def extract_snow_columns(table_ddl: str) -> str:
    start_idx = table_ddl.find('(')
    end_idx = table_ddl.rfind(')')

    if start_idx != -1 and end_idx != -1:
        columns_block = table_ddl[start_idx + 1:end_idx].strip()
        columns_block_list = columns_block.split(',')  # Split by commas for each column definition
       
        for i, col in enumerate(columns_block_list):
            col = col.strip()
           
            if 'TIMESTAMP_' in col:
                # Separate the column name and its type (e.g., 'insert_time TIMESTAMP_NTZ(6)')
                parts = col.split(' ', 1)
               
                if len(parts) == 2:
                    col_name, col_type = parts
                   
                    # Replace TIMESTAMP_* with just TIMESTAMP, removing the precision/argument inside parentheses
                    if col_type.startswith('TIMESTAMP_'):
                        col_type = 'TIMESTAMP'  # Keep only 'TIMESTAMP', ignore the rest
                   
                    # Rebuild the column definition
                    columns_block_list[i] = f"{col_name} {col_type}"

        # Rebuild the final columns block into a single string
        columns_block = ', '.join(columns_block_list).strip()
       
        return columns_block

    else:
        return ""      
   
def create_table(database_name, table_name, col_definition_str, output_location):
    column_def = convert_to_athena_schema(col_definition_str)
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
        {column_def}
    )
    LOCATION '{output_location}'
    TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet',
    'write_compression' = 'snappy'
    );
    """
    logger.info(f" SQL to create in athena is {create_table_query} ")
    # Start query execution for table creation
    response = athena_client.start_query_execution(
        QueryString=create_table_query,
        QueryExecutionContext={'Database': '{database_name}'},
        ResultConfiguration={'OutputLocation': output_location}
    )

    query_execution_id = response['QueryExecutionId']
   
    # Wait for the query execution to complete
    while True:
        result = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = result['QueryExecution']['Status']['State']
       
        if status == 'SUCCEEDED':
            logger.info(f"{table_name} created successfully in database {database_name}. ")
            return True
        elif status in ['FAILED', 'CANCELLED']:
            logger.exception(f" Failed to create {table_name} in database {database_name}. ")
            return False
        time.sleep(2)  # Wait 2 seconds before checking again

   

def drop_and_recreate_if_diff(database_name, table_name, new_column_list_str, s3_location):
    result = compare_schema(database_name, table_name, new_column_list_str)

    if result == 'diff':
        try:
            glue_client.delete_table(DatabaseName=database_name, Name=table_name)
            logger.info(f"Dropped mismatched table: {database_name}.{table_name}")            
        except ClientError as e:
            logger.exception(f" {table_name} could not be deleted (might not exist): {e}")
       
        create_table(database_name, table_name, new_column_list_str, s3_location)
        return 'recreated'
    else:
        logger.info(f" Table schema is in sycn with snowflake src table: {database_name}.{table_name}")
        return 'nothing done'
 
# Check and create the table if it doesn't exist
def check_and_create_table(database_name, table_name, table_definition, output_location):
    snow_table_column_definition = extract_snow_columns (table_definition)

    if not check_table_exists(database_name, table_name):
        logger.info(f"Table {table_name} does not exist. Creating table...")
        logger.debug(f" table_definition is {snow_table_column_definition} ")
        logger.debug(f" output_location is {output_location} ")
        return create_table(database_name, table_name, snow_table_column_definition, output_location)
    else:
        logger.info(f"Table '{table_name}' check table change. Recreated if needed...")
        recreate_flag = drop_and_recreate_if_diff(database_name, table_name, snow_table_column_definition, output_location)
        return recreate_flag

$$;
