/* =============================================
Procedure: check_db_table_glue_catalog_func
Description:  This function checks if a db or a table exists in the Glue catalog.  It can be checked for one table or a list of tables. 
              to create the function:
                 - replace aws_glue_access_int_with_token with your own external access integration
                 - replace aws_glue_creds_secret_token with your own secret token 
                 - replace us-west-2 with your own region

 Sample Call:
 -----------------------------------------------
select check_db_table_glue_catalog ('aj_test', 'simple_schema_smi2');

select athena_db_name, athena_table_name, check_db_table_glue_catalog_func(athena_db_name, athena_table_name)
from iceberg_table_list ;
----------------------------------------------- 
===============================================
 Change History
===============================================
 Date        | Author        | Description
-------------|---------------|------------------------------------------------------
2025-08-20   | J. Ma         | Initial creation   
===============================================
*/

CREATE OR REPLACE function check_db_table_glue_catalog_func (
    athena_database_name string, 
    athena_table_name string)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('boto3','botocore', 'snowflake-snowpark-python')
HANDLER = 'check_and_done'
EXTERNAL_ACCESS_INTEGRATIONS = (aws_glue_access_int_with_token)
SECRETS = ('cred'=aws_glue_creds_secret_token)
AS 
$$
import _snowflake 
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import logging
from datetime import datetime
import inspect
import traceback
import json

logger = logging.getLogger("python_logger")
# logging.basicConfig(level=logging.INFO) 

def log_with_context(level, message):
    frame = inspect.currentframe().f_back
    func_name = frame.f_code.co_name
    line_no = frame.f_lineno
    logger.log(level, f"{func_name}:{line_no} - {message}")

def log_exception(e, msg=None):
    frame = inspect.currentframe().f_back
    func_name = frame.f_code.co_name
    line_no = frame.f_lineno
    stack = traceback.format_exc()
    full_msg = f"{msg} | Exception: {e}" if msg else f"Exception: {e}"
    logger.error(f"{func_name}:{line_no} - {full_msg}\n{stack}")

log_with_context(logging.INFO, f"Logging from [{__name__}].")


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
        log_with_context(logging.INFO, f"Database '{athena_database_name}' exists.")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            log_with_context(logging.INFO, f"Database '{athena_database_name}' does not exist.")
            return False
        else:
            log_exception(e, f"Error checking if database '{athena_database_name}' exists.")
    except Exception as e:
        log_exception(e, "An unhandled exception occurred in check_database_exists.")
        raise Exception (f"An unhandled exception occurred in check_database_exists.  Error: {e}")

    
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
            log_exception(e, f"Sth wrong when checking if {table_name} exists in athena. ")
    except Exception as e:
        log_exception(e, "An unhandled exception occurred in check_table_exists.")
        raise Exception (f"An unhandled exception occurred in check_table_exists. Error: {e}")
        

def find_table_in_glue(table_name: str):
    found_in_databases = []
    messages = []
    
    # Get all Glue databases
    paginator = glue_client.get_paginator("get_databases")
    for page in paginator.paginate():
        for db in page["DatabaseList"]:
            db_name = db["Name"]
            try:
                # Try to get the table in this database
                glue_client.get_table(DatabaseName=db_name, Name=table_name)
                # log_with_context(logging.INFO, f" Found table '{table_name}' in database: {db_name}")
                found_in_databases.append(db_name)
            except ClientError as e:
                if e.response["Error"]["Code"] == "EntityNotFoundException":
                    # Table not found in this db
                    continue
                else:
                    raise 
    if found_in_databases:
        return True, found_in_databases
    else: 
        return False, []
    
def check_and_done(athena_database_name, athena_table_name):
    messages = []
    
    try:
        db_exists = check_database_exists (athena_database_name)

        table_exists_in_target_db = False
        if db_exists:
            table_exists_in_target_db = check_table_exists(athena_database_name, athena_table_name)    

        other_dbs_with_table = []
        if not table_exists_in_target_db:
            exists_elsewhere, other_dbs_with_table = find_table_in_glue(athena_table_name)
        
        if db_exists and table_exists_in_target_db:
            message = f"001: Table '{athena_table_name}' exists in database '{athena_database_name}'. "
        elif db_exists and not table_exists_in_target_db:
            if other_dbs_with_table:
                message = f"002: Table '{athena_table_name}' not found in '{athena_database_name}', but exists in other databases: {other_dbs_with_table}."
            else:
                message = f"003: Table '{athena_table_name}' not found in '{athena_database_name}' or any other database."
        else:
            if other_dbs_with_table:
                message = f"004: Database '{athena_database_name}' does not exist, but table '{athena_table_name}' exists in other databases: {other_dbs_with_table}."
            else:
                message = f"005: Database '{athena_database_name}' does not exist, and table '{athena_table_name}' is not found in any database."

        result = {
            "db_exists": db_exists,
            "table_in_target_db": table_exists_in_target_db,
            "other_dbs_with_table": other_dbs_with_table,
            "message": message
        }
        return json.dumps(result)
        
    except Exception as e:
        log_exception(e, "An unhandled exception occurred. ")
        raise Exception (f"006: ERROR: Failed to check table '{athena_table_name}'. Error: {e}")
        return json.dumps({
            "db_exists": False,
            "table_in_target_db": False,
            "other_dbs_with_table": [],
            "message": f"ERROR: Failed to process table '{athena_table_name}'. Error: {e}"
        })
$$
;