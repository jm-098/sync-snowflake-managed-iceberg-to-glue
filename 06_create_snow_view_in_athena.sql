/* =============================================
Procedure: sync_views_to_athena
Description:  This procedure creates views in athena from snowflake views.
              The procedure takes the DDL of a snowflake view and the athena database name as input. 
              It does not refactor the view defintion for athena, so the view definition must be compatible with athena.
              It assumes same view definition can be used in both snowflake and athena, with same base tables. The view should exist in the same database and schema as the base table in snowflake, and in the same database same in athena
              The view defintion in Snowflake shouldn't contain database and schema name for the view or the underlying tables.  
              For the view definition, it simply uses the output from get_ddl('view', 'my_snow_db.my_snow_schema.my_snow_view'), only to delete the column list in the view name.
              Ie: 
                  the result of get_ddl: 
                    CREATE OR REPLACE VIEW my_snow_db.my_snow_schema.my_snow_view (col1, col2) AS SELECT * FROM my_snow_db.my_snow_schema.my_snow_table;
                  the query sent to athena will be:
                    CREATE OR REPLACE VIEW my_snow_db.my_snow_schema.my_snow_view AS SELECT * FROM my_snow_db.my_snow_schema.my_snow_table;
              An example of analytical views: 
                  the result from get_ddl in snwoflake:
                    create or replace view JTMPV_2(
                                CUSTOMER_NUM,
                                CUSTOMER_NAME
                            ) as
                            WITH RankedData AS (
                            SELECT
                                myid,
                                col2,
                                ROW_NUMBER() OVER (PARTITION BY myid ORDER BY col2 DESC) AS rank_num
                            FROM iceberg_table_from_boto5
                            )
                            SELECT
                                myid AS customer_num,
                                col2 AS customer_name,
                                rank_num
                            FROM RankedData
                            WHERE rank_num = 1;
                  the view definition to be passed on to athena is:
                    create or replace view jtmpv_2
                        as
                        WITH RankedData AS (
                        SELECT
                            myid,
                            col2,
                            ROW_NUMBER() OVER (PARTITION BY myid ORDER BY col2 DESC) AS rank_num
                        FROM iceberg_table_from_boto5
                        )
                        SELECT
                            myid AS customer_id,
                            col2 AS customer_name,
                            rank_num
                        FROM RankedData
                        WHERE rank_num = 1;
              To create the procedure:
                 - replace aws_glue_access_int_with_token with your own external access integration
                 - replace aws_glue_creds_secret_token with your own secret token 
                 - replace us-west-2 with your own region
                 - the proc execution requires these permissions to athena and s3 in IAM role
                    athena: "athena:StartQueryExecution", "athena:GetQueryExecution","athena:GetQueryResults","athena:StopQueryExecution"
                    s3: "s3:PutObject", "s3:GetObject", "s3:GetObjectVersion", "s3:DeleteObject", "s3:DeleteObjectVersion"
                 - add these 2 in value list for the network rule for the external access integration:
                    'athena.us-west-2.amazonaws.com', 
                    'athena.us-west-2.api.aws'
                 - it assumes athena databbase already exists, if not, create it first.


 Sample Call:
 -----------------------------------------------
 -- Update glue-athena table with snowflake iceberg table data and structure change: 
 CALL sync_views_to_athena(
     get_ddl('table', 'my_snow_db.my_snow_schema.my_snow_iceberg_table'),
     'my_athena_database' , 
     'my_athena_output_location' 
 );
----------------------------------------------- 
===============================================
 Change History
===============================================
 Date        | Author        | Description
-------------|---------------|------------------------------------------------------
2025-08-07   | J. Ma         | Initial creation   
2025-08-08   | J. Ma         | Added parameter to pass in outlocation for athena query result
             |               | Droped unused glue_client
===============================================
*/

CREATE OR REPLACE PROCEDURE SYNC_VIEWS_TO_ATHENA(
    P_SNOWFLAKE_VIEW_DDL string,
    P_GLUE_DATABASE_NAME string, 
    P_ATHENA_OUTPUT_LOCATION string)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('boto3','botocore', 'snowflake-snowpark-python')
HANDLER = 'create_views'
EXTERNAL_ACCESS_INTEGRATIONS = (aws_glue_access_int)
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
import time


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


athena_client = boto3.client(
    service_name='athena',
    aws_access_key_id = cloud_provider_object.access_key_id,
    aws_secret_access_key = cloud_provider_object.secret_access_key,
    aws_session_token=cloud_provider_object.token,
    region_name='us-west-2'
)


def remove_view_column_list(ddl_string: str) -> str:
    pattern = r'(CREATE(?:\s+OR\s+REPLACE)?\s+VIEW\s+[\w.\"_]+)\s*\([^)]*\)\s*(AS\s+.*)'
    modified_ddl = re.sub(pattern, r'\1 \2', ddl_string, flags=re.DOTALL | re.IGNORECASE)
    
    return modified_ddl
    
def parse_create_view_statement(sql_statement: str) -> dict:
    """
    Parses a CREATE VIEW statement to extract view name and query text.
    Also extracts the columns from the DDL.
    """
    statement = sql_statement.strip().strip(';')
    
    # This regex extracts the view name and the SELECT statement.
    # It handles optional column list and quoted identifiers.
    match = re.search(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+([\w.\"]+)(?:\s*\(([\w\s,.\"_]+)\))?\s+AS\s+(.*)", 
        statement, 
        re.DOTALL | re.IGNORECASE
    )

    if not match:
        logger.exception(f"Invalid CREATE VIEW statement format: {statement}")
        raise ValueError(f"Invalid CREATE VIEW statement format: {statement}")

    view_name_part = match.group(1).strip()
    columns_string = match.group(2)
    query_text = remove_view_column_list(statement) # match.group(3).strip() # need delete column list 
    view_name = view_name_part.split('(')[0].strip().strip('"')

    columns = []
    if columns_string:
        # Extract columns from the DDL's column list
        column_names = [col.strip().strip('"') for col in columns_string.split(',')]
        for name in column_names:
            columns.append({'Name': name, 'Type': 'string'}) # Type is assumed for this example
    else:
        # Fallback if no column list is in the DDL
        logger.exception("Could not infer column names from DDL. Explicit column list is required for this procedure.")
        raise ValueError("Could not infer column names from DDL. Explicit column list is required for this procedure.")


    logger.info(f"the view name is {view_name}, the query_text is {query_text}, the columns are {columns}") 
    logger.debug(f"the query_text is {query_text}") 
    return {
        'view_name': view_name,
        'query_text': query_text,
        'columns': columns
    }
    
        
def create_views(snowflake_view_ddl, glue_database_name, athena_output_location):
   
    try:

        # Parse the input DDL to get the view name and query
        view_info = parse_create_view_statement (snowflake_view_ddl)
        glue_view_name = view_info['view_name']
        query_text = view_info['query_text']
        inferred_columns = view_info['columns']

        output_location = athena_output_location
 
        
        logger.debug(f"Parsed DDL. View Name: {glue_view_name}, Query Text: {query_text}. ")
        
        
        # Now, create the view. This call will be safe whether the view existed before or not.
        response = athena_client.start_query_execution(
        QueryString=query_text,
        QueryExecutionContext={'Database': glue_database_name},
        ResultConfiguration={'OutputLocation': output_location} 
        )

        query_execution_id = response['QueryExecutionId']
        status = 'RUNNING'
        while status in ['QUEUED', 'RUNNING']:
            status_response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = status_response['QueryExecution']['Status']['State']
            logger.info(f"Athena query status: {status}")
            if status in ['RUNNING', 'QUEUED']:
                time.sleep(5)
        if status == 'SUCCEEDED':
            logger.info(f"SUCCESS: Successfully created/replaced Athena view. Query ID: {query_execution_id}")
            return f"SUCCESS: Successfully created/replaced Athena view. Query ID: {query_execution_id}"
        else:
            error_message = status_response['QueryExecution']['Status']['StateChangeReason']
            logger.error(f"ERROR: Failed to sync view to Athena. Query execution id: {query_execution_id}, Error: {error_message}")
            raise Exception (f"ERROR: Failed to sync view to Athena.Query execution id: {query_execution_id}, Error: {error_message}")

   
    except Exception as e:
        logger.exception("An unhandled exception occurred.Failed to sync view to Glue.  Query execution id: {query_execution_id}. Error: {e}")
        raise Exception ( f"ERROR: Failed to sync view to Athena. Query execution id: {query_execution_id}, Error: {e}")
$$;