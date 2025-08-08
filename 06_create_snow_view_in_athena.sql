/* =============================================
Procedure: sync_views_to_athena
Description:  This procedure creates views in athena from snowflake views.
              The procedure takes the DDL of a snowflake view and the athena database name as input. 
              It does not refactor the view defintion for athena, so the view definition must be compatible with athena.
              For the view definition, it simply uses the output from get_ddl('view', 'my_snow_db.my_snow_schema.my_snow_view'), only to delete the column list in the view name.
              ie: 
                  the result of get_ddl: 
                    CREATE OR REPLACE VIEW my_snow_db.my_snow_schema.my_snow_view (col1, col2) AS SELECT * FROM my_snow_db.my_snow_schema.my_snow_table;
                  the query sent to athena will be:
                    CREATE OR REPLACE VIEW my_snow_db.my_snow_schema.my_snow_view AS SELECT * FROM my_snow_db.my_snow_schema.my_snow_table;
              to create the procedure:
                 - replace aws_glue_access_int_with_token with your own external access integration
                 - replace aws_glue_creds_secret_token with your own secret token 
                 - replace us-west-2 with your own region
                 - if applicable, add permissions to athena and s3 in IAM role
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
     'my_athena_database'  
 );
----------------------------------------------- 
===============================================
 Change History
===============================================
 Date        | Author        | Description
-------------|---------------|------------------------------------------------------
2025-08-07   | J. Ma         | Initial creation         
===============================================
*/

CREATE OR REPLACE PROCEDURE SYNC_VIEWS_TO_ATHENA(
    P_SNOWFLAKE_VIEW_DDL string,
    P_GLUE_DATABASE_NAME string)
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
        logger.excepption(f"Invalid CREATE VIEW statement format: {statement}")
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
    return {
        'view_name': view_name,
        'query_text': query_text,
        'columns': columns
    }
        
def create_views(snowflake_view_ddl, glue_database_name):
   
    try:

        # Parse the input DDL to get the view name and query
        logger.info(f"get here jie 1")
        view_info = parse_create_view_statement (snowflake_view_ddl)
        glue_view_name = view_info['view_name']
        query_text = view_info['query_text']
        inferred_columns = view_info['columns']

        output_location = 's3://jieuswest2/csv/ICEBERG_DB/TESTSC/'
 
        
        logger.info(f"Parsed DDL. View Name: {glue_view_name}, Query Text: {query_text[:50]}...")
        
        
        # Now, create the view. This call will be safe whether the view existed before or not.
        response = athena_client.start_query_execution(
        QueryString=query_text,
        QueryExecutionContext={'Database': glue_database_name},
        ResultConfiguration={'OutputLocation': output_location} 
        )

        query_execution_id = response['QueryExecutionId'] 
        logger.info(f"Create view '{glue_view_name}', with query_id: {query_execution_id},  Response: {response} ")
        return f"Create view '{glue_view_name}', with query_id: {query_execution_id},  Response: {response} "
    
    except Exception as e:
        logger.exception("An unhandled exception occurred.Failed to sync view to Glue. Error: {e}")
        return f"Failed to sync view to Glue. Error: {e}"
$$;
