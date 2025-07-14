
CREATE OR REPLACE FUNCTION update_glue_metadata_location(database_name string, table_name string, new_metadata_location string)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
HANDLER = 'update_table'
EXTERNAL_ACCESS_INTEGRATIONS = (aws_glue_access_int)
PACKAGES = ('boto3','botocore')
SECRETS = ('cred' = aws_glue_creds_secret_key)
AS
$$

import _snowflake
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


config = Config(
    retries = dict(
        max_attempts = 3
    )
)


username_password_object = _snowflake.get_username_password('cred');

# init glue client with credentials from the secret passed to the function
# note this is hardcoded to us-west-2 for now, but could be changed to do a lookup of the region if needed
glue_client = boto3.client(
    service_name='glue',
    aws_access_key_id = username_password_object.username,
    aws_secret_access_key = username_password_object.password,
    region_name='us-west-2'
)


# gets the current table info object from glue
def get_table_details(database_name, table_name):
    response = glue_client.get_table(
        DatabaseName=database_name, 
        Name=table_name
    )

    return response


# updates the table's metadata location in glue
def update_table(database_name, table_name, new_metadata_location):

    # get current table metadata, so we can reuse most of the object info
    existing_table = get_table_details(database_name, table_name)

    # start building the update table request object, reformatting since the response for the current info is a bit different than needed for the update
    update_table_dict = {
        'DatabaseName': database_name,
        'TableInput': existing_table['Table']
    }

    # update metadata locations - store current as previous and set new location, using the parameter passed to the function
    # not clear to me why athena needs the previous location stored as a separate parameter, but including it since that's the default behavior when making a change from athena
    update_table_dict['TableInput']['Parameters']['previous_metadata_location'] = update_table_dict['TableInput']['Parameters']['metadata_location']
    update_table_dict['TableInput']['Parameters']['metadata_location'] = new_metadata_location

    # remove keys from the "get" operationthat can't be included in update
    keys_to_remove = ['DatabaseName', 'CreatedBy', 'IsRegisteredWithLakeFormation', 'CatalogId', 'VersionId', 'IsMultiDialectView', 'CreateTime', 'UpdateTime']
    
    for key in keys_to_remove:
        if key in update_table_dict['TableInput']:
            del update_table_dict['TableInput'][key]
    
    try:
        # use the glue client to update the table with the new metadata location
        response = glue_client.update_table(**update_table_dict)
        return 'successful update'
    except Exception as e:
        return 'failed to update: ' + str(e)

    

$$;