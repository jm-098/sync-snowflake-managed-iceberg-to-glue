/* 
Here are two options to authenticate with AWS:
   Access tokens
   Application-based authentication using an application ID
Choose one of these methods based on your needs and security requirements.
 */
-------------------------------------------------------------------
-- OPTION 1: Create a secret and external access integration to allow access to AWS Glue
-- create a secret to store the aws access key and secret key
-- not the best way to do this, but it works for now
CREATE OR REPLACE SECRET aws_glue_creds_secret_key
  TYPE = password
  USERNAME = ''
  PASSWORD = '';


-- create network rule to allow access to glue, and athena
-- note this is hardcoded to us-west-2 for now, but can be changed to whatever region necessary
CREATE OR REPLACE NETWORK RULE aws_glue_access_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('glue.us-west-2.amazonaws.com', 
                'glue.us-west-2.api.aws',
                'athena.us-west-2.amazonaws.com',
                'athena.us-west-2.api.aws'); 


-- create external access integration to allow access to glue
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION aws_glue_access_int
  ALLOWED_NETWORK_RULES = (aws_glue_access_rule)
  ALLOWED_AUTHENTICATION_SECRETS =(aws_glue_creds_secret_key)
  ENABLED = true;


-------------------------------------------------------------------
-- OPTION 2: Create a security integration to allow access to AWS Glue using an AWS application ID
-- additional notes on usign aws application id access
-- create securty integration to allow access to aws application id


CREATE OR REPLACE SECURITY INTEGRATION aws_glue_security_integration
  TYPE = API_AUTHENTICATION
  AUTH_TYPE = AWS_IAM
  ENABLED = TRUE
  AWS_ROLE_ARN = 'xxx';

-- execute the following SQL statement to get the STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID values for the IAM user:
DESC SECURITY INTEGRATION aws_glue_security_integration;

-- using the STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID values, follow Step 5 in Option 1: Configuring a Snowflake storage integration to access Amazon S3 to grant the IAM user access to the Amazon S3 service.

-- create a token to use for authentication with the AWS S3 service
CREATE OR REPLACE SECRET aws_glue_creds_secret_token
  TYPE = CLOUD_PROVIDER_TOKEN
  API_AUTHENTICATION = aws_glue_security_integration
  ;

-- update the external access integration to use the new secret, or create a new one
ALTER EXTERNAL ACCESS INTEGRATION aws_glue_access_int
  SET ALLOWED_AUTHENTICATION_SECRETS = (aws_glue_creds_secret_token, aws_glue_creds_secret_key);


CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION aws_glue_access_int_with_token
  ALLOWED_NETWORK_RULES = (aws_glue_access_rule)
  ALLOWED_AUTHENTICATION_SECRETS =(aws_glue_creds_secret_token)
  ENABLED = true;