-- create a secret to store the aws access key and secret key
-- not the best way to do this, but it works for now
CREATE OR REPLACE SECRET aws_glue_creds_secret_key
  TYPE = password
  USERNAME = ''
  PASSWORD = '';


-- create network rule to allow access to glue
-- note this is hardcoded to us-west-2 for now, but can be changed to whatever region necessary
CREATE OR REPLACE NETWORK RULE aws_glue_access_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('glue.us-west-2.amazonaws.com', 
                'glue.us-west-2.api.aws'); 


-- create external access integration to allow access to glue
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION aws_glue_access_int
  ALLOWED_NETWORK_RULES = (aws_glue_access_rule)
  ALLOWED_AUTHENTICATION_SECRETS =(aws_glue_creds_secret_key)
  ENABLED = true;