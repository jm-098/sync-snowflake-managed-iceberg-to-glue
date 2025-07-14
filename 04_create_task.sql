/*
create a triggered task to update the glue metadata location for the table.
this is hardcoded to use the `simple_schema_smi` table for now on the snowflake side and `iceberg_table_from_boto2` on the glue side, but could/should be changed to run off of metadata for any table.
this should also do a DML statement so the stream is cleared after the task runs. potentially something like either a log table, or even just a dummy table doing something like:
insert into dummy_clear_stream
select 1 from simple_schema_smi2_stream where 1=0
*/

CREATE OR REPLACE TASK orchestrate_update_glue_metadata_location
    TARGET_COMPLETION_INTERVAL='1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('simple_schema_smi2_stream')
  AS
    select update_glue_metadata_location('iceberg', 'iceberg_table_from_boto2', CAST(GET(PARSE_JSON(SYSTEM$GET_ICEBERG_TABLE_INFORMATION('simple_schema_smi2')), 'metadataLocation') AS VARCHAR));