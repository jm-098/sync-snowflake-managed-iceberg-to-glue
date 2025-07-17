-- This script creates a sample driving table . 
-- The driving table will contain the necessary information to create streams and tasks dynamically.
-- It creates a stored procedure to create streams and task for each Iceberg table based on the driving table.
 
-- create the driving table
 create or replace table iceberg_table_list (id number, snow_table_name varchar, athena_db_name varchar, athena_table_name varchar, task_schedule_minutes number, updated_date datetime);
 -- populate the driving table with the list of iceberg tables and their corresponding Athena database and table names
 

-- create streams and tasks for each iceberg table that in driving table


CREATE OR REPLACE PROCEDURE CREATE_STREAMS_AND_TASKS_FOR_TABLES()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
 declare
    rs RESULTSET default (select snow_table_name, athena_db_name, athena_table_name, task_schedule_minutes from iceberg_table_list);
    vw1_cur CURSOR for rs;
    my_sql varchar;
    stream_name varchar;
    task_name varchar;
begin
    for vw1 in vw1_cur do
       stream_name := concat(vw1.snow_table_name, '_str');
       my_sql := 'CREATE OR REPLACE STREAM ' || :stream_name || ' ON TABLE ' || vw1.snow_table_name || ';';
       execute immediate :my_sql;
       
       task_name := concat(vw1.snow_table_name, '_task');
       my_sql :=  '
            CREATE OR REPLACE TASK ' || :task_name || '
            WAREHOUSE = XSMALL_WH  
            SCHEDULE = '''|| :task_schedule_min ||' MINUTE''
            WHEN SYSTEM$STREAM_HAS_DATA('''||:stream_name||''')
            AS
            BEGIN
                call cci_update_glue_metadata_location('''||vw1.athena_db_name||''', '''||vw1.athena_table_name||''',   
                CAST(GET(PARSE_JSON(SYSTEM$GET_ICEBERG_TABLE_INFORMATION('''||vw1.snow_table_name||''')), ''metadataLocation'') AS VARCHAR));
            END;
        ';
        
       execute immediate :my_sql;
    end for;
end;
$$;