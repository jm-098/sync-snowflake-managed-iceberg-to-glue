
/* ==============================================
  File: 05_create_stream_task_by_driving_table.sql
  Description:  This script leverages a driving table to create streams and tasks for each iceberg table in Snowflake that needs to be synced over to Athena.
                It also resumes the tasks upon creation.  Commented this out if not desired.
                To process the tables in the list, their include_flags need to be set to 'Y'. 
                The include_flag is set to 'N' for each table after its stream and task are created 
                Streams and tasks are created in the same database and schema as the snowflake iceberg tables. 
                To create the procedure, do the following: 
                - If the driving table is named different, update the table name in the script accordingly.
                - It assumes the update proc, update_glue_metadata_location, is in the same db and schema as this proc. Adjust if necessary.
                - Replace update_glue_metadata_location with the your procedure name if it is named differently.
                - It is assumed that update_glue_metadata_location is in the same database and schema as the procedure as this proc.  Adjust if necessary.
                - Modify the script if to create stream and tasks in different database or schema than those for the iceberg tables.
                - streams are created with the name of the table with '_str' suffix, and tasks are created with the name of the table with '_task' suffix. 
                  Update the sufffix if needed.
                
Sample Call:
 -----------------------------------------------
call CREATE_STREAMS_AND_TASKS_FOR_TABLES('XSMALL_WH');
----------------------------------------------- 
 ===============================================
 Change History
===============================================
 Date        | Author        | Description
-------------|---------------|------------------------------------------------------
2025-07-10   | J. Ma         | Created
2025-07-25   | J. Ma         | Updated the call in task to update_glue_metadata_location to pass on get_ddl
2025-07-25   | J. Ma         | Updated the call in task to fully qualify all identifiers: streams, snowflake tables, tasks.  
2025-07-28   | J. Ma         | Updated the procedure to take warehouse name as a parameter for task creation. Added error handling and logging.
             |               | Update the procedure to generate tasks with fually qualified objects, and update include_flag to 'N'.
2025-08-05   | J. Ma         | Add my_db.my_schema to this procedure name, the driving table, and the update_glue_metadata_location procedure. Make it easier to change the database and schema names.             |               | 
===============================================
*/
 
-- create the driving table
  use database my_db;
  use schema my_schema;
  create or replace table iceberg_table_list (id number, snow_db_name varchar, 
  snow_schema_name varchar, snow_table_name varchar, athena_db_name varchar, 
  athena_table_name varchar, task_schedule_minutes number, updated_date datetime, included_flag varchar); 
 
-- insert some sample data into the driving table, ie:
 insert into iceberg_table_list values ( 1, 'iceberg_db', 'testsc',  'simple_schema_smi2', 'aj_test', 'iceberg_table_from_boto2', 1, current_date, 'Y');

-- create streams and tasks for each iceberg table that in driving table

CREATE OR REPLACE PROCEDURE my_db.my_schema.CREATE_STREAMS_AND_TASKS_FOR_TABLES(wh_name varchar)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
 declare
    rs RESULTSET default (select snow_db_name, snow_schema_name, snow_table_name, athena_db_name, athena_table_name, task_schedule_minutes 
                          from my_db.my_schema.iceberg_table_list where include_flag = 'Y');
    vw1_cur CURSOR for rs;
    my_sql varchar;
    stream_name varchar;
    task_name varchar ;
    lcnt number default 0;
    V_PROC_NAME VARCHAR(255) DEFAULT 'CREATE_STREAMS_AND_TASKS_FOR_TABLES';
begin
    for vw1 in vw1_cur do
       lcnt := lcnt + 1;
       stream_name := vw1.snow_db_name||'.'||vw1.snow_schema_name||'.'|| concat(vw1.snow_table_name, '_str');
    
       my_sql := 'CREATE OR REPLACE STREAM ' || :stream_name || ' ON TABLE ' || vw1.snow_db_name||'.'||vw1.snow_schema_name||'.'|| vw1.snow_table_name || ';';
       execute immediate :my_sql;
       
       task_name := vw1.snow_db_name||'.'||vw1.snow_schema_name||'.'||concat(vw1.snow_table_name, '_task');
       my_sql :=  '
            CREATE OR REPLACE TASK ' || :task_name || '
            WAREHOUSE = '|| :wh_name ||' 
            SCHEDULE = '''|| vw1.task_schedule_minutes ||' MINUTE''
            WHEN SYSTEM$STREAM_HAS_DATA('''||:stream_name||''')
            AS
            BEGIN
                call my_db.my_schema.update_glue_metadata_location('''||vw1.athena_db_name||''', 
                '''||vw1.athena_table_name||''',  
                get_ddl(''table'', '''||vw1.snow_db_name||'.'||vw1.snow_schema_name||'.'||vw1.snow_table_name||'''),
                CAST(GET(PARSE_JSON(SYSTEM$GET_ICEBERG_TABLE_INFORMATION('''||vw1.snow_db_name||'.'||vw1.snow_schema_name||'.'||vw1.snow_table_name||''')), ''metadataLocation'') AS VARCHAR),
                '''||:stream_name||''');
            END;
        ';
       execute immediate :my_sql;

       my_sql :=  ' alter task ' || :task_name || ' resume ';
       execute immediate :my_sql;

       my_sql := 'UPDATE my_db.my_schema.iceberg_table_list SET include_flag = ''N'' WHERE snow_db_name = '''||vw1.snow_db_name||''' AND snow_schema_name = '''||vw1.snow_schema_name||''' AND snow_table_name = '''||vw1.snow_table_name||''';';
       execute immediate :my_sql;
    end for;
    
    
    SYSTEM$LOG_INFO('PROCEDURE ' || :V_PROC_NAME || ' completed successfully. Total records processed: ' || :lcnt);
    return 'Success. Number of records processed: '||:lcnt;
EXCEPTION
    WHEN OTHER THEN
        SYSTEM$LOG_ERROR(
            'PROCEDURE ' || :V_PROC_NAME || ' failed. ' ||
            'Error Code: ' || SQLCODE || '. ' ||
            'Error Message: ' || SQLERRM 
        );

    RETURN 'Error recording log event: ' || SQLERRM;

end;
$$; 


/* ==============================================
  File: update_glue_metadata_location_4_list_tables.sql
  Description:  This script leverages a driving table to run update_glue_metadata_location for all the tables marked as 'Y'.
                The include_flag is set to 'N' for its metadata is sync.  This can be a different driving table than the one used for above proc.  
                To create the procedure, do the following: 
                - If the driving table is named different, update the table name in the script accordingly.
                - Change iceberg_db to your db name, testsc to your schema name. 
                - Replace update_glue_metadata_location with the your procedure name if it is named differently.
                - It is assumed that update_glue_metadata_location is in the same database and schema as the procedure as this proc.  Adjust if necessary.
                
Sample Call:
 -----------------------------------------------
call update_glue_metadata_location_4_list_tables();
----------------------------------------------- 
 ===============================================
 Change History
===============================================
 Date        | Author        | Description
-------------|---------------|------------------------------------------------------
2025-08-14   | J. Ma         | Created
===============================================
*/

CREATE OR REPLACE PROCEDURE iceberg_db.testsc.update_glue_metadata_location_4_list_tables()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
 declare
    rs RESULTSET default (select snow_db_name, snow_schema_name, 
                            snow_table_name, athena_db_name, athena_table_name, task_schedule_minutes 
                            from iceberg_db.testsc.iceberg_table_list where include_flag = 'Y');
    vw1_cur CURSOR for rs;
    my_sql varchar;
    stream_name varchar;
    task_name varchar ;
    lcnt number default 0;
    V_PROC_NAME VARCHAR(255) DEFAULT 'update_glue_metadata_location_4_list_tables';
begin
    for vw1 in vw1_cur do
       lcnt := lcnt + 1;

       my_sql :=  '
                call iceberg_db.testsc.update_glue_metadata_location('''||vw1.athena_db_name||''', 
                '''||vw1.athena_table_name||''',  
                get_ddl(''table'', '''||vw1.snow_db_name||'.'||vw1.snow_schema_name||'.'||vw1.snow_table_name||'''),                
                    CAST(GET(PARSE_JSON(SYSTEM$GET_ICEBERG_TABLE_INFORMATION('''||vw1.snow_db_name||'.'||vw1.snow_schema_name||'.'||vw1.snow_table_name||''')), ''metadataLocation'') AS VARCHAR),
                    null)
        ';

       begin 
         execute immediate :my_sql;
       exception
         when other then
            SYSTEM$LOG_ERROR('PROCEDURE ' || :V_PROC_NAME || ' failed for table '||vw1.snow_db_name||'.'||vw1.snow_schema_name||'.'||vw1.snow_table_name||'.  Error: ' || SQLERRM);
          raise;
       end;

        my_sql := 'UPDATE iceberg_db.testsc.iceberg_table_list SET include_flag = ''N'' WHERE snow_db_name = '''||vw1.snow_db_name||''' AND snow_schema_name = '''||vw1.snow_schema_name||''' AND snow_table_name = '''||vw1.snow_table_name||''';';
        execute immediate :my_sql;
       
    end for;
    
    SYSTEM$LOG_INFO('PROCEDURE ' || :V_PROC_NAME || ' completed successfully. Total records processed: ' || :lcnt);
    return 'Success. Number of records processed: '||:lcnt;
    
EXCEPTION
    WHEN OTHER THEN
        SYSTEM$LOG_ERROR(
            'PROCEDURE ' || :V_PROC_NAME || ' failed. ' ||
            'Error Code: ' || SQLCODE || '. ' ||
            'Error Message: ' || SQLERRM 
        );

    RETURN 'Error recording log event: ' || SQLERRM;

end;
$$; 