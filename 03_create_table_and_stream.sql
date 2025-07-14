-- create a simple iceberg table for testing purposes
create iceberg table simple_schema_smi (
    col1 int,
    col2 string
);

-- create a stream on the table so the task can use it to only run when needed
CREATE or replace STREAM simple_schema_smi_stream ON TABLE simple_schema_smi;