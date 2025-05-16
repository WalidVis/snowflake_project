/* ------------------------------------------------------------------------------
   File Name: 
       V1.0.0__raw_objects.sql

   Authors:
       Matthieu NOIRET

   Description: 
       This SQL script creates the required stages to ingest the data files coming
       from Azure blob storage.
       It also creates the files formats to ingest '.csv' and '.json' files, the
       monitoring table to keep track of ingestion results and the parameter table. 

   Schemas: 
       RAW_LAYER
       BRONZE_LAYER
       MONITORING_LAYER
       ORCHESTRATION_SCHEMA

   Objects Created:
       1. Stages:
           - ARCHIVE_INTERNAL_STAGE
           - LANDING_INTERNAL_STAGE
           - ERROR_INTERNAL_STAGE

       2. Files formats:
           - csv_file_format_comma
           - csv_file_format_semicolon
           - json_file_format

       3. Tables:
           - MONITORING_INGEST
           - TABLE_NAMES_MANPRM
           
------------------------------------------------------------------------------- */

/* WARNING : CREATE EXTERNAL STAGE IN RAW_LAYER : "EXTERNAL_AZUR_STAGE" 
USE Blob URL : azure://viseomdpdevsnowflakeproj.blob.core.windows.net/source-test/Files/
SAS Token -> See with Issam / Achref */

------------------------------ Create stages -----------------------------------

create stage if not exists RAW_LAYER.ARCHIVE_INTERNAL_STAGE DIRECTORY = ( ENABLE = true );

create stage if not exists  RAW_LAYER.LANDING_INTERNAL_STAGE DIRECTORY = ( ENABLE = true );

create stage if not exists  RAW_LAYER.ERROR_INTERNAL_STAGE DIRECTORY = ( ENABLE = true );

------------------------------ Create File formats -----------------------------

create or replace file format BRONZE_LAYER.csv_file_format_comma
    TYPE = CSV 
    PARSE_HEADER = TRUE -- Use first row for column names
    TRIM_SPACE = TRUE -- deletes spaces outter string 
    EMPTY_FIELD_AS_NULL = TRUE
    FIELD_DELIMITER = ',',
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE -- Because loading CSV with INCLUDE_METADATA
;

create or replace file format BRONZE_LAYER.csv_file_format_semicolon
    TYPE = CSV 
    PARSE_HEADER = TRUE -- Use first row for column names
    TRIM_SPACE = TRUE -- deletes spaces outter string 
    EMPTY_FIELD_AS_NULL = TRUE
    FIELD_DELIMITER = ';',
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE -- Because loading CSV with INCLUDE_METADATA
;

create or replace file format BRONZE_LAYER.json_file_format 
    TYPE = JSON 
    -- NULL_IF = ('\N', 'NULL', 'NUL', '') // To convert to SQL NULL 
    TRIM_SPACE = TRUE // deletes spaces outter white spaces 
;

--------------------------- Create Monitoring Table -----------------------------

CREATE OR REPLACE TABLE MONITORING_LAYER.MONITORING_INGEST
(
  file VARCHAR(200),
  src_table ARRAY,
  layer VARCHAR(20),
  status VARCHAR(50) not null,
  ingestion_time TIMESTAMP_LTZ,
  rows_parsed NUMBER(15,0), // A QUOI CORRESPOND RAW_PARSED
  rows_loaded NUMBER(15,0),
  errors_seen NUMBER(15,0),
  first_error VARCHAR(2000),
  first_error_line NUMBER(15,0),
  first_error_character_position  NUMBER(15,0),
  first_error_column_name VARCHAR(100)
);

--------------------------- Create Parameters Table -----------------------------

create or replace TABLE ORCHESTRATION_SCHEMA.TABLE_NAMES_MANPRM (
	TABLE_NAME VARCHAR,
	FILE_FORMAT VARCHAR,
	SILVER_TABLE_NAME VARCHAR,
	SILVER_TABLE_TECHNICAL_KEY_FIELDS VARCHAR COMMENT 'List each fields which make up the technical key in order separated by '',''',
	SILVER_PRIMARY_KEY VARCHAR
)COMMENT='Parameters table used to parse arguments in multiple notebooks and stored procedures in the notebook, the data used to fill this table can be found on the github repo under the \"data\" folder (TABLE_NAMES_MANPRM.csv).'
;