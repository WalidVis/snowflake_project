/* ------------------------------------------------------------------------------
   File Name: V1.1.1__initial_objects.sql
   Description: 
       This SQL script sets up the initial objects required Snowflake project, 
       including stages. 
       The tables and stages are designed to ingest CSV-formatted and JSON files

   Schemas: 
       RAW_LAYER
       BRONZE_LAYER
       SILVER_LAYER
       GOLD_LAYER

   Objects Created:
       1. Stages:
           - product_data
           - location_data
           - sales_data

       2. Pipes :
           - STG_LOCATION_PP / STG_PRODUCT_PP / STG_SALES_PP

       3. Tables:
           - STG_LOCATION_RAW / STG_PRODUCT_RAW / STG_SALES_RAW
           - STG_LOCATION / STG_PRODUCT / STG_SALES   (after cleansing)
        
        4. Streams :
           - STG_LOCATION_STRM / STG_PRODUCT_STRM / STG_SALES_STRM (Keeps track of changes in STG_X_RAW tables)

        5. Tasks:
           - STG_LOCATION_TSK / STG_PRODUCT_TSK / STG_SALES_TSK  (move data from tables STG_X_RAW to STG_X based on a stream)
        
        6. Views:
           - STG_LOCATION_VIEW / STG_PRODUCT_VIEW / STG_SALES_VIEW  (Reference streams present in the raw_dtv schema)

    Schema: 
       raw_dtv

   Objects Created:

       1. Tables:
           - HUB_LOCATION
           - HUB_PRODUCT
           - HUB_SALES
           - SAT_LOCATION
           - SAT_PRODUCT
           - SAT_SALES
           - LNK_PRODUCT_SALES
           - LNK_LOCATION_SALES

        2. Streams :
           - LOCATION_OUTBOUND_STRM / PRODUCT_OUTBOUND_STRM / SALES_OUTBOUND_STRM (Keeps track of changes in STG_X tables)
        
        3. Tasks :
           - LOCATION_STRM_TSK / PRODUCT_STRM_TSK / SALES_STRM_TSK ( Tasks to orchestrate movement from staging to raw_dtv , se basant sur les vues dans STAGING)



   Author:  Walid Cheriet  -- VISEO
------------------------------------------------------------------------------- */

-------------------------------------------- Create stages ---------------------
create stage if not exists RAW_LAYER.ARCHIVE_INTERNAL_STAGE;

create stage if not exists  RAW_LAYER.LANDING_INTERNAL_STAGE;

create stage if not exists  RAW_LAYER.ERROR_INTERNAL_STAGE;

create or replace stage RAW_LAYER.EXTERNAL_AZUR_STAGE
      URL = 'azure://viseomdpdevsnowflakeproj.blob.core.windows.net/source-test'
     CREDENTIALS = ( AZURE_SAS_TOKEN = 'sp=racwdlmeop&st=2025-02-25T08:40:42Z&se=2026-04-18T15:40:42Z&spr=https&sv=2022-11-02&sr=c&sig=5Q4x2IiL71%2FhP31RYDV4ryUcsy978h1qGPnwDBied3M%3D'  );



 -------------------------------------------- Create staging BRONZE layer Tables ---------------------

create or replace table BRONZE_LAYER.PRC_BENCHMARK_BRZ
(
  APUKCode VARCHAR(15) not null,
  Anabench2Code VARCHAR(10),
  Anabench2 VARCHAR(100),
  SKUGroup VARCHAR(10),
  SYS_SOURCE_DATE VARCHAR(50),
  CREATE_DATE TIMESTAMP_LTZ, -- valued with copy into command metadata
  file_name VARCHAR(200) -- valued with copy into command metadata
);


create or replace table BRONZE_LAYER.PRC_CUSTOMER_ERP_PRICING_MARKET_BRZ
(
  CustomerCode VARCHAR(30),
  HouseKey VARCHAR(5),
  PricingMarketCode VARCHAR(20),
  SYS_SOURCE_DATE VARCHAR(50),
  create_date TIMESTAMP_LTZ, -- valued with copy into command metadata
  file_name VARCHAR(200) -- valued with copy into command metadata
);


create or replace table BRONZE_LAYER.PRC_CAMPAIGN_MARKET_BRZ (
	PricingMarketCode VARCHAR(30),
	CampaignCode VARCHAR(10),
	HouseKey VARCHAR(20),
	BaseCampaignCode VARCHAR(20),
	RateType VARCHAR(20),
	RateDate VARCHAR,
	SYS_SOURCE_DATE VARCHAR,
    CREATE_DATE TIMESTAMP_LTZ, -- valued with copy into command metadata
    file_name VARCHAR(200) -- valued with copy into command metadata
);


 -------------------------------------------- Create Monitoring Tables ---------------------

create or replace table MONITORING_LAYER.MONITORING_INGEST
(
  file VARCHAR(200),
  src_table VARCHAR(100),
  layer VARCHAR(20),
  status VARCHAR(50) not null,
  ingestion_time TIMESTAMP_LTZ,
  rows_parsed NUMBER(15,0),
  rows_loaded NUMBER(15,0),
  errors_seen NUMBER(15,0),
  first_error VARCHAR(2000),
  first_error_line NUMBER(15,0),
  first_error_character_position  NUMBER(15,0),
  first_error_column_name VARCHAR(100)
);



 -------------------------------------------- Create File formats ---------------------

create or replace file format bronze_layer.csv_file_format 
    TYPE = CSV 
    PARSE_HEADER = TRUE -- Use first row for column names
    TRIM_SPACE = TRUE -- deletes spaces outter string 
    EMPTY_FIELD_AS_NULL = TRUE
    FIELD_DELIMITER = ';',
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE -- Because loading CSV with INCLUDE_METADATA
;

create or replace file format bronze_layer.json_file_format 
    TYPE = JSON 
    -- NULL_IF = ('\N', 'NULL', 'NUL', '') // To convert to SQL NULL 
    TRIM_SPACE = TRUE // deletes spaces outter white spaces 
;