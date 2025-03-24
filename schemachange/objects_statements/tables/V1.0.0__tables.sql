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

        2. Streams :
           - LOCATION_OUTBOUND_STRM / PRODUCT_OUTBOUND_STRM / SALES_OUTBOUND_STRM (Keeps track of changes in STG_X tables)
        
        3. Tasks :
           - LOCATION_STRM_TSK / PRODUCT_STRM_TSK / SALES_STRM_TSK ( Tasks to orchestrate movement from staging to raw_dtv , se basant sur les vues dans STAGING)


------------------------------------------------------------------------------- */

-------------------------------------------- Create stages ---------------------
create stage if not exists RAW_LAYER.ARCHIVE_INTERNAL_STAGE DIRECTORY = ( ENABLE = true );

create stage if not exists  RAW_LAYER.LANDING_INTERNAL_STAGE DIRECTORY = ( ENABLE = true );

create stage if not exists  RAW_LAYER.ERROR_INTERNAL_STAGE DIRECTORY = ( ENABLE = true );

create or replace stage RAW_LAYER.EXTERNAL_AZUR_STAGE
      URL = 'azure://viseomdpdevsnowflakeproj.blob.core.windows.net/source-test'
     CREDENTIALS = ( AZURE_SAS_TOKEN = 'sp=racwdlmeop&st=2025-02-25T08:40:42Z&se=2026-04-18T15:40:42Z&spr=https&sv=2022-11-02&sr=c&sig=5Q4x2IiL71%2FhP31RYDV4ryUcsy978h1qGPnwDBied3M%3D'  )
    DIRECTORY = ( ENABLE = true );


 -------------------------------------------- Create staging BRONZE layer Tables ---------------------
-----------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE_LAYER.PRC_BENCHMARK_BRZ
(
  APUKCode VARCHAR not null,
  Anabench2Code VARCHAR not null,
  Anabench2 VARCHAR,
  SKUGroup VARCHAR,
  SYS_SOURCE_DATE VARCHAR,
  CREATE_DATE TIMESTAMP_LTZ, -- valued with copy into command metadata
  file_name VARCHAR -- valued with copy into command metadata
);


CREATE OR REPLACE TABLE BRONZE_LAYER.PRC_CUSTOMER_ERP_PRICING_MARKET_BRZ
(
  CustomerCode VARCHAR not null,
  HouseKey VARCHAR not null,
  PricingMarketCode VARCHAR not null,
  SYS_SOURCE_DATE VARCHAR,
  CREATE_DATE TIMESTAMP_LTZ COMMENT 'valued with Copy into command metadata',
  file_name VARCHAR COMMENT 'valued with Copy into command metadata',
  PRIMARY KEY(CustomerCode, HouseKey, PricingMarketCode)
);


CREATE OR REPLACE TABLE BRONZE_LAYER.PRC_CAMPAIGN_MARKET_BRZ (
	PricingMarketCode VARCHAR not null,
	CampaignCode VARCHAR not null,
	HouseKey VARCHAR not null,
	BaseCampaignCode VARCHAR,
	RateType VARCHAR,
	RateDate VARCHAR,
	SYS_SOURCE_DATE VARCHAR,
    CREATE_DATE TIMESTAMP_LTZ COMMENT 'valued with Copy into command metadata',
    file_name VARCHAR COMMENT 'valued with Copy into command metadata',
    PRIMARY KEY(PricingMarketCode, CampaignCode)
);


CREATE OR REPLACE TABLE BRONZE_LAYER.PRC_PRODUCT_BRZ (
  APUKCode VARCHAR,
  HouseKey NUMBER not null,
  IDProduct VARCHAR not null,
  IDProductName VARCHAR,
  SYS_SOURCE_DATE VARCHAR,
  Size NUMBER,
  Source VARCHAR,
  CREATE_DATE TIMESTAMP_LTZ COMMENT 'valued with Copy into command metadata',
  file_name VARCHAR COMMENT 'valued with Copy into command metadata',
  PRIMARY KEY(HouseKey, IDProduct)
);

create or replace TABLE BRONZE_LAYER.PRC_GEOGRAPHY_BRZ (
    IDGeo VARCHAR NOT NULL,
    IDGeoName VARCHAR,
    AGUKCode VARCHAR,
    Source VARCHAR,
    SYS_SOURCE_DATE VARCHAR,
    CREATE_DATE TIMESTAMP_LTZ, -- valued with copy into command metadata
    file_name VARCHAR, -- valued with copy into command metadata
	primary key (IDGeo)
);


create or replace TABLE BRONZE_LAYER.PRC_CAMPAIGN_BRZ (
	HOUSEKEY VARCHAR(16777216) NOT NULL,
	CAMPAIGNCODE VARCHAR(16777216) NOT NULL,
	CAMPAIGNNAME VARCHAR(16777216),
	CAMPAIGNDESCRIPTION VARCHAR(16777216),
	HISTORICALSELLINFIRSTMONTH VARCHAR(16777216),
	HISTORICALSELLINLASTMONTH VARCHAR(16777216),
	CAMPAIGNDATE VARCHAR(16777216),
    SYS_SOURCE_DATE VARCHAR(16777216),
    CREATE_DATE TIMESTAMP_LTZ, -- valued with copy into command metadata
    file_name VARCHAR(16777216), -- valued with copy into command metadata
	primary key (HOUSEKEY, CAMPAIGNCODE)
);


 -------------------------------------------- Create staging SILVER layer Tables ---------------------
-----------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------


create or replace TABLE SILVER_LAYER.DIM_PRC_CAMPAIGN_PRC_SLV (
	PricingCampaignPrcIntKey NUMBER NOT NULL PRIMARY KEY,
    PricingCampaignPrcKey VARCHAR(16777216) NOT NULL,
    HOUSEKEY VARCHAR(16777216) NOT NULL,
	CAMPAIGNCODE VARCHAR(16777216) NOT NULL,
	CAMPAIGNNAME VARCHAR(16777216),
	CAMPAIGNDESCRIPTION VARCHAR(16777216),
	HISTORICALSELLINFIRSTMONTH VARCHAR(16777216),
	HISTORICALSELLINLASTMONTH VARCHAR(16777216),
	CAMPAIGNDATE VARCHAR(16777216),
    SYS_DATE_CREATE	TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE SILVER_LAYER.DIM_PRC_CAMPAIGN_MARKET_SLV(
    PricingCampaignMarketPrcIntKey NUMBER NOT NULL,
    PricingCampaignMarketPrcKey	VARCHAR NOT NULL,
    HouseKey VARCHAR NOT NULL,
    CampaignCode VARCHAR NOT NULL,
    PricingMarketCode VARCHAR NOT NULL,
    RateType VARCHAR,
    RateDate DATE,
    BaseCampaignCode VARCHAR,
    SYS_DATE_CREATE	TIMESTAMP_LTZ NOT NULL,
    PRIMARY KEY (PricingCampaignMarketPrcIntKey)
);



CREATE OR REPLACE TABLE SILVER_LAYER.DIM_PRC_CUSTOMER_ERP_PRICING_MARKET_SLV(
    PricingCustomerErpPricingMarketPrcIntKey NUMBER	NOT NULL,
    PricingCustomerErpPricingMarketPrcKey VARCHAR NOT NULL,
    HouseKey VARCHAR NOT NULL,
    CustomerCode VARCHAR NOT NULL,
    PricingMarketCode VARCHAR NOT NULL,
    SYS_DATE_CREATE	TIMESTAMP_LTZ NOT NULL,
    PRIMARY KEY (PricingCustomerErpPricingMarketPrcIntKey)
);


CREATE OR REPLACE TABLE SILVER_LAYER.DIM_PRC_PRODUCT_SLV (
  PricingProductPrcIntKey NUMBER not null,
  PricingProductPrcKey VARCHAR not null,
  APUKCode VARCHAR,
  HouseKey NUMBER not null,
  IDProduct VARCHAR not null,
  IDProductName VARCHAR,
  SYS_SOURCE_DATE VARCHAR,
  Size NUMBER,
  Source VARCHAR,
  SYS_DATE_CREATE TIMESTAMP_LTZ not null,
  PRIMARY KEY (PricingProductPrcIntKey)
);

create or replace TABLE SILVER_LAYER.DIM_PRC_GEOGRAPHY_SLV (
    PricingGeographyPrcIntkey NUMERIC NOT NULL,
    PricingGeographyPrcKey VARCHAR NOT NULL,
    IDGeo VARCHAR NOT NULL,
    IDGeoName VARCHAR,
    AGUKCode VARCHAR,
    Source VARCHAR,
    SYS_DATE_CREATE TIMESTAMP_LTZ, -- valued with copy into command metadata
   
	primary key (PricingGeographyPrcIntkey)
);


 -------------------------------------------- Create staging GOLD layer Tables ---------------------
-----------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE GOLD_LAYER.DIM_PRC_CAMPAIGN_MARKET_GLD(
    PrcPcsCampaignMarketIntKey NUMBER NOT NULL PRIMARY KEY,
    HouseKey VARCHAR NOT NULL,
    CampaignCode VARCHAR NOT NULL,
    PricingMarketCode VARCHAR NOT NULL,
    RateType VARCHAR,
    RateDate DATE,
    BaseCampaignCode VARCHAR,
    SYS_DATE_CREATE	TIMESTAMP_LTZ,
    SYS_DATE_UPDATE	TIMESTAMP_LTZ NOT NULL
);


CREATE OR REPLACE TABLE GOLD_LAYER.DIM_PRC_CUSTOMER_ERP_PRICING_MARKET_GLD(
    PricingCustomerErpPricingMarketPrcIntKey NUMBER	NOT NULL PRIMARY KEY,
    HouseKey VARCHAR NOT NULL,
    CustomerCode VARCHAR NOT NULL,
    PricingMarketCode VARCHAR NOT NULL,
    SYS_DATE_CREATE	TIMESTAMP_LTZ NOT NULL,
    SYS_DATE_UPDATE	TIMESTAMP NOT NULL
);



 -------------------------------------------- Create Monitoring Tables ---------------------


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



 -------------------------------------------- Create File formats ---------------------

create or replace file format BRONZE_LAYER.csv_file_format 
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


