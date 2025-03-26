/* ------------------------------------------------------------------------------
   File Name: V1.0.0__tables.sql
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
           - LANDING_INTERNAL_STAGE
           - ERROR_INTERNAL_STAGE
           - ARCHIVE_INTERNAL_STAGE

       2. Pipes :
           - 

       3. Tables:
           - 
           -
        
        4. Streams :
           - 

        6. Views:
           - 

------------------------------------------------------------------------------- */

-------------------------------------------- Create stages ---------------------

create stage if not exists RAW_LAYER.ARCHIVE_INTERNAL_STAGE DIRECTORY = ( ENABLE = true );

create stage if not exists  RAW_LAYER.LANDING_INTERNAL_STAGE DIRECTORY = ( ENABLE = true );

create stage if not exists  RAW_LAYER.ERROR_INTERNAL_STAGE DIRECTORY = ( ENABLE = true );



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
  CREATE_DATE TIMESTAMP_LTZ COMMENT 'valued with Copy into command metadata',
  file_name VARCHAR COMMENT 'valued with Copy into command metadata',
  PRIMARY KEY(APUKCODE, Anabench2Code)
);

create or replace TABLE BRONZE_LAYER.PRC_BENCHMARK_LEVEL_BRZ (
	ANABENCH2CODE VARCHAR,
	ANABENCH2 VARCHAR,
	ANABENCH1CODE VARCHAR,
	ANABENCH1 VARCHAR,
	HOUSECODE VARCHAR,
	SYS_SOURCE_DATE VARCHAR,
	CREATE_DATE TIMESTAMP_LTZ COMMENT 'valued with Copy into command metadata',
	FILE_NAME VARCHAR COMMENT 'valued with Copy into command metadata'
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

create or replace TABLE BRONZE_LAYER.PRC_GENERIC_GEOGRAPHY_BRZ (
	AGUKCODE VARCHAR NOT NULL,
	SHORTNAME VARCHAR,
	AREANAME VARCHAR,
	DISTRIBUTIONCHANNEL VARCHAR,
	TOUCHPOINT VARCHAR,
	ISOCOUNTRYCODE VARCHAR,
	AREAKEY VARCHAR,
	ISACTIVE VARCHAR,
	LOGDESCRIPTION VARCHAR,
	CREATIONDATE VARCHAR,
	SYS_SOURCE_DATE VARCHAR,
	CREATE_DATE TIMESTAMP_LTZ(9),
	FILE_NAME VARCHAR,
    PRIMARY KEY(AGUKCODE)
);

create or replace TABLE BRONZE_LAYER.PRC_GENERIC_PRODUCT_BRZ (
	APUKCODE VARCHAR NOT NULL,
	APUKHOUSESWITCHCODE VARCHAR,
	APUKHOUSESWITCHDATE VARCHAR,
	SHORTNAME VARCHAR,
	ISINTERNAL VARCHAR,
	PRICINGREFCODE VARCHAR,
	ISINNOVATION VARCHAR,
	SIZE VARCHAR,
	UNIT VARCHAR,
	PANELCOMMERCIALLINEKEY VARCHAR,
	PANELSUBSEGMENTKEY VARCHAR,
	ISACTIVE VARCHAR,
	LOGDESCRIPTION VARCHAR,
	CREATIONDATE VARCHAR,
	SYS_SOURCE_DATE VARCHAR,
	CREATE_DATE TIMESTAMP_LTZ(9),
	FILE_NAME VARCHAR,
    PRIMARY KEY(APUKCODE)
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
  HouseKey VARCHAR not null,
  IDProduct VARCHAR not null,
  IDProductName VARCHAR,
  SYS_SOURCE_DATE VARCHAR,
  Size VARCHAR,
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


CREATE OR REPLACE TABLE BRONZE_LAYER.PRC_SYRUSMARKET_NON_ERP_PRICING_MARKET_BRZ
(
  HouseKey VARCHAR,
  SyrusMarketCode VARCHAR not null,
  PricingMarketCode VARCHAR not null,
  SYS_SOURCE_DATE VARCHAR,
  CREATE_DATE TIMESTAMP_LTZ COMMENT 'valued with Copy into command metadata',
  file_name VARCHAR COMMENT 'valued with Copy into command metadata',
  PRIMARY KEY(HouseKey, SyrusMarketCode, PricingMarketCode)
);

create or replace TABLE BRONZE_LAYER.PRC_PRICING_MARKET_BRZ (
    HouseKey VARCHAR,
    PricingMarketCode VARCHAR,
    PricingMarketName VARCHAR,
    PricingMarketRule VARCHAR,
    RegionCode VARCHAR,
    RegionName VARCHAR,
    CountryCode VARCHAR,
    CountryName VARCHAR,
    DefaultCurrency VARCHAR,
    VAT VARCHAR,
    NoticePeriod VARCHAR,
    IsSrpMarket VARCHAR,
    SellInParentMarket VARCHAR,
    SapCode VARCHAR,
    OldOrionCode VARCHAR,
    DistributionChannelCode VARCHAR,
    DistributionChannelName VARCHAR,
    SalesOrganisationCode VARCHAR,
    SalesOrganisationName VARCHAR,
    CustomerPriceGroupCode VARCHAR,
    CustomerPriceGroupName VARCHAR,
    CustomerCode VARCHAR,
    CustomerName VARCHAR,
    LastCampaignCode VARCHAR,
    LastMarketRational VARCHAR,
    LastEvolution VARCHAR,
    SYS_SOURCE_DATE VARCHAR,
    CREATE_DATE TIMESTAMP_LTZ, -- valued with copy into command metadata
    file_name VARCHAR, -- valued with copy into command metadata
	primary key (HouseKey , PricingMarketCode )
);


create or replace TABLE BRONZE_LAYER.PRC_RETAIL_PRICE_BRZ (
    PanelistSource	VARCHAR,
    HouseKey	VARCHAR,
    PriceCollectionLine	VARCHAR,
    IDProduct	VARCHAR,
    IDGeo	VARCHAR,
    APUKCode	VARCHAR,
    AGUKCode	VARCHAR,
    APUKShortName	VARCHAR,
    AGUKShortName	VARCHAR,
    CollectedDate	VARCHAR,
    LoadedDate	VARCHAR,
    PriceType	VARCHAR,
    Price	VARCHAR,
    Currency	VARCHAR,
    EAN	VARCHAR,
    Size	VARCHAR,
    Unit	VARCHAR,
    PictureUrl	VARCHAR,
    ProductPageURL	VARCHAR,
    SYS_SOURCE_DATE	VARCHAR,
    CREATE_DATE TIMESTAMP_LTZ, -- valued with copy into command metadata
    file_name VARCHAR, -- valued with copy into command metadata
	primary key (PanelistSource , HouseKey, PriceCollectionLine, CollectedDate )
);

CREATE or replace TABLE BRONZE_LAYER.PRC_HOUSE_PRICE_BRZ (
    HouseKey VARCHAR NOT NULL,
    CampaignCode VARCHAR NOT NULL,
    PricingMarketCode VARCHAR,
    PricingRefCode VARCHAR,
    SrpCurrency VARCHAR,
    SRP VARCHAR,
    SrpPositioningGroup VARCHAR,
    CollectedDate VARCHAR,
    BkdCalculationGroup VARCHAR,
    BkdRefSrpPricingMarket VARCHAR,
    EwspCurrency VARCHAR,
    EwspCoef VARCHAR,
    EWSP VARCHAR,
    GipCoef VARCHAR,
    GIP VARCHAR,
    GipStartValidityDate VARCHAR,
    GipEndValidityDate VARCHAR,
    GipEnipCurrency VARCHAR,
    MinWorldwideCostEUR VARCHAR,
    EnipDiscount VARCHAR,
    ENIP VARCHAR,
    SellInGrossQTY VARCHAR,
    SellInGrossAmountEUR VARCHAR,
    ItemStatus VARCHAR,
    ItemTypology VARCHAR,
    EOD VARCHAR,
    EOL VARCHAR,
    OCD VARCHAR,
    IsDiscontinued VARCHAR,
    FollowUpItem VARCHAR,
    PremiumStatus VARCHAR,
    IsPremiumMargin VARCHAR,
    IsSet VARCHAR,
    IsNewLaunch VARCHAR,
    IsSelectForSrp VARCHAR,
    IsSelectForBkd VARCHAR,
    MarketComp1 VARCHAR,
    MarketComp2 VARCHAR,
    MarketComp3 VARCHAR,
    MarketComp4 VARCHAR,
    MarketComp5 VARCHAR,
    MarketComp6 VARCHAR,
    MarketCompPrice1 VARCHAR,
    MarketCompPrice2 VARCHAR,
    MarketCompPrice3 VARCHAR,
    MarketCompPrice4 VARCHAR,
    MarketCompPrice5 VARCHAR,
    MarketCompPrice6 VARCHAR(16777216),
    SYS_SOURCE_DATE VARCHAR,
    CREATE_DATE TIMESTAMP_LTZ, -- valued with copy into command metadata
    file_name VARCHAR, -- valued with copy into command metadata
    PRIMARY KEY (HouseKey, CampaignCode)
);
 -------------------------------------------- Create SILVER layer Tables ---------------------
-----------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------

create or replace TABLE SILVER_LAYER.DIM_PRC_BENCHMARK_LEVEL_SLV (
	PRICINGBENCHMARKLEVELPRCINTKEY NUMBER NOT NULL,
	PRICINGBENCHMARKLEVELPRCKEY VARCHAR NOT NULL,
	ANABENCH2 VARCHAR,
	ANABENCH1 VARCHAR,
	HOUSECODE VARCHAR,
    SYS_SOURCE_DATE	TIMESTAMP_LTZ,
	SYS_DATE_CREATE TIMESTAMP_LTZ(9) NOT NULL,
	primary key (PRICINGBENCHMARKLEVELPRCINTKEY)
);

create or replace TABLE SILVER_LAYER.DIM_PRC_BENCHMARK_SLV (
	PRICINGBENCHMARKPRCINTKEY NUMBER NOT NULL,
	PRICINGBENCHMARKPRCKEY VARCHAR NOT NULL,
	APUKCODE VARCHAR NOT NULL,
	ANABENCH2 VARCHAR NOT NULL,
	SKUGROUP VARCHAR,
    SYS_SOURCE_DATE	TIMESTAMP_LTZ,
	SYS_DATE_CREATE TIMESTAMP_LTZ(9) NOT NULL,
	primary key (PRICINGBENCHMARKPRCINTKEY)
);
  
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
    SYS_SOURCE_DATE	TIMESTAMP_LTZ,
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
    SYS_SOURCE_DATE	TIMESTAMP_LTZ,
    SYS_DATE_CREATE	TIMESTAMP_LTZ NOT NULL,
    PRIMARY KEY (PricingCampaignMarketPrcIntKey)
);

CREATE OR REPLACE TABLE SILVER_LAYER.DIM_PRC_CUSTOMER_ERP_PRICING_MARKET_SLV(
    PricingCustomerErpPricingMarketPrcIntKey NUMBER	NOT NULL,
    PricingCustomerErpPricingMarketPrcKey VARCHAR NOT NULL,
    HouseKey VARCHAR NOT NULL,
    CustomerCode VARCHAR NOT NULL,
    PricingMarketCode VARCHAR NOT NULL,
    SYS_SOURCE_DATE	TIMESTAMP_LTZ,
    SYS_DATE_CREATE	TIMESTAMP_LTZ NOT NULL,
    PRIMARY KEY (PricingCustomerErpPricingMarketPrcIntKey)
);

create or replace TABLE SILVER_LAYER.DIM_PRC_GENERIC_GEOGRAPHY_SLV (
	PricingGenericGeographyPrcIntKey NUMBER NOT NULL,
	PricingGenericGeographyPrcKey VARCHAR NOT NULL,
	AgukCode VARCHAR NOT NULL,
	AgukShortName VARCHAR,
	DistributionChannel VARCHAR,
    Touchpoint VARCHAR,
    ISOCountryCode VARCHAR,
    AreaKey VARCHAR,
    AreaName VARCHAR,
    AgukCreationDate DATE,
    SYS_SOURCE_DATE	TIMESTAMP_LTZ,
	SYS_DATE_CREATE TIMESTAMP_LTZ(9) NOT NULL,
	primary key (PricingGenericGeographyPrcIntKey)
);

create or replace TABLE SILVER_LAYER.DIM_PRC_GENERIC_PRODUCT_SLV (
	PricingGenericProductPrcIntKey NUMBER NOT NULL,
	PricingGenericProductPrcKey VARCHAR NOT NULL,
	APUKCode VARCHAR NOT NULL,
	APUKHouseSwitchCode VARCHAR,
	APUKHouseSwitchDate DATE,
    ShortName VARCHAR,
    isInternal BOOLEAN,
    PricingRefCode VARCHAR,
    isInnovation BOOLEAN,
    Size VARCHAR,
    Unit VARCHAR,
    PanelCommercialLineKey VARCHAR,
    PanelSubSegmentKey VARCHAR,
    isActive BOOLEAN,
    LogDescription VARCHAR,
    CreationDate DATE,
    SYS_SOURCE_DATE	TIMESTAMP_LTZ,
	SYS_DATE_CREATE TIMESTAMP_LTZ(9) NOT NULL,
	primary key (PricingGenericProductPrcIntKey)
);

CREATE OR REPLACE TABLE SILVER_LAYER.DIM_PRC_PRODUCT_SLV (
  PricingProductPrcIntKey NUMBER not null,
  PricingProductPrcKey VARCHAR not null,
  APUKCode VARCHAR,
  HouseKey VARCHAR not null,
  IDProduct VARCHAR not null,
  IDProductName VARCHAR,
  Size NUMBER,
  Source VARCHAR,
  SYS_SOURCE_DATE	TIMESTAMP_LTZ,
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
    SYS_SOURCE_DATE	TIMESTAMP_LTZ,
    SYS_DATE_CREATE TIMESTAMP_LTZ, -- valued with copy into command metadata
   
	primary key (PricingGeographyPrcIntkey)
);



CREATE OR REPLACE TABLE SILVER_LAYER.DIM_PRC_SYRUSMARKET_NON_ERP_PRICING_MARKET_SLV(
    PricingSyrusMarketNonErpPricingMarketPrcIntKey NUMBER	NOT NULL,
    PricingSyrusMarketNonErpPricingMarketPrcKey VARCHAR NOT NULL,
    HouseKey VARCHAR NOT NULL,
    SyrusMarketCode VARCHAR NOT NULL,
    PricingMarketCode VARCHAR NOT NULL,
    SYS_SOURCE_DATE	TIMESTAMP_LTZ,
    SYS_DATE_CREATE	TIMESTAMP_LTZ NOT NULL,
    PRIMARY KEY (PricingSyrusMarketNonErpPricingMarketPrcIntKey)
);

create or replace TABLE SILVER_LAYER.DIM_PRC_PRICING_MARKET_SLV (
    PricingMarketPrcIntKey NUMERIC NOT NULL,
    PricingMarketPrcKey VARCHAR NOT NULL,
    HouseKey VARCHAR,
    PricingMarketCode VARCHAR,
    PricingMarketName VARCHAR,
    PricingMarketRule VARCHAR,
    RegionCode VARCHAR,
    RegionName VARCHAR,
    CountryCode VARCHAR,
    CountryName VARCHAR,
    DefaultCurrency VARCHAR,
    VAT VARCHAR,
    NoticePeriod VARCHAR,
    IsSrpMarket VARCHAR,
    SellInParentMarket VARCHAR,
    SapCode VARCHAR,
    OldOrionCode VARCHAR,
    DistributionChannelCode VARCHAR,
    DistributionChannelName VARCHAR,
    SalesOrganisationCode VARCHAR,
    SalesOrganisationName VARCHAR,
    CustomerPriceGroupCode VARCHAR,
    CustomerPriceGroupName VARCHAR,
    CustomerCode VARCHAR,
    CustomerName VARCHAR,
    LastCampaignCode VARCHAR,
    LastMarketRational VARCHAR,
    LastEvolution VARCHAR,
    SYS_SOURCE_DATE	TIMESTAMP_LTZ,
    SYS_DATE_CREATE TIMESTAMP_LTZ,
	primary key (PricingMarketPrcIntKey)
);


create or replace TABLE SILVER_LAYER.FACT_PRC_RETAIL_PRICE_SLV (
    PricingRetailPricePrcIntKey	NUMBER,
    PricingRetailPricePrcKey	VARCHAR,
    PanelistSource	VARCHAR,
    HouseKey	VARCHAR,
    PriceCollectionLine	VARCHAR,
    IDProduct	VARCHAR,
    IDGeo	VARCHAR,
    APUKCode	VARCHAR,
    AGUKCode	VARCHAR,
    APUKShortName	VARCHAR,
    AGUKShortName	VARCHAR,
    CollectedDate	DATE,
    LoadedDate	DATE,
    PriceType	VARCHAR,
    Price	NUMBER,
    Currency	VARCHAR,
    EAN	VARCHAR,
    Size	VARCHAR,
    Unit	VARCHAR,
    PictureUrl	VARCHAR,
    ProductPageURL	VARCHAR,
    SYS_SOURCE_DATE	TIMESTAMP_LTZ,
    SYS_DATE_CREATE	TIMESTAMP_LTZ,
    PRIMARY KEY (PricingRetailPricePrcIntKey)
);

CREATE or replace TABLE SILVER_LAYER.PRICING_FACT_HOUSE_PRICE_PRC (
    PricingHousePricePrcIntKey NUMBER NOT NULL PRIMARY KEY,
    PricingHousePricePrcKey VARCHAR,
    HouseKey VARCHAR NOT NULL,
    CampaignCode VARCHAR NOT NULL,
    PricingMarketCode VARCHAR NOT NULL,
    PricingRefCode VARCHAR NOT NULL,
    SrpCurrency VARCHAR,
    SRP NUMBER,
    SRPPositioningGroup VARCHAR,
    CollectedDate DATE,
    BkdCalculationGroup VARCHAR,
    BkdRefSrpPricingMarket VARCHAR,
    EwspCurrency VARCHAR,
    EwspCoef NUMBER,
    EWSP NUMBER,
    GipCoef NUMBER,
    GIP NUMBER,
    GIPStartValidityDate DATE,
    GIPEndValidityDate DATE,
    GipEnipCurrency VARCHAR,
    MinWorldwideCostEUR NUMBER,
    EnipDiscount NUMBER,
    ENIP NUMBER,
    SellInGrossQTY NUMBER,
    SellInGrossAmountEUR NUMBER,
    ItemStatus VARCHAR,
    ItemTypology VARCHAR,
    EOD DATE,
    EOL DATE,
    OCD DATE,
    IsDiscontinued BOOLEAN,
    FollowUpItem VARCHAR,
    PremiumStatus VARCHAR,
    IsPremiumMargin BOOLEAN,
    IsSet BOOLEAN,
    IsNewLaunch BOOLEAN,
    IsSelectForSrp BOOLEAN,
    IsSelectForBkd BOOLEAN,
    MarketComp1 VARCHAR,
    MarketComp2 VARCHAR,
    MarketComp3 VARCHAR,
    MarketComp4 VARCHAR,
    MarketComp5 VARCHAR,
    MarketComp6 VARCHAR,
    MarketCompPrice1 NUMERIC,
    MarketCompPrice2 NUMERIC,
    MarketCompPrice3 NUMERIC,
    MarketCompPrice4 NUMERIC,
    MarketCompPrice5 NUMERIC,
    MarketCompPrice6 NUMERIC,
    SYS_SOURCE_DATE TIMESTAMP_LTZ,
    SYS_DATE_CREATE TIMESTAMP_LTZ NOT NULL
);

 -------------------------------------------- Create GOLD layer Tables ---------------------
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
    SYS_SOURCE_DATE	TIMESTAMP_LTZ,
    SYS_DATE_CREATE	TIMESTAMP_LTZ,
    SYS_DATE_UPDATE	TIMESTAMP_LTZ NOT NULL
);


CREATE OR REPLACE TABLE GOLD_LAYER.DIM_PRC_CUSTOMER_ERP_PRICING_MARKET_GLD(
    PricingCustomerErpPricingMarketPrcIntKey NUMBER	NOT NULL PRIMARY KEY,
    HouseKey VARCHAR NOT NULL,
    CustomerCode VARCHAR NOT NULL,
    PricingMarketCode VARCHAR NOT NULL,
    SYS_SOURCE_DATE	TIMESTAMP_LTZ,
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




------------------------------------------------------------------------------- */

-------------------------------------------- Create STREAMS ---------------------

ALTER TABLE SILVER_LAYER.DIM_PRC_GENERIC_PRODUCT_SLV SET CHANGE_TRACKING = TRUE;
ALTER TABLE SILVER_LAYER.DIM_PRC_GEOGRAPHY_SLV SET CHANGE_TRACKING = TRUE;
ALTER TABLE SILVER_LAYER.DIM_PRC_GENERIC_GEOGRAPHY_SLV SET CHANGE_TRACKING = TRUE;
ALTER TABLE SILVER_LAYER.DIM_PRC_PRODUCT_SLV SET CHANGE_TRACKING = TRUE;

create or replace stream SILVER_LAYER.stream_DIM_PRC_GEOGRAPHY_SLV on table SILVER_LAYER.DIM_PRC_GEOGRAPHY_SLV;
create or replace stream SILVER_LAYER.stream_DIM_GENERIC_GEOGRAPHY_SLV on table SILVER_LAYER.DIM_PRC_GENERIC_GEOGRAPHY_SLV;
create or replace stream SILVER_LAYER.stream_DIM_PRC_GENERIC_PRODUCT_SLV on table SILVER_LAYER.DIM_PRC_GENERIC_PRODUCT_SLV;
create or replace stream SILVER_LAYER.stream_DIM_PRC_PRODUCT_SLV on table SILVER_LAYER.DIM_PRC_PRODUCT_SLV;
