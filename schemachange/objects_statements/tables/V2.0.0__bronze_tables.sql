/* ------------------------------------------------------------------------------
   File Name: 
       V2.0.0__bronze_tables.sql

   Authors:
       Matthieu NOIRET

   Description: 
       This SQL script sets up the bronze tables for the Snowflake Project. 
       The tables and stages are designed to ingest CSV-formatted and JSON files

   Schemas: 
       BRONZE_LAYER

   Objects Created:
       1. Tables:
           - PRC_BENCHMARK_BRZ
           - PRC_BENCHMARK_LEVEL_BRZ
           - PRC_CUSTOMER_ERP_PRICING_MARKET_BRZ
           - PRC_GENERIC_GEOGRAPHY_BRZ
           - PRC_GENERIC_PRODUCT_BRZ
           - PRC_CAMPAIGN_MARKET_BRZ
           - PRC_PRODUCT_BRZ
           - PRC_GEOGRAPHY_BRZ
           - PRC_CAMPAIGN_BRZ
           - PRC_SYRUSMARKET_NON_ERP_PRICING_MARKET_BRZ
           - PRC_PRICING_MARKET_BRZ
           - PRC_RETAIL_PRICE_BRZ
           - PRC_HOUSE_PRICE_BRZ
       
------------------------------------------------------------------------------- */

------------------------------ Create Bronze Tables -----------------------------

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

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

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

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

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

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

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

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

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

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

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

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

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

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

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

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

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

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

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

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

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

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

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

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

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