/* ------------------------------------------------------------------------------
   File Name: 
       V3.0.0__silver_tables.sql

   Authors:
       Matthieu NOIRET

   Description: 
       This SQL script sets up the silver tables for the Snowflake Project. 

   Schemas: 
       SILVER_LAYER

   Objects Created:
       1. Tables:
           - DIM_PRC_BENCHMARK_LEVEL_SLV
           - DIM_PRC_BENCHMARK_SLV
           - DIM_PRC_CAMPAIGN_SLV
           - DIM_PRC_CAMPAIGN_MARKET_SLV
           - DIM_PRC_CUSTOMER_ERP_PRICING_MARKET_SLV
           - DIM_PRC_GENERIC_GEOGRAPHY_SLV
           - DIM_PRC_GENERIC_PRODUCT_SLV
           - DIM_PRC_PRODUCT_SLV
           - DIM_PRC_GEOGRAPHY_SLV
           - DIM_PRC_SYRUSMARKET_NON_ERP_PRICING_MARKET_SLV
           - DIM_PRC_PRICING_MARKET_SLV
           - FACT_PRC_RETAIL_PRICE_SLV
           - FACT_PRC_HOUSE_PRICE_SLV
           
------------------------------------------------------------------------------- */

------------------------------ Create SILVER Tables -----------------------------

create or replace TABLE SILVER_LAYER.DIM_PRC_BENCHMARK_LEVEL_SLV (
	PRICINGBENCHMARKLEVELPRCINTKEY NUMBER NOT NULL,
	PRICINGBENCHMARKLEVELPRCKEY VARCHAR NOT NULL,
    ANABENCH2CODE VARCHAR,
    ANABENCH1CODE VARCHAR,
	ANABENCH2 VARCHAR,
	ANABENCH1 VARCHAR,
	HOUSECODE VARCHAR,
    SYS_SOURCE_DATE	VARCHAR,
	SYS_DATE_CREATE TIMESTAMP_LTZ(9) NOT NULL,
	primary key (PRICINGBENCHMARKLEVELPRCINTKEY)
);

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace TABLE SILVER_LAYER.DIM_PRC_BENCHMARK_SLV (
	PRICINGBENCHMARKPRCINTKEY NUMBER NOT NULL,
	PRICINGBENCHMARKPRCKEY VARCHAR NOT NULL,
    ANABENCH2CODE VARCHAR,
	APUKCODE VARCHAR NOT NULL,
	ANABENCH2 VARCHAR NOT NULL,
	SKUGROUP VARCHAR,
    SYS_SOURCE_DATE	VARCHAR,
	SYS_DATE_CREATE TIMESTAMP_LTZ(9) NOT NULL,
	primary key (PRICINGBENCHMARKPRCINTKEY)
);

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------
  
create or replace TABLE SILVER_LAYER.DIM_PRC_CAMPAIGN_SLV (
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

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

CREATE OR REPLACE TABLE SILVER_LAYER.DIM_PRC_CAMPAIGN_MARKET_SLV(
    PricingCampaignMarketPrcIntKey NUMBER NOT NULL,
    PricingCampaignMarketPrcKey	VARCHAR NOT NULL,
    HouseKey VARCHAR NOT NULL,
    CampaignCode VARCHAR NOT NULL,
    PricingMarketCode VARCHAR NOT NULL,
    RateType VARCHAR,
    RateDate DATE,
    BaseCampaignCode VARCHAR,
    SYS_SOURCE_DATE	VARCHAR,
    SYS_DATE_CREATE	TIMESTAMP_LTZ NOT NULL,
    PRIMARY KEY (PricingCampaignMarketPrcIntKey)
);

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

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

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace TABLE SILVER_LAYER.DIM_PRC_GENERIC_GEOGRAPHY_SLV (
	PricingGenericGeographyPrcIntKey NUMBER NOT NULL,
	PricingGenericGeographyPrcKey VARCHAR NOT NULL,
	AgukCode VARCHAR NOT NULL,
	ShortName VARCHAR,
	DistributionChannel VARCHAR,
    Touchpoint VARCHAR,
    ISOCountryCode VARCHAR,
    AreaKey VARCHAR,
    AreaName VARCHAR,
	ISACTIVE VARCHAR,
	LOGDESCRIPTION VARCHAR,
    CreationDate DATE,
    SYS_SOURCE_DATE	VARCHAR,
	SYS_DATE_CREATE TIMESTAMP_LTZ(9) NOT NULL,
	primary key (PricingGenericGeographyPrcIntKey)
);

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

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

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

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

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

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

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

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

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

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

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

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

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

CREATE or replace TABLE SILVER_LAYER.FACT_PRC_HOUSE_PRICE_SLV (
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