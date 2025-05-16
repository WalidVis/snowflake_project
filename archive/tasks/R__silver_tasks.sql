/* ------------------------------------------------------------------------------
   File Name: 
      R__silver_tasks.sql

   Authors:
       Matthieu NOIRET

   Description: 
       This SQL script creates the tasks that serves as data pipelines to ingest
	   data from the bronze to the silver layer.

   Schemas: 
       ORCHESTRATION_SCHEMA

   Objects Created:
       1. Tasks:
           - SLV_CLEAN_PRC_BENCHMARK
           - SLV_CLEAN_PRC_BENCHMARK_LEVEL
		   - SLV_CLEAN_PRC_CAMPAIGN
		   - SLV_CLEAN_PRC_CAMPAIGN_MARKET
		   - SLV_CLEAN_PRC_CUSTOMER_ERP_PRICING_MARKET
		   - SLV_CLEAN_PRC_GENERIC_GEOGRAPHY
		   - SLV_CLEAN_PRC_GENERIC_PRODUCT
		   - SLV_CLEAN_PRC_GEOGRAPHY
		   - SLV_CLEAN_PRC_HOUSE_PRICE
		   - SLV_CLEAN_PRC_PRICING_MARKET
		   - SLV_CLEAN_PRC_PRODUCT
		   - SLV_CLEAN_PRC_RETAIL_PRICE
		   - SLV_CLEAN_PRC_SYRUSMARKET_NON_ERP_PRICING_MARKET
           
------------------------------------------------------------------------------- */

create or replace task ORCHESTRATION_SCHEMA.SLV_CLEAN_PRC_BENCHMARK
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_BENCHMARK_CSV
	as CALL ORCHESTRATION_SCHEMA.BRONZE_TO_SILVER_CLEAN_PROC('PRC_BENCHMARK');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.SLV_CLEAN_PRC_BENCHMARK_LEVEL
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_BENCHMARK_LEVEL_CSV
	as CALL ORCHESTRATION_SCHEMA.BRONZE_TO_SILVER_CLEAN_PROC('PRC_BENCHMARK_LEVEL');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.SLV_CLEAN_PRC_CAMPAIGN
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_CAMPAIGN_CSV
	as CALL ORCHESTRATION_SCHEMA.BRONZE_TO_SILVER_CLEAN_PROC('PRC_CAMPAIGN');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.SLV_CLEAN_PRC_CAMPAIGN_MARKET
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_CAMPAIGN_MARKET_CSV
	as CALL ORCHESTRATION_SCHEMA.BRONZE_TO_SILVER_CLEAN_PROC('PRC_CAMPAIGN_MARKET');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.SLV_CLEAN_PRC_CUSTOMER_ERP_PRICING_MARKET
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_CUSTOMER_ERP_PRICING_MARKET_JSON
	as CALL ORCHESTRATION_SCHEMA.BRONZE_TO_SILVER_CLEAN_PROC('PRC_CUSTOMER_ERP_PRICING_MARKET');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.SLV_CLEAN_PRC_GENERIC_GEOGRAPHY
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_GENERIC_GEOGRAPHY_JSON
	as CALL ORCHESTRATION_SCHEMA.BRONZE_TO_SILVER_CLEAN_PROC('PRC_GENERIC_GEOGRAPHY');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.SLV_CLEAN_PRC_GENERIC_PRODUCT
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_GENERIC_PRODUCT_JSON
	as CALL ORCHESTRATION_SCHEMA.BRONZE_TO_SILVER_CLEAN_PROC('PRC_GENERIC_PRODUCT');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.SLV_CLEAN_PRC_GEOGRAPHY
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_GEOGRAPHY_JSON
	as CALL ORCHESTRATION_SCHEMA.BRONZE_TO_SILVER_CLEAN_PROC('PRC_GEOGRAPHY');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.SLV_CLEAN_PRC_HOUSE_PRICE
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_HOUSE_PRICE_JSON
	as CALL ORCHESTRATION_SCHEMA.BRONZE_TO_SILVER_CLEAN_PROC('PRC_HOUSE_PRICE');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.SLV_CLEAN_PRC_PRICING_MARKET
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_PRICING_MARKET_JSON
	as CALL ORCHESTRATION_SCHEMA.BRONZE_TO_SILVER_CLEAN_PROC('PRC_PRICING_MARKET');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.SLV_CLEAN_PRC_PRODUCT
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_PRODUCT_JSON
	as CALL ORCHESTRATION_SCHEMA.BRONZE_TO_SILVER_CLEAN_PROC('PRC_PRODUCT');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.SLV_CLEAN_PRC_RETAIL_PRICE
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_RETAIL_PRICE_JSON
	as CALL ORCHESTRATION_SCHEMA.BRONZE_TO_SILVER_CLEAN_PROC('PRC_RETAIL_PRICE');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.SLV_CLEAN_PRC_SYRUSMARKET_NON_ERP_PRICING_MARKET
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_SYRUSMARKET_NON_ERP_PRICING_MARKET_JSON
	as CALL ORCHESTRATION_SCHEMA.BRONZE_TO_SILVER_CLEAN_PROC('PRC_SYRUSMARKET_NON_ERP_PRICING_MARKET');
