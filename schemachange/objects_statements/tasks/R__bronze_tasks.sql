/* ------------------------------------------------------------------------------
   File Name: 
      R__bronze_tasks.sql

   Authors:
       Matthieu NOIRET

   Description: 
       This SQL script creates the tasks that serves as data pipelines to ingest
	   data from the raw files to the bronze layer.

   Schemas: 
       ORCHESTRATION_SCHEMA

   Objects Created:
       1. Tasks:
           - PRICING_RAW_TO_BRONZE_ROOT_TASK (root)
		   - PRICING_RAW_TO_BRONZE_END_TASK (end)
           - BRZ_INGEST_PRC_BENCHMARK_CSV
           - BRZ_INGEST_PRC_BENCHMARK_LEVEL_CSV
		   - BRZ_INGEST_PRC_CAMPAIGN_CSV
		   - BRZ_INGEST_PRC_CAMPAIGN_MARKET_CSV
		   - BRZ_INGEST_PRC_CUSTOMER_ERP_PRICING_MARKET_JSON
		   - BRZ_INGEST_PRC_GENERIC_GEOGRAPHY_JSON
		   - BRZ_INGEST_PRC_GENERIC_PRODUCT_JSON
		   - BRZ_INGEST_PRC_GEOGRAPHY_JSON
		   - BRZ_INGEST_PRC_HOUSE_PRICE_JSON
		   - BRZ_INGEST_PRC_PRICING_MARKET_JSON
		   - BRZ_INGEST_PRC_PRODUCT_JSON
		   - BRZ_INGEST_PRC_RETAIL_PRICE_JSON
		   - BRZ_INGEST_PRC_SYRUSMARKET_NON_ERP_PRICING_MARKET_JSON
           
------------------------------------------------------------------------------- */

create or replace task ORCHESTRATION_SCHEMA.PRICING_RAW_TO_BRONZE_ROOT_TASK
	warehouse={{ ENVIRONMENT }}_WH
	schedule='USING CRON 0 5 * * * UTC'
	as EXECUTE NOTEBOOK ORCHESTRATION_SCHEMA.BATCH_LOAD_AZURE_TO_RAW();

ALTER TASK ORCHESTRATION_SCHEMA.PRICING_RAW_TO_BRONZE_ROOT_TASK SUSPEND;

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.PRICING_RAW_TO_BRONZE_END_TASK
	warehouse={{ ENVIRONMENT }}_WH
	finalize=ORCHESTRATION_SCHEMA.PRICING_RAW_TO_BRONZE_ROOT_TASK
	as SELECT '1';

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_BENCHMARK_CSV
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.PRICING_RAW_TO_BRONZE_ROOT_TASK
	as CALL ORCHESTRATION_SCHEMA.INGEST_FILE_PROC('PRC_BENCHMARK');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_BENCHMARK_LEVEL_CSV
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.PRICING_RAW_TO_BRONZE_ROOT_TASK
	as CALL ORCHESTRATION_SCHEMA.INGEST_FILE_PROC('PRC_BENCHMARK_LEVEL');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_CAMPAIGN_CSV
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.PRICING_RAW_TO_BRONZE_ROOT_TASK
	as CALL ORCHESTRATION_SCHEMA.INGEST_FILE_PROC('PRC_CAMPAIGN');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_CAMPAIGN_MARKET_CSV
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.PRICING_RAW_TO_BRONZE_ROOT_TASK
	as CALL ORCHESTRATION_SCHEMA.INGEST_FILE_PROC('PRC_CAMPAIGN_MARKET');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_CUSTOMER_ERP_PRICING_MARKET_JSON
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.PRICING_RAW_TO_BRONZE_ROOT_TASK
	as CALL ORCHESTRATION_SCHEMA.INGEST_FILE_PROC('PRC_CUSTOMER_ERP_PRICING_MARKET');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_GENERIC_GEOGRAPHY_JSON
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.PRICING_RAW_TO_BRONZE_ROOT_TASK
	as CALL ORCHESTRATION_SCHEMA.INGEST_FILE_PROC('PRC_GENERIC_GEOGRAPHY');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_GENERIC_PRODUCT_JSON
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.PRICING_RAW_TO_BRONZE_ROOT_TASK
	as CALL ORCHESTRATION_SCHEMA.INGEST_FILE_PROC('PRC_GENERIC_PRODUCT');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_GEOGRAPHY_JSON
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.PRICING_RAW_TO_BRONZE_ROOT_TASK
	as CALL ORCHESTRATION_SCHEMA.INGEST_FILE_PROC('PRC_GEOGRAPHY');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_HOUSE_PRICE_JSON
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.PRICING_RAW_TO_BRONZE_ROOT_TASK
	as CALL ORCHESTRATION_SCHEMA.INGEST_FILE_PROC('PRC_HOUSE_PRICE');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_PRICING_MARKET_JSON
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.PRICING_RAW_TO_BRONZE_ROOT_TASK
	as CALL ORCHESTRATION_SCHEMA.INGEST_FILE_PROC('PRC_PRICING_MARKET');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_PRODUCT_JSON
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.PRICING_RAW_TO_BRONZE_ROOT_TASK
	as CALL ORCHESTRATION_SCHEMA.INGEST_FILE_PROC('PRC_PRODUCT');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_RETAIL_PRICE_JSON
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.PRICING_RAW_TO_BRONZE_ROOT_TASK
	as CALL ORCHESTRATION_SCHEMA.INGEST_FILE_PROC('PRC_RETAIL_PRICE');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_SYRUSMARKET_NON_ERP_PRICING_MARKET_JSON
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.PRICING_RAW_TO_BRONZE_ROOT_TASK
	as CALL ORCHESTRATION_SCHEMA.INGEST_FILE_PROC('PRC_SYRUSMARKET_NON_ERP_PRICING_MARKET');