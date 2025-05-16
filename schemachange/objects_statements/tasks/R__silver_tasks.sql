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
	   	   - PRICING_BRONZE_TO_SILVER_ROOT_TASK (root)
		   - PRICING_BRONZE_TO_SILVER_END_TASK (end)
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
		   - SLV_CLEAN_PRC_SYRUSMARKET_NON_ERP_PRICING_MARKET
		   - SLV_TRUNCATE_PRC_RETAIL_PRICE # besoin de séparer en deux tasks pour les tables de faits
           - SLV_CLEAN_PRC_RETAIL_PRICE /!\ ne fonctionne pas car la requête génère un produit cartésien... à reprendre quand on aura de bonnes specs
------------------------------------------------------------------------------- */

create or replace task ORCHESTRATION_SCHEMA.PRICING_BRONZE_TO_SILVER_ROOT_TASK
	warehouse={{ ENVIRONMENT }}_WH
	schedule='USING CRON 0 6 * * * UTC'
	as SELECT '1';

ALTER TASK ORCHESTRATION_SCHEMA.PRICING_BRONZE_TO_SILVER_ROOT_TASK SUSPEND;

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.PRICING_BRONZE_TO_SILVER_END_TASK
	warehouse={{ ENVIRONMENT }}_WH
	finalize=ORCHESTRATION_SCHEMA.PRICING_BRONZE_TO_SILVER_ROOT_TASK
	as SELECT '1';

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.SLV_CLEAN_PRC_BENCHMARK
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.PRICING_BRONZE_TO_SILVER_ROOT_TASK
	as CALL ORCHESTRATION_SCHEMA.BRONZE_TO_SILVER_CLEAN_PROC('PRC_BENCHMARK');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.SLV_CLEAN_PRC_BENCHMARK_LEVEL
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.PRICING_BRONZE_TO_SILVER_ROOT_TASK
	as CALL ORCHESTRATION_SCHEMA.BRONZE_TO_SILVER_CLEAN_PROC('PRC_BENCHMARK_LEVEL');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.SLV_CLEAN_PRC_CAMPAIGN
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.PRICING_BRONZE_TO_SILVER_ROOT_TASK
	as CALL ORCHESTRATION_SCHEMA.BRONZE_TO_SILVER_CLEAN_PROC('PRC_CAMPAIGN');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.SLV_CLEAN_PRC_CAMPAIGN_MARKET
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.PRICING_BRONZE_TO_SILVER_ROOT_TASK
	as CALL ORCHESTRATION_SCHEMA.BRONZE_TO_SILVER_CLEAN_PROC('PRC_CAMPAIGN_MARKET');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.SLV_CLEAN_PRC_CUSTOMER_ERP_PRICING_MARKET
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.PRICING_BRONZE_TO_SILVER_ROOT_TASK
	as CALL ORCHESTRATION_SCHEMA.BRONZE_TO_SILVER_CLEAN_PROC('PRC_CUSTOMER_ERP_PRICING_MARKET');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.SLV_CLEAN_PRC_GENERIC_GEOGRAPHY
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.PRICING_BRONZE_TO_SILVER_ROOT_TASK
	as CALL ORCHESTRATION_SCHEMA.BRONZE_TO_SILVER_CLEAN_PROC('PRC_GENERIC_GEOGRAPHY');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.SLV_CLEAN_PRC_GENERIC_PRODUCT
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.PRICING_BRONZE_TO_SILVER_ROOT_TASK
	as CALL ORCHESTRATION_SCHEMA.BRONZE_TO_SILVER_CLEAN_PROC('PRC_GENERIC_PRODUCT');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.SLV_CLEAN_PRC_GEOGRAPHY
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.PRICING_BRONZE_TO_SILVER_ROOT_TASK
	as CALL ORCHESTRATION_SCHEMA.BRONZE_TO_SILVER_CLEAN_PROC('PRC_GEOGRAPHY');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.SLV_CLEAN_PRC_HOUSE_PRICE
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.PRICING_BRONZE_TO_SILVER_ROOT_TASK
	as CALL ORCHESTRATION_SCHEMA.BRONZE_TO_SILVER_CLEAN_PROC('PRC_HOUSE_PRICE');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.SLV_CLEAN_PRC_PRICING_MARKET
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.PRICING_BRONZE_TO_SILVER_ROOT_TASK
	as CALL ORCHESTRATION_SCHEMA.BRONZE_TO_SILVER_CLEAN_PROC('PRC_PRICING_MARKET');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.SLV_CLEAN_PRC_PRODUCT
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.PRICING_BRONZE_TO_SILVER_ROOT_TASK
	as CALL ORCHESTRATION_SCHEMA.BRONZE_TO_SILVER_CLEAN_PROC('PRC_PRODUCT');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.SLV_CLEAN_PRC_SYRUSMARKET_NON_ERP_PRICING_MARKET
	warehouse={{ ENVIRONMENT }}_WH
	after ORCHESTRATION_SCHEMA.PRICING_BRONZE_TO_SILVER_ROOT_TASK
	as CALL ORCHESTRATION_SCHEMA.BRONZE_TO_SILVER_CLEAN_PROC('PRC_SYRUSMARKET_NON_ERP_PRICING_MARKET');

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

/* A GARDER COMMENTEE TANT QUE LA SPEC N'A PAS ETE REVUE, LA REQUETE SQL AVEC JOINTURE GENERE UN PRODUIT CARTESIEN

create or replace task TEST_POC_VISEO_DB.ORCHESTRATION_SCHEMA.SLV_TRUNCATE_PRC_RETAIL_PRICE
	warehouse=TEST_WH
	after TEST_POC_VISEO_DB.ORCHESTRATION_SCHEMA.SLV_CLEAN_PRC_GENERIC_GEOGRAPHY, TEST_POC_VISEO_DB.ORCHESTRATION_SCHEMA.SLV_CLEAN_PRC_GENERIC_PRODUCT, TEST_POC_VISEO_DB.ORCHESTRATION_SCHEMA.SLV_CLEAN_PRC_GEOGRAPHY, TEST_POC_VISEO_DB.ORCHESTRATION_SCHEMA.SLV_CLEAN_PRC_PRODUCT
	as TRUNCATE TABLE SILVER_LAYER.FACT_PRC_RETAIL_PRICE_SLV;

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task TEST_POC_VISEO_DB.ORCHESTRATION_SCHEMA.SLV_CLEAN_PRC_RETAIL_PRICE
	warehouse=TEST_WH
	after TEST_POC_VISEO_DB.ORCHESTRATION_SCHEMA.SLV_TRUNCATE_PRC_RETAIL_PRICE
	as INSERT INTO SILVER_LAYER.FACT_PRC_RETAIL_PRICE_SLV (
    select
    HASH(CONCAT(COALESCE(REPLACE(retail.PanelistSource, ' ', ''), 'N/A'), COALESCE(REPLACE(retail.HouseKey, ' ', ''), 'N/A'), COALESCE(REPLACE(retail.PriceCollectionLine, ' ', ''), 'N/A'), COALESCE(REPLACE(retail.APUKCode, ' ', ''), 'N/A'), COALESCE(REPLACE(retail.AGUKCode, ' ', ''), 'N/A'), COALESCE(REPLACE(retail.CollectedDate, ' ', ''), 'N/A'))) as PRICINGRETAILPRICEPRCINTKEY, 
    CONCAT(COALESCE(REPLACE(retail.PanelistSource, ' ', ''), 'N/A'), COALESCE(REPLACE(retail.HouseKey, ' ', ''), 'N/A'), COALESCE(REPLACE(retail.PriceCollectionLine, ' ', ''), 'N/A'), COALESCE(REPLACE(retail.APUKCode, ' ', ''), 'N/A'), COALESCE(REPLACE(retail.AGUKCode, ' ', ''), 'N/A'), COALESCE(REPLACE(retail.CollectedDate, ' ', ''), 'N/A')) as PRICINGRETAILPRICEPRCKEY,
    retail.PanelistSource,
    retail.HouseKey,
    retail.PriceCollectionLine,
    retail.IDProduct,
    retail.IDGeo,
    retail.APUKCode,
    retail.AGUKCode,
    retail.APUKShortName,
    retail.AGUKShortName,
    retail.CollectedDate,
    retail.LoadedDate,
    retail.PriceType,
    retail.Price,
    retail.Currency,
    retail.EAN,
    retail.Size,
    retail.Unit,
    retail.PictureUrl,
    retail.ProductPageURL,
    retail.SYS_SOURCE_DATE,
    CURRENT_TIMESTAMP() AS SYS_DATE_CREATE 
from 
    BRONZE_LAYER.PRC_RETAIL_PRICE_BRZ retail
LEFT JOIN 
    BRONZE_LAYER.PRC_GEOGRAPHY_BRZ GEO 
ON 
    retail.IDGeo=GEO.IDGeo
LEFT JOIN 
    BRONZE_LAYER.PRC_PRODUCT_BRZ PROD 
ON 
    retail.IDProduct=PROD.IDProduct 
    AND retail.HouseKey=PROD.HouseKey
LEFT JOIN 
    BRONZE_LAYER.PRC_GENERIC_PRODUCT_BRZ GP 
ON 
    PROD.APUKCode=GP.APUKCode 
LEFT JOIN 
    BRONZE_LAYER.PRC_GENERIC_GEOGRAPHY_BRZ GG 
ON 
    GEO.AGUKCode=GG.AGUKCode);*/