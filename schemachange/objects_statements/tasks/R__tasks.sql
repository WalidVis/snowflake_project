-- !!!!!!!EXECUTE IMMEDIATE IS MANDATORY TO AVOID <EOF> error during Schemachange execution of the CREATE TASK !!!!!!!
---------------------------------------------------------------------------------------------------------------------
---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

-- BATCH PIPELINE TASKS




---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------


create or replace task BRONZE_LAYER.ingest_prc_benchmark_csv
	warehouse={{ ENVIRONMENT}}_WH
	schedule='USING CRON 0 5 * * * Europe/Paris'
	config='{"params":"''{ \\"src_schema\\" : \\"raw_layer\\", \\"external_stage_root_path\\": \\"@RAW_LAYER.EXTERNAL_AZUR_STAGE/Files\\", \\"stage_name\\": \\"@raw_layer.landing_internal_stage\\",  \\"stage_path_suffix\\" :\\"/PRC_BENCHMARK/\\", \\"pattern_file_name\\": \\".*.csv\\",  \\"on_error\\": \\"CONTINUE\\", \\"file_format\\" : \\"bronze_layer.csv_file_format\\",  \\"bronze_table\\": \\"bronze_layer.PRC_BENCHMARK_BRZ\\",  \\"silver_table\\" :\\"silver_layer.DIM_PRC_BENCHMARK_SLV\\", \\"silver_technicalKey_name\\" : \\"PricingBenchmarkPrcIntKey\\", \\"silver_functionalKey_name\\" : \\"PRICINGBENCHMARKPRCKEY\\", \\"silver_ruleTechnicalKey\\": \\"HASH(CONCAT(COALESCE(REPLACE(APUKCODE, ''\\\\''\\\\ \\\\'''', ''\\\\''\\\\''''), ''\\\\''N/A\\\\''''), ''\\\\''_\\\\'''', COALESCE(REPLACE(ANABENCH2, ''\\\\''\\\\ \\\\'''', ''\\\\''\\\\''''), ''\\\\''N/A\\\\'''')))\\", \\"silver_ruleFunctionalKey\\" : \\"CONCAT(COALESCE(REPLACE(APUKCODE, ''\\\\''\\\\ \\\\'''', ''\\\\''\\\\''''), ''\\\\''N/A\\\\''''), ''\\\\''_\\\\'''', COALESCE(REPLACE(ANABENCH2, ''\\\\''\\\\ \\\\'''', ''\\\\''\\\\''''), ''\\\\''N/A\\\\''''))\\"}''"}'
	as EXECUTE IMMEDIATE $$ BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "BRONZE_LAYER"."INGEST_RAW_FILES_INTO_BRONZE_LAYER"(:PARAMS); END;$$;

ALTER TASK BRONZE_LAYER.ingest_prc_benchmark_csv SUSPEND;

create or replace task BRONZE_LAYER.ingest_prc_benchmark_silver
	warehouse={{ ENVIRONMENT}}_WH
	after BRONZE_LAYER.ingest_prc_benchmark_csv
	as EXECUTE IMMEDIATE $$ 
	BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "SILVER_LAYER"."INGEST_INTO_SILVER_LAYER"(:PARAMS); END;$$;

ALTER TASK BRONZE_LAYER.ingest_prc_benchmark_silver RESUME;
ALTER TASK BRONZE_LAYER.ingest_prc_benchmark_csv RESUME;



---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------


create or replace task BRONZE_LAYER.ingest_prc_benchmark_level_csv
	warehouse={{ ENVIRONMENT}}_WH
	schedule='USING CRON 0 5 * * * Europe/Paris'
	config='{"params":"''{ \\"src_schema\\" : \\"raw_layer\\", \\"external_stage_root_path\\": \\"@RAW_LAYER.EXTERNAL_AZUR_STAGE/Files\\", \\"stage_name\\": \\"@raw_layer.landing_internal_stage\\",  \\"stage_path_suffix\\" :\\"/PRC_BENCHMARK_LEVEL/\\", \\"pattern_file_name\\": \\".*.csv\\",  \\"on_error\\": \\"CONTINUE\\", \\"file_format\\" : \\"bronze_layer.csv_file_format\\",  \\"bronze_table\\": \\"bronze_layer.PRC_BENCHMARK_LEVEL_BRZ\\",  \\"silver_table\\" :\\"silver_layer.DIM_PRC_BENCHMARK_LEVEL_SLV\\", \\"silver_technicalKey_name\\" : \\"PricingBenchmarkLevelPrcIntKey\\", \\"silver_functionalKey_name\\" : \\"PricingBenchmarkLevelPrcKey\\", \\"silver_ruleTechnicalKey\\": \\"HASH(COALESCE(REPLACE(ANABENCH2, ''\\\\''\\\\ \\\\'''', ''\\\\''\\\\''''), ''\\\\''N/A\\\\''''))\\", \\"silver_ruleFunctionalKey\\" : \\"COALESCE(REPLACE(ANABENCH2, ''\\\\''\\\\ \\\\'''', ''\\\\''\\\\''''), ''\\\\''N/A\\\\'''')\\"}''"}'
	as EXECUTE IMMEDIATE $$ 
	 BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "BRONZE_LAYER"."INGEST_RAW_FILES_INTO_BRONZE_LAYER"(:PARAMS); END;$$;

ALTER TASK BRONZE_LAYER.ingest_prc_benchmark_level_csv SUSPEND;

create or replace task BRONZE_LAYER.ingest_prc_benchmark_level_silver
	warehouse={{ ENVIRONMENT}}_WH
	after BRONZE_LAYER.ingest_prc_benchmark_level_csv
	as EXECUTE IMMEDIATE $$ 
	BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "SILVER_LAYER"."INGEST_INTO_SILVER_LAYER"(:PARAMS); END;$$;

ALTER TASK BRONZE_LAYER.ingest_prc_benchmark_level_silver RESUME;
ALTER TASK BRONZE_LAYER.ingest_prc_benchmark_level_csv RESUME;

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------


create or replace task BRONZE_LAYER.ingest_prc_campaign_market_csv
	warehouse={{ ENVIRONMENT}}_WH
	schedule='USING CRON 0 5 * * * Europe/Paris'
	config='{"params":"''{ \\"src_schema\\" : \\"raw_layer\\", \\"external_stage_root_path\\": \\"@RAW_LAYER.EXTERNAL_AZUR_STAGE/Files\\", \\"stage_name\\": \\"@raw_layer.landing_internal_stage\\",  \\"stage_path_suffix\\" :\\"/PRC_CAMPAIGN_MARKET/\\", \\"pattern_file_name\\": \\".*.csv\\",  \\"on_error\\": \\"CONTINUE\\", \\"file_format\\" : \\"bronze_layer.csv_file_format\\",  \\"bronze_table\\": \\"bronze_layer.PRC_CAMPAIGN_MARKET_BRZ\\",  \\"silver_table\\" :\\"silver_layer.DIM_PRC_CAMPAIGN_MARKET_SLV\\", \\"silver_technicalKey_name\\" : \\"PricingCampaignMarketPrcIntKey\\", \\"silver_functionalKey_name\\" : \\"PricingCampaignMarketPrcKey\\", \\"silver_ruleTechnicalKey\\": \\"HASH(CONCAT(COALESCE(REPLACE(PricingMarketCode, ''\\\\''\\\\ \\\\'''', ''\\\\''\\\\''''), ''\\\\''N/A\\\\''''), ''\\\\''_\\\\'''', COALESCE(REPLACE(CampaignCode, ''\\\\''\\\\ \\\\'''', ''\\\\''\\\\''''), ''\\\\''N/A\\\\'''')))\\", \\"silver_ruleFunctionalKey\\" : \\"CONCAT(COALESCE(REPLACE(PricingMarketCode, ''\\\\''\\\\ \\\\'''', ''\\\\''\\\\''''), ''\\\\''N/A\\\\''''), ''\\\\''_\\\\'''', COALESCE(REPLACE(CampaignCode, ''\\\\''\\\\ \\\\'''', ''\\\\''\\\\''''), ''\\\\''N/A\\\\''''))\\"}''"}'
	as EXECUTE IMMEDIATE $$ BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "BRONZE_LAYER"."INGEST_RAW_FILES_INTO_BRONZE_LAYER"(:PARAMS); END;$$;

ALTER TASK BRONZE_LAYER.ingest_prc_campaign_market_csv SUSPEND;

create or replace task BRONZE_LAYER.ingest_prc_campaign_market_silver
	warehouse={{ ENVIRONMENT}}_WH
	after BRONZE_LAYER.ingest_prc_campaign_market_csv
	as EXECUTE IMMEDIATE $$ 
	BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "SILVER_LAYER"."INGEST_INTO_SILVER_LAYER"(:PARAMS); END;$$;

ALTER TASK BRONZE_LAYER.ingest_prc_campaign_market_silver RESUME;
ALTER TASK BRONZE_LAYER.ingest_prc_campaign_market_csv RESUME;


---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------


create or replace task BRONZE_LAYER.ingest_prc_geography_json
	warehouse={{ ENVIRONMENT}}_WH
	schedule='USING CRON 0 5 * * * Europe/Paris'
	config='{"params":"''{ \\"src_schema\\" : \\"raw_layer\\", \\"external_stage_root_path\\": \\"@RAW_LAYER.EXTERNAL_AZUR_STAGE/Files\\", \\"stage_name\\": \\"@raw_layer.landing_internal_stage\\",  \\"stage_path_suffix\\" :\\"/PRC_GEOGRAPHY/\\", \\"pattern_file_name\\": \\".*.json\\",  \\"on_error\\": \\"CONTINUE\\", \\"file_format\\" : \\"bronze_layer.json_file_format\\",  \\"bronze_table\\": \\"bronze_layer.PRC_GEOGRAPHY_BRZ\\",  \\"silver_table\\" :\\"silver_layer.DIM_PRC_GEOGRAPHY_SLV\\", \\"silver_technicalKey_name\\" : \\"PricingGeographyPrcIntkey\\", \\"silver_functionalKey_name\\" : \\"PricingGeographyPrcKey\\", \\"silver_ruleTechnicalKey\\": \\"HASH(COALESCE(REPLACE(IDGeo, ''\\\\''\\\\ \\\\'''', ''\\\\''\\\\''''), ''\\\\''N/A\\\\''''))\\", \\"silver_ruleFunctionalKey\\" : \\"COALESCE(REPLACE(IDGeo, ''\\\\''\\\\ \\\\'''', ''\\\\''\\\\''''), ''\\\\''N/A\\\\'''')\\"}''"}'
	as EXECUTE IMMEDIATE $$ BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "BRONZE_LAYER"."INGEST_RAW_FILES_INTO_BRONZE_LAYER"(:PARAMS); END;$$;

ALTER TASK BRONZE_LAYER.ingest_prc_campaign_market_csv SUSPEND;

create or replace task BRONZE_LAYER.ingest_prc_geography_silver
	warehouse={{ ENVIRONMENT}}_WH
	after BRONZE_LAYER.ingest_prc_geography_json
	as EXECUTE IMMEDIATE $$ 
	BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "SILVER_LAYER"."INGEST_INTO_SILVER_LAYER"(:PARAMS); END;$$;

ALTER TASK BRONZE_LAYER.ingest_prc_geography_silver RESUME;
ALTER TASK BRONZE_LAYER.ingest_prc_geography_json RESUME;


---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task BRONZE_LAYER.ingest_prc_customer_erp_pricing_market_json
	warehouse={{ ENVIRONMENT}}_WH
	schedule='USING CRON 0 5 * * * Europe/Paris'
	config='{"params":"''{ \\"src_schema\\" : \\"raw_layer\\", \\"external_stage_root_path\\": \\"@RAW_LAYER.EXTERNAL_AZUR_STAGE/Files\\", \\"stage_name\\": \\"@raw_layer.landing_internal_stage\\",  \\"stage_path_suffix\\" :\\"/PRC_CUSTOMER_ERP_PRICING_MARKET/\\", \\"pattern_file_name\\": \\".*.json\\",  \\"on_error\\": \\"CONTINUE\\", \\"file_format\\" : \\"bronze_layer.json_file_format\\",  \\"bronze_table\\": \\"bronze_layer.PRC_CUSTOMER_ERP_PRICING_MARKET_BRZ\\",  \\"silver_table\\" :\\"silver_layer.DIM_PRC_CUSTOMER_ERP_PRICING_MARKET_SLV\\", \\"silver_technicalKey_name\\" : \\"PricingCustomerErpPricingMarketPrcIntKey\\", \\"silver_functionalKey_name\\" : \\"PricingCustomerErpPricingMarketPrcKey\\", \\"silver_ruleTechnicalKey\\": \\"HASH(CONCAT(COALESCE(REPLACE(PricingMarketCode, ''\\\\''\\\\ \\\\'''', ''\\\\''\\\\''''), ''\\\\''N/A\\\\''''), ''\\\\''_\\\\'''', COALESCE(REPLACE(CustomerCode, ''\\\\''\\\\ \\\\'''', ''\\\\''\\\\''''), ''\\\\''N/A\\\\'''')))\\", \\"silver_ruleFunctionalKey\\" : \\"CONCAT(COALESCE(REPLACE(PricingMarketCode, ''\\\\''\\\\ \\\\'''', ''\\\\''\\\\''''), ''\\\\''N/A\\\\''''), ''\\\\''_\\\\'''', COALESCE(REPLACE(CustomerCode, ''\\\\''\\\\ \\\\'''', ''\\\\''\\\\''''), ''\\\\''N/A\\\\''''))\\"}''"}'
	as EXECUTE IMMEDIATE $$ BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "BRONZE_LAYER"."INGEST_RAW_FILES_INTO_BRONZE_LAYER"(:PARAMS); END;$$;

ALTER TASK BRONZE_LAYER.ingest_prc_customer_erp_pricing_market_json SUSPEND;

create or replace task BRONZE_LAYER.ingest_prc_customer_erp_pricing_market_silver
	warehouse={{ ENVIRONMENT}}_WH
	after BRONZE_LAYER.ingest_prc_customer_erp_pricing_market_json
	as EXECUTE IMMEDIATE $$ 
	BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "SILVER_LAYER"."INGEST_INTO_SILVER_LAYER"(:PARAMS); END;$$;

ALTER TASK BRONZE_LAYER.ingest_prc_customer_erp_pricing_market_silver RESUME;
ALTER TASK BRONZE_LAYER.ingest_prc_customer_erp_pricing_market_json RESUME;

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task BRONZE_LAYER.ingest_prc_generic_geography_json
	warehouse={{ ENVIRONMENT}}_WH
	schedule='USING CRON 0 5 * * * Europe/Paris'
	config='{"params":"''{ \\"src_schema\\" : \\"raw_layer\\", \\"external_stage_root_path\\": \\"@RAW_LAYER.EXTERNAL_AZUR_STAGE/Files\\", \\"stage_name\\": \\"@raw_layer.landing_internal_stage\\",  \\"stage_path_suffix\\" :\\"/PRC_GENERIC_GEOGRAPHY/\\", \\"pattern_file_name\\": \\".*.json\\",  \\"on_error\\": \\"CONTINUE\\", \\"file_format\\" : \\"bronze_layer.json_file_format\\",  \\"bronze_table\\": \\"bronze_layer.PRC_GENERIC_GEOGRAPHY_BRZ\\",  \\"silver_table\\" :\\"silver_layer.DIM_PRC_GENERIC_GEOGRAPHY_SLV\\", \\"silver_technicalKey_name\\" : \\"PricingGenericGeographyPrcIntKey\\", \\"silver_functionalKey_name\\" : \\"PricingGenericGeographyPrcKey\\", \\"silver_ruleTechnicalKey\\": \\"HASH(COALESCE(REPLACE(AGUKCODE, ''\\\\''\\\\ \\\\'''', ''\\\\''\\\\''''), ''\\\\''N/A\\\\''''))\\", \\"silver_ruleFunctionalKey\\" : \\"COALESCE(REPLACE(AGUKCODE, ''\\\\''\\\\ \\\\'''', ''\\\\''\\\\''''), ''\\\\''N/A\\\\'''')\\"}''"}'
	as EXECUTE IMMEDIATE $$ 
	 BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "BRONZE_LAYER"."INGEST_RAW_FILES_INTO_BRONZE_LAYER"(:PARAMS); END;$$;

ALTER TASK BRONZE_LAYER.ingest_prc_generic_geography_json SUSPEND;

create or replace task BRONZE_LAYER.ingest_prc_generic_geography_silver
	warehouse={{ ENVIRONMENT}}_WH
	after BRONZE_LAYER.ingest_prc_generic_geography_json
	as EXECUTE IMMEDIATE $$ 
	BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "SILVER_LAYER"."INGEST_INTO_SILVER_LAYER"(:PARAMS); END;$$;

ALTER TASK BRONZE_LAYER.ingest_prc_generic_geography_silver RESUME;
ALTER TASK BRONZE_LAYER.ingest_prc_generic_geography_json RESUME;

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task BRONZE_LAYER.ingest_prc_generic_product_json
	warehouse={{ ENVIRONMENT}}_WH
	schedule='USING CRON 0 5 * * * Europe/Paris'
	config='{"params":"''{ \\"src_schema\\" : \\"raw_layer\\", \\"external_stage_root_path\\": \\"@RAW_LAYER.EXTERNAL_AZUR_STAGE/Files\\", \\"stage_name\\": \\"@raw_layer.landing_internal_stage\\",  \\"stage_path_suffix\\" :\\"/PRC_GENERIC_PRODUCT/\\", \\"pattern_file_name\\": \\".*.json\\",  \\"on_error\\": \\"CONTINUE\\", \\"file_format\\" : \\"bronze_layer.json_file_format\\",  \\"bronze_table\\": \\"bronze_layer.PRC_GENERIC_PRODUCT_BRZ\\",  \\"silver_table\\" :\\"silver_layer.DIM_PRC_GENERIC_PRODUCT_SLV\\", \\"silver_technicalKey_name\\" : \\"PricingGenericProductPrcIntKey\\", \\"silver_functionalKey_name\\" : \\"PricingGenericProductPrcKey\\", \\"silver_ruleTechnicalKey\\": \\"HASH(COALESCE(REPLACE(APUKCODE, ''\\\\''\\\\ \\\\'''', ''\\\\''\\\\''''), ''\\\\''N/A\\\\''''))\\", \\"silver_ruleFunctionalKey\\" : \\"COALESCE(REPLACE(APUKCODE, ''\\\\''\\\\ \\\\'''', ''\\\\''\\\\''''), ''\\\\''N/A\\\\'''')\\"}''"}'
	as EXECUTE IMMEDIATE $$ 
	 BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "BRONZE_LAYER"."INGEST_RAW_FILES_INTO_BRONZE_LAYER"(:PARAMS); END;$$;

ALTER TASK BRONZE_LAYER.ingest_prc_generic_product_json SUSPEND;

create or replace task BRONZE_LAYER.ingest_prc_generic_product_silver
	warehouse={{ ENVIRONMENT}}_WH
	after BRONZE_LAYER.ingest_prc_generic_product_json
	as EXECUTE IMMEDIATE $$ 
	BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "SILVER_LAYER"."INGEST_INTO_SILVER_LAYER"(:PARAMS); END;$$;

ALTER TASK BRONZE_LAYER.ingest_prc_generic_product_silver RESUME;
ALTER TASK BRONZE_LAYER.ingest_prc_generic_product_json RESUME;

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------


create or replace task BRONZE_LAYER.ingest_prc_campaign_csv
	warehouse={{ ENVIRONMENT}}_WH
	schedule='USING CRON 0 5 * * * Europe/Paris'
	config='{"params":"''{ \\"src_schema\\" : \\"raw_layer\\", \\"external_stage_root_path\\": \\"@RAW_LAYER.EXTERNAL_AZUR_STAGE/Files\\", \\"stage_name\\": \\"@raw_layer.landing_internal_stage\\",  \\"stage_path_suffix\\" :\\"/PRC_CAMPAIGN/\\", \\"pattern_file_name\\": \\".*.csv\\",  \\"on_error\\": \\"CONTINUE\\", \\"file_format\\" : \\"bronze_layer.csv_file_format\\",  \\"bronze_table\\": \\"bronze_layer.PRC_CAMPAIGN_BRZ\\",  \\"silver_table\\" :\\"silver_layer.DIM_PRC_CAMPAIGN_SLV\\", \\"silver_technicalKey_name\\" : \\"PricingCampaignPrcIntKey\\", \\"silver_functionalKey_name\\" : \\"PricingCampaignPrcKey\\", \\"silver_ruleTechnicalKey\\": \\"HASH(COALESCE(REPLACE(CampaignCode, ''\\\\''\\\\ \\\\'''', ''\\\\''\\\\''''), ''\\\\''N/A\\\\''''))\\", \\"silver_ruleFunctionalKey\\" : \\"COALESCE(REPLACE(CampaignCode, ''\\\\''\\\\ \\\\'''', ''\\\\''\\\\''''), ''\\\\''N/A\\\\'''')\\"}''"}'
	as EXECUTE IMMEDIATE $$ BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "BRONZE_LAYER"."INGEST_RAW_FILES_INTO_BRONZE_LAYER"(:PARAMS); END;$$;

ALTER TASK BRONZE_LAYER.ingest_prc_campaign_csv SUSPEND;

create or replace task BRONZE_LAYER.ingest_prc_campaign_silver
	warehouse={{ ENVIRONMENT}}_WH
	after BRONZE_LAYER.ingest_prc_campaign_csv
	as EXECUTE IMMEDIATE $$ 
	BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "SILVER_LAYER"."INGEST_INTO_SILVER_LAYER"(:PARAMS); END;$$;

ALTER TASK BRONZE_LAYER.ingest_prc_campaign_silver RESUME;
ALTER TASK BRONZE_LAYER.ingest_prc_campaign_csv RESUME;




---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task BRONZE_LAYER.ingest_prc_product_json
	warehouse={{ ENVIRONMENT}}_WH
	schedule='USING CRON 0 5 * * * Europe/Paris'
	config='{"params":"''{ \\"src_schema\\" : \\"raw_layer\\", \\"external_stage_root_path\\": \\"@RAW_LAYER.EXTERNAL_AZUR_STAGE/Files\\", \\"stage_name\\": \\"@raw_layer.landing_internal_stage\\",  \\"stage_path_suffix\\" :\\"/PRC_PRODUCT/\\", \\"pattern_file_name\\": \\".*.json\\",  \\"on_error\\": \\"CONTINUE\\", \\"file_format\\" : \\"bronze_layer.json_file_format\\",  \\"bronze_table\\": \\"bronze_layer.PRC_PRODUCT_BRZ\\",  \\"silver_table\\" :\\"silver_layer.DIM_PRC_PRODUCT_SLV\\", \\"silver_technicalKey_name\\" : \\"PricingProductPrcIntKey\\", \\"silver_functionalKey_name\\" : \\"PricingProductPrcKey\\", \\"silver_ruleTechnicalKey\\": \\"HASH(CONCAT(COALESCE(REPLACE(Housekey, ''\\\\''\\\\ \\\\'''', ''\\\\''\\\\''''), ''\\\\''N/A\\\\''''), ''\\\\''_\\\\'''', COALESCE(REPLACE(IDProduct, ''\\\\''\\\\ \\\\'''', ''\\\\''\\\\''''), ''\\\\''N/A\\\\'''')))\\", \\"silver_ruleFunctionalKey\\" : \\"CONCAT(COALESCE(REPLACE(HouseKey, ''\\\\''\\\\ \\\\'''', ''\\\\''\\\\''''), ''\\\\''N/A\\\\''''), ''\\\\''_\\\\'''', COALESCE(REPLACE(IDProduct, ''\\\\''\\\\ \\\\'''', ''\\\\''\\\\''''), ''\\\\''N/A\\\\''''))\\"}''"}'
	as EXECUTE IMMEDIATE $$ BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "BRONZE_LAYER"."INGEST_RAW_FILES_INTO_BRONZE_LAYER"(:PARAMS); END;$$;

ALTER TASK BRONZE_LAYER.ingest_prc_product_json SUSPEND;

create or replace task BRONZE_LAYER.ingest_prc_product_silver
	warehouse={{ ENVIRONMENT}}_WH
	after BRONZE_LAYER.ingest_prc_product_json
	as EXECUTE IMMEDIATE $$ 
	BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "SILVER_LAYER"."INGEST_INTO_SILVER_LAYER"(:PARAMS); END;$$;

ALTER TASK BRONZE_LAYER.ingest_prc_product_silver RESUME;
ALTER TASK BRONZE_LAYER.ingest_prc_product_json RESUME;



---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task BRONZE_LAYER.ingest_prc_syrusmarket_non_erp_pricing_market_csv
	warehouse={{ ENVIRONMENT}}_WH
	schedule='USING CRON 0 5 * * * Europe/Paris'
	config='{"params":"''{ \\"src_schema\\" : \\"raw_layer\\", \\"external_stage_root_path\\": \\"@RAW_LAYER.EXTERNAL_AZUR_STAGE/Files\\", \\"stage_name\\": \\"@raw_layer.landing_internal_stage\\",  \\"stage_path_suffix\\" :\\"/PRC_SYRUSMARKET_NON_ERP_PRICING_MARKET/\\", \\"pattern_file_name\\": \\".*.csv\\",  \\"on_error\\": \\"CONTINUE\\", \\"file_format\\" : \\"bronze_layer.csv_file_format\\",  \\"bronze_table\\": \\"bronze_layer.PRC_SYRUSMARKET_NON_ERP_PRICING_MARKET_BRZ\\",  \\"silver_table\\" :\\"silver_layer.DIM_PRC_SYRUSMARKET_NON_ERP_PRICING_MARKET_SLV\\", \\"silver_technicalKey_name\\" : \\"PricingSyrusMarketNonErpPricingMarketPrcIntKey\\", \\"silver_functionalKey_name\\" : \\"PricingSyrusMarketNonErpPricingMarketPrcKey\\", \\"silver_ruleTechnicalKey\\": \\"HASH(CONCAT(COALESCE(REPLACE(PricingMarketCode, ''\\\\''\\\\ \\\\'''', ''\\\\''\\\\''''), ''\\\\''N/A\\\\''''), ''\\\\''_\\\\'''', COALESCE(REPLACE(SyrusMarketCode, ''\\\\''\\\\ \\\\'''', ''\\\\''\\\\''''), ''\\\\''N/A\\\\'''')))\\", \\"silver_ruleFunctionalKey\\" : \\"CONCAT(COALESCE(REPLACE(PricingMarketCode, ''\\\\''\\\\ \\\\'''', ''\\\\''\\\\''''), ''\\\\''N/A\\\\''''), ''\\\\''_\\\\'''', COALESCE(REPLACE(SyrusMarketCode, ''\\\\''\\\\ \\\\'''', ''\\\\''\\\\''''), ''\\\\''N/A\\\\''''))\\"}''"}'
	as EXECUTE IMMEDIATE $$ 
	 BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "BRONZE_LAYER"."INGEST_RAW_FILES_INTO_BRONZE_LAYER"(:PARAMS); END;$$;

ALTER TASK BRONZE_LAYER.ingest_prc_syrusmarket_non_erp_pricing_market_csv SUSPEND;

create or replace task BRONZE_LAYER.ingest_prc_syrusmarket_non_erp_pricing_market_silver
	warehouse={{ ENVIRONMENT}}_WH
	after BRONZE_LAYER.ingest_prc_syrusmarket_non_erp_pricing_market_csv
	as EXECUTE IMMEDIATE $$ 
	BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "SILVER_LAYER"."INGEST_INTO_SILVER_LAYER"(:PARAMS); END;$$;

ALTER TASK BRONZE_LAYER.ingest_prc_syrusmarket_non_erp_pricing_market_silver RESUME;
ALTER TASK BRONZE_LAYER.ingest_prc_syrusmarket_non_erp_pricing_market_csv RESUME;