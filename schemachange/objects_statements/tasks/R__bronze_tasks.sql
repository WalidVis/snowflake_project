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
           - PRICING_TRIGGER_DATA_PIPELINES (root)
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

create or replace task ORCHESTRATION_SCHEMA.PRICING_TRIGGER_DATA_PIPELINES
	warehouse={{ ENVIRONMENT }}_WH
	schedule='USING CRON 0 5 * * * UTC'
	as CALL ORCHESTRATION_SCHEMA.EXECUTE_ALL_TASKS_BRONZE();

ALTER TASK ORCHESTRATION_SCHEMA.PRICING_TRIGGER_DATA_PIPELINES SUSPEND;

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_BENCHMARK_CSV
	warehouse={{ ENVIRONMENT }}_WH
	schedule='USING CRON 0 6 * * * UTC'
	config='{"params":"''{ \\"pattern_file_name\\": \\".*.csv\\",  \\"on_error\\": \\"CONTINUE\\", \\"file_format\\" : \\"csv_file_format_semicolon\\",  \\"bronze_table\\": \\"PRC_BENCHMARK_BRZ\\"}''"}'
	as EXECUTE IMMEDIATE $$ BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "ORCHESTRATION_SCHEMA"."INGEST_RAW_FILES_INTO_BRONZE_V2"(:PARAMS); END;$$;

ALTER TASK ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_BENCHMARK_CSV SUSPEND;

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_BENCHMARK_LEVEL_CSV
	warehouse={{ ENVIRONMENT }}_WH
	schedule='USING CRON 0 6 * * * UTC'
	config='{"params":"''{ \\"pattern_file_name\\": \\".*.csv\\",  \\"on_error\\": \\"CONTINUE\\", \\"file_format\\" : \\"csv_file_format_semicolon\\",  \\"bronze_table\\": \\"PRC_BENCHMARK_LEVEL_BRZ\\"}''"}'
	as EXECUTE IMMEDIATE $$ BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "ORCHESTRATION_SCHEMA"."INGEST_RAW_FILES_INTO_BRONZE_V2"(:PARAMS); END;$$;

ALTER TASK ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_BENCHMARK_LEVEL_CSV SUSPEND;

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_CAMPAIGN_CSV
	warehouse={{ ENVIRONMENT }}_WH
	schedule='USING CRON 0 6 * * * UTC'
	config='{"params":"''{ \\"pattern_file_name\\": \\".*.csv\\",  \\"on_error\\": \\"CONTINUE\\", \\"file_format\\" : \\"csv_file_format_semicolon\\",  \\"bronze_table\\": \\"PRC_CAMPAIGN_BRZ\\"}''"}'
	as EXECUTE IMMEDIATE $$ BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "ORCHESTRATION_SCHEMA"."INGEST_RAW_FILES_INTO_BRONZE_V2"(:PARAMS); END;$$;

ALTER TASK ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_CAMPAIGN_CSV SUSPEND;

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_CAMPAIGN_MARKET_CSV
	warehouse={{ ENVIRONMENT }}_WH
	schedule='USING CRON 0 6 * * * UTC'
	config='{"params":"''{ \\"pattern_file_name\\": \\".*.csv\\",  \\"on_error\\": \\"CONTINUE\\", \\"file_format\\" : \\"csv_file_format_semicolon\\",  \\"bronze_table\\": \\"PRC_CAMPAIGN_MARKET_BRZ\\"}''"}'
	as EXECUTE IMMEDIATE $$ BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "ORCHESTRATION_SCHEMA"."INGEST_RAW_FILES_INTO_BRONZE_V2"(:PARAMS); END;$$;

ALTER TASK ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_CAMPAIGN_MARKET_CSV SUSPEND;

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_CUSTOMER_ERP_PRICING_MARKET_JSON
	warehouse={{ ENVIRONMENT }}_WH
	schedule='USING CRON 0 6 * * * UTC'
	config='{"params":"''{ \\"pattern_file_name\\": \\".*.json\\",  \\"on_error\\": \\"CONTINUE\\", \\"file_format\\" : \\"json_file_format\\",  \\"bronze_table\\": \\"PRC_CUSTOMER_ERP_PRICING_MARKET_BRZ\\"}''"}'
	as EXECUTE IMMEDIATE $$ BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "ORCHESTRATION_SCHEMA"."INGEST_RAW_FILES_INTO_BRONZE_V2"(:PARAMS); END;$$;

ALTER TASK ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_CUSTOMER_ERP_PRICING_MARKET_JSON SUSPEND;

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_GENERIC_GEOGRAPHY_JSON
	warehouse={{ ENVIRONMENT }}_WH
	schedule='USING CRON 0 6 * * * UTC'
	config='{"params":"''{ \\"pattern_file_name\\": \\".*.json\\",  \\"on_error\\": \\"CONTINUE\\", \\"file_format\\" : \\"json_file_format\\",  \\"bronze_table\\": \\"PRC_GENERIC_GEOGRAPHY_BRZ\\"}''"}'
	as EXECUTE IMMEDIATE $$ BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "ORCHESTRATION_SCHEMA"."INGEST_RAW_FILES_INTO_BRONZE_V2"(:PARAMS); END;$$;

ALTER TASK ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_GENERIC_GEOGRAPHY_JSON SUSPEND;

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_GENERIC_PRODUCT_JSON
	warehouse={{ ENVIRONMENT }}_WH
	schedule='USING CRON 0 6 * * * UTC'
	config='{"params":"''{ \\"pattern_file_name\\": \\".*.json\\",  \\"on_error\\": \\"CONTINUE\\", \\"file_format\\" : \\"json_file_format\\",  \\"bronze_table\\": \\"PRC_GENERIC_PRODUCT_BRZ\\"}''"}'
	as EXECUTE IMMEDIATE $$ BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "ORCHESTRATION_SCHEMA"."INGEST_RAW_FILES_INTO_BRONZE_V2"(:PARAMS); END;$$;

ALTER TASK ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_GENERIC_PRODUCT_JSON SUSPEND;

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_GEOGRAPHY_JSON
	warehouse={{ ENVIRONMENT }}_WH
	schedule='USING CRON 0 6 * * * UTC'
	config='{"params":"''{ \\"pattern_file_name\\": \\".*.json\\",  \\"on_error\\": \\"CONTINUE\\", \\"file_format\\" : \\"json_file_format\\",  \\"bronze_table\\": \\"PRC_GEOGRAPHY_BRZ\\"}''"}'
	as EXECUTE IMMEDIATE $$ BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "ORCHESTRATION_SCHEMA"."INGEST_RAW_FILES_INTO_BRONZE_V2"(:PARAMS); END;$$;

ALTER TASK ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_GEOGRAPHY_JSON SUSPEND;

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_HOUSE_PRICE_JSON
	warehouse={{ ENVIRONMENT }}_WH
	schedule='USING CRON 0 6 * * * UTC'
	config='{"params":"''{ \\"pattern_file_name\\": \\".*.json\\",  \\"on_error\\": \\"CONTINUE\\", \\"file_format\\" : \\"json_file_format\\",  \\"bronze_table\\": \\"PRC_HOUSE_PRICE_BRZ\\"}''"}'
	as EXECUTE IMMEDIATE $$ BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "ORCHESTRATION_SCHEMA"."INGEST_RAW_FILES_INTO_BRONZE_V2"(:PARAMS); END;$$;

ALTER TASK ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_HOUSE_PRICE_JSON SUSPEND;

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_PRICING_MARKET_JSON
	warehouse={{ ENVIRONMENT }}_WH
	schedule='USING CRON 0 6 * * * UTC'
	config='{"params":"''{ \\"pattern_file_name\\": \\".*.json\\",  \\"on_error\\": \\"CONTINUE\\", \\"file_format\\" : \\"json_file_format\\",  \\"bronze_table\\": \\"PRC_PRICING_MARKET_BRZ\\"}''"}'
	as EXECUTE IMMEDIATE $$ BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "ORCHESTRATION_SCHEMA"."INGEST_RAW_FILES_INTO_BRONZE_V2"(:PARAMS); END;$$;

ALTER TASK ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_PRICING_MARKET_JSON SUSPEND;

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_PRODUCT_JSON
	warehouse={{ ENVIRONMENT }}_WH
	schedule='USING CRON 0 6 * * * UTC'
	config='{"params":"''{ \\"pattern_file_name\\": \\".*.json\\",  \\"on_error\\": \\"CONTINUE\\", \\"file_format\\" : \\"json_file_format\\",  \\"bronze_table\\": \\"PRC_PRODUCT_BRZ\\"}''"}'
	as EXECUTE IMMEDIATE $$ BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "ORCHESTRATION_SCHEMA"."INGEST_RAW_FILES_INTO_BRONZE_V2"(:PARAMS); END;$$;

ALTER TASK ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_PRODUCT_JSON SUSPEND;

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_RETAIL_PRICE_JSON
	warehouse={{ ENVIRONMENT }}_WH
	schedule='USING CRON 0 6 * * * UTC'
	config='{"params":"''{ \\"pattern_file_name\\": \\".*.json\\",  \\"on_error\\": \\"CONTINUE\\", \\"file_format\\" : \\"json_file_format\\",  \\"bronze_table\\": \\"PRC_RETAIL_PRICE_BRZ\\"}''"}'
	as EXECUTE IMMEDIATE $$ BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "ORCHESTRATION_SCHEMA"."INGEST_RAW_FILES_INTO_BRONZE_V2"(:PARAMS); END;$$;

ALTER TASK ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_RETAIL_PRICE_JSON SUSPEND;

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_SYRUSMARKET_NON_ERP_PRICING_MARKET_JSON
	warehouse={{ ENVIRONMENT }}_WH
	schedule='USING CRON 0 6 * * * UTC'
	config='{"params":"''{ \\"pattern_file_name\\": \\".*.json\\",  \\"on_error\\": \\"CONTINUE\\", \\"file_format\\" : \\"json_file_format\\",  \\"bronze_table\\": \\"PRC_SYRUSMARKET_NON_ERP_PRICING_MARKET_BRZ\\"}''"}'
	as EXECUTE IMMEDIATE $$ BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "ORCHESTRATION_SCHEMA"."INGEST_RAW_FILES_INTO_BRONZE_V2"(:PARAMS); END;$$;

ALTER TASK ORCHESTRATION_SCHEMA.BRZ_INGEST_PRC_SYRUSMARKET_NON_ERP_PRICING_MARKET_JSON SUSPEND;