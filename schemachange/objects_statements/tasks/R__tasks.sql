-- !!!!!!!EXECUTE IMMEDIATE IS MANDATORY TO AVOID <EOF> error during Schemachange execution of the CREATE TASK !!!!!!!
---------------------------------------------------------------------------------------------------------------------
---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

-- BATCH PIPELINE TASKS

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task BRONZE_LAYER."ingest_PRC_BENCHMARK_LEVEL_csv"
	warehouse=COMPUTE_WH
	schedule='USING CRON 0 5 * * * Europe/Paris'
	--  [ TASK_AUTO_RETRY_ATTEMPTS = <num> ] 
	config='{
  "params": "''{ \\"src_schema\\": \\"raw_layer\\",  \\"target_table\\": \\"bronze_layer.PRC_BENCHMARK_LEVEL\\",  \\"stage_name\\": \\"@raw_layer.landing_internal_stage\\",  \\"stage_path_suffix\\" :\\"/PRC_BENCHMARK_LEVEL/\\",  \\"pattern_file_name\\": \\".*.csv\\",  \\"file_format\\" : \\"bronze_layer.csv_file_format\\", \\"on_error\\": \\"SKIP_FILE\\", \\"external_stage_root_path\\": \\"@RAW_LAYER.EXTERNAL_AZUR_STAGE/Files\\"}''"
}'
as
EXECUTE IMMEDIATE $$  BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "BRONZE_LAYER"."INGEST_RAW_FILES_INTO_BRONZE_LAYER"(:PARAMS); END;$$;

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

create or replace task BRONZE_LAYER."ingest_PRC_CUSTOMER_ERP_PRICING_MARKET_json"
	warehouse=COMPUTE_WH
	schedule='USING CRON 0 5 * * * Europe/Paris'
	--  [ TASK_AUTO_RETRY_ATTEMPTS = <num> ] 
	config='{"params":"''{ \\"src_schema\\": \\"raw_layer\\", \\"target_table\\": \\"BRONZE_LAYER.PRC_CUSTOMER_ERP_PRICING_MARKET_BRZ\\", \\"stage_name\\": \\"@raw_layer.landing_internal_stage\\", \\"stage_path_suffix\\" :\\"/PRC_CUSTOMER_ERP_PRICING_MARKET/\\", \\"pattern_file_name\\": \\".*.json\\", \\"file_format\\" : \\"bronze_layer.json_file_format\\", \\"on_error\\": \\"CONTINUE\\", \\"external_stage_root_path\\": \\"@RAW_LAYER.EXTERNAL_AZUR_STAGE/Files\\"}''"}'
as
EXECUTE IMMEDIATE $$
	BEGIN
	    LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string;
	    EXECUTE NOTEBOOK "BRONZE_LAYER"."INGEST_RAW_FILES_INTO_BRONZE_LAYER"(:PARAMS);
	END;
	$$;

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------


create or replace task BRONZE_LAYER."ingest_PRC_BENCHMARK_csv"
	warehouse=COMPUTE_WH
	schedule='USING CRON 0 5 * * * Europe/Paris'
	--  [ TASK_AUTO_RETRY_ATTEMPTS = <num> ]
	config='{
  "params": "''{ \\"src_schema\\": \\"raw_layer\\",  \\"target_table\\": \\"bronze_layer.PRC_BENCHMARK_BRZ\\",  \\"stage_name\\": \\"@raw_layer.landing_internal_stage\\",  \\"stage_path_suffix\\" :\\"/PRC_BENCHMARK/\\",  \\"pattern_file_name\\": \\".*.csv\\",  \\"file_format\\" : \\"bronze_layer.csv_file_format\\", \\"on_error\\": \\"SKIP_FILE\\", \\"external_stage_root_path\\": \\"@RAW_LAYER.EXTERNAL_AZUR_STAGE/Files\\"}''"
}'
	as EXECUTE IMMEDIATE $$ BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "BRONZE_LAYER"."INGEST_RAW_FILES_INTO_BRONZE_LAYER"(:PARAMS); END; $$;

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------


create or replace task BRONZE_LAYER."ingest_PRC_CAMPAIGN_MARKET_csv"
	warehouse=COMPUTE_WH
	--  [ TASK_AUTO_RETRY_ATTEMPTS = <num> ]
	schedule='USING CRON 1 1 * * * Europe/Paris'
	config='{
  "params": "''{ \\"src_schema\\": \\"raw_layer\\",  \\"target_table\\": \\"bronze_layer.PRC_CAMPAIGN_MARKET_BRZ\\",  \\"stage_name\\": \\"@raw_layer.landing_internal_stage\\",  \\"stage_path_suffix\\" :\\"/PRC_CAMPAIGN_MARKET/\\",  \\"pattern_file_name\\": \\".*.csv\\",  \\"file_format\\" : \\"bronze_layer.csv_file_format\\", \\"on_error\\": \\"SKIP_FILE\\", \\"external_stage_root_path\\": \\"@RAW_LAYER.EXTERNAL_AZUR_STAGE/Files\\"}''"
}'
	as EXECUTE IMMEDIATE $$ BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "BRONZE_LAYER"."INGEST_RAW_FILES_INTO_BRONZE_LAYER"(:PARAMS); END; $$;


---------------------------------------------------
-------------------------------- SILVER LAYER INGEST TASKS (MUST BE CREATED IN BRONZE LAYER)
---------------------------------------------------


CREATE OR REPLACE TASK BRONZE_LAYER."ingest_PRC_CAMPAIGN_MARKET_silver"
WAREHOUSE = compute_wh
AFTER BRONZE_LAYER."ingest_PRC_CAMPAIGN_MARKET_csv"
AS 
EXECUTE IMMEDIATE
$$
BEGIN 
truncate SILVER_LAYER.DIM_PRC_CAMPAIGN_MARKET_SLV;
insert into SILVER_LAYER.DIM_PRC_CAMPAIGN_MARKET_SLV(
PricingCampaignMarketPrcIntKey,
    PricingCampaignMarketPrcKey,
    HouseKey ,
    CampaignCode ,
    PricingMarketCode ,
    RateType ,
    RateDate ,
    BaseCampaignCode ,
    SYS_DATE_CREATE	)  (
        SELECT hash(COALESCE(CONCAT(PricingMarketCode,'_',CampaignCode), 'N/A')),
        COALESCE(CONCAT(PricingMarketCode,'_',CampaignCode), 'N/A'),
            HouseKey ,
            CampaignCode ,
            PricingMarketCode ,
            RateType ,
            RateDate ,
            BaseCampaignCode ,
            CURRENT_TIMESTAMP 
        from BRONZE_LAYER.PRC_CAMPAIGN_MARKET_BRZ
    );
    let execution_status VARCHAR;
    let error_code VARCHAR;
    let error_message VARCHAR;
    let rows_produced NUMBER;
    let rows_inserted NUMBER;
    SELECT EXECUTION_STATUS, ERROR_CODE,ERROR_MESSAGE,  ROWS_PRODUCED, ROWS_INSERTED
INTO :execution_status, :error_code, error_message, :rows_produced, :rows_inserted
FROM table (information_schema.QUERY_HISTORY_BY_SESSION())
WHERE QUERY_ID = LAST_QUERY_ID();
    
    insert into monitoring_layer.monitoring_ingest( src_table,layer, status, ingestion_time, rows_parsed, rows_loaded, first_error)  
   select ARRAY_CONSTRUCT('PRC_CAMPAIGN_MARKET_BRZ'), 'SILVER_LAYER', :execution_status, CURRENT_TIMESTAMP(3), :rows_produced, :rows_inserted, :error_message;
   
   END; $$;