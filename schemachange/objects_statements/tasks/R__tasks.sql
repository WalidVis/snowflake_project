create or replace task BRONZE_LAYER."ingest_PRC_BENCHMARK_LEVEL_csv"
	warehouse=COMPUTE_WH
	schedule='USING CRON 0 5 * * * Europe/Paris'
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
	config='{"params":"''{ \\"src_schema\\": \\"raw_layer\\", \\"target_table\\": \\"BRONZE_LAYER.PRC_CUSTOMER_ERP_PRICING_MARKET_BRZ\\", \\"stage_name\\": \\"@raw_layer.landing_internal_stage\\", \\"stage_path_suffix\\" :\\"/PRC_CUSTOMER_ERP_PRICING_MARKET/\\", \\"pattern_file_name\\": \\".*.json\\", \\"file_format\\" : \\"bronze_layer.json_file_format\\", \\"on_error\\": \\"CONTINUE\\", \\"external_stage_root_path\\": \\"@RAW_LAYER.EXTERNAL_AZUR_STAGE/Files\\"}''"}'
as
EXECUTE IMMEDIATE $$
	BEGIN
	    LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string;
	    EXECUTE NOTEBOOK "BRONZE_LAYER"."INGEST_RAW_FILES_INTO_BRONZE_LAYER"(:PARAMS);
	END;
	$$;