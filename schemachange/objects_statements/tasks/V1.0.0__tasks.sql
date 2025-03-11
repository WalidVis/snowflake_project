create or replace task BRONZE_LAYER."ingest_PRC_CAMPAIGN_MARKET_csv"
	warehouse=COMPUTE_WH
	schedule='USING CRON 1 1 * * * Europe/Paris'
	config='{
  "params": "''{ \\"src_schema\\": \\"DEV_POC_VISEO_DB.raw_layer\\",
  \\"target_table\\": \\"DEV_POC_VISEO_DB.bronze_layer.PRC_CAMPAIGN_MARKET_BRZ\\",
  \\"stage_name\\": \\"@DEV_POC_VISEO_DB.raw_layer.landing_internal_stage\\",
  \\"stage_path_suffix\\" :\\"/PRC_CAMPAIGN_MARKET/\\",
  \\"pattern_file_name\\": \\".*.csv\\",
  \\"file_format\\" : \\"DEV_POC_VISEO_DB.bronze_layer.csv_file_format\\",
   \\"on_error\\": \\"SKIP_FILE\\",
   \\"external_stage_root_path\\": \\"@DEV_POC_VISEO_DB.RAW_LAYER.EXTERNAL_AZUR_STAGE/Files\\"}''"
}'
as
    BEGIN
        LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string;
	    EXECUTE NOTEBOOK "DEV_POC_VISEO_DB"."BRONZE_LAYER"."INGEST_RAW_FILES_INTO_BRONZE_LAYER"(:PARAMS);
    END;

    ---------------------------------------------------
    --------------------------------------------------------------------------
    ---------------------------------------------------

    create or replace task BRONZE_LAYER."ingest_PRC_CUSTOMER_ERP_PRICING_MARKET_json"
	warehouse=COMPUTE_WH
	schedule='USING CRON 0 5 * * * Europe/Paris'
	config='{"params":"''{ \\"src_schema\\": \\"DEV_POC_VISEO_DB.raw_layer\\",
	 \\"target_table\\": \\"DEV_POC_VISEO_DB.BRONZE_LAYER.PRC_CUSTOMER_ERP_PRICING_MARKET_BRZ\\",
	   \\"stage_name\\": \\"@DEV_POC_VISEO_DB.raw_layer.landing_internal_stage\\",
	    \\"stage_path_suffix\\" :\\"/PRC_CUSTOMER_ERP_PRICING_MARKET/\\",
	     \\"pattern_file_name\\": \\".*.json\\",
	      \\"file_format\\" : \\"DEV_POC_VISEO_DB.bronze_layer.json_file_format\\",
	      \\"on_error\\": \\"CONTINUE\\",
	      \\"external_stage_root_path\\": \\"@DEV_POC_VISEO_DB.RAW_LAYER.EXTERNAL_AZUR_STAGE/Files\\"}''"}'
as
	BEGIN
	    LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string;
	    EXECUTE NOTEBOOK "DEV_POC_VISEO_DB"."BRONZE_LAYER"."INGEST_RAW_FILES_INTO_BRONZE_LAYER"(:PARAMS);
	END;