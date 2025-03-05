create or replace task TEST_POC_VISEO_DB.BRONZE_LAYER."ingest_PRC_BENCHMARK_LEVEL_csv"
	warehouse=COMPUTE_WH
	schedule='USING CRON 0 5 * * * Europe/Paris'
	config='{
  "params": "''{ \\"src_schema\\": \\"TEST_POC_VISEO_DB.raw_layer\\",  \\"target_table\\": \\"TEST_POC_VISEO_DB.bronze_layer.PRC_BENCHMARK_LEVEL\\",  \\"stage_name\\": \\"@TEST_POC_VISEO_DB.raw_layer.landing_internal_stage\\",  \\"stage_path_suffix\\" :\\"/PRC_BENCHMARK_LEVEL/\\",  \\"pattern_file_name\\": \\".*.csv\\",  \\"file_format\\" : \\"TEST_POC_VISEO_DB.bronze_layer.csv_file_format\\", \\"on_error\\": \\"SKIP_FILE\\", \\"external_stage_root_path\\": \\"@TEST_POC_VISEO_DB.RAW_LAYER.EXTERNAL_AZUR_STAGE/Files\\"}''"
}'
	as BEGIN LET PARAMS STRING := SYSTEM$GET_TASK_GRAPH_CONFIG('params')::string; EXECUTE NOTEBOOK "TEST_POC_VISEO_DB"."BRONZE_LAYER"."INGEST_RAW_FILES_INTO_BRONZE_LAYER"(:PARAMS); END;