CREATE OR REPLACE PROCEDURE DEV_POC_VISEO_DB.BRONZE_LAYER.INGEST_FILE_PROC("SRC_SCHEMA" VARCHAR, "TARGET_TABLE_ABS_NAME" VARCHAR, "STAGE_NAME" VARCHAR, "STAGE_SUFFIX_DIRECTORY_PATH" VARCHAR, "PATTERN_FILE_NAME" VARCHAR, "FILE_FORMAT" VARCHAR, "ON_ERROR_OPTION" VARCHAR)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS CALLER
AS 'DECLARE
    landing_directory_abs_path VARCHAR ;
    archive_abs_path VARCHAR;
    error_abs_path VARCHAR;
    query_result RESULTSET DEFAULT (SELECT -1);
    query VARCHAR;
    layer VARCHAR(30) DEFAULT ''\\''BRONZE_LAYER\\'''';
    monitoring_table_name VARCHAR := ''monitoring_layer.monitoring_ingest'';
    builded_file_name VARCHAR := stage_suffix_directory_path || pattern_file_name ;

BEGIN
    
    let dateTime DATETIME := CURRENT_TIMESTAMP();
    
    landing_directory_abs_path := :stage_name || :stage_suffix_directory_path;
    archive_abs_path := ''@'' || :src_schema || ''.'' || ''archive_internal_stage'' || :stage_suffix_directory_path;
    error_abs_path := ''@'' || :src_schema || ''.'' || ''error_internal_stage'' || :stage_suffix_directory_path;
    
    query := ''copy into '' || target_table_abs_name || '' from ''||  landing_directory_abs_path ||'' 
        FILE_FORMAT = (FORMAT_NAME='' || file_format||'')
        PATTERN = ''||''\\'''' || pattern_file_name ||''\\'' 
        ON_ERROR = ''|| on_error_option ||'' 
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        INCLUDE_METADATA = (create_date = METADATA$START_SCAN_TIME, file_name = METADATA$FILENAME)'';
    
    query_result := (execute immediate query);
     
    
    FOR item IN query_result DO
        let v_status varchar := item."status";

        // Non existing file or already loaded 
        IF (v_status != ''LOADED'' AND v_status != ''LOAD_FAILED'' AND v_status != ''PARTIALLY_LOADED'' ) THEN
            
            //copy files into :archive_file_abs_path from :landing_file_abs_path;
            insert into monitoring_layer.monitoring_ingest(file, layer, status, rows_parsed, rows_loaded, ingestion_time, FIRST_ERROR)  values(:builded_file_name, ''BRONZE_LAYER'', :v_status, 0, 0, CURRENT_TIMESTAMP(3), ''No files OR Files already loaded'');
   
            BREAK;
        END IF;
        
        let v_file_name varchar := COALESCE(item."file",''null'');
        let v_rows_parsed number := item."rows_parsed";
        let v_rows_loaded number := item."rows_loaded";
        let v_errors_seen number := item."errors_seen";
        let v_first_error varchar := COALESCE(item."first_error",''null'');
        let v_first_error_line number := COALESCE(item."first_error_line", -1);
        let  v_first_error_character number:= COALESCE(item."first_error_character", -1);
        let v_first_error_column_name varchar := COALESCE(item."first_error_column_name",''null'');
        v_first_error := (SELECT REPLACE(:v_first_error, '''''''', ''''));// Delete quotes in the string

        // Write into monitoring table 
         query := ''insert into ''|| monitoring_table_name ||''(file, layer, status, ingestion_time, rows_parsed, rows_loaded, errors_seen, first_error, first_error_line, first_error_character_position, first_error_column_name) 
values(\\''''|| v_file_name ||''\\'', ''|| layer ||'', \\''''|| v_status || ''\\'', CURRENT_TIMESTAMP(3), '' || v_rows_parsed ||'', ''|| v_rows_loaded ||'', ''|| v_errors_seen ||'', \\''''|| v_first_error ||''\\'', ''|| v_first_error_line ||'', ''
|| v_first_error_character ||'', \\'''' || v_first_error_column_name || ''\\'')'';

         execute immediate query;

        //Move source files with target directory depending on Copy ingestion result status
        let  landing_file_abs_path := ''@'' || src_schema || ''.'' || v_file_name;
        let dateTimeFormat VARCHAR := to_char(dateTime, ''YYYY-MM-DD-HH24:MI:SS'');
        IF (v_status = ''LOADED'') THEN
            //copy files into :archive_abs_path from :archive_file_abs_path;
            query := ''copy files into '' || archive_abs_path ||   dateTimeFormat ||  ''/'' ||'' from '' || landing_file_abs_path;
            execute immediate query;
        ELSE 
            //copy files into :error_abs_path from :error_file_abs_path;
            query := ''copy files into '' || error_abs_path  ||   dateTimeFormat ||  ''/'' || '' from '' || landing_file_abs_path;
            execute immediate query;
        END IF;

        
    END FOR;
  
   //Remove files and directory
   query := ''REMOVE '' || landing_directory_abs_path;
   execute immediate query;
   
   RETURN TABLE(query_result);
   

EXCEPTION
  WHEN statement_error THEN
    query_result := (SELECT ''Statement_error'' as ERROR_TYPE, :sqlcode as SQL_CODE, :sqlerrm as ERROR_MESS , 
    :sqlstate as STATE, ''FAILED'' as "status", :query as QUERY);
    insert into monitoring_layer.monitoring_ingest(file, layer, status, rows_parsed, rows_loaded, ingestion_time, FIRST_ERROR)  values(:builded_file_name, ''BRONZE_LAYER'', ''LOAD_FAILED'', 0, 0, CURRENT_TIMESTAMP(3), :sqlerrm);
    return table(query_result);
  
  WHEN EXPRESSION_ERROR THEN
     query_result := (SELECT ''Expression_error'' as ERROR_TYPE, :sqlcode as SQL_CODE, :sqlerrm as ERROR_MESS,
     :sqlstate as STATE, ''FAILED'' as "status", :query as QUERY);
     insert into monitoring_layer.monitoring_ingest(file, layer, status, rows_parsed, rows_loaded, ingestion_time, FIRST_ERROR)  values(:builded_file_name, ''BRONZE_LAYER'', ''LOAD_FAILED'', 0, 0, CURRENT_TIMESTAMP(3), :sqlerrm);
    return table(query_result);
    
  WHEN OTHER THEN
    query_result := (SELECT ''Other'' as ERROR_TYPE, :sqlcode as SQL_CODE, :sqlerrm as ERROR_MESSAGE , 
    :sqlstate as SQL_STATE, ''FAILED'' as "status", :query as QUERY);
     insert into monitoring_layer.monitoring_ingest(file, layer, status,rows_parsed, rows_loaded, ingestion_time, FIRST_ERROR)  values(:builded_file_name, ''BRONZE_LAYER'', ''LOAD_FAILED'', 0, 0, CURRENT_TIMESTAMP(3), :sqlerrm);
    return table(query_result);

END';