/* ------------------------------------------------------------------------------
   File Name: 
       R__procedures.sql

   Authors:
       Matthieu NOIRET

   Description: 
       This SQL script sets up all the required stored procedures for our Data workflows

   Schemas: 
       ORCHESTRATION_SCHEMA

   Objects Created:
       1. Procedures:
           - INGEST_FILE_PROC
           - BRONZE_TO_SILVER_CLEAN_PROC
           - EXECUTE_ALL_TASKS_BRONZE
------------------------------------------------------------------------------- */

-------------------------------------------- Create Procedures ---------------------

CREATE OR REPLACE PROCEDURE ORCHESTRATION_SCHEMA.INGEST_FILE_PROC("BRONZE_TABLE_NAME" VARCHAR)
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    file_format VARCHAR;
    snowflake_file_format VARCHAR;
    builded_file_name VARCHAR;
    stage_suffix_directory_path VARCHAR DEFAULT ''/'' || :BRONZE_TABLE_NAME || ''/'';
    src_schema VARCHAR DEFAULT ''raw_layer'';
    stage_name VARCHAR DEFAULT ''@raw_layer.landing_internal_stage'';
    pattern_file_name VARCHAR;
    on_error_option VARCHAR DEFAULT ''CONTINUE'';

    landing_directory_abs_path VARCHAR;
    archive_abs_path VARCHAR;
    error_abs_path VARCHAR;
    layer VARCHAR(30) DEFAULT ''BRONZE_LAYER'';
    monitoring_table_name VARCHAR DEFAULT ''monitoring_layer.monitoring_ingest'';
    query_result RESULTSET DEFAULT (SELECT -1);
    query VARCHAR;
    dateTime DATETIME DEFAULT CURRENT_TIMESTAMP();

BEGIN
    -- Récupération du FILE_FORMAT depuis la table de paramétrage
    SELECT FILE_FORMAT
    INTO :file_format
    FROM ORCHESTRATION_SCHEMA.TABLE_NAMES_MANPRM
    WHERE TABLE_NAME = :BRONZE_TABLE_NAME;

    -- Déduction du format Snowflake à utiliser
    SELECT CASE FILE_FORMAT
         WHEN ''CSV'' THEN ''bronze_layer.csv_file_format_semicolon''
         WHEN ''JSON'' THEN ''bronze_layer.json_file_format''
       END
    INTO :snowflake_file_format
    FROM ORCHESTRATION_SCHEMA.TABLE_NAMES_MANPRM
    WHERE TABLE_NAME = :BRONZE_TABLE_NAME;

    -- Construction des autres variables
    pattern_file_name := ''.*.'' || LOWER(:file_format);
    builded_file_name := stage_suffix_directory_path || pattern_file_name;

    landing_directory_abs_path := :stage_name || :stage_suffix_directory_path;
    archive_abs_path := ''@'' || :src_schema || ''.archive_internal_stage'' || :stage_suffix_directory_path;
    error_abs_path := ''@'' || :src_schema || ''.error_internal_stage'' || :stage_suffix_directory_path;
    
    query := ''COPY INTO '' || ''BRONZE_LAYER.'' || :BRONZE_TABLE_NAME || ''_BRZ'' || '' FROM '' || landing_directory_abs_path || ''
        FILE_FORMAT = (FORMAT_NAME='''''' || snowflake_file_format || '''''')
        PATTERN = '''''' || pattern_file_name || ''''''
        ON_ERROR = '' || on_error_option || ''
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        INCLUDE_METADATA = (create_date = METADATA$START_SCAN_TIME, file_name = METADATA$FILENAME)'';

    query_result := (EXECUTE IMMEDIATE :query);

    FOR item IN query_result DO
        LET v_status VARCHAR := item."status";

        IF (v_status != ''LOADED'' AND v_status != ''LOAD_FAILED'' AND v_status != ''PARTIALLY_LOADED'') THEN
            INSERT INTO monitoring_layer.monitoring_ingest(file, layer, status, rows_parsed, rows_loaded, ingestion_time, FIRST_ERROR)
            VALUES(:builded_file_name, :layer, :v_status, 0, 0, CURRENT_TIMESTAMP(3), ''No files OR Files already loaded'');
            BREAK;
        END IF;

        LET v_file_name VARCHAR := COALESCE(item."file", ''null'');
        LET v_rows_parsed NUMBER := item."rows_parsed";
        LET v_rows_loaded NUMBER := item."rows_loaded";
        LET v_errors_seen NUMBER := item."errors_seen";
        LET v_first_error VARCHAR := COALESCE(item."first_error", ''null'');
        LET v_first_error_line NUMBER := COALESCE(item."first_error_line", -1);
        LET v_first_error_character NUMBER := COALESCE(item."first_error_character", -1);
        LET v_first_error_column_name VARCHAR := COALESCE(item."first_error_column_name", ''null'');

        v_first_error := (SELECT REPLACE(:v_first_error, '''''''''''', ''''''''));

        query := ''INSERT INTO '' || monitoring_table_name || ''(file, layer, status, ingestion_time, rows_parsed, rows_loaded, errors_seen, first_error, first_error_line, first_error_character_position, first_error_column_name)
        VALUES('''''' || v_file_name || '''''', '''''' || layer || '''''', '''''' || v_status || '''''', CURRENT_TIMESTAMP(3), '' || v_rows_parsed || '', '' || v_rows_loaded || '', '' || v_errors_seen || '', '''''' || v_first_error || '''''', '' || v_first_error_line || '', '' || v_first_error_character || '', '''''' || v_first_error_column_name || '''''')'';

        EXECUTE IMMEDIATE query;

        LET landing_file_abs_path := ''@'' || src_schema || ''.'' || v_file_name;
        LET dateTimeFormat VARCHAR := TO_CHAR(dateTime, ''YYYY-MM-DD-HH24:MI:SS'');

        IF (v_status = ''LOADED'') THEN
            query := ''COPY FILES INTO '' || archive_abs_path || dateTimeFormat || ''/'' || '' FROM '' || landing_file_abs_path;
        ELSE
            query := ''COPY FILES INTO '' || error_abs_path || dateTimeFormat || ''/'' || '' FROM '' || landing_file_abs_path;
        END IF;

        EXECUTE IMMEDIATE query;

    END FOR;

    query := ''REMOVE '' || landing_directory_abs_path;
    EXECUTE IMMEDIATE query;
    
    RETURN TABLE(query_result);

EXCEPTION
    WHEN STATEMENT_ERROR THEN
        query_result := (SELECT ''Statement_error'' AS ERROR_TYPE, :SQLCODE AS SQL_CODE, :SQLERRM AS ERROR_MESS,
                         :SQLSTATE AS STATE, ''FAILED'' AS "status", :query AS QUERY);
        INSERT INTO monitoring_layer.monitoring_ingest(file, layer, status, rows_parsed, rows_loaded, ingestion_time, FIRST_ERROR)
        VALUES(:builded_file_name, :layer, ''LOAD_FAILED'', 0, 0, CURRENT_TIMESTAMP(3), :SQLERRM);
        RETURN TABLE(query_result);

    WHEN EXPRESSION_ERROR THEN
        query_result := (SELECT ''Expression_error'' AS ERROR_TYPE, :SQLCODE AS SQL_CODE, :SQLERRM AS ERROR_MESS,
                         :SQLSTATE AS STATE, ''FAILED'' AS "status", :query AS QUERY);
        INSERT INTO monitoring_layer.monitoring_ingest(file, layer, status, rows_parsed, rows_loaded, ingestion_time, FIRST_ERROR)
        VALUES(:builded_file_name, :layer, ''LOAD_FAILED'', 0, 0, CURRENT_TIMESTAMP(3), :SQLERRM);
        RETURN TABLE(query_result);

    WHEN OTHER THEN
        query_result := (SELECT ''Other'' AS ERROR_TYPE, :SQLCODE AS SQL_CODE, :SQLERRM AS ERROR_MESSAGE,
                         :SQLSTATE AS SQL_STATE, ''FAILED'' AS "status", :query AS QUERY);
        INSERT INTO monitoring_layer.monitoring_ingest(file, layer, status, rows_parsed, rows_loaded, ingestion_time, FIRST_ERROR)
        VALUES(:builded_file_name, :layer, ''LOAD_FAILED'', 0, 0, CURRENT_TIMESTAMP(3), :SQLERRM);
        RETURN TABLE(query_result);
END;
';

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

CREATE OR REPLACE PROCEDURE ORCHESTRATION_SCHEMA.BRONZE_TO_SILVER_CLEAN_PROC("BRONZE_TABLE_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS '
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit, current_timestamp, array_construct

def run(session: Session, bronze_table_name):
    query = f"""
        SELECT SILVER_TABLE_NAME, SILVER_TABLE_TECHNICAL_KEY_FIELDS, SILVER_PRIMARY_KEY
        FROM TABLE_NAMES_MANPRM 
        WHERE TABLE_NAME = ''{bronze_table_name}''
    """
    
    result = session.sql(query).collect()
    if not result:
        return f"Aucun paramètre trouvé pour la table {bronze_table_name}"

    for row in result:
        silver_table_name = row[''SILVER_TABLE_NAME'']
        silver_pk = row[''SILVER_PRIMARY_KEY'']
        silver_technical_key = silver_pk.replace(''KEY'', ''INTKEY'')
        technical_key_fields = row[''SILVER_TABLE_TECHNICAL_KEY_FIELDS'']

        if not technical_key_fields:
            return f"Clé technique manquante pour {bronze_table_name}"

        technical_key_fields_list = [field.strip() for field in technical_key_fields.split('','')]
        cleaned_fields = [
            f"COALESCE(REPLACE(TO_VARCHAR({field}), '' '', ''''), ''N/A'')" 
            for field in technical_key_fields_list
        ]
        technical_key_query = f"CONCAT({'', ''.join(cleaned_fields)})"

        select_query = f"""
            SELECT 
                HASH({technical_key_query}) AS {silver_technical_key}, 
                {technical_key_query} AS {silver_pk},
                * EXCLUDE(CREATE_DATE, FILE_NAME),
                CURRENT_TIMESTAMP() AS SYS_DATE_CREATE
            FROM BRONZE_LAYER.{bronze_table_name}_BRZ
        """

        try:
            df = session.sql(select_query)
            df.write.mode("overwrite").save_as_table(f"SILVER_LAYER.{silver_table_name}")

            # Récupération des infos de monitoring
            monitor_query = """
                SELECT 
                    EXECUTION_STATUS, 
                    ERROR_MESSAGE, 
                    ROWS_PRODUCED, 
                    ROWS_INSERTED
                FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION())
                WHERE QUERY_ID = LAST_QUERY_ID()
                ORDER BY START_TIME DESC LIMIT 1
            """
            monitoring = session.sql(monitor_query)

            # Construction du DataFrame de log
            insert_result_df = monitoring.to_df()
            monitoring_final_df = insert_result_df \\
                .with_column("src_table", array_construct(lit(f"BRONZE_LAYER.{bronze_table_name}_BRZ"))) \\
                .with_column("ingestion_time", current_timestamp()) \\
                .with_column("layer", lit("SILVER_LAYER"))

            # Écriture dans la table de monitoring
            monitoring_final_df.write.mode("append").save_as_table(
                "MONITORING_LAYER.MONITORING_INGEST", column_order="name"
            )

            return f"Données chargées dans {silver_table_name} et monitoring enregistré."

        except Exception as e:
            return f"Erreur lors de l''insertion dans {silver_table_name} : {str(e)}"
';

---------------------------------------------------
--------------------------------------------------------------------------
---------------------------------------------------

CREATE OR REPLACE PROCEDURE ORCHESTRATION_SCHEMA.EXECUTE_ALL_TASKS_BRONZE()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS ' 
from snowflake.snowpark import Session
def run(session: Session):
    query = "SELECT TABLE_NAME, FILE_FORMAT FROM TABLE_NAMES_MANPRM"
    
    result = session.sql(query).collect()

    for row in result:
        nom_de_table = row[''TABLE_NAME'']
        format_de_fichier = row[''FILE_FORMAT'']
        
        task_name = f"BRZ_INGEST_{nom_de_table}_{format_de_fichier}"
        
        execute_task_query = f"EXECUTE TASK {task_name}"
        try:
            session.sql(execute_task_query).collect()
            print(f"Tâche {task_name} exécutée avec succès.")
        except Exception as e:
            print(f"Erreur lors de l''exécution de la tâche {task_name}: {str(e)}")
            
    return "Toutes les tâches ont été traitées."

';