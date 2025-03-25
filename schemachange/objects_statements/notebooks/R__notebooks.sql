--PUT the notebook files in the landing stage
-- Working directory is schemachange directory (set in the CI and CD schemachange step)
PUT file://objects_statements/notebooks/INGEST_RAW_FILES_INTO_BRONZE_LAYER.ipynb @raw_layer.landing_internal_stage/staged_notebooks
AUTO_COMPRESS = FALSE;

PUT file://objects_statements/notebooks/INGEST_INTO_SILVER_LAYER.ipynb @raw_layer.landing_internal_stage/staged_notebooks
AUTO_COMPRESS = FALSE;

CREATE OR REPLACE NOTEBOOK BRONZE_LAYER.INGEST_RAW_FILES_INTO_BRONZE_LAYER
FROM '@raw_layer.landing_internal_stage/staged_notebooks' 
 MAIN_FILE = 'INGEST_RAW_FILES_INTO_BRONZE_LAYER.ipynb' 
 QUERY_WAREHOUSE = '{{ ENVIRONMENT }}_WH'
COMMENT = 'Notebook to ingest file data into bronze layer tables';

CREATE OR REPLACE NOTEBOOK SILVER_LAYER.INGEST_INTO_SILVER_LAYER
FROM '@raw_layer.landing_internal_stage/staged_notebooks' 
MAIN_FILE = 'INGEST_INTO_SILVER_LAYER.ipynb' 
QUERY_WAREHOUSE = '{{ ENVIRONMENT }}_WH'
  COMMENT = 'Notebook to ingest data from bronze tables to silver layer tables';



-- To execute a notebook, you must add a live version to it first.
-- L’exemple suivant définit la version LAST actuelle de my_notebook sur la version LIVE :
-- https://docs.snowflake.com/fr/sql-reference/sql/alter-notebook

 ALTER NOTEBOOK BRONZE_LAYER.INGEST_RAW_FILES_INTO_BRONZE_LAYER ADD LIVE VERSION FROM LAST;
ALTER NOTEBOOK BRONZE_LAYER.INGEST_INTO_SILVER_LAYER ADD LIVE VERSION FROM LAST;


REMOVE @raw_layer.landing_internal_stage/staged_notebooks