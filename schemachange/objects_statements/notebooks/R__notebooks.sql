/* ------------------------------------------------------------------------------
   File Name: 
       R__notebooks.sql

   Authors:
       Matthieu NOIRET

   Description: 
       This SQL script puts all of the notebooks required for our Data workflows in the landing stage
       in order create and enable them in the ORCHESTRATION_SCHEMA

   Schemas: 
       ORCHESTRATION_SCHEMA

   Objects Created:
       1. Notebooks:
           - INGEST_RAW_FILES_INTO_BRONZE_LAYER
           - INGEST_INTO_SILVER_LAYER
           - INGEST_FACT_RETAIL_INTO_SILVER
------------------------------------------------------------------------------- */

---------------- PUT Notebooks into 'landing_internal_stage' ---------------------

PUT file://objects_statements/notebooks/INGEST_INTO_SILVER_LAYER.ipynb @raw_layer.landing_internal_stage/staged_notebooks
AUTO_COMPRESS = FALSE;

PUT file://objects_statements/notebooks/INGEST_FACT_RETAIL_INTO_SILVER.ipynb @raw_layer.landing_internal_stage/staged_notebooks
AUTO_COMPRESS = FALSE;

PUT file://objects_statements/notebooks/INGEST_RAW_FILES_INTO_BRONZE_LAYER.ipynb @raw_layer.landing_internal_stage/staged_notebooks
AUTO_COMPRESS = FALSE;

------------------------------ CREATE Notebooks ----------------------------------

CREATE OR REPLACE NOTEBOOK ORCHESTRATION_SCHEMA.INGEST_RAW_FILES_INTO_BRONZE_LAYER
FROM '@raw_layer.landing_internal_stage/staged_notebooks' 
MAIN_FILE = 'INGEST_RAW_FILES_INTO_BRONZE_LAYER.ipynb' 
QUERY_WAREHOUSE = '{{ ENVIRONMENT }}_WH'
COMMENT = 'Notebook to ingest file data into bronze layer tables';

CREATE OR REPLACE NOTEBOOK ORCHESTRATION_SCHEMA.INGEST_INTO_SILVER_LAYER
FROM '@raw_layer.landing_internal_stage/staged_notebooks' 
MAIN_FILE = 'INGEST_INTO_SILVER_LAYER.ipynb' 
QUERY_WAREHOUSE = '{{ ENVIRONMENT }}_WH'
COMMENT = 'Notebook to ingest data from bronze tables to silver layer tables';

CREATE OR REPLACE NOTEBOOK ORCHESTRATION_SCHEMA.INGEST_FACT_RETAIL_INTO_SILVER
FROM '@raw_layer.landing_internal_stage/staged_notebooks' 
MAIN_FILE = 'INGEST_FACT_RETAIL_INTO_SILVER.ipynb' 
QUERY_WAREHOUSE = '{{ ENVIRONMENT }}_WH'
COMMENT = 'Notebook to ingest data from bronze tables to silver layer tables';

-- To execute a notebook, you must add a live version to it first.
-- L’exemple suivant définit la version LAST actuelle de my_notebook sur la version LIVE :
-- https://docs.snowflake.com/fr/sql-reference/sql/alter-notebook

ALTER NOTEBOOK ORCHESTRATION_SCHEMA.INGEST_RAW_FILES_INTO_BRONZE_LAYER ADD LIVE VERSION FROM LAST;
ALTER NOTEBOOK ORCHESTRATION_SCHEMA.INGEST_INTO_SILVER_LAYER ADD LIVE VERSION FROM LAST;
ALTER NOTEBOOK ORCHESTRATION_SCHEMA.INGEST_FACT_RETAIL_INTO_SILVER ADD LIVE VERSION FROM LAST;

REMOVE @raw_layer.landing_internal_stage/staged_notebooks