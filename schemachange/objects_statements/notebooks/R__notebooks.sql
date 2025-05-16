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
           - BATCH_LOAD_AZURE_TO_RAW
------------------------------------------------------------------------------- */

---------------- PUT Notebooks into 'landing_internal_stage' ---------------------

PUT file://objects_statements/notebooks/BATCH_LOAD_AZURE_TO_RAW.ipynb @raw_layer.landing_internal_stage/staged_notebooks
AUTO_COMPRESS = FALSE;

------------------------------ CREATE Notebooks ----------------------------------

CREATE OR REPLACE NOTEBOOK ORCHESTRATION_SCHEMA.BATCH_LOAD_AZURE_TO_RAW
FROM '@raw_layer.landing_internal_stage/staged_notebooks' 
MAIN_FILE = 'BATCH_LOAD_AZURE_TO_RAW.ipynb' 
QUERY_WAREHOUSE = '{{ ENVIRONMENT }}_WH'
COMMENT = 'Notebook to copy data files from Azure blob to landing internal stage';

-- To execute a notebook, you must add a live version to it first.
-- L’exemple suivant définit la version LAST actuelle de my_notebook sur la version LIVE :
-- https://docs.snowflake.com/fr/sql-reference/sql/alter-notebook

ALTER NOTEBOOK ORCHESTRATION_SCHEMA.BATCH_LOAD_AZURE_TO_RAW ADD LIVE VERSION FROM LAST;


REMOVE @raw_layer.landing_internal_stage/staged_notebooks