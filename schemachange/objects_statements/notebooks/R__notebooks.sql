--PUT the notebook files in the user stage
PUT file://notebooks/INGEST_RAW_FILES_INTO_BRONZE_LAYER.ipynb @~/staged_notebooks;


CREATE OR REPLACE NOTEBOOK INGEST_RAW_FILES_INTO_BRONZE_LAYER
  FROM '@~/staged_notebooks' 
  MAIN_FILE = 'INGEST_RAW_FILES_INTO_BRONZE_LAYER.ipynb' 
   COMMENT = 'Notebook to ingest file data into bronze layer tables';