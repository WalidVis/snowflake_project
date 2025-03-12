PUT file://../schemachange/notebooks/INGEST_RAW_FILES_INTO_BRONZE_LAYER.ipynb @~/staged_notebooks;


CREATEOR OR REPLACE NOTEBOOK INGEST_RAW_FILES_INTO_BRONZE_LAYER
  FROM '@~/staged_notebooks' 
  MAIN_FILE = 'INGEST_RAW_FILES_INTO_BRONZE_LAYER.ipynb' 
   COMMENT = 'Notebook to ingest file data into bronze layer tables';