import pandas_gbq as pgbp
import pandas as pd
from os_environ import osEnviron
from settings import config
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import datetime as dt
from google.cloud import storage
from gcsfs import GCSFileSystem
import os

osEnviron.set_os_environ(config.KEY_PATH)

PROJECT_ID = config.PROJECT_ID
CONFIG_TABLE = "config.CNF_CDM_CONFIG"
CMD_LOG_TABLE = "log.CDM_log"
DOMAIN_NAME = config.DOMAIN_NAME
SESSION_USER = config.SESSION_USER

BUCKET_NAME = config.BUCKET_NAME
BUCKET_SQL_FOLDER_NAME = config.bucket_sql_folder_name

# destination_blob_name = f"{BUCKET_SQL_FOLDER_NAME}/BD_SERVICIOS_SQL.parquet"

client = storage.Client()
for blob in client.list_blobs(BUCKET_NAME, prefix=BUCKET_SQL_FOLDER_NAME):
    file_name, file_extension = os.path.splitext(blob.name)
    
    if file_extension == '.parquet':
        print(str(blob.name))

# gcs = GCSFileSystem()
# with gcs.open(f"{BUCKET_NAME}/{destination_blob_name}", "rb") as file:
#     df = pd.read_parquet(file)
    
# for column in list(df.columns.values):
#     print(column)
