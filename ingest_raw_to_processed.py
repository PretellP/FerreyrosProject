import pandas_gbq as pgbp
from os_environ import osEnviron
from settings import config
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound
import datetime as dt
import pytz
import os

osEnviron.set_os_environ(config.KEY_PATH)

PROJECT_ID = config.PROJECT_ID
CONFIG_TABLE = "config.CNF_CDM_CONFIG"
CMD_LOG_TABLE = "log.CDM_log"
DOMAIN_NAME = config.DOMAIN_NAME
SESSION_USER = config.SESSION_USER

BUCKET_PROC_PARQUET_NAME = config.BUCKET_PROC_PARQUET_NAME
BUCKET_PARQUET_FOLDER = config.BUCKET_PARQUET_FOLDER
BUCKET_PARQUET_HTRY_FOLDER = config.BUCKET_PARQUET_HTRY_FOLDER

local_parquet_temp_folder = config.local_parquet_temp_folder

sql_conf_tbl = f"""
SELECT 
TABLA, DATASET_STAGE,
DATASET_DESTINO,
COLUMNAS
FROM {CONFIG_TABLE}
WHERE DOMINIO = "{DOMAIN_NAME}"
ORDER BY TABLA
"""

class RawToProcessed:
    
    def check_table_existence(self, client, TABLE_ID):
        
        try:
            table = client.get_table(TABLE_ID)
            if table:
                return True
        except NotFound as error:
            print(f'La tabla {TABLE_ID} no existe. Creando tabla...')
            return False
        
    def delete_distinct_date_rows(self, client, DEST_TABLE, ORIG_TABLE, month_field):
        
        sql = f"""
                DELETE FROM `{DEST_TABLE}` a
                WHERE EXISTS 
                (
                    SELECT DATE_TRUNC(DATE(b.{month_field}), MONTH) FROM 
                    `{ORIG_TABLE}` b 
                    WHERE
                    DATE_TRUNC(DATE(a.{month_field}), MONTH) = DATE_TRUNC(DATE(b.{month_field}), MONTH)
                )
                """
        query = client.query(sql)
        query.result()
                
    def get_month_column(self, dataframe):
        
        month_column = "Mes_encuesta"
        for column in dataframe.columns.to_list():
            
            if column.lower() == 'mes_encuesta':    
                month_column = column
                break
            
        return month_column
    
    def process_history_table(self, client, DEST_TABLE_ID, ORIG_TABLE_ID, COLUMNS, LOAD_TMST, TABLE):
        
        history_table_id = f"{DEST_TABLE_ID}_HISTORY"
        
        history_table_exists = self.check_table_existence(client, f"{history_table_id}")
        if history_table_exists:
            
            sql = f"""
                    INSERT INTO `{history_table_id}`
                    SELECT {COLUMNS}, SESSION_USER() AS USUARIO,
                    TIMESTAMP('{LOAD_TMST}') AS FECHA_CARGA FROM
                    `{ORIG_TABLE_ID}`
                """
            query = client.query(sql)
            query.result()
            
            print(f'Tabla {history_table_id} actualizada.')
            
        else:
            sql = f"""
                    CREATE TABLE `{history_table_id}`
                    PARTITION BY DATE(FECHA_CARGA) AS
                    SELECT {COLUMNS}, SESSION_USER() AS USUARIO,
                    TIMESTAMP('{LOAD_TMST}') AS FECHA_CARGA FROM
                    `{ORIG_TABLE_ID}`
                """
            query = client.query(sql)
            query.result()
            
            print(f'Tabla {history_table_id} creada.')
            
        self.insert_to_log_table(client, ORIG_TABLE_ID, TABLE, LOAD_TMST, context="HISTORY")
        
            
    def get_rows_count(self, client, TABLE_ID):
        
        sql = f"""
        BEGIN
            CREATE OR REPLACE TEMP TABLE t0 AS
                SELECT COUNT(*) AS ROWS_COUNT FROM `{TABLE_ID}`;
            
            SELECT ROWS_COUNT FROM t0;
            DROP TABLE IF EXISTS t0;
        END;
            """
        query_job = client.query(sql)
        query_job.result()
    
        for job in client.list_jobs(parent_job=query_job.job_id):
            if job.statement_type == 'SELECT':
                rows = job.result()
                for row in rows:
                    return row['ROWS_COUNT']
            
    
    def insert_to_log_table(self, client, ORIG_TABLE_ID, TABLE_NAME, LOAD_TMST, context:str):
        
        TOTAL_ROWS = self.get_rows_count(client, ORIG_TABLE_ID)
        
        name_sufix = 'RawToProcessedHistory' if context == 'HISTORY' else 'RawToProcessed'
        process_type = f'{TABLE_NAME}_{name_sufix}'
        
        data = [
            {u"DOMINIO": u""+ DOMAIN_NAME +"",
             u"TABLA": u""+ TABLE_NAME +"",
             u"TIPO_DE_PROCESO": u""+ process_type +"",
             u"CANT_REGISTROS": TOTAL_ROWS,
             u"USUARIO": f"{SESSION_USER}",
             u"FECHA_CARGA": f"{LOAD_TMST}"
             }
        ]
        
        errors = client.insert_rows_json(CMD_LOG_TABLE, data)
        if errors != []:
            print(f"Se encontraron errores al insertar los registros: {errors}")
        
        
    def update_processed_table(self, client, DEST_TABLE_ID, ORIG_TABLE_ID, COLUMNS, LOAD_TMST, TABLE):
        
        sql_get_table = f"""
        SELECT * FROM {ORIG_TABLE_ID} LIMIT 1;
        """
        df_origin = pgbp.read_gbq(sql_get_table, project_id=PROJECT_ID)
        month_field = self.get_month_column(df_origin)
        
        self.process_history_table(client, DEST_TABLE_ID, ORIG_TABLE_ID, COLUMNS, LOAD_TMST, TABLE)
        
        self.delete_distinct_date_rows(client, DEST_TABLE_ID, ORIG_TABLE_ID, month_field)
        
        sql_insert_raw_to_processed = f"""
                                        INSERT INTO 
                                        {DEST_TABLE_ID}
                                        SELECT {COLUMNS}, SESSION_USER() AS USUARIO, 
                                        TIMESTAMP('{LOAD_TMST}') AS FECHA_CARGA
                                        FROM {ORIG_TABLE_ID}
                                    """
        query = client.query(sql_insert_raw_to_processed)
        query.result()

        print(f'Tabla {DEST_TABLE_ID} actualizada.')
    
    def create_processed_table(self, client, DEST_TABLE_ID, ORIG_TABLE_ID, COLUMNS, LOAD_TMST, TABLE):
        
        self.process_history_table(client, DEST_TABLE_ID, ORIG_TABLE_ID, COLUMNS, LOAD_TMST, TABLE)
        
        sql_create = f"""
                        CREATE TABLE {DEST_TABLE_ID}
                        PARTITION BY DATE(FECHA_CARGA) AS 
                        SELECT {COLUMNS}, SESSION_USER() AS USUARIO,
                        TIMESTAMP('{LOAD_TMST}') AS FECHA_CARGA
                        FROM {ORIG_TABLE_ID}
                    """
        
        query = client.query(sql_create)
        query.result()
        
        print(f'Tabla {DEST_TABLE_ID} creada.')
        
    def convert_to_parquet(self, query, TABLE):
        
        df = pgbp.read_gbq(query, project_id=PROJECT_ID)
        local_file_path = f"{local_parquet_temp_folder}/{TABLE}.parquet"
        df.to_parquet(local_file_path)
        
        return local_file_path
    
    def get_local_file_path(self, title, local_folder):
        
        return f"{local_folder}/{title}.parquet"
    
    def upload_parquet_to_blob(self, bucket, bucket_folder, table_name, local_file_path):
        
        destination_blob_name = f"{bucket_folder}/{table_name}.parquet"
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(local_file_path)
        os.remove(local_file_path)
        
        print(f'Archivo {local_file_path} subido a {destination_blob_name}.')
        
    def upload_prc_parquets_to_blob(self, str_client, DEST_TABLE_ID, TABLE):
        
        query_proc = f"""
        SELECT * FROM `{DEST_TABLE_ID}`;
        """
        query_hist = f"""
        SELECT * FROM `{DEST_TABLE_ID}_HISTORY`;
        """
        
        now = dt.datetime.now()
        history_file_name  = f"{TABLE}_HISTORY_{now.strftime('%Y%m%d_%H%M%S')}"
        
        local_file_prc = self.convert_to_parquet(query_proc, TABLE=TABLE)
        local_file_htr = self.convert_to_parquet(query_hist, TABLE=history_file_name)
        
        bucket = str_client.bucket(BUCKET_PROC_PARQUET_NAME)
        
        self.upload_parquet_to_blob(bucket, BUCKET_PARQUET_FOLDER, TABLE, local_file_prc)
        self.upload_parquet_to_blob(bucket, BUCKET_PARQUET_HTRY_FOLDER, history_file_name, local_file_htr)
        
        
    def ingest_row_to_processed_massive(self):
        
        client = bigquery.Client(project = PROJECT_ID)
        cnfg_tbls = pgbp.read_gbq(sql_conf_tbl, project_id=PROJECT_ID)

        for index, config_table in cnfg_tbls.iterrows():

            LOAD_TIMESTAMP = dt.datetime.now(pytz.timezone('America/Lima'))

            TABLE = config_table['TABLA']
            DATASET_DESTINATION = config_table['DATASET_DESTINO']
            DATASET_STAGE = config_table['DATASET_STAGE']
            COLUMNS = config_table['COLUMNAS']

            DEST_TABLE_ID = f"{PROJECT_ID}.{DATASET_DESTINATION}.{TABLE}"
            ORIG_TABLE_ID = f"{PROJECT_ID}.{DATASET_STAGE}.{TABLE}"

            proc_table_exists = self.check_table_existence(client, DEST_TABLE_ID)

            if proc_table_exists:
                self.update_processed_table(client, DEST_TABLE_ID, ORIG_TABLE_ID, COLUMNS, LOAD_TIMESTAMP, TABLE)
            else:
                self.create_processed_table(client, DEST_TABLE_ID, ORIG_TABLE_ID, COLUMNS, LOAD_TIMESTAMP, TABLE)

            storage_client = storage.Client()
            self.upload_prc_parquets_to_blob(storage_client, DEST_TABLE_ID, TABLE)
            self.insert_to_log_table(client, ORIG_TABLE_ID, TABLE, LOAD_TIMESTAMP, context="PROD")
        