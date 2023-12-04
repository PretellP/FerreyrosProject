import pandas_gbq as pgbp
from os_environ import osEnviron
from settings import config
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import datetime as dt
import pytz

osEnviron.set_os_environ(config.KEY_PATH)

PROJECT_ID = config.PROJECT_ID
CONFIG_TABLE = "config.CNF_CDM_CONFIG"

sql_conf_tbl = f"""
SELECT 
TABLA, DATASET_STAGE,
DATASET_DESTINO,
COLUMNAS
FROM {CONFIG_TABLE}
WHERE DOMINIO = "{config.DOMAIN_NAME}"
ORDER BY TABLA
"""

class RawToProcessed:
    
    def check_table_existence(self, client, TABLE_ID):
        
        try:
            table = client.get_table(TABLE_ID)
            if table:
                return True
        except NotFound as error:
            print(f'la tabla {TABLE_ID} no existe. Creando tabla...')
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
    
    def update_processed_table(self, client, DEST_TABLE_ID, ORIG_TABLE_ID, COLUMNS, LOAD_TMST):
        
        sql_get_table = f"""
        SELECT * FROM {ORIG_TABLE_ID} LIMIT 1;
        """
        df_origin = pgbp.read_gbq(sql_get_table, project_id=PROJECT_ID)
        month_field = self.get_month_column(df_origin)
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
    
    def create_processed_table(self, client, DEST_TABLE, ORIG_TABLE, COLUMNS, LOAD_TMST):
        
        sql_create = f"""
                        CREATE TABLE {DEST_TABLE}
                        PARTITION BY DATE(FECHA_CARGA) AS 
                        SELECT {COLUMNS}, SESSION_USER() AS USUARIO,
                        TIMESTAMP('{LOAD_TMST}') AS FECHA_CARGA
                        FROM {ORIG_TABLE}
                    """
        
        query = client.query(sql_create)
        query.result()
        
        print(f'Tabla {DEST_TABLE} creada.')
        
    


client = bigquery.Client(project = PROJECT_ID)
cnfg_tbls = pgbp.read_gbq(sql_conf_tbl, project_id=PROJECT_ID)

r_to_p = RawToProcessed()

for index, config_table in cnfg_tbls.iterrows():
    
    LOAD_TIMESTAMP = dt.datetime.now(pytz.timezone('America/Lima'))
    
    TABLE = config_table['TABLA']
    DATASET_DESTINATION = config_table['DATASET_DESTINO']
    DATASET_STAGE = config_table['DATASET_STAGE']
    COLUMNS = config_table['COLUMNAS']
    
    DEST_TABLE_ID = f"{PROJECT_ID}.{DATASET_DESTINATION}.{TABLE}"
    ORIG_TABLE_ID = f"{PROJECT_ID}.{DATASET_STAGE}.{TABLE}"
    
    table_exists = r_to_p.check_table_existence(client, DEST_TABLE_ID)

    if table_exists:
        r_to_p.update_processed_table(client, DEST_TABLE_ID, ORIG_TABLE_ID, COLUMNS, LOAD_TIMESTAMP)
    else:
        r_to_p.create_processed_table(client, DEST_TABLE_ID, ORIG_TABLE_ID, COLUMNS, LOAD_TIMESTAMP)
else:
    print(f"NO HAY REGISTROS EN LA TABLA {CONFIG_TABLE}")
        