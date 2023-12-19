import pandas as pd
import requests
import logging
from requests.auth import HTTPBasicAuth
#import xmltodict
from utils import Utils
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os
from settings import config
## Ignorar las advertencias
import warnings 
warnings.filterwarnings('ignore')


'''
Change environment variable to develop, production o queality
'''
#------------ dev, prd, qa --------------
env = 'dev'
#----------------------------------------


from dotenv import load_dotenv
try:
    if env == 'prd':
        load_dotenv('.env.prd')
    elif env == 'dev':
        load_dotenv('.env.dev')
    elif env == 'qa':
        load_dotenv('.env.qa')
except Exception as e:
    result = 'Ocurrio Algun error en ejecucion: '+str(e)


DOMAIN_NAME = config.PARSE_CONF_DOMAIN
NAME_PROJECT = config.PROJECT_ID
PATH_GCLOUD_AUTH_JSON = config.KEY_PATH
NAME_DATASET_BQ_CONF = config.DATA_SET_CONF
VAR_ORIGEN_DS = config.DATA_SET_RAW
VAR_DESTINO_DS = config.DATA_SET_PRC
VAR_BOOL_TRUNCATE = config.VAR_BOOL_TRUNCATE
NAME_DATASET_TABLE = config.DATA_SET_CONF_TABLE
SCHEMAS_PATH = config.SCHEMAS_PATH


credentials = service_account.Credentials.from_service_account_file(PATH_GCLOUD_AUTH_JSON, scopes=["https://www.googleapis.com/auth/cloud-platform"])
client_bigquery = bigquery.Client(credentials=credentials, project=NAME_PROJECT)

class ParserConfig:
    
    def cast_type(self, column_name, column_type):
        if column_type == 'INT64':
            return f"CASE WHEN TRIM(`{column_name}`) = '' THEN NULL ELSE CAST(`{column_name}` AS INT64) END AS `{column_name}`"
        elif column_type == 'FLOAT64':
            return f"CASE WHEN TRIM({column_name}) = '' THEN NULL ELSE SAFE_CAST({column_name} AS FLOAT64) END AS {column_name}"
        elif column_type == 'DATE':
            return f"CASE WHEN TRIM({column_name}) = '' THEN NULL ELSE PARSE_DATE('%Y-%m-%d', {column_name}) END AS {column_name}"
        elif column_type == 'DATETIME':
            return f"CASE WHEN REGEXP_CONTAINS({column_name}, r'^\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}$') THEN PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*S', {column_name}) WHEN REGEXP_CONTAINS({column_name}, r'^\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}Z$') THEN PARSE_DATETIME('%Y-%m-%dT%H:%MZ', {column_name}) WHEN REGEXP_CONTAINS({column_name}, r'^\d{{4}}-\d{{2}}-\d{{2}} \d{{2}}:\d{{2}}Z$') THEN PARSE_DATETIME('%Y-%m-%d %H:%MZ', {column_name}) WHEN REGEXP_CONTAINS({column_name}, r'^\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}$') THEN PARSE_DATETIME('%Y-%m-%dT%H:%M', {column_name}) WHEN REGEXP_CONTAINS({column_name}, r'^\d{{4}}-\d{{2}}-\d{{2}} \d{{2}}:\d{{2}}$') THEN PARSE_DATETIME('%Y-%m-%d %H:%M', {column_name}) WHEN REGEXP_CONTAINS({column_name}, r'^\d{{4}}-\d{{2}}-\d{{2}} \d{{2}}:\d{{2}}:\d{{2}}$') THEN PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S', {column_name}) WHEN REGEXP_CONTAINS({column_name}, r'^\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}Z$') THEN PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*SZ', {column_name}) WHEN REGEXP_CONTAINS({column_name}, r'^\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}\.\d{{3}}Z$') THEN PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*SZ', {column_name}) ELSE NULL END AS {column_name}"
        if column_type == 'BOOL':
            return f"CASE WHEN TRIM({column_name}) = '' THEN NULL ELSE CAST({column_name} AS BOOLEAN) END AS {column_name}"
        else:
            return column_name

    def get_join_column(self, list_pkey):
        str_join = ''
        for i in list_pkey:
            str_join = str_join + f"A.{i} = B.{i}"
            if i != list_pkey[-1] and len(list_pkey)>1:
                str_join = str_join + ' AND '

        return str_join


    def create_compare_notprimary_key(self, column_name, column_type):
        if column_type == 'INT64' or column_type == 'FLOAT64':
            return f"COALESCE(A.{column_name},0) <> COALESCE(B.{column_name},0)"
        elif column_type == 'DATETIME':
            return f"COALESCE(A.{column_name},'9999-12-31 00:00:00') <> COALESCE(B.{column_name},'9999-12-31 00:00:00')"
        elif column_type == 'DATE':
            return f"COALESCE(A.{column_name},'9999-12-31') <> COALESCE(B.{column_name},'9999-12-31')"
        elif column_type == 'BOOL':
            return f"COALESCE(A.{column_name},False) <> COALESCE(B.{column_name},False)"
        else:
            return f"COALESCE(A.{column_name},'') <> COALESCE(B.{column_name},'')"
        

    def generate_config_rows(self, GNF_TABLES_NAME, config_rows_list):

        config_rows_insert = []
        dict_table = {}

        dict_table["DOMINIO"] = config.DOMAIN_NAME
        dict_table["TABLA"] = GNF_TABLES_NAME
        dict_table["ORIGEN"] = DOMAIN_NAME
        dict_table["DATASET_STAGE"] = VAR_ORIGEN_DS
        dict_table["DATASET_DESTINO"] = VAR_DESTINO_DS
        dict_table["COLUMNAS"] = f"{config_rows_list[0]};{config_rows_list[1]};{config_rows_list[2]}"
        dict_table['SQL_1'] = None
        dict_table["PK"] = None
        #dict_table["PK"] = self.get_join_column(fields_primary_key)
        dict_table["TRUNCATE_TABLE"] = VAR_BOOL_TRUNCATE
        #dict_table["NPK"] = " OR ".join(compare_notprimary_key)
        dict_table["NPK"] = None
        config_rows_insert.append(dict_table)

        return config_rows_insert
    
    
    def generate_config_principal_table(self, raw_name_schema, sql_name_schema):
        utils = Utils()
        raw_object_schema = utils.read_schema_object(raw_name_schema)
        sql_object_schema = utils.read_schema_object(sql_name_schema)
        
        config_list = list(map(lambda v: {'name': v["name"], 'master_type': v['master_type']}, raw_object_schema['fields']))
        sql_list = list(map(lambda v: {'name': v["name"], 'master_type': v['master_type']}, sql_object_schema['fields']))

        for sql_item in sql_list:
            if sql_item['name'].upper() not in list(map(lambda v: v['name'].upper(), config_list)):
                config_list.append(sql_item)
        
        config_values_str = ",".join(list(map(lambda v: f'{v["name"]} {v["master_type"]}', config_list)))
        raw_rows_str = ",".join(list(map(lambda v: self.cast_type(v['name'], v['master_type']), raw_object_schema['fields'])))
        sql_rows_str = ",".join(list(map(lambda v: self.cast_type(v['name'], v['master_type']), sql_object_schema['fields'])))
         
        return [config_values_str, raw_rows_str, sql_rows_str]
        

    def delet_to_update_row(self, name_table):
        table_id = f"`{NAME_PROJECT}.{NAME_DATASET_BQ_CONF}.{NAME_DATASET_TABLE}`"
        query_client = f"""
                DELETE FROM {table_id} WHERE TABLA = @name_table
                """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("name_table", "STRING", name_table),
            ])

        try:
            execute_bigquery = client_bigquery.query(query_client, job_config=job_config)
            result = execute_bigquery.result()
        except Exception as e:
            logging.error("Encountered errors while delet row: {}".format(e))


    def run_generation_bigquery(self, name_schema):
        rows_insert_json, name_table_schema = self.generate_config_rows(name_schema)
        #rows_insert_tuplas = list(map(lambda v: tuple(v.values()),rows_insert_json)) 
        table_id = f"{NAME_PROJECT}.{NAME_DATASET_BQ_CONF}.{NAME_DATASET_TABLE}"
        #TODO: Mejorar ya que es striming y no sierra sesion
        '''
        try:
            print(table_id)
            table_bigquery = client_bigquery.get_table(table_id)
            errors = client_bigquery.insert_rows(table_bigquery, rows_insert_tuplas)
            client_bigquery.close()
            print(errors)
            return 'OK'
        except Exception as e:
            print(f"Ocurrio error al insertar en: {str(e)}")
            return f"Ocurrio error al insertar en: {str(e)}!"
        '''
        
    def run_generation_pandas_to_gbq(self, TABLES_DICT, table_name):
        
        raw_table_name = TABLES_DICT.get(table_name)[0]
        sql_table_name = TABLES_DICT.get(table_name)[1]
        
        raw_schema_name = f'{raw_table_name}.input.schema'
        sql_schema_name = f'{sql_table_name}.input.schema'
        
        GNF_TABLES_NAME = f'{table};{raw_table_name};{sql_table_name}'
        
        config_rows_list = prsc.generate_config_principal_table(raw_schema_name, sql_schema_name)
        rows_insert_json = self.generate_config_rows(GNF_TABLES_NAME, config_rows_list)
        table_id = f"{NAME_PROJECT}.{NAME_DATASET_BQ_CONF}.{NAME_DATASET_TABLE}"
        pd_config = pd.DataFrame(rows_insert_json)
        
        self.delet_to_update_row(GNF_TABLES_NAME)
        
        try:
            pd_config.to_gbq(table_id, project_id=NAME_PROJECT, if_exists='append', credentials=credentials)
            print(table_name)
            return True
        except Exception as e:
            logging.error("Encountered errors while register rows: {}".format(e))
            return f"Ocurrio error al insertar en: {str(e)}!"    


prsc = ParserConfig()

TABLES_DICT = config.SURVEYS_TABLES_RELAT

for table in TABLES_DICT:
    prsc.run_generation_pandas_to_gbq(TABLES_DICT, table)
