from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
from google.api_core.exceptions import GoogleAPIError
import datetime as dt
import pandas as pd
import base64
import logging
import json
import os
from settings import config

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


# PATH_GCLOUD_AUTH_JSON = os.environ.get("PATH_GCLOUD_AUTH_JSON")
# NAME_PROJECT = os.environ.get("NAME_PROJECT")
# PATH_BUCKET = os.environ.get("PATH_BUCKET")
# PATH_PROJECT_BUCKET = os.environ.get("PATH_PROJECT_BUCKET")
# NAME_DATASET_BQ = os.environ.get("NAME_DATASET_BQ")
# PATH_PROCEDURE = os.environ.get("PATH_PROCEDURE")

PATH_GCLOUD_AUTH_JSON=config.KEY_PATH
NAME_PROJECT=config.PROJECT_ID
PATH_BUCKET=config.BUCKET_RAW_PARQUET_NAME
PATH_PROJECT_BUCKET="DLealtad_SQL"
NAME_DATASET_BQ=config.DATA_SET_RAW
PATH_PROCEDURE= f"{NAME_DATASET_BQ}.SP_CDM_SAP_RawToProcessedSourceHistory"
#PATH_PROCEDURE="programs.SP_CDM_SAP_RawToProcessedSourceHistory_API"

#pe-fesa-datalake-dev01.DataSetTest.SP_CDM_SAP_RawToProcessedSourceHistory

credentials = service_account.Credentials.from_service_account_file(PATH_GCLOUD_AUTH_JSON, scopes=["https://www.googleapis.com/auth/cloud-platform"])

client_bigquery = bigquery.Client(credentials=credentials, project=NAME_PROJECT)

# Inicializacion de un cliente
#client_storage = storage.Client()
client_storage = storage.Client.from_service_account_json(PATH_GCLOUD_AUTH_JSON)

# Accediendo al bucket existente
bucket = client_storage.get_bucket(PATH_BUCKET)

class ServicesGCP:
    
    def save_frame_pandas_to_gbq(self, data, object_schema):
        table_schema_name = object_schema["name"]
        table_id = f"{NAME_PROJECT}.{NAME_DATASET_BQ}.{table_schema_name}"
        _df = pd.DataFrame(data)
        #add string all
        _df = _df.astype(str)
        
        _df = _df.replace({"None": pd.NA})
        _df = _df.replace({None: pd.NA})
        
        #_df =_df.fillna('null')

        #_df['UUID'] = _df.apply(lambda row: uuid.uuid4(), axis=1)
        data_frame = _df.drop_duplicates()
        
        try:
            data_frame.to_gbq(table_id, project_id=NAME_PROJECT, if_exists='replace', credentials=credentials)
        except Exception as e:
            logging.error("Encountered errors while register rows: {}".format(e))
            
            
    def save_frame_to_storage_csv(self, data, table_schema_name):        
        _df = pd.DataFrame(data, dtype=str)
        df_to_insert = _df.drop_duplicates()

        last_name_api = f"{table_schema_name}-({dt.datetime.now().strftime('%Y:%m:%d %H:%M:%S')})"
        url_to_save = f"{PATH_PROJECT_BUCKET}/CSV/{table_schema_name}/{last_name_api}"
        blob = bucket.blob(url_to_save)
        
        blob.upload_from_string(df_to_insert.to_csv(index=False), content_type='text/csv')
        #print(f"success save dataframe CSV to storage: {url_to_save}")
    def save_frame_xml_to_storage(self, data, table_schema_name, table_id):
        url_to_save = f"{PATH_PROJECT_BUCKET}/XML/{table_schema_name}/{table_id}.xml"
        blob = bucket.blob(url_to_save)
        blob.upload_from_string(data, content_type='application/xml')
        
    def save_frame_origin_to_storage_json(self, data, table_schema_name):
        last_name_api = f"{table_schema_name}-({dt.datetime.now().strftime('%Y:%m:%d %H:%M:%S')})"
        url_to_save = f"{PATH_PROJECT_BUCKET}/JSON/{table_schema_name}/{last_name_api}"
        blob = bucket.blob(url_to_save)
        
        blob.upload_from_string(data=json.dumps(data),content_type='application/json')  
        #print(f"success save origen Json to storage: {url_to_save}")
        
    def save_schema_to_storage(self, data_json, source_schema_name):
        url_to_save = f"{PATH_PROJECT_BUCKET}/SCHEMAS/{source_schema_name}"
        blob = bucket.blob(url_to_save)
        
        blob.upload_from_string(data=json.dumps(data_json),content_type='application/json')  
        #print(f"success save schema Json to storage: {url_to_save}")
           
    def read_schema_of_storage(self, source_schema_name):
        url_to_save = f"{PATH_PROJECT_BUCKET}/SCHEMAS/{source_schema_name}"        
        blob = bucket.get_blob(url_to_save) #cuando existe el archivo
        json_file = dict()
        try:
            json_file = json.loads(blob.download_as_text(encoding="utf-8"))
        except Exception as e:
            print('no found schema, will be created...')
            json_file = None
            
        return json_file
    
    def read_all_xml_files_in_folder(self, folder_name):

        folder_path = f"{PATH_PROJECT_BUCKET}/XML_MANUAL/{folder_name}"        

        blobs = bucket.list_blobs(prefix=folder_path)
        files_xml_data_list = []
        for blob in blobs:
            if blob.name.endswith(".xml"):
                xml_name = os.path.basename(blob.name)
                xml_content = blob.download_as_text(encoding="utf-8")
                
                process_data_xml = self.process_xml(xml_name, xml_content)
                files_xml_data_list.append({"interfaceValue":process_data_xml})   
                
        return files_xml_data_list   
    
    
    def delete_files_directory_xml(self, folder_name):
        folder_path = f"{PATH_PROJECT_BUCKET}/XML_MANUAL/{folder_name}"        
        blobs = bucket.list_blobs(prefix=folder_path)

        for blob in blobs:
            if not blob.name.endswith("/"):
                blob.delete()
                #print(f"Move: {blob.name}")
                
    def process_xml(self, xml_name, xml_content):
        try:
            xml_content = base64.b64encode(xml_content.encode("utf-8")).decode("utf-8")
            return xml_content
        except Exception as e:
            print(f"Error processing XML file {xml_name}: {e}")

        
#     def transfer_raw_to_procedure(self, table_name):
#         #print(f"Ejecutando Procedimiento Almacenado para pasar datos de la capa RAW al PROCESADO")
#         #Pase de capas
#         sql_proc = f"CALL {PATH_PROCEDURE}(@my_param)"
#         #print('-->', sql_proc, table_name)
#         job_config = bigquery.QueryJobConfig()
#         job_config.query_parameters = [bigquery.ScalarQueryParameter('my_param', 'STRING', "AtencionCliente2")]
#         try:
#             print(f"Esperando a que el Procedure {table_name} se complete...")
#             query_job = client_bigquery.query(sql_proc, job_config=job_config)
#             query_job.result()
#             print(f'Ejecucion procedure correcto {table_name}')

#         except Exception as e:
#             # Capturar errores generales
#             print(f"Error al ejecutar PROCEDURE: {table_name}")           
#             raise e

            
    def save_frame_to_raw_bigquery(self, data, object_schema):
        table_schema_name = object_schema["name"]
        table_id = f"{NAME_PROJECT}.{NAME_DATASET_BQ}.{table_schema_name}"
        
        bigquery_schema = list(map(lambda v: bigquery.SchemaField(v["name"], "STRING"), object_schema['fields']))
        bigquery_table = bigquery.Table(table_id, schema=bigquery_schema)   
        
        client_bigquery.delete_table(table_id, not_found_ok=True)
      
        try:
            table_created = client_bigquery.create_table(bigquery_table)
            logging.info("Created table in bigquery")
        except e:
            logging.error("Error created table in bigquery. {}".format(e))

        try:
            client_bigquery.insert_rows_json(table_id, data)
            
            logging.info("New rows have been added.")
        except GoogleAPIError as e:
            logging.error("Encountered errors while inserting rows: {}".format(e))
