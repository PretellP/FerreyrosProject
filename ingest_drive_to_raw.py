import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
import load_google_drive as lgd
from settings import config
from os_environ import osEnviron
from gcsfs import GCSFileSystem
import datetime as dt
import os

osEnviron.set_os_environ(config.KEY_PATH)

BUCKET_NAME = config.BUCKET_RAW_PARQUET_NAME
local_folder_name = config.local_parquet_temp_folder

bucket_folder_name = config.BUCKET_PARQUET_FOLDER
bucket_htry_folder_name = config.BUCKET_PARQUET_HTRY_FOLDER

project_id = config.PROJECT_ID
dataset = config.DATA_SET_RAW

class IngestDriveToRaw:
        
    def get_local_file_path(self, title, local_folder):
        
        return f'{local_folder}/{title}.parquet'
        
    def convert_to_parquet(self, worksheet, local_folder):
        
        sheet_data = worksheet.get_all_values()
        headers = sheet_data.pop(0)
        BD = pd.DataFrame(sheet_data, columns=headers)
        
        local_file_path = self.get_local_file_path(worksheet.title, local_folder)
        BD.to_parquet(local_file_path)
        
        print(f'Archivo: {worksheet.title} se convirtio a parquet')
        
        
    def upload_blob(self, bucket_name, source_file_name, destination_blob_name):
        
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)

        print(f'Archivo {source_file_name} subido a {destination_blob_name}.')
        
    def read_parquet_from_cloud_storage(self, bucket_name, file_path):
        gcs = GCSFileSystem()
        with gcs.open(f"{bucket_name}/{file_path}", "rb") as file:
            df = pd.read_parquet(file)
        return df
    
    def get_month_column(self, dataframe):
        
        month_column = "Mes_encuesta"
        for column in dataframe.columns.to_list():
            
            if column.lower() == 'mes_encuesta':    
                month_column = column
                break
            
        return month_column
    
    def add_period_column(self, df, month_column):
        
        data_period = []
        for index, row in enumerate((df[month_column].to_numpy()).tolist()):
                
            period = row.split('-')
            del period[2]
            period = "".join(period)
            data_period.append(period)
        
        df['Period'] = data_period
        
    def get_table_schema(self, df):
        
        table_schema = []
        for column in list(df.columns.values):
            table_schema.append(bigquery.SchemaField(column, 'STRING'))
        
        return table_schema
    
    def ingest_to_raw(self, client, table_schema, df, table_id, file_name):
            
        job_config = bigquery.LoadJobConfig(
                        schema=table_schema,
                        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    )
        
        load_job = client.load_table_from_dataframe(
                    df, table_id, job_config=job_config
                )
        
        load_job.result()
        destination_table = client.get_table(table_id)

        print(f'Archivo: {file_name}.parquet cargado a RAW')
        print("Cargadas {} filas.".format(destination_table.num_rows))
        
        
    def upload_blob_to_raw(self, client, bucket_name, destination_blob_name, table_name):
        
        # INGEST FROM CLOUD STORAGE TO RAW
        df = self.read_parquet_from_cloud_storage(bucket_name, destination_blob_name)
    
        month_column = self.get_month_column(df)
        self.add_period_column(df, month_column)
        
        table_schema = self.get_table_schema(df)
        table_id = f'{project_id}.{dataset}.{table_name}'
        
        self.ingest_to_raw(client, table_schema, df, table_id, table_name)
        
        
    def upload_parquet_to_raw(self, client, worksheet, local_folder_name, bucket_name):
        
        self.convert_to_parquet(worksheet, local_folder_name)
        
        # Sube el archivo a Cloud Storage
        
        local_file_path = self.get_local_file_path(worksheet.title, local_folder_name)
        
        destination_blob_prime_name = f'{bucket_folder_name}/{worksheet.title}.parquet'
        self.upload_blob(bucket_name, local_file_path, destination_blob_prime_name)
        
        now = dt.datetime.now()
        destination_blob_htry_name = f'{bucket_htry_folder_name}/{worksheet.title}_HISTORY_{now.strftime("%Y%m%d_%H%M%S")}.parquet'
        self.upload_blob(bucket_name, local_file_path, destination_blob_htry_name)
        
        self.upload_blob_to_raw(client, bucket_name, destination_blob_prime_name, worksheet.title)
        
        os.remove(local_file_path)
        
    def upload_massive_drive_to_raw(self):
        
        client = bigquery.Client(project = project_id)

        for worksheet in lgd.worksheets:
            self.upload_parquet_to_raw(client, worksheet, local_folder_name, BUCKET_NAME)

        for worksheet_sql in lgd.worksheets_sql:
            self.upload_parquet_to_raw(client, worksheet_sql, local_folder_name, BUCKET_NAME)
            
    



