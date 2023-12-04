import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
import load_google_drive as lgd
from settings import config
from os_environ import osEnviron
from gcsfs import GCSFileSystem

osEnviron.set_os_environ(config.KEY_PATH)

BUCKET_NAME = config.BUCKET_NAME
local_folder_name = config.local_folder_name
bucket_folder_name = config.bucket_folder_name

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
    
    def ingest_to_raw(self, table_schema, df, table_id, file_name):
            
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
        
        
    def upload_massive(self, client, worksheet, local_folder_name, bucket_name, bucket_folder_name):
        
        self.convert_to_parquet(worksheet, local_folder_name)
        
        # Sube el archivo a Cloud Storage
        destination_blob_name = f'{bucket_folder_name}/{worksheet.title}.parquet'
        local_file_path = self.get_local_file_path(worksheet.title, local_folder_name)
        self.upload_blob(bucket_name, local_file_path, destination_blob_name)
        
        # INGEST FROM CLOUD STORAGE TO RAW
        df = self.read_parquet_from_cloud_storage(bucket_name, destination_blob_name)
        month_column = self.get_month_column(df)
        self.add_period_column(df, month_column)
        
        table_schema = self.get_table_schema(df)
        table_id = f'{project_id}.{dataset}.{worksheet.title}'
        
        self.ingest_to_raw(table_schema, df, table_id, worksheet.title)
            
    
ingest_drive = IngestDriveToRaw()
client = bigquery.Client(project = project_id)

for worksheet in lgd.worksheets:
    ingest_drive.upload_massive(client, worksheet, local_folder_name, BUCKET_NAME, bucket_folder_name)