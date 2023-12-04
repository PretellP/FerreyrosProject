import sys
sys.path.append('..')
from google.cloud import bigquery
import pandas as pd
import os
from gcsfs import GCSFileSystem
from settings import config
from os_environ import osEnviron

osEnviron.set_os_environ(config.KEY_PATH)
relative_dataset_path = '../dataset/'

def read_parquet_from_cloud_storage(bucket_name, file_path):
    gcs = GCSFileSystem()
    with gcs.open(f"{bucket_name}/{file_path}", "rb") as file:
        df = pd.read_parquet(file)
    return df

bucket_name = config.BUCKET_NAME
bucket_folder_name = config.bucket_folder_name
project_id = config.PROJECT_ID
dataset = config.DATA_SET

files = os.listdir(relative_dataset_path)

for file in files:
    
    file_path = f'{bucket_folder_name}/{file}'
    df = read_parquet_from_cloud_storage(bucket_name, file_path)
    
    client = bigquery.Client(project = project_id)
    
    table_id = f'{project_id}.{dataset}.{os.path.splitext(file)[0]}'
    
    
    month_column = "Mes_encuesta"
    for column in df.columns.to_list():
        
        if column.lower() == 'mes_encuesta':    
            month_column = column
            break
        
    data_period = []
    
    for index, row in enumerate((df[month_column].to_numpy()).tolist()):
                
        period = row.split('-')
        del period[2]
        period = "".join(period)
        data_period.append(period)
    
    df['Period'] = data_period
    
    table_schema = []
    for column in list(df.columns.values):
        table_schema.append(bigquery.SchemaField(column, 'STRING'))
        
    job_config = bigquery.LoadJobConfig(
                    schema=table_schema,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                )
    
    load_job = client.load_table_from_dataframe(
                df, table_id, job_config=job_config
            )
    
    load_job.result()
    destination_table = client.get_table(table_id)

    print(f'Archivo: {file} cargado')
    print("Cargadas {} filas.".format(destination_table.num_rows))

