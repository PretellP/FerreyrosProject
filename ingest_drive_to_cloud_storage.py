from google.cloud import storage
import pandas as pd
import load_google_drive as lgd
from settings import config
from os_environ import osEnviron

osEnviron.set_os_environ(config.KEY_PATH)

bucket_name = config.BUCKET_NAME
# Nombre del folder del bucket en Cloud Storage
local_folder_name = config.local_folder_name
# Nombre del folder del bucket en Cloud Storage
bucket_folder_name = config.bucket_folder_name

# Configura las credenciales de autenticación si es necesario
# No olvides reemplazar 'my-project' por tu proyecto de Google Cloud
# En este caso, el archivo Excel se llamará 'local_file.xlsx'


class IngestDrive:

    def upload_blob(self, bucket_name, source_file_name, destination_blob_name):
        """Sube un archivo a un bucket de Google Cloud Storage."""
        # Crea un cliente de almacenamiento
        storage_client = storage.Client()

        # Obtiene el bucket
        bucket = storage_client.bucket(bucket_name)

        # Define el nombre del archivo en Cloud Storage
        blob = bucket.blob(destination_blob_name)

        # Carga el archivo local en el almacenamiento en la nube
        blob.upload_from_filename(source_file_name)

        print(f'Archivo {source_file_name} subido a {destination_blob_name}.')
        
        
    def upload_massive(self, worksheets, local_folder_name, bucket_name, bucket_folder_name):
        
        for worksheet in worksheets:
            
            sheet_data = worksheet.get_all_values()
            headers = sheet_data.pop(0)
            BD = pd.DataFrame(sheet_data, columns=headers)
            
            local_file_path = f'{local_folder_name}/{worksheet.title}.parquet'
            BD.to_parquet(local_file_path)
            
            print(f'Archivo: {worksheet.title} se convirtio a parquet')
            
            destination_blob_name = f'{bucket_folder_name}/{worksheet.title}.parquet'
            # Sube el archivo a Cloud Storage
            self.upload_blob(bucket_name, local_file_path, destination_blob_name)
            
    
ingest_drive = IngestDrive()
ingest_drive.upload_massive(lgd.worksheets, local_folder_name, bucket_name, bucket_folder_name)
        




