import gspread 
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from settings import config
from ingest_drive_to_cloud_storage import IngestDrive

bucket_name = config.BUCKET_NAME
bucket_folder_name = config.bucket_folder_name

key_path = config.KEY_PATH

scope = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive'
]

credentials = ServiceAccountCredentials.from_json_keyfile_name(key_path, scope)

# TEST 1
worksheet_id = '1ScJ-6JLvI5Lis1kr8xrS5-JRK5HXq2eTgTojJHLVxd4'
worksheet_title = 'BD_Prime_test'

worksheet = gspread.authorize(credentials).open_by_key(worksheet_id).worksheet(worksheet_title)

sheet_data = worksheet.get_all_values()
headers = sheet_data.pop(0) 

BD = pd.DataFrame(sheet_data, columns=headers)  

local_file_path = f'{config.local_folder_name}/{worksheet.title}.parquet'
BD.to_parquet(local_file_path)

print(f'Archivo: {worksheet.title} se convirtio a parquet')

destination_blob_name = f'{bucket_folder_name}/{worksheet.title}.parquet'

ingest_drive = IngestDrive()
ingest_drive.upload_blob(bucket_name, local_file_path, destination_blob_name)

