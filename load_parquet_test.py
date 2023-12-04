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
worksheet_id = '1X38-Ahd5m-1khlF2U3XwTMpv4Bf0U5w80qd0x58N9sA'
worksheet_title = 'BD_Prime_test'

worksheet = gspread.authorize(credentials).open_by_key(worksheet_id).worksheet(worksheet_title)

sheet_data = worksheet.get_all_values()
headers = sheet_data.pop(0) 

BD = pd.DataFrame(sheet_data, columns=headers) 

month_column = ""
for column in BD.columns.to_list():
    
    if column.lower() == 'mes_encuesta':    
        month_column = column
        break
    

data = []
for index, row in enumerate((BD['Mes_encuesta'].to_numpy()).tolist()):
            
    period = row.split('-')
    del period[2]
    period = "".join(period)
    data.append(period)
  
BD['Period'] = data


local_file_path = f'{config.local_folder_name}/{worksheet.title}.parquet'
BD.to_parquet(local_file_path)

print(f'Archivo: {worksheet.title} se convirtio a parquet')

destination_blob_name = f'{bucket_folder_name}/{worksheet.title}.parquet'

# ingest_drive = IngestDrive()
# ingest_drive.upload_blob(bucket_name, local_file_path, destination_blob_name)

