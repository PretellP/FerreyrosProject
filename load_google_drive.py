import gspread 
from oauth2client.service_account import ServiceAccountCredentials
from settings import config

key_path = config.KEY_PATH

# Update the scope to include the Google Sheets API
scope = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive'
]

credentials = ServiceAccountCredentials.from_json_keyfile_name(key_path, scope)
worksheet_id = config.WORKSHEET_ID

worksheets = gspread.authorize(credentials).open_by_key(worksheet_id).worksheets()


