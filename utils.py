import json
import base64
from services_gcp import ServicesGCP
from datetime import datetime, timedelta

class Utils:
    
    def create_bq_table(self, client, dataset_id, table_id, data):
        print("create table")
        
    def read_schema_object(self, name):
        services_gcp = ServicesGCP()
        schema = services_gcp.read_schema_of_storage(name)
        if schema is None:
            path = f"schemas/{name}"
            with open(path, 'r') as file:
                schema = json.load(file)
        return schema  
    
    def write_schema_object(self, data, name):
        services_gcp = ServicesGCP()
        services_gcp.save_schema_to_storage(data, name)
        #path = f"schemas/{name}"
        #with open(path, 'w') as file:
        #    json.dump(data, file, indent=4)
    
    def create_path_api_cursor(self, schema_config, params_request):
        api_data = dict()
        api_data['path'] = schema_config['api_path']
        api_data['payload'] = {}
        if schema_config['fecinicio_query'] and schema_config['fecfin_query'] and schema_config['interfaz_query']:
            datetime_sistem_initial = datetime.now()
            datetime_sistem_format = datetime_sistem_initial.strftime("%Y%m%d")
            
            if schema_config['fecinicio_query'] == datetime_sistem_format:
                demo_days = 1
                date_timedelta = timedelta(days=demo_days)
                date_time_past = datetime_sistem_initial - date_timedelta
                date_time_past_format = date_time_past.strftime("%Y%m%d")
                
                api_data['payload'] = { "FecInicio": date_time_past_format,
                                        "HoraInicio": schema_config['horainicio_query'], 
                                        "FecFin": schema_config['fecfin_query'],  #datetime_sistem_format,
                                        "HoraFin":schema_config['horafin_query'], 
                                        "Interfaz":schema_config['interfaz_query'] }
                
            else:
                api_data['payload'] = { "FecInicio": schema_config['fecinicio_query'],
                                        "HoraInicio": schema_config['horainicio_query'], 
                                        "FecFin": schema_config['fecfin_query'],  #datetime_sistem_format,
                                        "HoraFin":schema_config['horafin_query'], 
                                        "Interfaz":schema_config['interfaz_query'] }
        else:
            demo_days = 30
            date_timedelta = timedelta(days=demo_days)
            date_time_past = datetime_sistem_initial - date_timedelta
            date_time_past_format = date_time_past.strftime("%Y%m%d")

            api_data['payload'] = { "FecInicio": date_time_past_format,
                                    "HoraInicio": schema_config['horainicio_query'], 
                                    "FecFin": datetime_sistem_format, #schema_config['fecfin_query'], 
                                    "HoraFin":schema_config['horafin_query'], 
                                    "Interfaz":schema_config['interfaz_query'] }
        return api_data
        
    #TODO: se debe validar desde el config el key del cursor, ejemplo nextCursor debe ser dinamico desde confg   
    def update_cursor_decode(self, schema_config, datetime_sistem_format):
        
        schema_config["FecInicio"] = datetime_sistem_format
        schema_config["actual_load_date"] = datetime_sistem_format
        
        # cursor_metadata = schema_config["cursor_metadata"]
        # if (cursor_metadata in response_api) and (response_api[cursor_metadata] is not None):
        #     #print(f"metadata: {response_api[cursor_metadata]}")
        #     nextCursor = response_api[cursor_metadata]["nextCursor"]
        #     if (nextCursor is not None) and (nextCursor!=""):
        #         schema_config["actual_cursor_encode"] = nextCursor
        #         next_cursor_decode = base64.b64decode(nextCursor)
        #         schema_config["actual_cursor_decode"] = f'{next_cursor_decode}'
        #         if schema_config["load_period"] == "daily-reset":
        #             schema_config["last_cursor_reset"] = f'{next_cursor_decode}'
        # else:
        #     if schema_config["load_period"] == "daily-reset":
        #         schema_config["actual_cursor_encode"] = ""
        #         schema_config["actual_cursor_decode"] = ""
        #         datetime_sistem_initial = datetime.datetime.now()
        #         datetime_sistem_format = datetime_sistem_initial.strftime("%Y-%m-%d")
        #         schema_config["actual_load_date"] = datetime_sistem_format
                
        return schema_config
