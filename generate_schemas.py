import json
import pandas as pd

class GenerateSchema:
                        
    def store_schema(self, schema_dict):
        
        json_object = json.dumps(schema_dict, indent=4)

        with open(f"schemas/{schema_dict['name']}.input.schema", "w") as outfile:
            outfile.write(json_object)
        
        print(f'Esquema: {schema_dict["name"]} generado')
    
    def get_schema_dictionary(self, df_db, column_item, column_type):
            
        column_name = column_item[0]
        schema = {}
        schema["name"] = column_name
        schema["$schema"] = "http://json-schema.org/draft-07/schema#"
        schema["title"] = "Equipment Data"
        schema["type"] = "object"
        schema["fields"] = []
        schema["config"] = {
            "api_path": "https://services.cat.com/catDigital/assetSummary/v1/assets",
            "load_period": "daily-reset",
            "data_name_key": "assetSummaries",
            "cursor_metadata": "responseMetadata",
            "main_column_iterable": "",
            "paginator_query": "cursor",
            "actual_cursor_encode": "",
            "actual_cursor_decode": "",
            "actual_load_date": "",
            "last_cursor_reset": ""
        }
        
        for index, row in enumerate((df_db[column_name].to_numpy()).tolist()):
            
            data_type = column_type[index]
                    
            if type(row) != float:
                row_schema = self.get_row_schema(row, data_type)
                schema["fields"].append(row_schema)
                
        return schema
                    
                    
    def get_row_schema(self, row: str, data_type: str):
        
        row_schema = {}
        row_schema["name"] = row
        row_schema["primary_key"] = "false"
        row_schema["raw_type"] = "str"
        row_schema["master_type"] = data_type.upper()
        row_schema["default"] = self.get_default_field_value(data_type)
        
        return row_schema     
    
    def get_default_field_value(self, data_type: str):
        
        if data_type.upper() == 'INT64':
            return "0"
        elif data_type.upper() == 'FLOAT64':
            return "0.0"
        elif data_type.upper() == 'DATE':
            return "0001-01-01"
        else:
            return "X0"
        

df = pd.read_excel('schema_dict/schema_dict.xlsx')
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
df_db = df.loc[:, ~df.columns.str.contains('DATA_TYPE')]

grt_sch = GenerateSchema()

for column_item in df_db.items():
    
    schema_dict = grt_sch.get_schema_dictionary(df_db, column_item, df['DATA_TYPE'])
    grt_sch.store_schema(schema_dict)
        
                