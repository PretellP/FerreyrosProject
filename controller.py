from flatdict import FlatDict
from services_gcp import ServicesGCP
from utils import Utils
from services_https import ServicesHttps
import json
import xmltojson
import base64
import uuid
import re
from datetime import datetime, timedelta

class Controller:

    def to_upper_camel_case(self, key, value_object):
        key = "".join(_str[0].upper() + _str[1:] for _str in key.split("."))
        return key, value_object

    def list_to_flat(self, key, value_object):
        if isinstance(value_object, dict) and not bool(value_object):
            value_object = None
        if isinstance(value_object, list):
            if len(value_object)>0:
                value_object =  value_object[0]
            else:
                value_object =  None  
        return key, value_object   

    def json_flat(self, json_value):
        item = map(lambda kv: self.list_to_flat(*kv), json_value.items())
        item = FlatDict(item, delimiter='.')
        item = dict(map(lambda kv: self.to_upper_camel_case(*kv), item.items()))

        return item

    #TODO: Mejorar y realizar un mejora utilizando map en lugar de for    
    def fields_to_string_schema(self, fields_schema, data_object):
        _object = dict()
        for obj in fields_schema:
            key = obj["name"]
            if key in data_object:
                if isinstance(data_object[key], str) or (data_object[key] is None):
                    _object[key] = data_object[key]
                else:
                    _object[key] = str(data_object[key])
            else:
                _object[key] = None
                
            #TODO: Mejorar esta validacion en otra funcion, si el key viene null asignar default    
            if (obj["primary_key"]=="true") and (key in _object) and (_object[key] == None) and ("default" in obj):
                _object[key] = obj["default"]

        return _object

    def check_main_column_iterable(self, column_name, table_data):
        if (column_name is not None) and (column_name!=""):
            table_data_updated = []
            for item_x in table_data:
                main_iterable = item_x[column_name]
                _obj = dict(item_x)
                _obj.pop(column_name, None)
                if(main_iterable is not None):
                    for item_y in main_iterable:
                        item_updated_name_key = dict(map(lambda v: (f'{column_name}.{v[0]}',v[1]),item_y.items()))
                        _objmerge = dict()
                        _objmerge.update(_obj)
                        _objmerge.update(item_updated_name_key)
                        table_data_updated.append(_objmerge)
                else:
                    table_data_updated.append(item_x)

            return table_data_updated
        else:
            return table_data   

        
    def decode_xml_to_json(self, response_data, schema_config):

        element_name = schema_config["element_name"]
        sub_element = schema_config["sub_element"]
        tables_header = schema_config["tables_header"]
        tables_body = schema_config["tables_body"]
        tables_sub_body = schema_config["tables_sub_body"]
        
        decoded_data_list = dict()
        decoded_data_xml = []
        
        tables_key = tables_header + tables_body + tables_sub_body
        for table in tables_key:
            decoded_data_list[table] = []
        
        
        if type(response_data)==list:
            for xml_base64 in response_data:
                decoded_data = base64.b64decode(xml_base64['interfaceValue']).decode('utf-8')
                decoded_data = decoded_data.replace("n0:", "")
                decoded_data = decoded_data.replace("p:", "")
                document_xml_id = uuid.uuid4()
                
                decoded_data_xml.append({"id":document_xml_id, "data":decoded_data, "table_name":sub_element})
                decoded_data_json = xmltojson.parse(decoded_data)
                json_data = json.loads(decoded_data_json)
                
                if sub_element in json_data[element_name]:
                    if type(json_data[element_name][sub_element]) is list:
                        
                        for table_elements in json_data[element_name][sub_element]:
                            data_header_merge = dict()
                            
                            for table_name in tables_header:
                                if table_name in table_elements:
                                    sub_element_data = {key.upper(): value for key, value in table_elements[table_name].items()}
                                    data_header_merge.update(sub_element_data)

                            data_header_merge['UUID'] = uuid.uuid4()
                                
                            data_header_merge["DOCUMENTE_XML_ID"] = document_xml_id
                            data_header_merge['TRANSMITDATE_CT'] = data_header_merge['TRANSMITDATE']
                            if "TRANSMITDATE" in data_header_merge and data_header_merge['TRANSMITDATE'] is not None:
                                data_header_merge['TRANSMITDATE'] = re.sub(r'-\d{2}:\d{2}$', '', data_header_merge['TRANSMITDATE'])

                            if "INVOICEDATE" in data_header_merge:
                                data_header_merge['INVOICEDATE_CT'] = data_header_merge['INVOICEDATE']
                                data_header_merge['INVOICEDATE'] = re.sub(r'-\d{2}:\d{2}$', '', data_header_merge['INVOICEDATE'])    


                            for table_name in tables_header:
                                decoded_data_list[table_name].append(data_header_merge)

                            if "WORKORDERNUMBER" in data_header_merge:    
                                data_header_merge['WORKORDERNUMBER_HEADER'] = data_header_merge['WORKORDERNUMBER']

                            for table_name in tables_body:
                                if table_name in table_elements and type(table_elements[table_name]) is list: 
                                    for sub_element_data in table_elements[table_name]:
                                        data_body = dict()
                                        data_json_flat = self.json_flat(sub_element_data)
                                        data_body = {key.upper(): value for key, value in data_json_flat.items()}
                                        if "WORKORDERNUMBER" in data_body and "WORKORDERNUMBER" in data_header_merge:
                                            data_header_merge["WORKORDERNUMBER"] = data_body["WORKORDERNUMBER"]
                                        data_body.update(data_header_merge)
                                        data_body["UUID_HEAD"] = data_header_merge["UUID"]
                                        uuid_value = uuid.uuid4()
                                        data_body['UUID'] = uuid_value
                                        decoded_data_list[table_name].append(data_body) 


                                        for sub_item_name in tables_sub_body:
                                            if sub_item_name in sub_element_data and type(sub_element_data[sub_item_name]) is list:
                                                for item_sub_data in sub_element_data[sub_item_name]:
                                                    sub_data_body = dict()
                                                    data_json_flat = self.json_flat(item_sub_data)
                                                    sub_data_body = {key.upper(): value for key, value in data_json_flat.items()}
                                                    sub_data_body.update(data_body)
                                                    sub_data_body[sub_item_name] = ""
                                                    sub_data_body["UUID_BODY"] = data_body["UUID"]
                                                    sub_uuid_value = uuid.uuid4()
                                                    sub_data_body['UUID'] = sub_uuid_value 

                                                    decoded_data_list[sub_item_name].append(sub_data_body)     
                                            else:
                                                if sub_item_name in sub_element_data:
                                                    sub_data_body = dict()
                                                    data_json_flat = self.json_flat(sub_element_data[sub_item_name])
                                                    sub_data_body = {key.upper(): value for key, value in data_json_flat.items()}
                                                    sub_data_body.update(data_body)
                                                    sub_data_body[sub_item_name] = ""
                                                    sub_data_body["UUID_BODY"] = data_body["UUID"]
                                                    sub_uuid_value = uuid.uuid4()
                                                    sub_data_body['UUID'] = sub_uuid_value 

                                                    decoded_data_list[sub_item_name].append(sub_data_body)


                                else:
                                    if table_name in table_elements:
                                        data_body = dict()
                                        data_body = {key.upper(): value for key, value in table_elements[table_name].items()}
                                        if "WORKORDERNUMBER" in data_body and "WORKORDERNUMBER" in data_header_merge:
                                            data_header_merge["WORKORDERNUMBER"] = data_body["WORKORDERNUMBER"]
                                        data_body.update(data_header_merge)
                                        
                                        data_body["UUID_HEAD"] = data_header_merge["UUID"]
                                        uuid_value = uuid.uuid4()
                                        data_body['UUID'] = uuid_value
                                        
                                        decoded_data_list[table_name].append(data_body)

                                        for sub_item_name in tables_sub_body:
                                            if sub_item_name in data_body and type(data_body[sub_item_name]) is list:
                                                for item_sub_data in data_body[sub_item_name]:
                                                    sub_data_body = dict()
                                                    data_json_flat = self.json_flat(item_sub_data)
                                                    sub_data_body = {key.upper(): value for key, value in data_json_flat.items()}
                                                    sub_data_body.update(data_body)
                                                    sub_data_body[sub_item_name] = ""
                                                    sub_data_body["UUID_BODY"] = data_body["UUID"]
                                                    sub_uuid_value = uuid.uuid4()
                                                    sub_data_body['UUID'] = sub_uuid_value 

                                                    decoded_data_list[sub_item_name].append(sub_data_body)     
                                            else:
                                                if sub_item_name in data_body:
                                                    sub_data_body = dict()
                                                    data_json_flat = self.json_flat(data_body[sub_item_name])
                                                    sub_data_body = {key.upper(): value for key, value in data_json_flat.items()}
                                                    sub_data_body.update(data_body)
                                                    sub_data_body[sub_item_name] = ""
                                                    sub_data_body["UUID_BODY"] = data_body["UUID"]
                                                    sub_uuid_value = uuid.uuid4()
                                                    sub_data_body['UUID'] = sub_uuid_value 
                                                    decoded_data_list[sub_item_name].append(sub_data_body)


                    else:
                        data_header_merge = dict()
                        for table_name in tables_header:
                            if table_name in json_data[element_name][sub_element]:
                                sub_element_data = {key.upper(): value for key, value in json_data[element_name][sub_element][table_name].items()}
                                data_header_merge.update(sub_element_data)
                                    

                        data_header_merge['UUID'] = uuid.uuid4()
                        
                        data_header_merge['TRANSMITDATE_CT'] = data_header_merge['TRANSMITDATE']
                        if "TRANSMITDATE" in data_header_merge and data_header_merge['TRANSMITDATE'] is not None:
                            data_header_merge['TRANSMITDATE'] = re.sub(r'-\d{2}:\d{2}$', '', data_header_merge['TRANSMITDATE'])

                        if "INVOICEDATE" in data_header_merge:
                            data_header_merge['INVOICEDATE_CT'] = data_header_merge['INVOICEDATE']
                            data_header_merge['INVOICEDATE'] = re.sub(r'-\d{2}:\d{2}$', '', data_header_merge['INVOICEDATE'])  

                        for table_name in tables_header:
                            decoded_data_list[table_name].append(data_header_merge)

                        if "WORKORDERNUMBER" in data_header_merge:                            
                            data_header_merge['WORKORDERNUMBER_HEADER'] = data_header_merge['WORKORDERNUMBER']

                        for table_name in tables_body:
                                table_elements = json_data[element_name][sub_element]
                                if table_name in table_elements and type(table_elements[table_name]) is list: 
                                    for sub_element_data in table_elements[table_name]:
                                        data_body = dict()
                                        data_json_flat = self.json_flat(sub_element_data)
                                        data_body = {key.upper(): value for key, value in data_json_flat.items()}
                                        if "WORKORDERNUMBER" in data_body and "WORKORDERNUMBER" in data_header_merge:
                                            data_header_merge["WORKORDERNUMBER"] = data_body["WORKORDERNUMBER"]
                                        data_body.update(data_header_merge)
                                        data_body["UUID_HEAD"] = data_header_merge["UUID"]
                                        uuid_value = uuid.uuid4()
                                        data_body['UUID'] = uuid_value
                                        decoded_data_list[table_name].append(data_body)

                                        for sub_item_name in tables_sub_body:
                                            if sub_item_name in sub_element_data and type(sub_element_data[sub_item_name]) is list:
                                                for item_sub_data in sub_element_data[sub_item_name]:
                                                    sub_data_body = dict()
                                                    data_json_flat = self.json_flat(item_sub_data)
                                                    sub_data_body = {key.upper(): value for key, value in data_json_flat.items()}
                                                    sub_data_body.update(data_body)
                                                    sub_data_body[sub_item_name] = ""
                                                    sub_data_body["UUID_BODY"] = data_body["UUID"]
                                                    sub_uuid_value = uuid.uuid4()
                                                    sub_data_body['UUID'] = sub_uuid_value 

                                                    decoded_data_list[sub_item_name].append(sub_data_body)     
                                            else:
                                                if sub_item_name in sub_element_data:
                                                    sub_data_body = dict()
                                                    data_json_flat = self.json_flat(sub_element_data[sub_item_name])
                                                    sub_data_body = {key.upper(): value for key, value in data_json_flat.items()}
                                                    sub_data_body.update(data_body)
                                                    sub_data_body[sub_item_name] = ""
                                                    sub_data_body["UUID_BODY"] = data_body["UUID"]
                                                    sub_uuid_value = uuid.uuid4()
                                                    sub_data_body['UUID'] = sub_uuid_value 

                                                    decoded_data_list[sub_item_name].append(sub_data_body)
                                else:
                                    if table_name in table_elements:
                                        data_body = dict()
                                        data_body = {key.upper(): value for key, value in table_elements[table_name].items()}
                                        if "WORKORDERNUMBER" in data_body and "WORKORDERNUMBER" in data_header_merge:
                                            data_header_merge["WORKORDERNUMBER"] = data_body["WORKORDERNUMBER"]
                                        data_body.update(data_header_merge)
                                        
                                        data_body["UUID_HEAD"] = data_header_merge["UUID"]
                                        uuid_value = uuid.uuid4()
                                        data_body['UUID'] = uuid_value
                                        decoded_data_list[table_name].append(data_body) 

                                        for sub_item_name in tables_sub_body:
                                            if sub_item_name in data_body and type(data_body[sub_item_name]) is list:
                                                for item_sub_data in data_body[sub_item_name]:
                                                    sub_data_body = dict()
                                                    data_json_flat = self.json_flat(item_sub_data)
                                                    sub_data_body = {key.upper(): value for key, value in data_json_flat.items()}
                                                    sub_data_body.update(data_body)
                                                    sub_data_body[sub_item_name] = ""
                                                    sub_data_body["UUID_BODY"] = data_body["UUID"]
                                                    sub_uuid_value = uuid.uuid4()
                                                    sub_data_body['UUID'] = sub_uuid_value 

                                                    decoded_data_list[sub_item_name].append(sub_data_body)     
                                            else:
                                                if sub_item_name in data_body:
                                                    sub_data_body = dict()
                                                    data_json_flat = self.json_flat(data_body[sub_item_name])
                                                    sub_data_body = {key.upper(): value for key, value in data_json_flat.items()}
                                                    sub_data_body.update(data_body)
                                                    sub_data_body[sub_item_name] = ""
                                                    sub_data_body["UUID_BODY"] = data_body["UUID"]
                                                    sub_uuid_value = uuid.uuid4()
                                                    sub_data_body['UUID'] = sub_uuid_value 
                                                    decoded_data_list[sub_item_name].append(sub_data_body)

        else:
            decoded_data = base64.b64decode(res_data['interfaceValue']).decode('utf-8')
            decoded_data = decoded_data.replace("n0:", "")
            decoded_data_json = xmltojson.parse(decoded_data)
            json_data = json.loads(decoded_data_json)
            data_item = json_data[element_name][sub_element][item_name] 
            decoded_data_list.append(data_item)
            
        return decoded_data_list, decoded_data_xml
        

    def run_etl_by_schema(self, source_schema_name, params_request):
        utils = Utils()
        services_https = ServicesHttps()
        services_gcp = ServicesGCP()
        
        '''
        start date to create timer and validate if already load source on actual date
        '''
        datetime_sistem_initial = datetime.now()
        datetime_sistem_format = datetime_sistem_initial.strftime("%Y%m%d")
        
        
        while True:
            '''
            start get schema to get conf values.
            '''
            object_schema = utils.read_schema_object(source_schema_name)

            '''
            make query and get data from cat api
            '''
            schema_config = object_schema["config"]
            
            if params_request["load"] == "manual": 
                response_api = services_gcp.read_all_xml_files_in_folder(object_schema["name"])
            else: 
                api_path_params = utils.create_path_api_cursor(schema_config, params_request)
                response_api = services_https.get_cat_api_data(api_path_params)
            
            '''
            decode base 64 to XML then transfor to JSON formate
            '''
            if len(response_api) <= 0 : break
            response_api_data, response_xml_data = self.decode_xml_to_json(response_api, schema_config)
            
            '''
            start flatmaping type json item by item to get list flatmap data,
            '''
            for table_item in object_schema["tables"]:
                table_config = table_item["config"]
                element_name = table_config["element_name"]
                if element_name in response_api_data and len(response_api_data[element_name]) > 0:
                    data_flatmap = list(map(self.json_flat, response_api_data[element_name]))
                    data_result = list(map(lambda v:self.fields_to_string_schema(table_item['fields'], v),data_flatmap))
                    size_data_result = len(data_result)
                    table_item_name = table_item["name"]
                    print(f"TABLA: {table_item_name} - #REGISTROS: {size_data_result}")
                    
                    '''
                    start ingest data to GCP bigquery layer Raw,
                    '''
                    services_gcp.save_frame_pandas_to_gbq(data_result, table_item)
                    
                    '''
                    start ingest data to GCP bigquery layer Master(processed).
                    '''
                    services_gcp.transfer_raw_to_procedure(table_item["name"])

                    '''
                    save back data to GCP storage origin format data and csv format.
                    '''
                    services_gcp.save_frame_to_storage_csv(data_result, table_item["name"])
                    
            services_gcp.save_frame_origin_to_storage_json(response_api, object_schema["name"])
             
            for xml_source in response_xml_data:
                services_gcp.save_frame_xml_to_storage(xml_source["data"],xml_source["table_name"],xml_source["id"])
                
            if params_request["load"] == "manual": 
                services_gcp.delete_files_directory_xml(object_schema["name"])    
            else:    
                '''
                update schema config.
                '''
                udate_schema_config = utils.update_cursor_decode(schema_config, datetime_sistem_format)
                object_schema["config"] = udate_schema_config
                utils.write_schema_object(object_schema, source_schema_name)


            break
        
        print(f"Proceso ingesta finalizado: {source_schema_name}")
