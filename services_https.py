from requests.auth import HTTPBasicAuth
import requests
import json

#auth = HTTPBasicAuth('PIAPPLFPT_WS', 'Ferreyros2023$')
auth = HTTPBasicAuth('PIAPPLFPD_GCP', 'Ferreyros2023$')


class ServicesHttps:
    
    #TODO validar los posibles errores en caso falle el API
    def get_cat_api_data(self, api_data):
        try:

            #print(api_data)
            response = requests.get(api_data['path'], data=str(api_data['payload']), auth=auth)
            response_json = response.json()
            
            if "error" in response_json or "data" not in response_json:
                raise ValueError(f'cat api services problem: {response_json}')
        except Exception as e:
            raise ValueError(f'cat api services problem: no Authorization -> {e}')

        
        return response_json['data']
