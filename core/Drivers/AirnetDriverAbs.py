
import requests
import json
from abc import ABC, abstractmethod
import pandas as pd

import DjangoSetup


 
class AirnetDriverAbs(ABC):
    _restprotocol = { "prefix" : "https", "hostname" : "", "port":"", "path" : ""}
    _httpMethod = "GET"
    _payload = {}
    _headers = {}
    _authentication = None
    _changeColumns = { "from" : [], "to" : [] }
    _http_response = None
    _df = None
    _expiry_duration=None
    _requestList=[]
    _df_list=[]
    time_added = False
    _all_df = None
    _df_all_list=[]
    
    
    @abstractmethod
    def __init__(self,manufacturer_obj):
        self.manufacturer_obj=manufacturer_obj
    @abstractmethod
    def preprocess(self, deviceObj):
        pass


    def fetchConfigFile(self):   
        with open("./core/Drivers/config_files/config.json",'r') as f:         
            config = json.load(f)    
        return config

    def restGET(self, request, deviceObj):
        response = requests.get(self._url)
        pass
    # This is not abstract method.
    def restPOST(self,request, deviceOBj):
        return requests.post(request['_url'], data=request['_payload'], headers=request['_headers'])

    # This is not abstract method.
    # TODO:recive parameter start and end time
    def fetch(self, deviceObj=None):
        self.preprocess(deviceObj)
        if len(self._df_list) > 0:
            self._df_all = pd.concat(self._df_all_list, ignore_index=True)
            
        else:
            self._df_all = pd.DataFrame()
    
            

        

    
        # self.handleDF(deviceObj,request)   
        return self._df_all
        
    @abstractmethod
    def postprocess(self, deviceObj,request):
        pass
    @abstractmethod
    def handleDF(self, deviceObj,request):
        pass
    # TODO:
    def add_missing_data(self):
        pass
