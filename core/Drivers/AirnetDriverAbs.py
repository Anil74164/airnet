
import requests
import json
from abc import ABC, abstractmethod
import pandas as pd

import DjangoSetup


 
class AirnetDriverAbs(ABC):
    _restprotocol = { "prefix" : "https", "hostname" : "", "port":"", "path" : ""}
    _fetch_method = "GET"
    _payload = {}
    _headers = {}
    _authentication = None
    _changeColumns = { "from" : [], "to" : [] }
    _http_response = None
    _df = None
    _expiry_duration=None
    
    _df_list=[]
    time_added = False
    _df_all = None
    _df_all_list=[]
    
    
    @abstractmethod
    def __init__(self,manufacturer_obj):
        self.manufacturer_obj=manufacturer_obj
    @abstractmethod
    def preprocess(self, deviceObj):
        pass

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
        self.process(deviceObj)
        self.postprocess(deviceObj)
        return self._df_all
        
    @abstractmethod
    def postprocess(self, deviceObj):
        pass
    @abstractmethod
    def process(self,deviceObj):
        pass
    @abstractmethod
    def handleDF(self):
        
        pass
    # TODO:
    def add_missing_data(self):
        pass
    
    def co2_cov(self,key):
        self._df_all['key']=self._df_all['key']*0.873
 
    def so2_cov(self,key):
        self._df_all['key']=self._df_all['key']*0.381
 
    def no2_cov(self,key):
        self._df_all['key']=self._df_all['key']*0.531
 
    def no_cov(self,key):
        self._df_all['key']=self._df_all['key']*0.813
 
    def o3_cov(self,key):
        self._df_all['key']=self._df_all['key']*0.510