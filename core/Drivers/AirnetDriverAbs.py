
import requests
import json
from abc import ABC, abstractmethod
import pandas as pd

import DjangoSetup
from RawData.models import db_AirNet_Raw_Response
from core.models import db_missing_data,db_std_data,db_DEVICE

 
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
    start_time= None
    end_time= None
    _df_list=[]
    time_added = False
    _df_all = None
    _df_all_list=[]
    _missing_data_dict={}
    
    
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
        self.store_missing_data_info()
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

    def add_missing_data(self,device,param,error_code):
        if device in self._missing_data_dict.keys():
            self._missing_data_dict[device].append({param:error_code})
        else:
            self._missing_data_dict[device]=[]
            self._missing_data_dict[device].append({param:error_code})

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

    def insert_raw_response(self,req_url,manufacturer_name,param):
        db_AirNet_Raw_Response.objects.create(request_url=req_url,manufacturer=manufacturer_name ,data=self._http_response.json(),http_code=self._http_response.status_code,pollutant=param).save()

    def store_missing_data_info(self):
        for i in self._missing_data_dict:
            db_missing_data.objects.create(
                req_start_dt=self.start_time,
                req_end_dt=self.end_time,
                parameter=','.join(map(str, self._missing_data_dict[i])),
                device_id=i,

            )
    def store_std_data(self):
        for _, row in self._df_all.iterrows():
            device_obj=db_DEVICE.objects.get(device_id=row['device_id'])
            del row['device_id']

            db_std_data.objects.create(
                device_id=device_obj,**row
            ).save()