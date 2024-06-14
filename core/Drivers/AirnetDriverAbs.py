
import requests
import json
from abc import ABC, abstractmethod
import pandas as pd
from datetime import datetime
import logging
import DjangoSetup
from RawData.models import db_AirNet_Raw_Response
from core.models import db_missing_data, db_std_data, db_DEVICE,db_manufacturer_calibrated_data
logger=logging.getLogger('workspace')
import pytz

class AirnetDriverAbs(ABC):
    _restprotocol = {"prefix": "https", "hostname": "", "port": "", "path": ""}
    _fetch_method = "GET"
    _payload = {}
    _headers = {}
    _authentication = None
    _changeColumns = {"from": [], "to": []}
    _http_response = None
    _df = None
    _expiry_duration = None
    start_time = None
    end_time = None
    _df_list = []
    time_added = False
    _df_all = None
    _df_all_list = []
    
    _cal_df=None

    @abstractmethod
    def __init__(self, manufacturer_obj):
        self.manufacturer_obj = manufacturer_obj
        self._missing_data_dict = {}

    @abstractmethod
    def preprocess(self,start,end,deviceObj):
        pass

    def restGET(self, deviceObj):
        try:
            response = requests.get(self._url)
            return response
        except Exception as e:
            logger.error(f"Error in restGET: {e}")
            raise

    def restPOST(self, request, deviceObj):
        try:
            return requests.post(request['_url'], data=request['_payload'], headers=request['_headers'])
        except Exception as e:
            logger.error(f"Error in restPOST: {e}")
            raise
    # TODO: START AND END TIME, PARAMETER
    def fetch(self,start,end,param=None, deviceObj=None):
        try:
            print("fetch",start,end)
            self.preprocess(deviceObj=deviceObj,start=start,end=end)
            self.process(deviceObj=deviceObj,dag_param=param)
            self.postprocess(deviceObj)
            self.store_missing_data_info()
            return self._df_all
        except Exception as e:
            logger.error(f"Error in fetch: {e}")
            raise

    @abstractmethod
    def postprocess(self, deviceObj):
        pass

    @abstractmethod
    def process(self, deviceObj,dag_param):
        pass
    @abstractmethod
    def standardize_df(self):
        pass

    @abstractmethod
    def handleDF(self):
        pass

    def add_missing_data(self, device, param, error_code):
        try:
            if device in self._missing_data_dict.keys():
                self._missing_data_dict[device].append({param: error_code})
            else:
                self._missing_data_dict[device] = []
                self._missing_data_dict[device].append({param: error_code})
        except Exception as e:
            logger.error(f"Error in add_missing_data: {e}")
            raise

    def co2_cov(self, key):
        try:
            self._df_all[key] = self._df_all[key] * 0.873
        except Exception as e:
            logger.error(f"Error in co2_cov: {e}")
            raise

    def so2_cov(self, key):
        try:
            self._df_all[key] = self._df_all[key] * 0.381
        except Exception as e:
            logger.error(f"Error in so2_cov: {e}")
            raise

    def no2_cov(self, key):
        try:
            self._df_all[key] = self._df_all[key] * 0.531
        except Exception as e:
            logger.error(f"Error in no2_cov: {e}")
            raise

    def no_cov(self, key):
        try:
            self._df_all[key] = self._df_all[key] * 0.813
        except Exception as e:
            logger.error(f"Error in no_cov: {e}")
            raise

    def o3_cov(self, key):
        try:
            self._df_all[key] = self._df_all[key] * 0.510
        except Exception as e:
            logger.error(f"Error in o3_cov: {e}")
            raise

    def insert_raw_response(self, req_url,dev_id, manufacturer_name, param):
        try:
            db_AirNet_Raw_Response.objects.create(
                request_url=req_url, manufacturer=manufacturer_name,
                data=self._http_response.json(), http_code=self._http_response.status_code, pollutant=param
            ).save()
        except Exception as e:
            logger.error(f"Error in insert_raw_response: {e}")
            raise

    def store_missing_data_info(self):
        try:
            for i in self._missing_data_dict:
                db_missing_data.objects.create(
                    req_start_dt=self.start_time,
                    req_end_dt=self.end_time,
                    parameter=','.join(map(str, self._missing_data_dict[i])),
                    device_id=i,
                ).save()
        except Exception as e:
            logger.error(f"Error in store_missing_data_info: {e}")
            raise

    def store_std_data(self):
        try:
            
            for _, row in self._df_all.iterrows():
                device_obj = db_DEVICE.objects.get(device_id=row['device_id'])
                del row['device_id']
                db_std_data.objects.create(
                    device_id=device_obj, **row
                ).save()
        except Exception as e:
            logger.error(f"Error in store_std_data: {e}")
            raise

    def store_manufacturer_cal_data(self):
        try:
            for _, row in self._cal_df.iterrows():
                device_obj = db_DEVICE.objects.get(device_id=row['device_id'])
                del row['device_id']
                db_manufacturer_calibrated_data.objects.create(
                     device_id=device_obj,**row
                ).save()
        except Exception as e:
            logger.warning(f"Error in store_std_data: {e}")
            raise
    