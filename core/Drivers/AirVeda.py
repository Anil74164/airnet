
import requests
import json
from abc import ABC, abstractmethod
from datetime import datetime, timedelta 
import pandas as pd
import DjangoSetup
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode,lit,struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from RawData.models import db_AirNet_Raw,db_AirNet_Raw_Response
spark = SparkSession.builder \
        .appName("Django Spark Integration") \
        .getOrCreate()

 
from core.Drivers.AirnetDriverAbs import AirnetDriverAbs
class AirVeda(AirnetDriverAbs):
    def __init__(self,manufacturer_obj,id_token="",refresh_token=None,device_id=None,param=None):
        super().__init__(manufacturer_obj)
        
        self.fmt = "%Y-%m-%d %H:%M:%S"
        self.start_time= None
        self.end_time= None
        self._fetch_method = "POST"
        self.id_token=id_token
        self.refresh_token=refresh_token
        self.device_id=device_id
        self.param=param
        self._headers = {'Authorization' : 'Bearer ' +self.id_token}

        self._auth_url=''
        self._payload = { 'refreshToken' : self.refresh_token,
                         'deviceId'    : self.device_id,
                         'pollutant': self.param,
                        'startTime'   : self.start_time,
                        'endTime'     : self.end_time}
        self._restprotocol['prefix'] = 'https'
        self._restprotocol['hostname'] = ''
        self._changeColumns = { 'from' : ['pm_2_5'], 'to' : ['pm25'] }


    def preprocess(self, deviceObj):
        self.manufacturer_obj.email       
     
        
        self._authentication={'email': self.manufacturer_obj.email,'password': self.manufacturer_obj.api_or_pass}
        response = requests.post(self.manufacturer_obj.auth_url, data=self._authentication)
        response_data = response.json()
        self.id_token = response_data['idToken']
        self.refresh_token = response_data.get('refreshToken')
        self._expiry_duration = response_data['expiresIn']
        self.end_time=datetime.now()
        self.start_time=self.end_time-timedelta(minutes=15)
        print(self.start_time)
        print(self.end_time)
        
        
        
            
               
    def process(self,deviceObj):
        for dev in deviceObj:
            self.device_id=dev.device_id
            paramListStr=dev.parameters
            paramList=paramListStr.split(",")
            paramList=["so2Voltages", "no2Voltages", "ozoneVoltages", "coVoltages", "pm25_base", "pm10_base"]
            print(paramList)
            self._df=None
            self._df_list=[]
            self.time_added = False
            
            for param in paramList:
                req = {}
                req['param']=param
                start_time=self.start_time
                end_time=self.end_time
                req['_payload'] = { 'refreshToken' : self.refresh_token,
                                'deviceId'    : self.device_id,
                                'pollutant': param,
                            'startTime'   : start_time.strftime(self.fmt),
                            'endTime'     : end_time.strftime(self.fmt)}
            
                req['_headers'] = {'Authorization' : 'Bearer ' +self.id_token}
                req['_url']="https://data.airveda.com/api/data/voltage_data/"
                # self._requestList.append(req)
                if self._fetch_method =='POST':
                    response=self.restPOST(req, deviceObj)
                else:
                
                    response=self.restGET(req, deviceObj)
                self._http_response=response
                
                self.creating_df(dev,req)
                
            if len(self._df_list) > 0:
                self._df = pd.concat(self._df_list, axis=1)
                self._df['device_id'] = dev.device_id
            else:
                self._df = pd.DataFrame()
            
            self._df_all_list.append(self._df)
    def get_ColumnReplacement(self):
        _changeColumns = {'value_pm25_base': 'pm2_5_r', 'value_pm10_base': 'pm10_r'}
        diff_column={'so2_nv':['WorkingElectrodeVoltage_so2Voltages','AuxilliaryElectrodeVoltage_so2Voltages'],
                     'no2_nv':['WorkingElectrodeVoltage_no2Voltages','AuxilliaryElectrodeVoltage_no2Voltages'],
                     'o3_nv':['WorkingElectrodeVoltage_ozoneVoltages','AuxilliaryElectrodeVoltage_ozoneVoltages'],
                     'co_nv':['WorkingElectrodeVoltage_coVoltages','AuxilliaryElectrodeVoltage_coVoltages']
                    }
        for column in _changeColumns.keys():
            if column in self._df_all.columns:
                self._df_all.rename(columns={column:_changeColumns[column]}, inplace=True)
        print(self._df_all.columns)
        
        for column in diff_column:
            if diff_column[column][0] in self._df_all:
                self._df_all[column] = self._df_all[diff_column[column][0]] - self._df_all[diff_column[column][1]]
                self._df_all=self._df_all.drop(columns=[diff_column[column][0], diff_column[column][1]])
        
    def handleDF(self):
        dict1={'co2':'co2_cov','no2':'no2_cov','so2':'so2_cov','no':'no_cov','o3':'o3_cov'}
        for column in self._df_all.columns:
            if column in dict1:
              self.dict1[column](self._df_all,column)
    def creating_df(self, deviceObj,request):
        db_AirNet_Raw_Response.objects.create(request_url=request['_url'],manufacturer=deviceObj.manufacturer_id.name ,data=self._http_response.json(),http_code=self._http_response.status_code,pollutant=request['param']).save()

        df = pd.DataFrame(self._http_response.json())
        print(self._http_response.json())
        df=df['readings'].apply(pd.Series)
        # TODO: 
        if len(df) == 0:
            print(f"No data found for device_id {deviceObj.device_id} and parameter {request['param']}. "
                            f"for {request['_payload']['startTime']} to {request['_payload']['startTime']}")
            return 
        df['time'] = pd.to_datetime(df['time'], format="%Y-%m-%d %H:%M:%S")
        if not self.time_added:
            self._df_list.append(df[['time']]) 
            self.time_added = True
        
        df.columns = ['{}{}'.format(c,'' if c == 'time' else  '_'+request['param']) for c in df.columns]
        df.set_index('time', inplace=True)
        df.reset_index(drop=True, inplace=True)
        
        
        self._df_list.append(df)
       
    def postprocess(self, deviceObj):
        if len(self._df_list) > 0:
            self._df_all = pd.concat(self._df_all_list)
            
        else:
           self._df_all = pd.DataFrame()
        
           
    def standardization_df(self):
        self.get_ColumnReplacement()
        self.handleDF()
        