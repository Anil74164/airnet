
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
    def __init__(self,id_token="",refresh_token=None,device_id=None,param=None):
        super().__init__()
        self.fmt = "%Y-%m-%d %H:%M:%S"
        self.start_time= datetime.now()
        self.end_time= datetime.now()
        self._httpMethod = "POST"
        self.id_token=id_token
        self.refresh_token=refresh_token
        self.device_id=device_id
        self.param=param
        self._headers = {'Authorization' : 'Bearer ' +self.id_token}

        self._auth_url=''
        self._payload = { 'refreshToken' : self.refresh_token,
                         'deviceId'    : self.device_id,
                         'pollutant': self.param,
                        'startTime'   : self.start_time.strftime(self.fmt),
                        'endTime'     : self.end_time.strftime(self.fmt)}
        self._restprotocol['prefix'] = 'https'
        self._restprotocol['hostname'] = ''
        self._changeColumns = { 'from' : ['pm_2_5'], 'to' : ['pm25'] }


    def preprocess(self, deviceObj):
        config=self.fetchConfigFile()
        air=config['airveda']
        self.auth_url=air['url']
        self._authentication={'email':air['email'],'password':air["password"]}
        response = requests.post(self.auth_url, data=self._authentication)
        response_data = response.json()
        self.id_token = response_data['idToken']
        self.refresh_token = response_data.get('refreshToken')
        self._expiry_duration = response_data['expiresIn']
        self.device_id =[]
        self._requestList=[]
        paramListStr=None
        self.start_time=datetime(2024,1,1,3,5)
        self.end_time=datetime(2024,1,4,3,5)
        for dev in deviceObj:
            self.device_id=dev.device_id
            paramListStr=dev.parameters
            paramList=paramListStr.split(",")
            paramList=['no2','so2']
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
                if self._httpMethod =='POST':
                    response=self.restPOST(req, deviceObj)
                else:
                
                    response=self.restGET(req, deviceObj)
                self._http_response=response
                
                self.postprocess(dev,req)
                
            if len(self._df_list) > 0:
                self._df = pd.concat(self._df_list, axis=1)
                self._df['device_id'] = dev.device_id
            else:
                self._df = pd.DataFrame()
                
            print(self._df)
            
            
            self._df_all_list.append(self._df)
            
            
               
            
            
    @staticmethod
    def get_ColumnReplacement():
        _changeColumns = {'from': ['pm_2_5'], 'to': ['pm25']}
        return _changeColumns
    
    def handleDF(self,deviceObj,request):
        pass
    def postprocess(self, deviceObj,request):
        db_AirNet_Raw_Response.objects.create(request_url=request['_url'],manufacturer=deviceObj.manufacturer_id.name ,data=self._http_response.json(),http_code=self._http_response.status_code,pollutant=request['param']).save()
       
        
        json_string = self._http_response.text
        json_data = json.loads(json_string)

        for reading in json_data['readings']:
            reading[request['param']] = reading.pop('value')
            
        
        # readings_df = spark.createDataFrame(json_data['readings'])
      
        # readings_df = readings_df.withColumn("DeviceID", lit(json_data['DeviceID']))
        
        # readings_df = readings_df.withColumnRenamed("value", request['param'])

     
        
        # if(self._df==None):
        #     self._df=readings_df
        # else:
        #     self._df = self._df.join(readings_df,(self._df['DeviceID']==readings_df['DeviceID']) & (self._df['time']==readings_df['time']),'left')

        df = pd.DataFrame(self._http_response.json())
        print(df.columns)
        df = df['readings'].apply(pd.Series)
        print(df)
        if len(df) == 0:
            print(f"No data found for device_id {deviceObj.device_id} and parameter {request['param']}. "
                            f"for {request['_payload']['startTime']} to {request['_payload']['startTime']}")
            return 
        df['time'] = pd.to_datetime(df['time'], format="%Y-%m-%d %H:%M:%S")
        if not self.time_added:
            self._df_list.append(df[['time']])  # Add 'time' column only once
            self.time_added = True
        updated_json_data = json.dumps(json_data, indent=4,ensure_ascii=False)
        db_AirNet_Raw.objects.create(manufacturer=deviceObj.manufacturer_id.name,data_raw=updated_json_data).save()
        
        df.columns = ['{}{}'.format(c, '' if c == 'time' else '_' + request['param']) for c in df.columns]
        df.set_index('time', inplace=True)
        df.reset_index(drop=True, inplace=True)
        
        print(df)
        self._df_list.append(df)
       
        