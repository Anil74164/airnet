from django.db import models

class db_AirNet_Raw_Response(models.Model):
    timestamp=models.DateTimeField(auto_now_add=True)
    manufacturer=models.CharField(max_length=100)
    device_id=models.CharField(max_length=100)
    request_url=models.TextField()
    data=models.JSONField()
    http_code=models.CharField(max_length=255,null=True)
    is_calibrated=models.BooleanField(default=False)
    processed_dt=models.DateTimeField(null=True)
    pollutant=models.CharField(max_length=1000,null=True)
    
    
    @staticmethod
    def getRawData():
        db_AirNet_Raw_Response.objects.all()

class db_AirNet_Raw(models.Model):
    timestamp=models.DateTimeField(auto_now_add=True)
    manufacturer=models.CharField(max_length=100)
    # request_url=models.TextField()
    data_raw=models.JSONField()
    data_cal=models.JSONField(null=True)

    # httpCode=models.CharField(max_length=255,null=True)
    processed_dt=models.DateTimeField(null=True)
   
    
    
    @staticmethod
    def getRawData():
        return db_AirNet_Raw.objects.all()