from django.db import models
from django.db import models
from django.contrib.gis.db import models as gismodels
import datetime
 
 

class db_Role(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=255)
    desc = models.CharField(max_length=255)
    version = models.IntegerField(default=0)
    status=models.ImageField(default=0)
    created_dt = models.DateTimeField()
    modified_dt = models.DateTimeField()
    created_By = models.IntegerField(null=True)
    modified_By = models.IntegerField(null=True)
    is_current = models.SmallIntegerField()
 
from django.contrib.auth.models import AbstractUser,Permission
 
class db_User(AbstractUser):
    
    mobile = models.CharField(max_length=45)
    version = models.IntegerField()
    is_current = models.SmallIntegerField()
    created_dt = models.DateTimeField()
    modified_dt = models.DateTimeField()
    role = models.ForeignKey(db_Role, on_delete=models.CASCADE)
    created_by = models.ForeignKey('self', on_delete=models.CASCADE, null=True, related_name='created_by_user')
    modified_by= models.ForeignKey('self', on_delete=models.CASCADE, null=True, related_name='modified_by_user')
 
 
 
    
 
db_User._meta.get_field('groups').remote_field.related_name = 'user_groups'
db_User._meta.get_field('user_permissions').remote_field.related_name = 'user_user_permissions'
 
class db_Permission(Permission):
    
    desc = models.CharField(max_length=255)
    status= models.IntegerField(default=0)
    created_dt = models.DateTimeField()
    modified_dt = models.DateTimeField()
    created_by = models.ForeignKey(db_User,on_delete=models.CASCADE,null=True,related_name='Created_permissions')
    modified_by = models.ForeignKey(db_User,on_delete=models.CASCADE,null=True,related_name='modified_permissions')
    version = models.IntegerField()
    is_current = models.SmallIntegerField()
 
class db_RoleHasPermissions(models.Model):
    role = models.ForeignKey(db_Role, on_delete=models.CASCADE)
    permissions = models.ForeignKey(db_Permission, on_delete=models.CASCADE)
 
class db_Host(models.Model):
    id=models.AutoField(primary_key=True)
    name= models.CharField(max_length=255,null=True)
    mobile= models.CharField(max_length=255,null=True)
    email= models.CharField(max_length=255,null=True)
    address= models.TextField(max_length=1000,null=True)
    user_id=models.ForeignKey(db_User,on_delete=models.CASCADE,null=True)
    status=models.IntegerField(default=0)
    create_dt=models.DateTimeField(null=True)
    modified_dt=models.DateTimeField(null=True)
    created_by=models.ForeignKey(db_User,on_delete=models.CASCADE,null=True,related_name='Created_host')
    modified_by=models.ForeignKey(db_User,on_delete=models.CASCADE,null=True,related_name='Modified_host')
 
 
class db_Network(models.Model):
    id=models.AutoField(primary_key=True)
    name= models.CharField(max_length=255,null=True)
    organization= models.CharField(max_length=255,null=True)
    person_name= models.CharField(max_length=255,null=True)
    person_email= models.CharField(max_length=255,null=True)
    person_mobile= models.CharField(max_length=255,null=True)
    address= models.TextField(max_length=1000,null=True)
    city= models.CharField(max_length=50,null=True)
    state= models.CharField(max_length=50,null=True)
    country= models.CharField(max_length=50,null=True)
    status=models.IntegerField(default=0)
    create_dt=models.DateTimeField(null=True)
    modified_dt=models.DateTimeField(null=True)
    created_by=models.ForeignKey(db_User,on_delete=models.CASCADE,null=True,related_name='Created_Network')
    modified_by=models.ForeignKey(db_User,on_delete=models.CASCADE,null=True,related_name='Modified_Network')
 
class db_LOCATION(models.Model):
    id=models.AutoField(primary_key=True)
    name= models.CharField(max_length=255,null=True)
    latitude=models.DecimalField(max_digits=9,decimal_places=6,null=True)
    longitude=models.DecimalField(max_digits=9,decimal_places=6,null=True)
    location=gismodels.PointField(null=True,blank=True)
    address= models.TextField(max_length=1000,null=True)
    status=models.IntegerField(default=0)
    create_dt=models.DateTimeField(null=True)
    modified_dt=models.DateTimeField(null=True)
    created_by=models.ForeignKey(db_User,on_delete=models.CASCADE,null=True,related_name='Created_Location')
    modified_by=models.ForeignKey(db_User,on_delete=models.CASCADE,null=True,related_name='Modified_Location')
    
    
    
class db_SITE(models.Model):
    id=models.AutoField(primary_key=True)
    name= models.CharField(max_length=255)
    location= models.IntegerField()
    host_id= models.ForeignKey(db_Host,null=True,on_delete=models.CASCADE)
    network_id= models.ForeignKey(db_Network,on_delete=models.CASCADE)
    location_id= models.ForeignKey(db_LOCATION,on_delete=models.CASCADE)
    status=models.IntegerField(default=0)
    create_dt=models.DateTimeField()
    modified_dt=models.DateTimeField()
    created_by=models.ForeignKey(db_User,on_delete=models.CASCADE,null=True,related_name='Created_Sit')
    modified_by=models.ForeignKey(db_User,on_delete=models.CASCADE,null=True,related_name='Modified_Site')
 
 
class db_DeviceModel(models.Model):
    id=models.AutoField(primary_key=True)
    name= models.CharField(max_length=255,null=True)
    description= models.TextField(max_length=1000,null=True)
    driver_name= models.CharField(max_length=255,null=True)
    class_name= models.CharField(max_length=255,null=True)
    company= models.CharField(max_length=100,null=True)
    company_url= models.TextField(max_length=1000,null=True)
    manual_url= models.TextField(max_length=1000,null=True)
    status=models.IntegerField(default=0)
    create_dt=models.DateTimeField(null=True)
    modified_dt=models.DateTimeField(null=True)
    created_by=models.ForeignKey(db_User,on_delete=models.CASCADE,null=True,related_name='Created_DeviceModel')
    modified_by=models.ForeignKey(db_User,on_delete=models.CASCADE,null=True,related_name='Modified_DeviceModel')
    
 
class db_MANUFACTURER(models.Model):
    id=models.AutoField(primary_key=True)
    name= models.CharField(max_length=255,null=True)
    auth_url=models.CharField(max_length=2000,null=True)
    cal_url=models.CharField(max_length=2000,null=True)
    api_or_pass=models.CharField(max_length=50,null=True)
    data_url=models.CharField(max_length=2000)
    access_id=models.CharField(max_length=50,null=True)
    access_key=models.CharField(max_length=100,null=True)
    email= models.CharField(max_length=255,null=True)
    address= models.TextField(max_length=1000,null=True)
    person_name= models.CharField(max_length=255,null=True)
    person_email= models.CharField(max_length=255,null=True)
    person_Contact= models.CharField(max_length=255,null=True)
    status=models.IntegerField(default=0)
    notes= models.TextField(max_length=1000,null=True)
    create_dt=models.DateTimeField(null=True)
    modified_dt=models.DateTimeField(null=True)
    
    created_by=models.ForeignKey(db_User,on_delete=models.CASCADE,null=True,related_name='Created_manufacturer')
    modified_by=models.ForeignKey(db_User,on_delete=models.CASCADE,null=True,related_name='Modified_manufacturer')
    
    @staticmethod
    def getManufacturer(i):
        return db_MANUFACTURER.objects.get(id=i)
 
class db_VENDOR(models.Model):
    id=models.AutoField(primary_key=True)
    name= models.CharField(max_length=255,null=True)
    mobile= models.CharField(max_length=255,null=True)
    email= models.CharField(max_length=255,null=True)
    address= models.TextField(max_length=1000,null=True)
    person_name= models.CharField(max_length=255,null=True)
    person_email= models.CharField(max_length=255,null=True)
    person_moblie= models.CharField(max_length=255,null=True)
    person_title= models.CharField(max_length=255,null=True)
    status=models.IntegerField(default=0)
    create_dt=models.DateTimeField(null=True)
    modified_dt=models.DateTimeField(null=True)
    created_by=models.ForeignKey(db_User,on_delete=models.CASCADE,null=True,related_name='Created_Vendor')
    modified_by=models.ForeignKey(db_User,on_delete=models.CASCADE,null=True,related_name='Modified_Vendor')
    
 
 
class db_DEVICE(models.Model):
    device_id = models.CharField(primary_key=True,max_length=255)
    serial_no = models.CharField(max_length=255)
    mac = models.CharField(max_length=25)
    sim_manufacturer = models.CharField(max_length=100)
    sim_number =  models.CharField(max_length=45)
    
    address = models.TextField(max_length=1000)
    parameters = models.CharField(max_length=1000)
    
    altitude = models.DecimalField(max_digits=9,decimal_places=6)
    install_dt = models.DateTimeField()
    approved_by =models.CharField(max_length=255)
    remarks =models.CharField(max_length=255)
    notes = models.TextField(max_length=1000)
    status=models.IntegerField(default=0)
    land_use = models.CharField(max_length=45)
    device_model_id = models.ForeignKey(db_DeviceModel, on_delete=models.CASCADE)
    manufacturer_id = models.ForeignKey(db_MANUFACTURER, on_delete=models.CASCADE)
    vendor_id = models.ForeignKey(db_VENDOR, on_delete=models.CASCADE)
    locations_id = models.ForeignKey(db_LOCATION, on_delete=models.CASCADE)
    site_id = models.ForeignKey(db_SITE, on_delete=models.CASCADE)
    network_id = models.ForeignKey(db_Network, on_delete=models.CASCADE)
    create_dt=models.DateTimeField(null=True)
    modified_dt=models.DateTimeField(null=True)
    created_by=models.ForeignKey(db_User,on_delete=models.CASCADE,null=True,related_name='Created_Device')
    modified_by=models.ForeignKey(db_User,on_delete=models.CASCADE,null=True,related_name='Modified_Device')
    
    @staticmethod
    def getAllDevice():
        return db_DEVICE.objects.all()
    @staticmethod
    def get_active_devices():
        return db_DEVICE.objects.filter(status=1)
    
    
class db_Parameter(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=255)
    units = models.CharField(max_length=100)
    status=models.IntegerField(default=0)
    created_dt = models.DateTimeField()
    modified_dt = models.DateTimeField()
    created_by=models.ForeignKey(db_User,on_delete=models.CASCADE,null=True,related_name='Created_Parameter')
    modified_by=models.ForeignKey(db_User,on_delete=models.CASCADE,null=True,related_name='Modified_Parameter')
    
    version = models.IntegerField()
    is_current = models.SmallIntegerField()
 
class db_Units(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=255)
    notation = models.CharField(max_length=45)
    desc = models.CharField(max_length=255)
    created_dt = models.DateTimeField()
    modified_dt = models.DateTimeField()
    created_by=models.ForeignKey(db_User,on_delete=models.CASCADE,null=True,related_name='Created_Units')
    modified_by=models.ForeignKey(db_User,on_delete=models.CASCADE,null=True,related_name='Modified_Units')
    version = models.IntegerField()
    is_current = models.SmallIntegerField()
    status=models.IntegerField(default=0)
 
class db_AQDataSparse(models.Model):
    id = models.AutoField(primary_key=True)
    parameter_id = models.ForeignKey(db_Parameter, on_delete=models.CASCADE)
    status = models.IntegerField(default=0)
    measure_dt = models.DateTimeField()
    created_dt = models.DateTimeField()
    is_raw = models.SmallIntegerField()
    param_value = models.FloatField()
    param_unit = models.ForeignKey(db_Units, on_delete=models.CASCADE,related_name='Unit1')
 
class db_AlertConfig(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=255)
    desc = models.CharField(max_length=255)
    error_code = models.IntegerField()
    status = models.SmallIntegerField(default=0)
    email_subject = models.CharField(max_length=255)
    create_dt=models.DateTimeField()
    modified_dt=models.DateTimeField()
    created_by=models.ForeignKey(db_User,on_delete=models.CASCADE,related_name='Created_AlterConfig',null=True)
    modified_by=models.ForeignKey(db_User,on_delete=models.CASCADE,related_name='Modified_AlterConfig',null=True)
    is_current = models.SmallIntegerField()
    version=models.IntegerField()
 
class db_AlertDL(models.Model):
    id = models.AutoField(primary_key=True)
    user = models.ForeignKey(db_User, on_delete=models.CASCADE)
    alert_config = models.ForeignKey(db_AlertConfig, on_delete=models.CASCADE)
 
class db_DataSource(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=255)
    description = models.TextField()
    status = models.SmallIntegerField(default=0)
    version = models.IntegerField()
    is_current = models.SmallIntegerField()
 
class db_Transformations(models.Model):
    id = models.AutoField(primary_key=True)
    transformation = models.TextField()
    executed_at = models.DateTimeField()
    source_id = models.ForeignKey(db_DataSource, related_name='transformations_source', on_delete=models.CASCADE)
    destination_id = models.ForeignKey(db_DataSource, related_name='transformations_destination', on_delete=models.CASCADE)
 
class db_AQData(models.Model):
    id = models.AutoField(primary_key=True)
    pm10= models.FloatField(null=True)

    co = models.FloatField(null=True)
    so2= models.FloatField(null=True)
    no = models.FloatField(null=True)
    no2 = models.FloatField(null=True)
    o3 = models.FloatField(null=True)
    pm2_5 = models.FloatField(null=True)
    nox = models.FloatField(null=True)
    wind_speed=models.FloatField(null=True)
    wind_direction=models.FloatField(null=True)
    rain_fall=models.FloatField(null=True)
    temperature=models.FloatField(null=True)
    relative_humidity=models.FloatField(null=True)
    barometric_pressure=models.FloatField(null=True)
    co2 = models.FloatField(null=True)
    no2_nv = models.FloatField(null=True)
    co_nv = models.FloatField(null=True)
    o3_nv = models.FloatField(null=True)
    so2_nv = models.FloatField(null=True)
    pm2_5 = models.FloatField(null=True)
    pm1_opc=models.FloatField(null=True)
    pm2_5_opc=models.FloatField(null=True)
    pm10_opc=models.FloatField(null=True)
    device_id = models.ForeignKey(db_DEVICE, on_delete=models.CASCADE)
    pm2_5_r=models.FloatField(null=True)
    pm10_r=models.FloatField(null=True)
    g1_rh=models.FloatField(null=True)
    g1_t=models.FloatField(null=True)
    g2_rh=models.FloatField(null=True)
    g2_t=models.FloatField(null=True)
    pm1 = models.FloatField(null=True)
    no=models.FloatField(null=True)
    no_nv=models.FloatField(null=True)
    status = models.IntegerField()
    date_time = models.DateTimeField()
    
    
 
class db_AirNet_Aggregated(models.Model):
    id = models.AutoField(primary_key=True)
    pm10= models.FloatField(null=True)
    co = models.FloatField(null=True)
    so2= models.FloatField(null=True)
    no = models.FloatField(null=True)
    no2 = models.FloatField(null=True)
    o3 = models.FloatField(null=True)
    pm2_5 = models.FloatField(null=True)
    nox = models.FloatField(null=True)
    wind_speed=models.FloatField(null=True)
    wind_direction=models.FloatField(null=True)
    rain_fall=models.FloatField(null=True)
    temperature=models.FloatField(null=True)
    relative_humidity=models.FloatField(null=True)
    barometric_pressure=models.FloatField(null=True)
    co2 = models.FloatField(null=True)
    no2_nv = models.FloatField(null=True)
    co_nv = models.FloatField(null=True)
    o3_nv = models.FloatField(null=True)
    so2_nv = models.FloatField(null=True)
    pm2_5 = models.FloatField(null=True)
    pm1_opc=models.FloatField(null=True)
    pm2_5_opc=models.FloatField(null=True)
    pm10_opc=models.FloatField(null=True)
    device_id = models.ForeignKey(db_DEVICE, on_delete=models.CASCADE)
    pm2_5_r=models.FloatField(null=True)
    pm10_r=models.FloatField(null=True)
    g1_rh=models.FloatField(null=True)
    g1_t=models.FloatField(null=True)
    g2_rh=models.FloatField(null=True)
    g2_t=models.FloatField(null=True)
    pm1 = models.FloatField(null=True)
    no=models.FloatField(null=True)
    no_nv=models.FloatField(null=True)
    status = models.IntegerField(default=0)
    time = models.DateTimeField(null=True)
    start_time=models.DateTimeField()
    end_time=models.DateTimeField()
    
class db_manufacturer_calibrated_data(models.Model):
    time = models.DateTimeField()
    id = models.AutoField(primary_key=True)
    o3_ppb=models.FloatField(null=True)
    so2_ppb=models.FloatField(null=True)
    no2_ppb=models.FloatField(null=True)
    co_ppm=models.FloatField(null=True)
    o3=models.FloatField(null=True)
    so2=models.FloatField(null=True)
    no2=models.FloatField(null=True)
    co=models.FloatField(null=True)
    pm25=models.FloatField(null=True)
    device_id = models.ForeignKey(db_DEVICE, on_delete=models.CASCADE)
    pm10=models.FloatField(null=True)
    ozone=models.FloatField(null=True)
    temperature=models.FloatField(null=True)
    humidity=models.FloatField(null=True)
      


class db_AQDataExt(models.Model):
    id = models.AutoField(primary_key=True)
    status = models.IntegerField()
    windspeed = models.FloatField()
    winddirection = models.IntegerField()
    rainfall = models.FloatField()
    aqi = models.FloatField()
    battery = models.FloatField()
    pwr = models.FloatField()
    g1_mh = models.FloatField()
    g1_mt = models.FloatField()
    measure_dt = models.DateTimeField()
    g1_co_t = models.FloatField()
    g1_no2_t = models.FloatField()
    g1_o3_t = models.FloatField()
    g2_mh = models.FloatField()
    g2_mt = models.FloatField()
    g2_so2_t = models.FloatField()
    AQData_id=models.ForeignKey(db_AQData,on_delete=models.CASCADE)
    
class db_missing_data(models.Model):
    id = models.AutoField(primary_key=True)
    created_dt = models.DateTimeField(auto_now_add=True)
    req_start_dt = models.DateTimeField()
    req_end_dt = models.DateTimeField()
    parameter=models.CharField(max_length=255)
    device_id = models.ForeignKey(db_DEVICE, on_delete=models.CASCADE)
    status = models.IntegerField(default=0)
    received_dt=models.DateTimeField(null=True)
    

class db_std_data(models.Model):
    id = models.AutoField(primary_key=True)
    pm10= models.FloatField(null=True)

    co = models.FloatField(null=True)
    so2= models.FloatField(null=True)
    no = models.FloatField(null=True)
    no2 = models.FloatField(null=True)
    o3 = models.FloatField(null=True)
    pm2_5 = models.FloatField(null=True)
    nox = models.FloatField(null=True)
    wind_speed=models.FloatField(null=True)
    wind_direction=models.FloatField(null=True)
    rain_fall=models.FloatField(null=True)
    temperature=models.FloatField(null=True)
    relative_humidity=models.FloatField(null=True)
    barometric_pressure=models.FloatField(null=True)
    co2 = models.FloatField(null=True)
    no2_nv = models.FloatField(null=True)
    co_nv = models.FloatField(null=True)
    o3_nv = models.FloatField(null=True)
    so2_nv = models.FloatField(null=True)
    pm2_5 = models.FloatField(null=True)
    pm1_opc=models.FloatField(null=True)
    pm2_5_opc=models.FloatField(null=True)
    pm10_opc=models.FloatField(null=True)
    device_id = models.ForeignKey(db_DEVICE, on_delete=models.CASCADE)
    pm2_5_r=models.FloatField(null=True)
    pm10_r=models.FloatField(null=True)
    g1_rh=models.FloatField(null=True)
    g1_t=models.FloatField(null=True)
    g2_rh=models.FloatField(null=True)
    g2_t=models.FloatField(null=True)
    pm1 = models.FloatField(null=True)
    no=models.FloatField(null=True)
    no_nv=models.FloatField(null=True)
    status = models.IntegerField(default=0)
    time = models.DateTimeField()
    
    