from datetime import datetime
from django.utils import timezone
import DjangoSetup
from core.models import db_DEVICE, db_User, db_DeviceModel, db_MANUFACTURER, db_VENDOR, db_LOCATION, db_SITE, db_Network


# Assuming db_User model is defined and imported properly

# Create a new user instance
user = db_User.objects.get(id=1)  # Assuming there's a user with primary key 1

# # Create a new manufacturer instance
# manufacturer = db_MANUFACTURER(
#     name="airveda",
#     auth_url="https://dashboard.airveda.com/api/token/",
#     api_or_pass="airveda123!@#",
#     data_url="https://data.airveda.com/api/data/voltage_data/",
    
    
#     email="airveda@cstep.in",
#     address="123 Example St, City, Country",
#     person_name="John Doe",
#     person_email="john@example.com",
#     person_Contact="+1234567890",
#     status=0,  # Assuming status 1 represents active
#     notes="Example notes about the manufacturer",
#     create_dt=timezone.now(),  # Current timestamp
#     modified_dt=timezone.now(),  # Current timestamp
#     created_by=user,  # Linking to the user who created this record
#     modified_by=user  # Linking to the same user who modified this record
# )

# # Save the manufacturer instance to the database
# manufacturer.save()
device_model = db_DeviceModel.objects.get(id=1)  # Assuming there's a device model with primary key 1
# manufacturer = db_MANUFACTURER.objects.get(id=2)
# print(manufacturer)  # Assuming there's a manufacturer with primary key 1
vendor = db_VENDOR.objects.get(id=1)  # Assuming there's a vendor with primary key 1
location = db_LOCATION.objects.get(id=1)  # Assuming there's a location with primary key 1
site = db_SITE.objects.get(id=1)  # Assuming there's a site with primary key 1
network = db_Network.objects.get(id=1)  # Assuming there's a network with primary key 1

# d_l=['1210230117', '1210230136', '1210230137']
# for i in d_l:
#         device = db_DEVICE(
#         device_id=i,
#         serial_no="ABC123",
#         mac="00:11:22:33:44:55",
#         sim_manufacturer="airtel",
#         sim_number="1234567890",
#         address="123 Example St, City, Country",
#         parameters='so2Voltages,no2Voltages,ozoneVoltages,coVoltages,pm25_base,pm10_base',
#         altitude=123.456,
#         install_dt=timezone.now(),
#         approved_by="John Doe",
#         remarks="Example remarks",
#         notes="Example notes",
#         status=1,  # Assuming status 1 represents active
#         land_use="Residential",
#         device_model_id=device_model,
#         manufacturer_id=manufacturer,
#         vendor_id=vendor,
#         locations_id=location,
#         site_id=site,
#         network_id=network,
#         create_dt=timezone.now(),  # Current timestamp
#         modified_dt=timezone.now(),  # Current timestamp
#         created_by=user,  # Linking to the user who created this record
#         modified_by=user  # Linking to the same user who modified this record
#         )

#         # Save the device instance to the database
#         device.save()

# manufacturer = db_MANUFACTURER(
#     id=3,
#     name="respirer",
    
#     api_or_pass="KLaP6Z6a7B",
#     data_url="http://atmos.urbansciences.in/adp/v4/getDeviceDataParam/imei/",

    
#     address="123 Example St, City, Country",
#     person_name="Anil",
#     person_email="john@example.com",
#     person_Contact="+1234567890",
#     status=0,  # Assuming status 1 represents active
#     notes="Example notes about the manufacturer",
#     create_dt=timezone.now(),  # Current timestamp
#     modified_dt=timezone.now(),  # Current timestamp
#     created_by=user,  # Linking to the user who created this record
#     modified_by=user  # Linking to the same user who modified this record
# ).save()


# manufacturer = db_MANUFACTURER(
#         id=5,
#     name="sensit_ramp_demo",
#     auth_url='https://api.sensitconnect.net/users/signin',
#     api_or_pass="Cstep123!@#",
#     data_url="https://api.sensitconnect.net/sensors-data/getRAMPDeviceLogBetweenTimePeriod",
    
    
#     email='emil.varghese@cstep.in',
#     address="123 Example St, City, Country",
#     person_name="John Doe",
#     person_email="john@example.com",
#     person_Contact="+1234567890",
#     status=0,  # Assuming status 1 represents active
#     notes="Example notes about the manufacturer",
#     create_dt=timezone.now(),  # Current timestamp
#     modified_dt=timezone.now(),  # Current timestamp
#     created_by=user,  # Linking to the user who created this record
#     modified_by=user  # Linking to the same user who modified this record
# ).save()


# d_l=[2000,2001]
# # # d_l=[1168,1170,1169,1171,1172]
# for i in d_l:
#         device = db_DEVICE(
#         device_id=i,
#         serial_no="ABC123",
#         mac="00:11:22:33:44:55",
#         sim_manufacturer="jio",
#         sim_number="1234567890",
#         address="123 Example St, City, Country",
#         altitude=123.456,
#         install_dt=timezone.now(),
#         approved_by="John Doe",
#         remarks="Example remarks",
#         notes="Example notes",
#         status=1,  # Assuming status 1 represents active
#         land_use="Residential",
#         device_model_id=device_model,
#         manufacturer_id=manufacturer,
#         vendor_id=vendor,
#         locations_id=location,
#         site_id=site,
#         network_id=network,
#         create_dt=timezone.now(),  # Current timestamp
#         modified_dt=timezone.now(),  # Current timestamp
#         created_by=user,  # Linking to the user who created this record
#         modified_by=user  # Linking to the same user who modified this record
#         )

#         # Save the device instance to the database
#         device.save()


# d_l= ["2CF4328C5D16", "2CF4328C5DBD", "4C752508CE02","807D3A3774F9","8CAAB5C89083"]

# # d_l= ["3C6105F42DC1", "CC50E36112FA", "2CF43219D002", "98F4ABB49814", "4C11AE13AFF5"]
# d_l=[2000,2001]
# # # d_l=[1168,1170,1169,1171,1172]
# for i in d_l:
#         device = db_DEVICE(
#         device_id=i,
#         serial_no="ABC123",
#         mac="00:11:22:33:44:55",
#         sim_manufacturer="jio",
#         sim_number="1234567890",
#         address="123 Example St, City, Country",
#         altitude=123.456,
#         install_dt=timezone.now(),
#         approved_by="John Doe",
#         remarks="Example remarks",
#         notes="Example notes",
#         status=0,  # Assuming status 1 represents active
#         land_use="Residential",
#         device_model_id=device_model,
#         manufacturer_id=manufacturer,
#         vendor_id=vendor,
#         locations_id=location,
#         site_id=site,
#         network_id=network,
#         create_dt=timezone.now(),  # Current timestamp
#         modified_dt=timezone.now(),  # Current timestamp
#         created_by=user,  # Linking to the user who created this record
#         modified_by=user  # Linking to the same user who modified this record
#         )

#         # Save the device instance to the database
#         device.save()


# manufacturer = db_MANUFACTURER(
#     id=6,
#     name="aurassure",
    
    
#     data_url='https://api.datoms.io/-/api/iot-platform/v1.1.0/clients/10340/applications/16/things/data',
    
#     access_id="BIHGuOjwbDJv1nAl",
#     access_key="o52dJ0QQIDjSeSFh3uMd8NqUdy9jc8mHsj3xzFkwybHOCi61UuiW6znjaZkJYVQG",
#     content_type="application/json",
#     address="123 Example St, City, Country",
#     person_name="John Doe",
#     person_email="john@example.com",
#     person_Contact="+1234567890",
#     status=0,  # Assuming status 1 represents active
#     notes="Example notes about the manufacturer",
#     create_dt=timezone.now(),  # Current timestamp
#     modified_dt=timezone.now(),  # Current timestamp
#     created_by=user,  # Linking to the user who created this record
#     modified_by=user  # Linking to the same user who modified this record
# )
# manufacturer.save()

# print(manufacturer)

# d_l=[12031,12032,12033,12034,12035,12036]
# for i in d_l:
#         device = db_DEVICE(
#         device_id=i,
#         serial_no="ABC123",
#         mac="00:11:22:33:44:55",
#         sim_manufacturer="jio",
#         sim_number="1234567890",
#         address="123 Example St, City, Country",
#         altitude=123.456,
#         install_dt=timezone.now(),
#         approved_by="John Doe",
#         remarks="Example remarks",
#         notes="Example notes",
#         parameters='pm2.5,pm10,co,co2,o3,so2,no2,temp,humid',
#         status=1,  # Assuming status 1 represents active
#         land_use="Residential",
#         device_model_id=device_model,
#         manufacturer_id=manufacturer,
#         vendor_id=vendor,
#         locations_id=location,
#         site_id=site,
#         network_id=network,
#         create_dt=timezone.now(),  # Current timestamp
#         modified_dt=timezone.now(),  # Current timestamp
#         created_by=user,  # Linking to the user who created this record
#         modified_by=user  # Linking to the same user who modified this record
#         )

#         # Save the device instance to the database
#         device.save()


# manufacturer = db_MANUFACTURER(
#     id=7,
#     name="aeron",
#     auth_url='https://getiotservice.aeronsystems.com:9007/v1.0/gettoken',
#     username='Aeron_Bangalore@2023',
#     api_or_pass='54b8c1c01b',
#     grant_type='password',
#     client_name='aeronapi',
#     client_secret='bm9yZWEyMDEy',
#     host='getiotservice.aeronsystems.com:9007',
#     data_url="https://getiotservice.aeronsystems.com:9007/v1.0/getdataondate",
#     accept='application/json',
    
#     content_type='application/x-www-form-urlencoded',
#     address="123 Example St, City, Country",
#     person_name="John Doe",
#     person_email="john@example.com",
#     person_Contact="+1234567890",
#     status=0,  # Assuming status 1 represents active
#     notes="Example notes about the manufacturer",
#     create_dt=timezone.now(),  # Current timestamp
#     modified_dt=timezone.now(),  # Current timestamp
#     created_by=user,  # Linking to the user who created this record
#     modified_by=user  # Linking to the same user who modified this record
# )
# manufacturer.save()

# d_l=['240091703829123', '240091702635515', '240091702293515', '240091703249662', '240091701926904']
# for i in d_l:
#         device = db_DEVICE(
#         device_id=i,
#         serial_no="ABC123",
#         mac="00:11:22:33:44:55",
#         sim_manufacturer="jio",
#         sim_number="1234567890",
#         address="123 Example St, City, Country",
#         altitude=123.456,
#         install_dt=timezone.now(),
#         approved_by="John Doe",
#         remarks="Example remarks",
#         notes="Example notes",
        
#         status=1,  # Assuming status 1 represents active
#         land_use="Residential",
#         device_model_id=device_model,
#         manufacturer_id=manufacturer,
#         vendor_id=vendor,
#         locations_id=location,
#         site_id=site,
#         network_id=network,
#         create_dt=timezone.now(),  # Current timestamp
#         modified_dt=timezone.now(),  # Current timestamp
#         created_by=user,  # Linking to the user who created this record
#         modified_by=user  # Linking to the same user who modified this record
#         )

#         # Save the device instance to the database
#         device.save()


manufacturer = db_MANUFACTURER(
    id=8,
    name="aqms",
    data_url="https://datalogger.vasthienviro.com/",
    content_type='application/json',
    
    address="123 Example St, City, Country",
    person_name="John Doe",
    person_email="john@example.com",
    person_Contact="+1234567890",
    status=0,  # Assuming status 1 represents active
    notes="Example notes about the manufacturer",
    create_dt=timezone.now(),  # Current timestamp
    modified_dt=timezone.now(),  # Current timestamp
    created_by=user,  # Linking to the user who created this record
    modified_by=user  # Linking to the same user who modified this record
)
manufacturer.save()

d_l=['1599','1598']
for i in d_l:
        device = db_DEVICE(
        device_id=i,
        serial_no="ABC123",
        mac="00:11:22:33:44:55",
        sim_manufacturer="jio",
        sim_number="1234567890",
        address="123 Example St, City, Country",
        altitude=123.456,
        install_dt=timezone.now(),
        approved_by="John Doe",
        remarks="Example remarks",
        notes="Example notes",
        
        status=1,  # Assuming status 1 represents active
        land_use="Residential",
        device_model_id=device_model,
        manufacturer_id=manufacturer,
        vendor_id=vendor,
        locations_id=location,
        site_id=site,
        network_id=network,
        create_dt=timezone.now(),  # Current timestamp
        modified_dt=timezone.now(),  # Current timestamp
        created_by=user,  # Linking to the user who created this record
        modified_by=user  # Linking to the same user who modified this record
        )

        # Save the device instance to the database
        device.save()