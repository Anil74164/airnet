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
manufacturer = db_MANUFACTURER.objects.get(id=2)
print(manufacturer)  # Assuming there's a manufacturer with primary key 1
vendor = db_VENDOR.objects.get(id=1)  # Assuming there's a vendor with primary key 1
location = db_LOCATION.objects.get(id=1)  # Assuming there's a location with primary key 1
site = db_SITE.objects.get(id=1)  # Assuming there's a site with primary key 1
network = db_Network.objects.get(id=1)  # Assuming there's a network with primary key 1

d_l=['1210230117', '1210230136', '1210230137']
for i in d_l:
        device = db_DEVICE(
        device_id=i,
        serial_no="ABC123",
        mac="00:11:22:33:44:55",
        sim_manufacturer="airtel",
        sim_number="1234567890",
        address="123 Example St, City, Country",
        parameters="Example parameters",
        altitude=123.456,
        install_dt=timezone.now(),
        approved_by="John Doe",
        remarks="Example remarks",
        notes="Example notes",
        status=0,  # Assuming status 1 represents active
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
