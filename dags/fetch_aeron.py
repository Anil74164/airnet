from airflow import DAG
import DjangoSetup
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from core.pyspark.DataIngestion import main_fetch
from core.models import db_DEVICE,db_MANUFACTURER

class Dict2Object(object):
    def __init__(self, my_dict):
        for key in my_dict:
            setattr(self, key, my_dict[key])

def get_aeron_devices():
    # Read from core_db_devices and filter devices
    manufacturer=db_MANUFACTURER.objects.get(name='aeron')
    return list(db_DEVICE.objects.filter(manufacturer_id=manufacturer.id,status=1))

def fetch_aeron_devices():
    # Define the end_date as datetime.now()
    end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=1)  # Yesterday

    
    # Fetch the list of Aeron devices
    aeron_devices = get_aeron_devices()
    print(aeron_devices)
    # Loop through each device and call main_fetch
    for device in aeron_devices:
        args_dict = {
            'start': start_date,
            'end': end_date,
            'device': device.device_id,
            'pollutant': None
        }

        args_obj = Dict2Object(args_dict)
        main_fetch(args_obj)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
   'start_date': datetime.today() - timedelta(days=1),  
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Define the DAG
dag = DAG(
    'fetch_aeron_devices_daily',
    default_args=default_args,
    description='A simple DAG to fetch data from AERON devices daily',
    schedule_interval='@daily',
)

# Define the PythonOperator
fetch_task = PythonOperator(
    task_id='fetch_aeron_devices_task',
    python_callable=fetch_aeron_devices,
    dag=dag,
)