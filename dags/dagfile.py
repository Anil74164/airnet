


import time
from airflow import DAG
import DjangoSetup
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from core.pyspark.DataIngestion import main_fetch
import os
import logging
 
logger=logging.getLogger('workspace')
 
 
# Define the timeit decorator
def timeit(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        start_time_str = time.strftime('%Y%m%d:%H%M%S', time.localtime(start))
        result = func(*args, **kwargs)
        end = time.time()
        logger.info(f"{start_time_str} {func.__name__} took {str((end - start) * 1000)} milliseconds")
        return result
    return wrapper
 

def get_fetch_window():
    now = datetime.now()
    rounded_minute = (now.minute // 15) * 15
    endDt = now.replace(minute=rounded_minute, second=0, microsecond=0)
    startDt = endDt - timedelta(minutes=15)

    return startDt, endDt #as a tuple

@timeit
def dataingestion_program(**kwargs):
    try:
        startDt,endDt=get_fetch_window()
        
        startDt_str = startDt.strftime('%Y%m%d%H%M%S')
        endDt_str = endDt.strftime('%Y%m%d%H%M%S')
        
        command = f"spark-submit --master local[*] ./core/pyspark/DataIngestion.py -s {startDt_str} -e {endDt_str}"
        result = os.system(command)
        
        if result != 0:
            logger.error(f"Command failed with exit code {result}")
        else:
            logger.info("Command executed successfully")
    except OSError as e:
        logger.error(f"OSError: {e.strerror}")
 
@timeit
def aggregation_program(**kwargs):
    try:
        startDt,endDt=get_fetch_window()
        
        startDt_str = startDt.strftime('%Y%m%d%H%M%S')
        endDt_str = endDt.strftime('%Y%m%d%H%M%S')

        command = f"spark-submit --master local[*] ./core/pyspark/aggregation.py -s {startDt_str} -e {endDt_str}"
        result = os.system(command)
        
        if result != 0:
            logger.error(f"Command failed with exit code {result}")
        else:
            logger.info("Command executed successfully")
    except OSError as e:
        logger.error(f"OSError: {e.strerror}")



def calibration_program():
    try:
        startDt,endDt=get_fetch_window()
        
        startDt_str = startDt.strftime('%Y%m%d%H%M%S')
        endDt_str = endDt.strftime('%Y%m%d%H%M%S')
        
        command = f"spark-submit --master local[*] ./core/pyspark/calibrate.py -s {startDt_str} -e {endDt_str}"
        result = os.system(command)
        
        if result != 0:
            logger.error(f"Command failed with exit code {result}")
        else:
            logger.info("Command executed successfully")
    except OSError as e:
        logger.error(f"OSError: {e.strerror}")


 
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': (datetime.today() - timedelta(days=1)),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}
 
dag = DAG(
    'data_fetch_dag',
    default_args=default_args,
    description='A DAG for data fetching',
    schedule_interval=timedelta(minutes=15),  # Run every 15 minute
)
 
# Task to execute the PySpark job
dataingestion_task = PythonOperator(
    task_id='DataIngestion',
    python_callable=dataingestion_program,
    dag=dag,
)
 
aggregation_task = PythonOperator(
        task_id='Aggregation',
        python_callable=aggregation_program,
        dag=dag,
)

cstep_calibration=PythonOperator(
    task_id='Calibration',
    python_callable=calibration_program,
    dag=dag,
)
dataingestion_task >> aggregation_task >> cstep_calibration