import os
import sys

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


TEST = False
tags = ['monitoring', 'S3', 'research', 'SM_data_integrity']
airflow_env = Variable.get('ENV_NAME')


 # Script files locations and names: 
s3_files_path = 'code-for-dags/data-integrity-sm-response/'  # path that holds the S3 files. used also for accessing the files locally
reslab_files_path = 'micro_services/data_integrity/SM_not_responding/'  # script files for reslab are accessible from here 
execution_file = 'main.py'

local_path_base = Variable.get('FILES_LOCAL_PATH_BASE') # python scripts will be accessible from here; true for all DAGS.

# depending of the airflow environment, files are served from different locations:
if airflow_env == 'mwaa-prod':
    mwaa_local_path = os.path.join(local_path_base, s3_files_path)  # the fiels on MWAA retain the same structure as on S3; script files are accessible from here
    execution_file_full = os.path.join(mwaa_local_path, execution_file) 

elif airflow_env == 'reslab':
    reslab_local_path = local_path_base + reslab_files_path  # script files for reslab are accessible from here 
    execution_file_full = os.path.join(reslab_local_path, execution_file)     

elif airflow_env == 'local':
    pass  # we're not really testing it locally; just checking for asic errors like syntax errors

else:
    raise Exception (f'Unknown Airflow installation. {airflow_env} was detected')



my_limit_var = 10
# Buildingthe DAG:
default_args = {
    'owner': 'research',
    'start_date': datetime(2023, 6, 23),
    'email': ['maoz.d@phytech.com'],
    'retries': 0,
    'retry_delay': timedelta(seconds=10),
}

       
if not TEST: 
    dag = DAG(
    dag_id='data_integrity_sm_response_002',
    default_args=default_args,
    catchup=False,
    schedule='01 12 * * *',
    tags=tags,
    description='Identifying faulty SM sensors',
    )


    # running the main task as a BashOperator:
    main_task_run = BashOperator(
        task_id='ETL_PROD',
        bash_command=f'cd {mwaa_local_path} && python3 {execution_file}',
        dag=dag
    )
