import os
import sys
from logic_parameters import ENV_NAME

try:
    from airflow.models import Variable
    os.environ['ENV_NAME'] = Variable.get('ENV_NAME')
except: 
    os.environ['ENV_NAME'] = ENV_NAME
print(f"ENV_NAME = {os.environ['ENV_NAME']}")

DB_TABLES = {
    'projects_metadata_table': 'projects_metadata',
    'soil_sensors_metadata': 'soil_sensors_metadata',
    'phytoweb_projects': 'view_phytoweb_projects',
    'phytoweb_projects_s': 'view_phytoweb_projects_slims',
    'soil_measurements': 'soil_measurements',
    'public.sensor_calculations_v2': 'sensor_calculations',
    'weather_area_calcs': 'WEATHER_AREA_CALCS',
    'rl': 'project_refill_line',
    'results_table': 'project_soil_prediction',
    'project_irrigation_spans': 'project_irrigation_spans_v2',
    'projects_hierachy': 'projects_hierachy'
}

ALLOWED_SM_TYPES  = (90, 91, 92, 98, 117, 118, 124, 127, 135, 137,)

if os.environ['ENV_NAME'] == 'local_env':
    #from tabulate import tabulate
    PHYTECH_DRIVE_PATH = os.environ['PHYTECH_DRIVE_PATH']
    GITHUB_PATH = PHYTECH_DRIVE_PATH + 'GitHub'
    DATA_WD = PHYTECH_DRIVE_PATH + '/Data Integrity/SM anomalies/data'
    CERT_PATH = PHYTECH_DRIVE_PATH + '/Data'
    if CERT_PATH not in sys.path:
        sys.path.append(CERT_PATH)
    try:
        import cert_aws as c
    except:
        print('fail import cert_aws')

elif os.environ['ENV_NAME'] == 'reslab':
    CERT_PATH = '/opt/hubshare/reslab/shared/micro_services/'
    DATA_WD = '/opt/hubshare/reslab/shared/micro_services/data_integrity/SM_not_responding/data'
    if CERT_PATH not in sys.path:
        sys.path.append(CERT_PATH)
    import cert as c
    
    os.environ['DATABASE_URL_PROD_JAVA'] = c.DATABASE_URL_production
    os.environ['DATABASE_URL_PROD_RUBY'] = c.DATABASE_URL_production_ruby
    os.environ['DATABASE_URL_DEV'] = c.DATABASE_URL_dev
    os.environ['DATABASE_URL_RESEARCH'] = c.DATABASE_URL_research

elif os.environ['ENV_NAME'] == 'mwaa-prod':
    import pandas as pd
    c = pd.Series(index=['DATABASE_URL_production','DATABASE_URL_production_ruby','DATABASE_URL_dev','DATABASE_URL_research'],
             data=[Variable.get('DATABASE_URL_PROD'), 
                   Variable.get('DATABASE_URL_PROD').replace('java','ruby'),
                   Variable.get('DATABASE_URL_DEV'),
                   Variable.get('DATABASE_URL_RESEARCH')])
    os.environ['AWS_S3_KEY'] = Variable.get('AWS_access_for_s3_files')
    os.environ['AWS_S3_SECRET'] = Variable.get('AWS_access_secret_for_S3_files')

else:
    print('No enviroment found')
    exit()


os.environ['DATABASE_URL_PROD'] = c.DATABASE_URL_production
os.environ['DATABASE_URL_PROD_RUBY'] = c.DATABASE_URL_production_ruby
os.environ['DATABASE_URL_DEV'] =  c.DATABASE_URL_dev
os.environ['DATABASE_URL_RESEARCH'] = c.DATABASE_URL_research

os.environ['DATABASE_URL_EXTRACT_JAVA'] = os.environ['DATABASE_URL_PROD']
os.environ['DATABASE_URL_EXTRACT_RUBY'] = os.environ['DATABASE_URL_PROD_RUBY']
os.environ['DATABASE_URL_LOAD'] = os.environ['DATABASE_URL_DEV']

