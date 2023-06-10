import os
import sys
import warnings
import numpy as np
import pandas as pd
import time
import datetime as dt
#from tabulate import tabulate

warnings.filterwarnings('ignore')
try:
    from sql_import_export import SqlImporter
    import cert_aws as c
    import project_class_data_extract
    import project_functions
    import sensor_functions
    from testing import test_project_data
    from logic_parameters import default_latitude, default_height
    from common_db import DB_TABLES, ALLOWED_SM_TYPES
except ModuleNotFoundError:
        import airflow
        from airflow.models import Variable
        os.environ['DATABASE_URL_PROD'] = Variable.get('DATABASE_URL_PROD')
        os.environ['DATABASE_URL_DEV'] = Variable.get('DATABASE_URL_DEV')
        os.environ['DATABASE_URL_RESEARCH'] = Variable.get('DATABASE_URL_RESEARCH')


sql_importer = SqlImporter(query='', conn_str=c.full_url_research)
sql_importer.get_data()