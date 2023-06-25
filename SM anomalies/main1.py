import os
import sys
import warnings
import numpy as np
import pandas as pd
import time
import datetime as dt
from tqdm import tqdm

warnings.filterwarnings('ignore')

local_env = True
debug_ = False

if local_env:
    import cert_aws as c
    from tabulate import tabulate
    PHYTECH_DRIVE_PATH = os.environ['PHYTECH_DRIVE_PATH']
    GITHUB_PATH = PHYTECH_DRIVE_PATH + 'GitHub'
    DATA_WD = PHYTECH_DRIVE_PATH + '/Data Integrity/SM anomalies/data'
    CERT_PATH = PHYTECH_DRIVE_PATH + '/Data'
    if CERT_PATH not in sys.path:
        sys.path.append(CERT_PATH)
else:
    DATA_WD = '/opt/hubshare/reslab/shared/micro_services/data_integrity/SM_not_responding/data'
    sys.path.append('/opt/hubshare/reslab/shared/micro_services/')
    import cert as c
    

from sql_import_export import SqlImporter
import project_class_data_extract
import project_functions
import sensor_functions
from testing import test_project_data
from logic_parameters import default_latitude, default_height
from common_db import DB_TABLES, ALLOWED_SM_TYPES

summary_df = pd.DataFrame()
yesterday = (dt.date.today() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
start_date = (dt.date.today() - dt.timedelta(days=8)).strftime("%Y-%m-%d")
failed_list, invalid_projects_list = [],[]

projects_list = project_functions.get_projects(start_date)
#print(projects_list)
#projects_list = [852093, 871812, 858363]
for p_id in tqdm(projects_list['project_id']):
    #if p_id != 848545:
    #    continue
    try:
        project_data = project_functions.load_project_data(project_id=p_id, min_date=start_date,
                                                   max_date=yesterday, min_depth=10, max_depth=91, debug=False)
        if project_data.valid_project:
            if len(project_data.df_irrigation) == 0:
                continue
            project_df = project_functions.aggregate_project_data(project_data, debug=False)
            if len(project_df) == 0: # project not valid for calc
                invalid_projects_list.append(p_id)
            summary_df = pd.concat([summary_df,project_df],axis=0,ignore_index=True)
            project_functions.write_results_to_db(summary_df)
        else:
            failed_list.append(p_id)
    except Exception as e:
        print(f'pid {p_id}:: {e}')
        failed_list.append((p_id, e))
        pass


#ts = dt.datetime.now().strftime("%Y_%m_%d %H-%M-%S")
#summary_df.to_csv(DATA_WD + f"/projects_df_{ts}.csv")

print(f'############## {(dt.date.today()).strftime("%Y-%m-%d")} run summary')
print(f'############## {len(projects_list)} projects total')
print(f'############## {len(projects_list)-len(project_data.df_irrigation)} projects without irrigation,{len(failed_list)} failed, {len(invalid_projects_list)} not valid for calc projects')
if debug_:
    print(f"failed_list: {failed_list}, invalid_projects_list: {invalid_projects_list}")

if local_env:
    print(tabulate(summary_df, headers='keys'))
else:
    print(summary_df)

