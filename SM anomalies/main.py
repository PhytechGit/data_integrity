import os
import sys
import warnings
#import numpy as np
import pandas as pd
#import time
import datetime as dt
from tqdm import tqdm
import logging
# project imports
import project_functions

warnings.filterwarnings('ignore')

local_env = True
debug_ = False

if local_env:
    from tabulate import tabulate
    PHYTECH_DRIVE_PATH = os.environ['PHYTECH_DRIVE_PATH']
    GITHUB_PATH = PHYTECH_DRIVE_PATH + 'GitHub'
    DATA_WD = PHYTECH_DRIVE_PATH + '/Data Integrity/SM anomalies/data'
    CERT_PATH = PHYTECH_DRIVE_PATH + '/Data'
    if CERT_PATH not in sys.path:
        sys.path.append(CERT_PATH)
    import cert_aws as c
else:
    CERT_PATH = '/opt/hubshare/reslab/shared/micro_services/'
    DATA_WD = '/opt/hubshare/reslab/shared/micro_services/data_integrity/SM_not_responding/data'
    if CERT_PATH not in sys.path:
        sys.path.append(CERT_PATH)
    import cert as c
    #logging.basicConfig(filename='logs/run_log_{}.log'.format(dt.date.today().strftime("%Y_%m_%d")) , format='%(asctime)s | %(levelname)s: %(message)s', level=logging.INFO)
    #logging.info(f"######### run start: date {dt.datetime.today().strftime('%Y/%m/%d  %H:%M:%S')} ###########")

def main():
    logging.info(f"######### run start: date {dt.datetime.today().strftime('%Y/%m/%d  %H:%M:%S')} ###########")
    
    summary_df = pd.DataFrame()
    yesterday = (dt.date.today() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = (dt.date.today() - dt.timedelta(days=7)).strftime("%Y-%m-%d")
    failed_list, invalid_projects_list = [],[]

    projects_list = project_functions.get_projects(start_date)
    #projects_list = pd.DataFrame(columns = ['project_id'], data=[852093, 871812, 858363])
    counter = 0
    for p_id in tqdm(projects_list['project_id']):
        if p_id not in [850514]: #, 871812, 858363]:
            continue
        try:
            project_data = project_functions.load_project_data(project_id=p_id, min_date=start_date,
                                                       max_date=yesterday, min_depth=10, max_depth=91, debug=False)
            if not project_data.valid_project: # project not valid for calc
                invalid_projects_list.append(p_id)

            project_df = project_functions.aggregate_project_data(project_data, debug=False)
            if project_functions.write_results_to_db(project_df) == 'OK':
                counter+=1
            summary_df = pd.concat([summary_df,project_df],axis=0,ignore_index=True)

        except Exception as e:
            failed_list.append((p_id, e))
            if local_env:
                print(f"pid {p_id}::Exception={e}")
            
            logging.error(f"pid {p_id}::Exception={e}")
            pass


    print(f'############## {(dt.date.today()).strftime("%Y-%m-%d")} run summary')
    print(f'############## {len(projects_list)} projects total, {counter} projects succesful in DB,{len(failed_list)} failed, {len(invalid_projects_list)} not valid for calc projects')

    if debug_:
        print(f"failed_list: {failed_list}, invalid_projects_list: {invalid_projects_list}")

    if local_env:
        print(tabulate(summary_df, headers='keys'))
    else:
        print(summary_df)

    logging.info(f'############## {(dt.date.today()).strftime("%Y-%m-%d")} run summary ##############')
    logging.info(f'{len(projects_list)} projects total, {counter} projects succesful in DB,{len(failed_list)} failed {len(invalid_projects_list)} not valid for calc projects')
    logging.info(f"######### run end: date {dt.datetime.today().strftime('%Y/%m/%d  %H:%M:%S')} ###########")
        
        
if __name__ == '__main__':
    logging.basicConfig(filename='logs/run_log_{}.log'.format(dt.date.today().strftime("%Y_%m_%d")) , format='%(asctime)s | %(levelname)s: %(message)s', level=logging.DEBUG)
    main()