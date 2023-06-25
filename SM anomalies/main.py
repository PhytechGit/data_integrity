import os
import sys
import warnings
import pandas as pd
import datetime as dt
#from tqdm import tqdm
import project_functions
import common_db
from logic_parameters import debug_

warnings.filterwarnings('ignore')
"""
if local_env:
    #from tabulate import tabulate
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
    os.environ['DATABASE_URL_PROD_JAVA'] = c.DATABASE_URL_production
    os.environ['DATABASE_URL_PROD_RUBY'] = c.DATABASE_URL_production_ruby
    os.environ['DATABASE_URL_RESEARCH'] = c.DATABASE_URL_research
"""
def main():    
    summary_df = pd.DataFrame()
    yesterday = (dt.date.today() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = (dt.date.today() - dt.timedelta(days=8)).strftime("%Y-%m-%d")
    failed_list, invalid_projects_list = [],[]
    
    if os.environ['ENV_NAME'] == 'reslab':
        import logging
        logging.basicConfig(filename='logs/run_log_{}.log'.format(dt.date.today().strftime("%Y_%m_%d")) , format='%(asctime)s | %(levelname)s: %(message)s', level=logging.DEBUG)
        logging.info(f"######### run start: date {dt.datetime.today().strftime('%Y/%m/%d  %H:%M:%S')} ###########")

    projects_list = project_functions.get_projects(start_date)
    counter = 0
    for p_id in projects_list['project_id']:
        if p_id not in [858363, 860702, 871812]:
            continue
        project_data = project_functions.load_project_data(project_id=p_id, min_date=start_date,
                                                       max_date=yesterday, min_depth=10, max_depth=91, debug=debug_)
        if project_data:
            if not project_data.valid_project: # project not valid for calc
                #print(f'{p_id} not valid for calc')
                invalid_projects_list.append(p_id)
            project_df = project_functions.aggregate_project_data(project_data, debug=False)

            if not project_df.empty:
                if project_functions.write_results_to_db(project_df) == 'OK':
                    counter+=1
                summary_df = pd.concat([summary_df,project_df],axis=0,ignore_index=True)
        else:
            #print(f'{p_id} failed')
            failed_list.append(p_id)
        if counter > 10:
            break

    if os.environ['ENV_NAME'] == 'local_env':
        print(summary_df)

    print(f'############## {(dt.date.today()).strftime("%Y-%m-%d")} run summary')
    print(f'############## {len(projects_list)} projects total, {counter} projects succesful in DB,{len(failed_list)} failed, {len(invalid_projects_list)} not valid for calc projects')

    if os.environ['ENV_NAME'] == 'reslab':
        logging.info(f'############## {(dt.date.today()).strftime("%Y-%m-%d")} run summary ##############')
        logging.info(f'{len(projects_list)} projects total, {counter} projects succesful in DB,{len(failed_list)} failed {len(invalid_projects_list)} not valid for calc projects')
        logging.info(f"######### run end: date {dt.datetime.today().strftime('%Y/%m/%d  %H:%M:%S')} ###########")
        
        
if __name__ == '__main__':
    main()
    
