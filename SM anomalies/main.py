import os
import sys
import warnings
import numpy as np
import pandas as pd
import time
import datetime as dt
from tabulate import tabulate

warnings.filterwarnings('ignore')

PHYTECH_DRIVE_PATH = os.environ['PHYTECH_DRIVE_PATH']
GITHUB_PATH = PHYTECH_DRIVE_PATH + 'GitHub'
DATA_WD = PHYTECH_DRIVE_PATH + '/Data Integrity/SM anomalies/data'
CERT_PATH = PHYTECH_DRIVE_PATH + '/Data'
if CERT_PATH not in sys.path:
    sys.path.append(CERT_PATH)
import cert_aws as c
from sql_import_export import SqlImporter
import project_class_data_extract
import functions
import logic_parameters
from logic_parameters import default_latitude, default_height
from common_db import DB_TABLES, ALLOWED_SM_TYPES

def get_projects(start_date):
    # import os
    # from .sql_import_export import SqlImporter
    query = f"""
            SELECT DISTINCT project_id
            FROM {DB_TABLES['projects_metadata_table']}
            WHERE active=true
            """
    query = f"""
        SELECT distinct(pm.project_id)
        FROM {DB_TABLES['projects_metadata_table']} pm
        JOIN {DB_TABLES['soil_sensors_metadata']}  ssm
        ON pm.project_id = ssm.project_id
        JOIN {DB_TABLES['project_irrigation_spans']} pis
        ON pis.project_id = pm.project_id
        WHERE time_zone like '%Los_Angeles'
        AND active=true
        AND season = 2023
        AND type_id IN (90, 91, 92, 98, 117, 118, 124, 127, 135, 137)
        AND start_date >= CAST((CAST('{start_date}' AS timestamp)) AS date)
        ORDER BY project_id
        """
    # sql_importer = SqlImporter(
    #                    query,
    #                    conn_str=c.full_url_production_java)
    sql_importer = SqlImporter(query=query, database=c.database_production, user=c.user_production,
                               password=c.password_production,
                               host=c.host_production, port=c.port_production, verbose=False)

    sql_importer.get_data()
    return sql_importer.data


# get_projects(start_date)
# project_data = functions.load_project_data(project_id=855929, min_date=start_date,
#                                           max_date=yesterday, min_depth=10, max_depth=91, debug=False)
summary_df = pd.DataFrame()
yesterday = (dt.date.today() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
start_date = (dt.date.today() - dt.timedelta(days=8)).strftime("%Y-%m-%d")
failed_list = []

projects_list = get_projects(start_date)
print(projects_list)
for p_id in projects_list['project_id'][:50]:
    if p_id != 849305:
        continue
    try:
        project_data = functions.load_project_data(project_id=p_id, min_date=start_date,
                                                   max_date=yesterday, min_depth=10, max_depth=91, debug=False)
        if project_data.valid_project:
            if len(project_data.df_irrigation) == 0:
                continue
            project_df = functions.get_project_results(project_data)
            summary_df = pd.concat([summary_df, project_df], axis=0)
        else:
            failed_list.append(p_id)
    except Exception as e:
        print(e)
        failed_list.append(p_id)
        pass

# print(summary_df[(summary_df.not_responding_events_count / summary_df.irrigation_events) > 0.5])
#projects_df = functions.get_project_results(project_data, False)
ts = dt.datetime.now().strftime("%Y_%m_%d %H-%M-%S")
summary_df.to_csv(DATA_WD + f"/projects_df_{ts}.csv")

print(tabulate(summary_df, headers='keys'))

