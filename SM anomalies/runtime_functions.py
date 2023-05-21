## functions for run time
## 1. define criteria for search (territory/crop)
## 2. fetch project list for inspection
## 

from common_db import DB_TABLES, ALLOWED_SM_TYPES
import cert_aws as c
from sql_import_export import SqlImporter
import pandas as pd

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
    sql_importer = SqlImporter(query=query, database=c.database_production, user=c.user_production,
                               password=c.password_production,
                               host=c.host_production, port=c.port_production, verbose=False)

    sql_importer.get_data()
    return sql_importer.data

#############
# define filtering logic for selecting faulty sensors
#
#
def find_projects_with_faulty_sensors(df):
    failed_sensors_list = []
    filtered_df = df[(df['percent_not responding'] > 0.9) & (df.max_SM_diff < 0.5)]
    failed_sensors_list = list(filtered_df.sensor_id.values)
    return(failed_sensors_list)
