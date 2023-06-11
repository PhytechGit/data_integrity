## functions for run time
## 1. define criteria for search (territory/crop)
## 2. fetch project list for inspection
## 

import sys
import pandas as pd
import numpy as np
import datetime as dt
import os
import logging
logging = logging.getLogger(__name__)

local_env = True
if local_env:
    PHYTECH_DRIVE_PATH = os.environ['PHYTECH_DRIVE_PATH']
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

from common_db import DB_TABLES, ALLOWED_SM_TYPES
import logic_parameters
from sql_import_export import SqlImporter
from sensor_functions import find_not_responding_events,find_events_without_irrigation# decide_sensor_status#,not_responding_logic



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
                               host=c.host_production, port=c.port_production, verbose=logic_parameters.sql_debug)

    sql_importer.get_data()
    return sql_importer.data

def load_project_data(project_id, min_date, max_date, min_depth=10, max_depth=91, debug=logic_parameters.debug_):
    from logic_parameters import default_latitude, default_height, default_max_depth
    from project_class_data_extract import Project
    project = Project(
        project_id=project_id,
        min_depth=min_depth,
        max_depth=default_max_depth,  # set the depth range we're interested in
        min_date=min_date,
        max_date=max_date,
        debug=debug)
    try:
        project.load_project_metadata()
        project.get_sm_depths()
        project.load_sm_project_data(min_depth=project.depths_found[0], max_depth=project.depths_found[
        -1], )  # change the min/max_depth if you dont want to load all depths
        #TODO
        #project.load_project_weather_data(future=14)  # load the weather date until max_date + 14 days.
        project.apply_transformers()
        project.group_data_to_depths()
        project.load_irrigation_spans()
        project.find_probe_local_saturation()
        project.SM_statistics_by_probe()
        project.get_sensor_daily_status()
        project.get_sensor_support_status()
        project.filter_irrigation_events_from_df()
        project.remove_rain_days()
    except Exception as e:
            if local_env:
                print(f"pid {project_id}::Exception={e}")
            
            logging.warning(f"pid {project_id}::Exception={e}")
            return()
    
    project.meta_data = {'project_id': project.project_id,
                         'latitude': project.latitude if project.latitude else default_latitude,
                         'height': project.height if project.height else default_height, 'app_link': project.app_link}
    
    return project

####################
def aggregate_project_data(project_data, debug=logic_parameters.debug_):
    try:
        project_dict = get_project_results(project_data, debug=debug)
        #get_support_status_information(project_data, project_dict)

        project_df = pd.DataFrame.from_dict(project_dict,orient='index').T

        project_df['percent_not_responding'] = 0
        project_df['algorithm_score'] = calc_algorithm_score(project_dict)
        project_df['notes'] = ''
        project_df['sensor_status'] = 'OK'

        fail_technical_days, fail_reason = count_fail_technical_days(project_data)
    except Exception as e:
        if local_env:
                print(f"pid {project_data.project_id}::Exception={e}")
            
        logging.warning(f"pid {project_data.project_id}::Exception={e}")
        return()

    if project_data.valid_project:
        project_df['percent_not_responding'] = ((project_df.not_responding_events_count / project_df.irrigation_events)[0] if project_df.irrigation_events.all()>0 else 0)

        if (project_df['percent_not_responding'][0] > logic_parameters.MAX_PCT_NOT_RESPONDING) & (project_df.events_max_diff[0] < logic_parameters.EVENT_MAX_DIFF):
            project_df['sensor_status'] = 'fault'
            project_df['notes'] = 'not responding'
        elif len(project_data.df_irrigation) == 0: # no irrigation events
            project_df['sensor_status'] = 'NA'
            project_df['notes'] = 'no irrigation events'
        
        if fail_technical_days > logic_parameters.MAX_FAIL_TECHNICAL_DAYS:
            project_df['sensor_status'] = 'NA'
            project_df['notes'] = f'{fail_reason[0]}'
        elif project_data.missing_data:
            project_df['sensor_status'] = 'NA'
            project_df['notes'] = 'missing data'
        elif project_data.df_sm_data_raw.empty:
            project_df['sensor_status'] = 'NA'
            project_df['notes'] = 'empty SM data'
        #else:
        #    project_df['sensor_status'] = 'OK'
    #project_df.apply(lambda x: decide_sensor_status(x, project_data), axis=1)
    #decide_sensor_status(project_df, project_data)
    else:
        project_df['sensor_status'] = 'NA'
        project_df['notes'] = 'not valid for calc'
        if local_env:
            print(f"project {project_data.project_id} not valid for calc")
        else:
            logging.info(f"project {project_data.project_id} not valid for calc")

    if project_df['sensor_status'][0] != 'OK':
        project_df['algorithm_score'] = None
    """
    cols = ['date', 'project_id', 'sensor_id', 'area_id', 'area_name','company_name', 'territory', 'crop_name', 'variety_id', 'probe_depths','irrigation_events', 'not_responding_events_count', 'event_timestamp','remarks', 'support_status', 'work_type', 'support_updated_at','days_since_task_complete', 'link', 'timezone', 'events_max_diff','max_diff', 'max_hourly_diff', 'percent_not_responding','sensor_status', 'algorithm_score', 'notes']
    """
    project_df['date'] = dt.date.today().strftime("%Y-%m-%d")
    cols = project_df.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    #project_df = project_df.reindex(cols, axis="columns")
    project_df = project_df[cols]
    
    return(project_df)

def get_project_results(project_data, debug=logic_parameters.debug_):
    
    project_dict = {}
    remarks = set()
    project_dict.update({'project_id' : project_data.project_id,
                         'sensor_id' : project_data.sensor_id,
                         'area_id' : project_data.area_id,
                         'area_name' : project_data.area_name,
                         'company_name' : project_data.company_name,
                         'territory' : project_data.territory,
                         'crop_name' : project_data.crop_name,
                         'variety_id' : project_data.variety_id,
                         'probe_depths': '',
                         'irrigation_events' : 0,
                         'not_responding_events_count' : -99,
                         'link' : project_data.app_link,
                         'timezone' : project_data['timezone'],
                         'events_max_diff': -99,
                         'event_timestamp' : '',
                         'remarks' : '',
                         'max_diff': -99,
                         'max_hourly_diff': -99})
    # support status information
    project_dict.update({'support_status' : project_data.sensor_support_status_dict['status'],
                         'work_type': project_data.sensor_support_status_dict['work_type'],
                         'support_updated_at' : project_data.sensor_support_status_dict['updated_at'],
                         'days_since_task_complete': project_data.sensor_support_status_dict['days_since_task_complete']})

    if project_data.valid_project:
        project_results = find_not_responding_events(project_data,debug)
        find_events_without_irrigation
        df_irr = project_data.df_irrigation[(project_data.df_irrigation.amount > logic_parameters.MIN_IRR_AMOUNT) | ((project_data.df_irrigation.amount > logic_parameters.MIN_IRR_AMOUNT_SPRINKLER) & (project_data.df_irrigation.system_type == 'sprinkler'))]
        project_dict.update({'probe_depths': project_results.get('probe_depths'),
                         'irrigation_events' : len(df_irr),
                         'not_responding_events_count' : project_results['events_count']})

        """projects_df = pd.DataFrame(columns                                 ['project_id','sensor_id','area_id','area_name','company_name','territory','crop_name','variety_id',
                                        'probe_depths', 'irrigation_events',
                                        'not_responding_events_count', 'events_max_diff',
                                        'SM_statistics',
                                        'event_timestamp',
                                        'support_status','work_type',
                                        'support_updated_at','days_since_task_complete',
                                        'remarks', 'link','timezone'])
    """
    
        # No not_responding events found
        if project_results['events_details']: # empty dict = No events
            # find probe with max not responding events
            #print(project_results['events_details'])
            for depth in project_results.get('probe_depths'): 
                #print('late_response', project_results['events_details'][d]['late_response'])
                project_dict.update({'events_max_diff': max(d['probe_SM_diff'] for d in project_results['events_details'].values()),
                                     'max_diff': max(d['max_diff'] for d in project_data.SM_statistics.values()),
                                     'max_hourly_diff': max(d['max_hourly_diff'] for d in project_data.SM_statistics.values()),
                                     'event_timestamp' : project_results.get('event_timestamp'),
                                    'remarks': project_results['remarks']})
                #if () & (project_data.SM_statistics.values()['trend'] < 0),
                #print(depth, project_data.SM_statistics[d]['trend'])
            #TODO 
            if pd.isna(project_dict['max_diff']):
                project_dict['max_diff'] = -99
            #elif np.isnan(project_dict['max_diff']):
            #    project_dict['max_diff'] = -99
            #elif (project_dict['max_diff']) != (project_dict['max_diff']):
            #    project_dict['max_diff'] = -99

            if pd.isna(project_dict['max_hourly_diff']):
                project_dict['max_hourly_diff'] = -99

    return(project_dict)

def calc_algorithm_score(project_dict):
    score = 100
    try:
        if project_dict['irrigation_events'] < 2:
            score-=10
    #if project_dict.max_diff > logic_parameters.MIN_SM_DIFF & project_dict.
    except Exception as e:
        score=-99
    return(score)
    

def count_fail_technical_days(project_data):
    fail_technicl_days = 0
    if not project_data.df_daily_status.empty:
        if 'FAIL' in project_data.df_daily_status['daily_status'].value_counts().index:
            fail_technicl_days = min(project_data.df_daily_status['daily_status'].value_counts()['FAIL'], project_data.return_project_period())
            if fail_technicl_days > logic_parameters.MAX_FAIL_TECHNICAL_DAYS:
                if local_env:
                    print(f"pid {project_data.project_id}::{fail_technicl_days} FAIL_TECHNICAL_DAYS")
                else:
                    logging.info(f"pid {project_data.project_id}::{fail_technicl_days} FAIL_TECHNICAL_DAYS")
                #return(fail_technicl_days)
        #else: # valid project= NO technical fail
        reason = project_data.df_daily_status.fail_reason.mode()[0]
    else:
        fail_technicl_days , reason = 7, 'NO daily status data'

    return fail_technicl_days, reason

def get_support_status_information(project_data, project_dict):
    """if not project_data.sensor_support_status:
        sensor_support_status = {'status': '', 'updated_at': (dt.date.today()).strftime("%Y-%m-%d"),
                         'work_type': '', 'days_since_task_complete': -99}
    else:
        sensor_support_status = project_data.sensor_support_status_dict[0]
        sensor_support_status['days_since_task_complete'] = (dt.datetime.today().date() -
                                    sensor_support_status['updated_at'].date()).days
    """
    sensor_support_status = project_data.sensor_support_status_dict[0]
    project_dict.update({'support_status' : sensor_support_status['status'],
                         'work_type': sensor_support_status['work_type'],
                         'support_updated_at' : sensor_support_status['updated_at'],
                         'days_since_task_complete': sensor_support_status['days_since_task_complete']})



#############
# write results to DB
def write_results_to_db(summary_df, debug=logic_parameters.sql_debug):
    
    cols = ['date', 'project_id', 'sensor_id', 'area_id', 'area_name',
       'company_name', 'territory', 'crop_name', 'variety_id', 'probe_depths',
       'irrigation_events', 'not_responding_events_count','event_timestamp','remarks',
       'support_status','work_type',
        'support_updated_at','days_since_task_complete',
        'link', 'timezone',
        'events_max_diff','max_diff','max_hourly_diff',
       'percent_not_responding', 'sensor_status','algorithm_score','notes']
    
    project_id =  summary_df.project_id[0]
    for row in summary_df[cols].iterrows():
        try:
            query = """INSERT INTO public.DI_SM_not_responding_daily_results ({columns}) 
                        VALUES 
                        (
                        '{date}'::DATE
                        ,{project_id}
                        ,{sensor_id}
                        ,{area_id}
                        ,'{area_name}'
                        ,'{company_name}'
                        ,'{territory}'
                        ,'{crop_name}'
                        ,{variety_id}
                        ,ARRAY{probe_depths}::integer[]
                        ,{irrigation_events}
                        ,{not_responding_events_count}
                        ,ARRAY{event_timestamp}::text[]
                        ,ARRAY{remarks}::text[]
                        ,'{support_status}'
                        ,'{work_type}'
                        ,CASE
                            WHEN '{support_updated_at}'::DATE=CURRENT_DATE THEN NULL::TIMESTAMP
                            ELSE '{support_updated_at}'::TIMESTAMP
                        END
                        ,CASE 
                            WHEN {days_since_task_complete}=-99 THEN NULL::NUMERIC 
                            ELSE {days_since_task_complete}
                        END
                        ,'{link}'
                        ,'{timezone}'
                        ,CASE 
                            WHEN {events_max_diff}=-99 THEN NULL::NUMERIC
                            ELSE {events_max_diff}
                        END
                        ,CASE 
                            WHEN {max_diff}=-99 THEN NULL::NUMERIC
                            ELSE {max_diff}
                        END
                        ,CASE 
                            WHEN {max_hourly_diff}=-99 THEN NULL::NUMERIC
                            ELSE {max_hourly_diff}
                        END
                        ,{percent_not_responding}
                        ,'{sensor_status}'
                        ,{algorithm_score}
                        ,'{notes}'
                        )
                        ON CONFLICT (date,project_id, sensor_id)
                        DO UPDATE SET ({columns}) = (
                                                    '{date}'::DATE
                                                    ,{project_id}
                                                    ,{sensor_id}
                                                    ,{area_id}
                                                    ,'{area_name}'
                                                    ,'{company_name}'
                                                    ,'{territory}'
                                                    ,'{crop_name}'
                                                    ,{variety_id}
                                                    ,ARRAY{probe_depths}::integer[]
                                                    ,{irrigation_events}
                                                    ,{not_responding_events_count}
                                                    ,ARRAY{event_timestamp}::text[]
                                                    ,ARRAY{remarks}::text[]
                                                    ,'{support_status}'
                                                    ,'{work_type}'
                                                    ,CASE
                                                        WHEN '{support_updated_at}'::DATE=CURRENT_DATE THEN NULL::TIMESTAMP
                                                        ELSE '{support_updated_at}'::TIMESTAMP
                                                    END
                                                    ,CASE 
                                                        WHEN {days_since_task_complete}=-99 THEN NULL::NUMERIC 
                                                        ELSE {days_since_task_complete}
                                                    END
                                                    ,'{link}'
                                                    ,'{timezone}'
                                                    ,CASE 
                                                        WHEN {events_max_diff}=-99 THEN NULL::NUMERIC
                                                        ELSE {events_max_diff}
                                                    END
                                                    ,CASE 
                                                        WHEN {max_diff}=-99 THEN NULL::NUMERIC
                                                        ELSE {max_diff}
                                                    END
                                                    ,CASE 
                                                        WHEN {max_hourly_diff}=-99 THEN NULL::NUMERIC
                                                        ELSE {max_hourly_diff}
                                                    END
                                                    ,{percent_not_responding}
                                                    ,'{sensor_status}'
                                                    ,{algorithm_score}
                                                    ,'{notes}'
                        );""".format(
                                columns = ', '.join(cols),
                                date = row[1]['date'],
                                project_id = row[1]['project_id'],
                                sensor_id = row[1]['sensor_id'],
                                area_id = row[1]['area_id'],
                                area_name = row[1]['area_name'].replace("'", "''"),
                                territory = row[1]['territory'],
                                company_name = row[1]['company_name'].replace("'", "''"),
                                crop_name = row[1]['crop_name'],
                                variety_id = row[1]['variety_id'],
                                probe_depths = row[1]['probe_depths'],
                                irrigation_events = row[1]['irrigation_events'],
                                not_responding_events_count = row[1]['not_responding_events_count'],
                                event_timestamp = list(x.strftime("%Y-%m-%d %H:%M:%S") for x in row[1]['event_timestamp']),
                                support_status = row[1]['support_status'],
                                work_type = row[1]['work_type'],
                                support_updated_at = row[1]['support_updated_at'],
                                days_since_task_complete = row[1]['days_since_task_complete'],
                                remarks = list(x for x in row[1]['remarks']),
                                link = row[1]['link'],
                                timezone = row[1]['timezone'],
                                events_max_diff = row[1]['events_max_diff'],
                                max_diff = row[1]['max_diff'],
                                max_hourly_diff = row[1]['max_hourly_diff'],
                                percent_not_responding = row[1]['percent_not_responding'],
                                sensor_status = row[1]['sensor_status'],
                                algorithm_score = row[1]['algorithm_score'],
                                notes = row[1]['notes']
                                )
            
            if debug:
                print(query)

            sql_importer = SqlImporter(database = c.database_research, user = c.user_research, password = c.password_research,host = c.host_research, port = c.port_research, verbose=False)
            sql_importer.query = query
            sql_importer.execute_command()
            return('OK')
        except Exception as e:
            if local_env:
                print(f"pid {project_id}::Exception={e}")
            
            logging.warning(f"pid {project_id}::Exception={e}")
            return()
