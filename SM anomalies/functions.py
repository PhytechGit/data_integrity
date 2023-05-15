import logic_parameters
import pandas as pd
import datetime as dt


def not_responding_logic(df_irr_span, probe_dict, event_timestamp, project_remarks, sm_hourly_diff, probe_depth, current_event_start, current_event_end, probeMaxDiff, probeMaxSM):
                
    df_irr_span_short = df_irr_span[df_irr_span.local_time < current_event_end + pd.Timedelta(hours=logic_parameters.IRR_SPAN_END_AFTER_X_HOURS)]
    
    #ProbeMinSM, ProbeMaxSM = min(df_irr_span.sm_val), max(df_irr_span.sm_val)
    #ProbeMaxDiff = ProbeMaxSM - ProbeMinSM
    #################################
    # Not responding conditions:
    #
    # normal irrigation span -1hr/ +4hr
    # soil moisture hourly diff > sm_hourly_diff according to depth
    # soil moisture hourly diff < sm_hourly_diff * 0.5 = Low sensor respnse
    # initial probe moisture is less than local saturation minus 0.5%
    #################################
    if ((min(df_irr_span_short.sm_val) > probeMaxSM - 0.5) and
                ((max(df_irr_span_short.sm_diff) > sm_hourly_diff*logic_parameters.LOW_RESPONSE_FACTOR))):
        project_remarks.add(f"{current_event_start}: probe {probe_depth} Local saturation, start point:{min(df_irr_span_short.sm_val)} max:{probeMaxSM}")
        return(probe_dict) # Do Not count as not_responding event
            
    # check if probe responding = at least one hourly diff > sm_hourly_diff
    if (max(df_irr_span_short.sm_diff) <= sm_hourly_diff) and (max(df_irr_span_short.sm_diff) > sm_hourly_diff*logic_parameters.LOW_RESPONSE_FACTOR): # low response
        probe_dict['low_reponse'] = True
        project_remarks.add(f"{current_event_start}: probe {probe_depth} Low sensor respnse")
        if max(df_irr_span_short.sm_diff) < (sm_hourly_diff*logic_parameters.LOW_RESPONSE_FACTOR): 
            probe_dict['not_responding'] = True
            #probe_events_list.append((probe_depth,current_event_start))
            event_timestamp.add(current_event_start)
            project_remarks.add(f"{current_event_start}: probe {probe_depth} Not responding to irrigation")
            
    elif probeMaxDiff < 1.0: # extended irrigation span
            probe_dict['not_responding'] = True
            event_timestamp.add(current_event_start)
            project_remarks.add(f"{current_event_start}: probe {probe_depth} Not responding to irrigation")
    elif (max(df_irr_span_short.sm_diff) <= sm_hourly_diff) and (probeMaxDiff > 1.0):
        probe_dict['late_reponse'] = True
        project_remarks.add(f"{current_event_start}: probe {probe_depth} Late response")
    #else:
        # DO Not count as not responding event

    return(probe_dict)

def find_not_responding_events(project_data, debug=False):
    # initialize
    not_responding_SM_sensors_project_dict,probe_events_dict = {},{}
    event_timestamp = set()
    probe_depths = project_data.df_sm_data.depth_cm.unique()
    total = 0
    project_remarks = set()
    not_responding_SM_sensors_project_dict.update({'probe_depths': list(probe_depths),
                                                   'events_count': total,
                                                   'events_details': {},
                                                  'remarks': project_remarks})
        
    df_irr = project_data.df_irrigation[project_data.df_irrigation.amount > logic_parameters.MIN_IRR_AMOUNT]
    # get SM by depth series mean and std
    
    
    for irr_event_counter,row in df_irr.iterrows():
        current_event_start = df_irr.start[irr_event_counter]
        current_event_end = df_irr.end[irr_event_counter]
        if debug:
            print(f"irrigation event: {row}")
            print(current_event_start)
        
        if row['amount'] > logic_parameters.MIN_IRR_AMOUNT:
            for probe_depth in probe_depths:
                probe_events_list = []

                if debug:
                    print("\nprobe_depth", probe_depth)

                probe_dict = {'not_responding': False,
                              'low_reponse': False,
                              'late_response': False,
                              #'irrigation_events': len(df_irr),
                              'events dates': [],
                              'probe_SM_diff': 0
                             }
                
                probe_depth_index = list(probe_depths).index(probe_depth)

                probe_local_saturation = project_data.local_saturation_by_depth[probe_depth_index][1]
                
                if project_data.multi_depths_sm[probe_depth].empty: # no SM data for this depth
                    project_remarks.add(f"missing SM data for {probe_depth}")
                    probe_events_dict[probe_depth] = probe_dict
                    continue # check next depth

                df = project_data.multi_depths_sm[probe_depth].reset_index(drop=False)
                df['date'] = df.local_time.dt.date
                
                df_irr_span = df[(df.local_time >= current_event_start - pd.Timedelta(hours=1)) 
                                 & (df.local_time < current_event_end + pd.Timedelta(hours=logic_parameters.IRR_SPAN_END_AFTER_X_HOURS))
                                ]
                df_irr_wide_span = df[(df.local_time >= current_event_start - pd.Timedelta(hours=1)) 
                                 & (df.local_time < current_event_end + pd.Timedelta(hours=3* logic_parameters.IRR_SPAN_END_AFTER_X_HOURS))
                                ].reset_index(drop=True)
                
                sm_hourly_diff = logic_parameters.SM_HOURLY_DIFF_FIRST_DEPTH if probe_depth_index==0 else logic_parameters.SM_HOURLY_DIFF_SECOND_DEPTH
                ProbeMinSM, ProbeMaxSM = min(df_irr_wide_span.sm_val), max(df_irr_wide_span.sm_val)
                probeMaxDiff = ProbeMaxSM - ProbeMinSM
                probeMaxSM = project_data.SM_statistics[probe_depth]['max']
                
                if debug:
                    print(df_irr_wide_span)
                
                probe_dict = not_responding_logic(df_irr_wide_span, probe_dict, event_timestamp, project_remarks, sm_hourly_diff, probe_depth, current_event_start, current_event_end, probeMaxDiff,probeMaxSM)
                  
                #probe_dict['irrigation_events'] = len(df_irr)
                #probe_dict['events dates'] = probe_events_list
                probe_dict['probe_SM_diff'] = probeMaxDiff
                probe_events_dict[probe_depth] = probe_dict
 
                if debug:
                    print(probe_dict)
                #    print(f"max hourly diff: {max(df_irr_span.sm_diff)}",
                #            f"probe local saturation: {probe_local_saturation}",
                #            f"initial probe SM: {max(df_irr_span.sm_val.iloc[:3])}")
            
            # Probes status summary
            # At least one probe responds normally = Normal sensor
            not_responding = True
            for d in probe_depths:
                not_responding *= probe_events_dict[d]['not_responding']

            if not_responding:
                total+=1

            if debug:
                print(f"""total: {total} ,{current_event_start} finished\n######################""")
            not_responding_SM_sensors_project_dict.update({'probe_depths': list(probe_depths),
                                                               'events_count': total,
                                                               'event_timestamp': event_timestamp,
                                                               'events_details': probe_events_dict,
                                                              'remarks': project_remarks})
        else: # irrigation event amount less than minimum
            continue

    return(not_responding_SM_sensors_project_dict)


def get_project_results(project_data, debug=False):
    project_results = find_not_responding_events(project_data,debug)
    df_irr = project_data.df_irrigation[project_data.df_irrigation.amount > logic_parameters.MIN_IRR_AMOUNT]

    projects_df = pd.DataFrame(columns=['project_id','sensor_id','area_id','area_name','company_name','crop_name','variety_id',
                                        'probe_depths', 'irrigation_events',
                                        'not_responding_events_count', 'max_SM_diff',
                                        'SM_statistics',
                                        'event_timestamp',
                                       'support_status', 'support_updated_at','days_since_task_complete',
                                        'remarks', 'link','timezone'])
    project_dict = {}

    remarks = set()
    
    project_dict.update({'project_id' : project_data.project_id,
                         'sensor_id' : project_data.sensor_id,
                         'area_id' : project_data.area_id,
                         'area_name' : project_data.area_name,
                         'company_name' : project_data.company,
                         'crop_name' : project_data.crop_name,
                         'variety_id' : project_data.variety_id,
                         'probe_depths': project_results.get('probe_depths'),
                         'irrigation_events' : len(df_irr),
                         'not_responding_events_count' : project_results['events_count'],
                         'link' : project_data.app_link,
                         'timezone' : project_data['timezone']})
    
    # No not_responding events found
    if not project_results['events_details']: # empty dict = No events
        project_dict.update({'max_SM_diff': None,
                             #'max_SM_diff_extended': None,
                             'event_timestamp' : None,
                             'remarks' : None})
    else:
        # find probe with max not responding events
        for d in project_results.get('probe_depths'): 
            project_dict.update({'max_SM_diff': max(d['probe_SM_diff'] for d in project_results['events_details'].values()),
                                 #'max_SM_diff_extended': max(d['probe_SM_max_diff_extended_period'] for d in project_results['events_details'].values()),
                                 'SM_statistics': project_data.SM_statistics,
                                 'event_timestamp' : project_results.get('event_timestamp'),
                                'remarks': project_results['remarks']})
    # Find support status information
    if not project_data.sensor_support_status:
        sensor_status = {'status': None, 'updated_at': None, 'days_since_task_complete': None}
    else:
        sensor_status = project_data.sensor_support_status_dict[0]
        sensor_status['days_since_task_complete'] = (dt.datetime.today().date() -
                                    sensor_status['updated_at'].date()).days
    
    project_dict.update({'support_status' : sensor_status['status'],
                         'support_updated_at' : sensor_status['updated_at'],
                         'days_since_task_complete': sensor_status['days_since_task_complete']})
    
    
    projects_df = pd.concat([projects_df, pd.DataFrame.from_dict(project_dict,orient='index').T], ignore_index=True)
    #projects_df['days_since_task_complete'] =  (dt.datetime.today().date() - projects_df['support_updated_at'][0].date()).days
    #cols = ['project_id', 'sensor_id', 'probe_depths', 'irrigation_events',
    #   'not_responding_events_count', 'max_SM_diff', 'event_timestamp',
    #   'support_status', 'support_updated_at','days_since_task_complete', 'remarks', 'link', 'timezone']
    #projects_df = projects_df[cols]
    return(projects_df)

def load_project_data(project_id, min_date, max_date, min_depth=10, max_depth=91, debug=True):
    from logic_parameters import default_latitude, default_height, default_max_depth
    from project_class_data_extract import Project
    project = Project(
        project_id=project_id,
        min_depth=min_depth,
        max_depth=default_max_depth,  # set the depth range we're interested in
        min_date=min_date,
        max_date=max_date,
        debug=debug)

    project.load_project_metadata()
    project.get_sm_depths()

    if len(project.depths_found) == 0:
        project.valid_project = False
        return project

    project.load_sm_project_data(min_depth=project.depths_found[0], max_depth=project.depths_found[
        -1], )  # change the min/max_depth if you dont want to load all depths
    if not project.valid_project:
        return project
    # project.load_project_weather_data(future=14)  # load the weather date until max_date + 14 days.
    project.apply_transformers()
    project.group_data_to_depths()
    project.load_irrigation_spans()
    project.find_probe_local_saturation()
    project.SM_statistics_by_probe()
    project.sensor_support_status_dict = project.get_sensor_support_status()
    
    project.meta_data = {'project_id': project.project_id,
                         'latitude': project.latitude if project.latitude else default_latitude,
                         'height': project.height if project.height else default_height, 'app_link': project.app_link}
    
    return project

def find_not_responding_events_old(project_data, debug=False):
    not_responding_SM_sensors_project_dict, probe_events_dict = {}, {}
    event_timestamp = set()
    probe_depths = project_data.df_sm_data.depth_cm.unique()
    total = 0
    df_irr = project_data.df_irrigation[project_data.df_irrigation.amount > logic_parameters.MIN_IRR_AMOUNT]
    for irr_event_counter, row in df_irr.iterrows():

        if debug:
            print(f"irrigation event: {df_irr[irr_event_counter]}")

        not_responding = False
        if row['amount'] > logic_parameters.MIN_IRR_AMOUNT:
            for probe_depth in probe_depths:
                counter = 0
                ProbeMinSM, ProbeMaxSM = 0, 0
                probe_events_list = []

                if debug:
                    print("\nprobe_depth", probe_depth)

                probe_dict = {}
                probe_depth_index = list(probe_depths).index(probe_depth)

                probe_local_saturation = project_data.local_saturation_by_depth[probe_depth_index][1]
                df = project_data.multi_depths_sm[probe_depth].reset_index(drop=False)
                df['date'] = df.local_time.dt.date
                #################################
                # Not responding conditions:
                #
                # time frame irrigation span -1hr/ +3hr
                # soil moisture hourly diff > sm_hourly_diff according to depth
                # initial probe moisture is less than local saturation minus 0.5%
                #################################
                df_irr_span = df[(df.local_time > df_irr.start[irr_event_counter] - pd.Timedelta(hours=1))
                                 & (df.local_time < current_event_end + pd.Timedelta(hours=4))
                                 ]
                ProbeMinSM, ProbeMaxSM = min(df_irr_span.sm_val), max(df_irr_span.sm_val)
                if debug:
                    print(df_irr.start[irr_event_counter], current_event_end,
                            df_irr_span)

                sm_hourly_diff = logic_parameters.sm_hourly_diff_FIRST_DEPTH if probe_depth_index == 0 else logic_parameters.sm_hourly_diff_SECOND_DEPTH
                probe_dict['remarks'] = ''
                # check if probe responding = at least one hourly diff above sm_hourly_diff
                if max(df_irr_span.sm_diff) < sm_hourly_diff:
                    # check if the probe initial moisture is near local saturation
                    if ("not_responding" in project_data.flag) | (
                            max(df_irr_span.sm_val.iloc[:3]) < probe_local_saturation - 0.5):
                        counter += 1  # count number of not responding probes
                        not_responding = True
                        # not_responding_sensor_id = int(project_data.df_sm_data[project_data.df_sm_data['depth_cm']==probe_depth].loc[0,'sensor_id'])
                        probe_events_list.append((probe_depth, df_irr.start[irr_event_counter]))
                        event_timestamp.add(df_irr.start[irr_event_counter])
                        probe_dict['remarks'] = '|Not responding to irrigation'
                        # Find sensors with low response (low peak in sensor graph)
                        if (max(df_irr_span.sm_diff) > sm_hourly_diff * logic_parameters.LOW_RESPONSE_FACTOR) & (
                                max(df_irr_span.sm_diff) < sm_hourly_diff):
                            probe_dict['remarks'] += '|Low sensor respnse'
                else:
                    if debug:
                        print(f"responding well, {df_irr.start[irr_event_counter]}")
                    break

                if debug:
                    print(f"max hourly diff: {max(df_irr_span.sm_diff)}",
                            f"probe local saturation: {probe_local_saturation}",
                            f"initial probe moisture: {max(df_irr_span.sm_val.iloc[:3])}")

                probe_dict['irrigation_events'] = len(df_irr)
                probe_dict['events dates'] = probe_events_list
                probe_dict['probe moisture diff'] = ProbeMaxSM - ProbeMinSM
                probe_events_dict[probe_depth] = probe_dict

            # count number of not responding probes per irrigation span
            if counter == len(probe_depths):  # all probes are NOT responding in this irrigation span
                probe_dict['remarks'] += '|All probes not responding'
            # probe_events_dict['count_non_responding_probes'] = counter
            if not_responding:
                total += 1

            if debug:
                print(f"""total: {total} ,{df_irr.start[irr_event_counter]} finished\n######################""")
            not_responding_SM_sensors_project_dict.update({'probe_depths': list(probe_depths),
                                                           'events_count': total,
                                                           'event_timestamp': event_timestamp,
                                                           'events_details': probe_events_dict})
    return (not_responding_SM_sensors_project_dict)
