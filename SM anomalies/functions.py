import logic_parameters
import pandas as pd
import datetime as dt


def find_not_responding_events(project_data, debug=False):
    # initialize
    not_responding_SM_sensors_project_dict, probe_events_dict = {}, {}
    event_timestamp = set()
    probe_depths = project_data.df_sm_data.depth_cm.unique()
    total = 0
    not_responding_SM_sensors_project_dict.update({'probe_depths': list(probe_depths),
                                                   'events_count': total,
                                                   'events_details': {}})

    df_irr = project_data.df_irrigation[project_data.df_irrigation.amount > logic_parameters.MIN_IRR_AMOUNT]
    for irr_event_counter, row in df_irr.iterrows():

        if debug:
            print(f"irrigation event: {df_irr[irr_event_counter]}")

        not_responding = False
        if row['amount'] > logic_parameters.MIN_IRR_AMOUNT:
            for probe_depth in probe_depths:
                counter = 0
                ProbeMinMoisture, ProbeMaxMoisture = 0, 0
                probe_events_list = []

                if debug:
                    print("\nprobe_depth", probe_depth)

                probe_dict = {}
                probe_depth_index = list(probe_depths).index(probe_depth)

                probe_local_saturation = project_data.local_saturation_by_depth[probe_depth_index][1]
                if project_data.multi_depths_sm[probe_depth].empty:  # no SM data for this depth
                    probe_dict.update({'irrigation_events': len(df_irr),
                                       'events dates': [],
                                       'probe SM diff': 0,
                                       'remarks': f"|missing SM data for {probe_depth}"})
                    probe_events_dict[probe_depth] = probe_dict
                    continue  # check next depth

                df = project_data.multi_depths_sm[probe_depth].reset_index(drop=False)
                df['date'] = df.local_time.dt.date
                #################################
                # Not responding conditions:
                #
                # time frame irrigation span -1hr/ +3hr
                # soil moisture hourly diff > SM_HOURLY_DIFF according to depth
                # initial probe moisture is less than local saturation minus 0.5%
                #################################
                df_irr_span = df[(df.local_time > df_irr.start[irr_event_counter] - pd.Timedelta(hours=1))
                                 & (df.local_time < df_irr.end[irr_event_counter] + pd.Timedelta(hours=4))
                                 ]
                if debug:
                    print(df_irr.start[irr_event_counter], df_irr.end[irr_event_counter],
                            df_irr_span)
                ProbeMinMoisture, ProbeMaxMoisture = min(df_irr_span.sm_val), max(df_irr_span.sm_val)

                SM_HOURLY_DIFF = logic_parameters.SM_HOURLY_DIFF_FIRST_DEPTH if probe_depth_index == 0 else logic_parameters.SM_HOURLY_DIFF_SECOND_DEPTH
                probe_dict['remarks'] = ''
                # check if probe responding = at least one hourly diff above SM_HOURLY_DIFF
                if max(df_irr_span.sm_diff) < SM_HOURLY_DIFF:
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
                        if (max(df_irr_span.sm_diff) > SM_HOURLY_DIFF * logic_parameters.LOW_RESPONSE_FACTOR) & (
                                max(df_irr_span.sm_diff) < SM_HOURLY_DIFF):
                            probe_dict['remarks'] += '|Low sensor respnse'
                    else:
                        probe_dict['remarks'] += '|probe_local_saturation'

                    probe_dict['irrigation_events'] = len(df_irr)
                    probe_dict['events dates'] = probe_events_list
                    probe_dict['probe SM diff'] = ProbeMaxMoisture - ProbeMinMoisture
                    probe_events_dict[probe_depth] = probe_dict
                else:
                    if debug:
                        print(f"responding well, {df_irr.start[irr_event_counter]}")
                    probe_dict.update({'irrigation_events': len(df_irr),
                                       'events dates': [],
                                       'probe SM diff': max(df_irr_span.sm_val) - min(df_irr_span.sm_val),
                                       'remarks': ''})
                    probe_events_dict[probe_depth] = probe_dict
                    #break

                if debug:
                    print(f"max hourly diff: {max(df_irr_span.sm_diff)}",
                            f"probe local saturation: {probe_local_saturation}",
                            f"initial probe SM: {max(df_irr_span.sm_val.iloc[:3])}")

            # count number of not responding probes per irrigation span
            if counter == len(probe_depths):  # all probes are NOT responding in this irrigation span
                probe_dict['remarks'] += '|All probes not responding'

            if not_responding:
                total += 1

            if debug:
                print(f"""total: {total} ,{df_irr.start[irr_event_counter]} finished\n######################""")
            not_responding_SM_sensors_project_dict.update({'probe_depths': list(probe_depths),
                                                           'events_count': total,
                                                           'event_timestamp': event_timestamp,
                                                           'events_details': probe_events_dict})
        else:  # irrigation event amount less than minimum
            continue

    return (not_responding_SM_sensors_project_dict)

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
                ProbeMinMoisture, ProbeMaxMoisture = 0, 0
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
                # soil moisture hourly diff > SM_HOURLY_DIFF according to depth
                # initial probe moisture is less than local saturation minus 0.5%
                #################################
                df_irr_span = df[(df.local_time > df_irr.start[irr_event_counter] - pd.Timedelta(hours=1))
                                 & (df.local_time < df_irr.end[irr_event_counter] + pd.Timedelta(hours=4))
                                 ]
                ProbeMinMoisture, ProbeMaxMoisture = min(df_irr_span.sm_val), max(df_irr_span.sm_val)
                if debug:
                    print(df_irr.start[irr_event_counter], df_irr.end[irr_event_counter],
                            df_irr_span)

                SM_HOURLY_DIFF = logic_parameters.SM_HOURLY_DIFF_FIRST_DEPTH if probe_depth_index == 0 else logic_parameters.SM_HOURLY_DIFF_SECOND_DEPTH
                probe_dict['remarks'] = ''
                # check if probe responding = at least one hourly diff above SM_HOURLY_DIFF
                if max(df_irr_span.sm_diff) < SM_HOURLY_DIFF:
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
                        if (max(df_irr_span.sm_diff) > SM_HOURLY_DIFF * logic_parameters.LOW_RESPONSE_FACTOR) & (
                                max(df_irr_span.sm_diff) < SM_HOURLY_DIFF):
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
                probe_dict['probe moisture diff'] = ProbeMaxMoisture - ProbeMinMoisture
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


def get_project_results(project_data, debug=False):
    project_results = find_not_responding_events(project_data, debug)
    df_irr = project_data.df_irrigation[project_data.df_irrigation.amount > logic_parameters.MIN_IRR_AMOUNT]

    projects_df = pd.DataFrame(columns=['project_id', 'sensor_id', 'probe_depths', 'irrigation_events',
                                        'not_responding_events_count', 'max_SM_diff', 'event_timestamp',
                                        'support_status', 'support_updated_at', 'days_since_task_complete',
                                        'remarks', 'link', 'timezone'])
    project_dict = {}
    # max_val = 0
    remarks = set()

    project_dict.update({'project_id': project_data.project_id,
                         'sensor_id': project_data.sensor_id,
                         'probe_depths': project_results.get('probe_depths'),
                         'irrigation_events': len(df_irr),
                         'not_responding_events_count': project_results['events_count'],  # max_val,
                         'link': project_data.app_link,
                         'timezone': project_data['timezone']})

    # No not_responding events found
    if not project_results['events_details']:  # empty dict = No events
        project_dict.update({'max_SM_diff': None,
                             # 'probe_depths': None,
                             'event_timestamp': None,
                             'remarks': None})
    else:
        # find probe with max not responding events
        for d in project_results.get('probe_depths'):
            remarks.add(project_results.get('events_details')[d]['remarks'])
            project_dict.update(
                {'max_SM_diff': max(d['probe SM diff'] for d in project_results['events_details'].values()),
                 # 'probe_depths': project_results.get('probe_depths'),
                 'event_timestamp': project_results.get('event_timestamp'),
                 'remarks': remarks})
    # Find support status information
    if not project_data.sensor_support_status:
        sensor_status = {'status': None, 'updated_at': None, 'days_since_task_complete': None}
    else:
        sensor_status = project_data.sensor_support_status_dict[0]
        sensor_status['days_since_task_complete'] = (dt.datetime.today().date() -
                                                     sensor_status['updated_at'].date()).days

    project_dict.update({'support_status': sensor_status['status'],
                         'support_updated_at': sensor_status['updated_at'],
                         'days_since_task_complete': sensor_status['days_since_task_complete']})

    projects_df = pd.concat([projects_df, pd.DataFrame.from_dict(project_dict, orient='index').T], ignore_index=True)
    # projects_df['days_since_task_complete'] =  (dt.datetime.today().date() - projects_df['support_updated_at'][0].date()).days
    cols = ['project_id', 'sensor_id', 'probe_depths', 'irrigation_events',
            'not_responding_events_count', 'max_SM_diff', 'event_timestamp',
            'support_status', 'support_updated_at', 'days_since_task_complete', 'remarks', 'link', 'timezone']
    projects_df = projects_df[cols]

    return (projects_df)

def get_project_results_old(project_data, debug_=False):
    project_results = find_not_responding_events(project_data, debug_)
    df_irr = project_data.df_irrigation[project_data.df_irrigation.amount > logic_parameters.MIN_IRR_AMOUNT]
    # project_df = pd.DataFrame.from_dict({k: v for k,v in probe_results.items()
    #                         if k in project_depths}).T.reset_index(drop=False).rename(columns=
    #                                                                                  {'index':'depth'})
    projects_df = pd.DataFrame(columns=['project_id', 'sensor_id', 'probe_depths', 'irrigation_events',
                                        'not_responding_events_count', 'max_moisture_diff', 'event_timestamp',
                                        'support_status', 'support_updated_at', 'remarks',
                                        'link', 'timezone'])
    project_dict = {}
    max_val = 0
    remarks = set()

    if not project_results['events_details']:  # NO events of not responding
        pass
    else:
        # find probe with max not responding events
        for d in project_results.get('probe_depths'):
            # max_val = max(project_results['events_details'][d]['not_responding_events_count'],max_val)
            remarks.add(project_results.get('events_details')[d]['remarks'])
    event_count = project_results['events_count']
    if not project_data.sensor_support_status:  # No support status information
        sensor_status = {'status': 'None', 'updated_at': 'None'}
    else:
        sensor_status = project_data.sensor_support_status_dict[0]

    project_dict.update({'project_id': project_data.project_id,
                         'sensor_id': project_data.sensor_id,
                         'probe_depths': project_results.get('probe_depths'),
                         'irrigation_events': len(df_irr),
                         'not_responding_events_count': event_count,  # max_val,
                         'max_moisture_diff': max(
                             d['probe moisture diff'] for d in project_results['events_details'].values()),
                         'event_timestamp': project_results.get('event_timestamp'),
                         'support_status': sensor_status['status'],
                         'support_updated_at': sensor_status['updated_at'],
                         'remarks': remarks,
                         'link': project_data.app_link,
                         'timezone': project_data['timezone']})

    # project_df = project_df.append(project_dict,ignore_index=True)

    projects_df = pd.concat([projects_df, pd.DataFrame.from_dict(project_dict, orient='index').T], ignore_index=True)

    projects_df['days_since_task_complete'] = ''
    if sensor_status['updated_at'] != 'None':
        projects_df['days_since_task_complete'] = (
                dt.datetime.today().date() - projects_df['support_updated_at'][0].date()).days

    cols = ['project_id', 'sensor_id', 'probe_depths', 'irrigation_events',
            'not_responding_events_count', 'max_moisture_diff', 'event_timestamp',
            'support_status', 'support_updated_at', 'days_since_task_complete', 'remarks', 'link', 'timezone']
    projects_df = projects_df[cols]

    return (projects_df)


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

    project.meta_data = {'project_id': project.project_id,
                         'latitude': project.latitude if project.latitude else default_latitude,
                         'height': project.height if project.height else default_height, 'app_link': project.app_link}
    project.sensor_support_status_dict = project.get_sensor_support_status()

    return project
