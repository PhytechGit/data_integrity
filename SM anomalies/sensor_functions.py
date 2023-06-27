import logic_parameters
import pandas as pd
import datetime as dt


def not_responding_logic(df_irr_span, probe_dict, event_timestamp, project_remarks, sm_hourly_diff, probe_depth, current_event_start, current_event_end, probeMaxDiff, probeMaxSM):
                
    df_irr_span_short = df_irr_span[df_irr_span.local_time < current_event_end + pd.Timedelta(hours=logic_parameters.IRR_SPAN_END_AFTER_X_HOURS)]
    
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
        #TODO add time from previous irrigation event to address local saturation
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
        probe_dict['late_response'] = True
        project_remarks.add(f"{current_event_start}: probe {probe_depth} Late response")
    #else:
        # DO Not count as not responding event

    return(probe_dict)

def find_not_responding_events(project_data, debug=logic_parameters.debug_):
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
    if df_irr.empty:
        project_remarks.add('No irrigations found')
        print(project_remarks, not_responding_SM_sensors_project_dict)
        return(not_responding_SM_sensors_project_dict)
    for irr_event_counter,row in df_irr.iterrows():
        current_event_start = df_irr.start[irr_event_counter]
        current_event_end = df_irr.end[irr_event_counter]
        if debug:
            print(f"irrigation event: {row}")
            print(current_event_start)
        #TODO
        # add MIN_IRR_AMOUNT_SPRINKLER
        if row['amount'] > logic_parameters.MIN_IRR_AMOUNT:
            for probe_depth in probe_depths:
                probe_events_list = []

                if debug:
                    print("\nprobe_depth", probe_depth)

                probe_dict = {'not_responding': False,
                              'low_reponse': False,
                              'late_response': False,
                              'missing_data': False,
                              'events dates': [],
                              'probe_SM_diff': 0
                             }
                
                probe_depth_index = list(probe_depths).index(probe_depth)

                probe_local_saturation = project_data.local_saturation_by_depth[probe_depth_index][1]
                
                if project_data.multi_depths_sm[probe_depth].empty: # no SM data for this depth
                    project_remarks.add(f"missing SM data for {probe_depth}")
                    probe_dict['missing_data'] = True
                    project_data.missing_data = True
                    probe_events_dict[probe_depth] = probe_dict
                    continue # check next depth

                df = project_data.multi_depths_sm[probe_depth].reset_index(drop=False)
                df['date'] = df.local_time.dt.date
                
                df_irr_span = df[(df.local_time >= current_event_start - pd.Timedelta(hours=1)) 
                                 & (df.local_time < current_event_end + pd.Timedelta(hours=logic_parameters.IRR_SPAN_END_AFTER_X_HOURS))
                                ]
                df_irr_wide_span = df[(df.local_time >= current_event_start - pd.Timedelta(hours=2)) 
                                 & (df.local_time < current_event_end + pd.Timedelta(hours=3* logic_parameters.IRR_SPAN_END_AFTER_X_HOURS))
                                ].reset_index(drop=True)

                # missing data: 
                # No SM values in irrigation event or less than 70% of SM hourly values per week
                max_len_of_SM_hourly_values = ((dt.datetime.strptime(project_data.max_date,"%Y-%m-%d").date().day - dt.datetime.strptime(project_data.min_date,"%Y-%m-%d").date().day) + 1) * 24
                max_len_of_SM_hourly_values = (project_data.return_project_period()+1) * 24

                if (len(df_irr_wide_span.sm_val) == 0) or (len(df) < logic_parameters.MIN_PCT_OF_SM_HOURLY_VALUES * max_len_of_SM_hourly_values):
                    project_remarks.add(f"missing SM data for {probe_depth}")
                    probe_dict['missing_data'] = True
                    project_data.missing_data = True
                    probe_events_dict[probe_depth] = probe_dict
                    continue
                    
                sm_hourly_diff = logic_parameters.SM_HOURLY_DIFF_FIRST_DEPTH if probe_depth_index==0 else logic_parameters.SM_HOURLY_DIFF_SECOND_DEPTH
                ProbeMinSM, ProbeMaxSM = min(df_irr_wide_span.sm_val), max(df_irr_wide_span.sm_val)
                probeMaxDiff = ProbeMaxSM - ProbeMinSM
                
                if debug:
                    print(df_irr_wide_span, 'probeMaxSM: ',ProbeMaxSM)
                
                probe_dict = not_responding_logic(df_irr_wide_span, probe_dict, event_timestamp, project_remarks, sm_hourly_diff, probe_depth, current_event_start, current_event_end, probeMaxDiff, ProbeMaxSM)

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
            #TODO
            # skip these steps and use project_data.missing_data flag
            # handle low response
            late_response, low_response = False, False
            for d in probe_depths:
                #missing_data += probe_events_dict[d]['missing_data']
                late_response += probe_events_dict[d]['late_response']
                #low_response += probe_events_dict[d]['low_response']
            missing_data = project_data.missing_data
            #if late_response:
                #project_remarks.add(f"probe {probe_depth} Late response")
            #    probe_events_dict['late_response'] = True
            #if low_response:
            #    probe_events_dict['low_response'] = True
            
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

def find_events_without_irrigation(project_data):
    # First check if df_irrigation is not empty = possible faulty IRG sebsor or wrong attribution
    event_days, count_events = [],0
    if project_data.df_irrigation.empty:
        return(False, count_events, event_days)

    event_days = project_data.sm_without_irr_df[project_data.sm_without_irr_df.sm_diff > logic_parameters.SM_HOURLY_DIFF_wo_IRR].date.unique().tolist()
    count_events = len(event_days)

    if (project_data.sm_without_irr_df[project_data.sm_without_irr_df.sm_diff > logic_parameters.SM_HOURLY_DIFF_wo_IRR]).empty:
        return(False, count_events, [])
    else:
        return(True, count_events, event_days)
    
def status_compare(df):
    df.reset_index(drop=True, inplace=True)
    if len(df) == 2:
        if (df.loc[0,'daily_status'] == 'FAIL') & (df.loc[1,'daily_status'] == 'FAIL'):
            return(2)
        elif (df.loc[0,'daily_status'] == 'FAIL') | (df.loc[1,'daily_status'] == 'FAIL'):
            return(1)
        else:
            return(0)
    elif len(df) == 1:
        if df.loc[0,'daily_status'] == 'FAIL':
            return(1)

##################################