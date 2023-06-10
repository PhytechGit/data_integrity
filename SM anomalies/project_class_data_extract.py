import numpy as np
import os
import pandas as pd  
import datetime as dt
from matplotlib.dates import date2num
import logging
logging = logging.getLogger(__name__)

import logic_parameters
from sql_import_export import SqlImporter
from common_db import DB_TABLES, ALLOWED_SM_TYPES, MIN_SM_SATURATION
import common_db

try:
    import cert as c
except Exception:
    import cert_aws as c

local_env = True
debug_ = logic_parameters.debug_

try:
    from IPython.display import display, HTML
    from plotly.subplots import make_subplots
    import plotly.graph_objects as go
except Exception:
    pass

class Project:
    def __init__(
            self, 
            project_id,
            min_date, 
            max_date,
            sensor_id = 0,
            max_depth = logic_parameters.MAX_PROBE_DEPTH_FOR_CALC,
            min_depth = logic_parameters.MIN_PROBE_DEPTH_FOR_CALC,
            debug=debug_):

        self.meta_data = None
        self.project_id = project_id
        self.sensor_id = sensor_id
        self.max_depth = max_depth
        self.min_depth = min_depth
        self.min_date = min_date
        self.max_date = max_date
        self.min_project_length = 24  # project is invalid if it has less than that

        self.min_sm_moist = 8
        self.max_sm_moist = 50
        self.height = 0
        self.min_sm_saturation = MIN_SM_SATURATION

        self.missing_data = False
        self.sensor_support_status = False
        self.df_sm_data_raw = pd.DataFrame([])
        self.df_sm_data = pd.DataFrame([])
        self.local_saturation_by_depth = []
        self.df_irrigation = pd.DataFrame([])
        self.df_daily_status = pd.DataFrame([])

        self.df_weather_data = pd.DataFrame([])
        self.df_daily_data = pd.DataFrame([])

        self.flag = ''
        self.valid_project = True
        self.debug = debug

    def __getitem__(self, name):  # this allows to dynamically access the property: class['property'] like class.property
        return getattr(self, name)

    def return_project_period(self):
        max_date = dt.datetime.strptime(self.max_date, "%Y-%m-%d")
        min_date = dt.datetime.strptime(self.min_date, "%Y-%m-%d")
        return(abs((max_date - min_date).days))

    def load_project_metadata(self):
        query = f"""
                SELECT 
                    pmt.project_id, 
                    pmt.plot_id, 
                    pmt.area_id, 
                    pmt.plot_name, 
                    pmt.area_name, 
                    pmt.company_id, 
                    pmt.time_zone, 
                    pmt.name, 
                    pmt.crop_type, 
                    pmt.variety_id
                FROM {DB_TABLES['projects_metadata_table']}  pmt
                WHERE project_id = {self.project_id}
                """
        if logic_parameters.sql_debug:
            print(query)
        sql_importer = SqlImporter(query=query, database=c.database_production, user=c.user_production, password=c.password_production,host=c.host_production, port=c.port_production, verbose=logic_parameters.sql_debug)

        sql_importer.get_data()
        self.project_metadata = sql_importer.data

        if self.project_metadata.empty:
            self.valid_project = False
            if local_env:
                print(f"pid {self.project_id}::no metadata found")
            else:
                logging.info(f"pid {self.project_id}::no metadata found")
            self.flag += 'no metadata found'
            raise Exception('no metadata found')

        self.timezone = self.project_metadata.time_zone.iloc[0]
        self.load_project_metadata_from_ruby()
        self.infer_project_metadata()
        if self.debug:
            print('load_project_metadata complete')

    def load_project_metadata_from_ruby(self):
        query = f"""
            SELECT latitude
            FROM {DB_TABLES['phytoweb_projects_s']}
            WHERE id = {self.project_id}"""
        
        if logic_parameters.sql_debug:
            print(query)

        sql_importer = SqlImporter(query=query, database=c.database_ruby_production, user=c.user_production, password=c.password_production, host=c.host_production, port=c.port_production, verbose=logic_parameters.sql_debug)

        sql_importer.execute_query()
        if sql_importer.res:
            lat = sql_importer.res[0][0]
        else:
            lat = None
        self.project_metadata['latitude'] = lat
        
        query = f"""
            select t.code territory
            from projects pr
            join public.territory_entities te
            on pr.plot_id = te.territoriable_id
            join public.territories t
            on te.territory_id = t.id
            where territoriable_type = 'Plot'
            and pr.id = {self.project_id}
            and t.code <> 'usa'
        """
        
        if logic_parameters.sql_debug:
            print(query)

        sql_importer.query = query
        sql_importer.execute_query()
        
        if sql_importer.res:
            terr = sql_importer.res[0][0]
        else:
            terr = None
        self.project_metadata['territory'] = terr
        
        query = f"""
            select first_name from public.users
            where id = {self.project_metadata.company_id.unique()[0]}
        """
        sql_importer.query = query
        sql_importer.execute_query()
        if sql_importer.res:
            company_name = sql_importer.res[0][0]
        else:
            company_name = None
        self.project_metadata['company_name'] = company_name

    def infer_project_metadata(self):
        self.timezone = self.project_metadata.time_zone.unique()[0]
        self.crop_name = self.project_metadata.crop_type.unique()[0].lower()
        self.variety_id = self.project_metadata.variety_id.unique()[0]
        self.plot_id = self.project_metadata.plot_id.unique()[0]
        self.area_id = self.project_metadata.area_id.unique()[0]
        self.company_id = self.project_metadata.company_id.unique()[0]
        #TODO
        self.company_name = self.project_metadata.company_name.unique()[0]
        self.plot_name = self.project_metadata.plot_name.unique()[0]
        self.area_name = self.project_metadata.area_name.unique()[0]
        self.project_name = self.project_metadata.name.unique()[0]
        self.territory = self.project_metadata.territory.unique()[0]
        self.latitude = self.project_metadata.latitude.unique()[0]

        project_link = 'https://app.phytech.com/%d/%d/%d' %(self.area_id, self.plot_id, self.project_id)
        self.app_link = project_link

    def get_sm_depths(self, depths_range=None):
        # get list of the SM depth relevant for the project. We only look on the last installation of SM sensors
        if depths_range is None:
            min_depth, max_depth = self.min_depth, self.max_depth
        else:
            min_depth, max_depth = depths_range[0], depths_range[1]
        query = f"""
                SELECT 
                    inner_index, depth_cm
                FROM {DB_TABLES['soil_sensors_metadata']} ssm
                WHERE project_id ='{self.project_id}' 
                AND depth_cm BETWEEN {min_depth} AND {max_depth}
                AND moisture = True 
                AND type_id IN {ALLOWED_SM_TYPES}
                AND ('{self.min_date}', '{self.max_date}') OVERLAPS (relevant_from, relevant_untill)
                --GROUP BY depth_cm
                """

        if logic_parameters.sql_debug:
            print(query)

        sql_importer = SqlImporter(query=query, database=c.database_production, user=c.user_production, password=c.password_production, host=c.host_production, port=c.port_production, verbose=logic_parameters.sql_debug)

        sql_importer.get_data()
        df = sql_importer.data
        self.depths_found = sorted(df.depth_cm.unique())
        self.depths_found_index = sorted(df.inner_index)
        
        if len(self.depths_found) == 0:
            self.valid_project = False
            if local_env:
                print(f"pid {self.project_id}::no depths found")
            else:
                logging.info(f"pid {self.project_id}::no depths found")
            raise Exception('no depths found')
        elif len(self.depths_found) == 1:
            self.depths_found_index.append(self.depths_found_index[0])

        if self.debug:
            print('get_sm_depths complete')

    def load_sm_project_data(self, min_depth, max_depth):
        # get the sm data of a single project
        # keep only  depths <= max_depth
        # query the cached_projects_sm_data if available

        query = f"""
                SELECT 
                    max(sm.date) local_date, timezone('{self.timezone}', to_timestamp(ts / 1000)) local_time, 
                    ts,
                    depth_cm,
                    MAX(ssm.sensor_id)::INTEGER sensor_id,
                    AVG(corrected_format) sm_val
                FROM {DB_TABLES['soil_measurements']} sm
                JOIN {DB_TABLES['soil_sensors_metadata']} ssm
                    ON sm.sensor_id = ssm.sensor_id
                    AND sm.inner_index = ssm.inner_index
                    AND date BETWEEN relevant_from AND relevant_untill
                WHERE project_id ='{self.project_id}' 
                    AND sm.date BETWEEN '{self.min_date}' AND '{self.max_date}' 
                    AND depth_cm BETWEEN {min_depth} AND {max_depth}
                    AND moisture = True
                GROUP BY ts, depth_cm
                """
        if logic_parameters.sql_debug:
            print(query)

        sql_importer = SqlImporter(query=query, database=c.database_production, user=c.user_production, password=c.password_production,host=c.host_production, port=c.port_production, verbose=logic_parameters.sql_debug)

        sql_importer.get_data()
        self.df_sm_data_raw = sql_importer.data.drop_duplicates()
        
        if self.df_sm_data_raw.empty:
            self.valid_project = False
            if local_env:
                print(f"pid {self.project_id}::empty SM data")
            else:
                logging.info(f"pid {self.project_id}::empty SM data")
            self.flag += '|empty SM data'
            if self.debug:
                print('empty SM table')
            raise Exception('empty SM data')

        else:
            self.sensor_id = self.df_sm_data_raw.sensor_id[0]

        if self.debug:
            print('load_sm_project_data complete')

    def apply_transformers(self):
        df = self.df_sm_data_raw
        self.sm_transform_allowed_range(df)
        self.transformer_fill_missing_values_of_sm(df)

    def sm_transform_allowed_range(self, df):
        # replace 'bad' sm values with None:
        df.loc[((df.sm_val < self.min_sm_moist) | (df.sm_val > self.max_sm_moist)), 'sm_val'] = None

    def transformer_fill_missing_values_of_sm(self, df):
        # takes the 'raw' sm data and fill missing data with None to ensure diff calculations are only for consecutive data-points
        _func_ = "transformer_fill_missing_values_of_sm"
        if self.debug:
            print(f"{_func_} started")

        self.df_sm_data = pd.DataFrame([])
        for d, mdf in df.groupby('depth_cm'):
            mdf = mdf.drop_duplicates(subset=['local_time'])  # remove duplicates due to DST
            mdf_i = mdf.set_index('local_time').resample('H').fillna(None).reset_index().sort_values('local_time')  # add missing hours to ensure diff calculations make sense
            mdf_i['depth_cm'] = d
            self.df_sm_data = pd.concat([self.df_sm_data, mdf_i], )

    def group_data_to_depths(self):
        _func_ = "group_data_to_depths"
        if self.debug:
            print(f"{_func_} started")

        self.multi_depths_sm = {}
        for d in self.df_sm_data.depth_cm.unique():
            mdf = self.df_sm_data.loc[self.df_sm_data.depth_cm == d][['local_time', 'sm_val']].copy().set_index('local_time')
            mdf.dropna(how='any', inplace=True)
            mdf['sm_diff'] = mdf['sm_val'].diff()

            max_len_of_SM_hourly_values = ((dt.datetime.strptime(self.max_date,"%Y-%m-%d").date().day - dt.datetime.strptime(self.min_date,"%Y-%m-%d").date().day) + 1) * 24
            if (len(mdf.sm_val) < logic_parameters.MIN_PCT_OF_SM_HOURLY_VALUES * max_len_of_SM_hourly_values):
                  self.missing_data = True

            self.multi_depths_sm.update({d: mdf})


    def load_irrigation_spans(self):
        _func_ = "load_irrigation_spans"
        if self.debug:
            print(f"{_func_} started")

        query = f"""
            SELECT project_id, amount, start_ts, end_ts, psi, start_date, 
            timezone('{self.timezone}', to_timestamp(start_ts / 1000)) start_lt,
            timezone('{self.timezone}', to_timestamp(end_ts / 1000)) end_lt
            FROM {DB_TABLES['project_irrigation_spans']}
            WHERE project_id = {self.project_id} 
            AND start_date >= CAST((CAST('{self.min_date}' AS timestamp)) AS date)
            AND start_date <= CAST((CAST('{self.max_date}' AS timestamp)) AS date)
        """
        query = f"""
            SELECT pis.project_id, amount, start_ts, end_ts, psi, start_date, 
            timezone('{self.timezone}', to_timestamp(start_ts / 1000)) start_lt,
            timezone('{self.timezone}', to_timestamp(end_ts / 1000)) end_lt,
            irrigation_system_type system_type
            FROM {DB_TABLES['project_irrigation_spans']} pis
            JOIN  {DB_TABLES['projects_hierachy']} ph
            ON pis.project_id = ph.project_id
            WHERE ((pis.project_id = {self.project_id}) OR 
                    (pis.project_id = (SELECT project_id from projects_hierachy
                    WHERE  main_project_const_id = (SELECT main_project_const_id FROM projects_hierachy WHERE project_id = {self.project_id})
                    AND season = 2023
                    AND project_id <> {self.project_id})))
            AND start_date >= CAST((CAST('{self.min_date}' AS timestamp)) AS date)
            AND start_date <= CAST((CAST('{self.max_date}' AS timestamp)) AS date)
            ORDER BY start_date
        """
        if logic_parameters.sql_debug:
            print(query)

        sql_importer = sql_importer = SqlImporter(query=query, database=c.database_production, user=c.user_production, password=c.password_production, host=c.host_production, port=c.port_production, verbose=logic_parameters.sql_debug)
        sql_importer.get_data()
        
        self.df_irrigation = sql_importer.data.copy().dropna()
        self.df_irrigation.rename(columns={'start_lt': 'start', 'end_lt': 'end'}, inplace=True)

    def find_probe_local_saturation(self):
        for depth in self.depths_found:
            max_sm_val = self.df_sm_data_raw.groupby('depth_cm').sm_val.max()[depth]
            min_sm_val = self.df_sm_data_raw.groupby('depth_cm').sm_val.min()[depth]
            self.local_saturation_by_depth.append((depth,max_sm_val)) 
            if max_sm_val < self.min_sm_saturation:
                self.flag += f"|probe_depth_{depth}_not_responding_local_saturation"

    def SM_statistics_by_probe(self):
        _func_ = "SM_statistics_by_probe"
        if self.debug:
            print(f"{_func_} started")

        SM_statistics = {}
        for d in self.multi_depths_sm.keys():
            SM_statistics[d] = {'mean': self.multi_depths_sm[d]['sm_val'].mean(),
                                'std': self.multi_depths_sm[d]['sm_val'].std(),
                               'max': self.multi_depths_sm[d]['sm_val'].max(),
                               'min': self.multi_depths_sm[d]['sm_val'].min(),
                               'max_diff': self.multi_depths_sm[d]['sm_val'].max() - self.multi_depths_sm[d]['sm_val'].min(),
                               'max_hourly_diff': self.multi_depths_sm[d]['sm_diff'].max(),
                               'trend': self.probe_SM_trend(depth=d)
                               }
        self.SM_statistics = SM_statistics

    def get_sensor_daily_status(self):
        _func_ = "get_sensor_daily_status"
        if self.debug:
            print(f"{_func_} started")

        # SOIL_TREND_7
        query = f"""
                select date,statuses from public.sensor_calculations_v2
                where sensor_id = {self.sensor_id}
                and date > '{self.min_date}'
                and inner_index in ({self.depths_found_index[0]},{self.depths_found_index[1]})
                """
        if logic_parameters.sql_debug:
            print(query)

        sql_importer = SqlImporter(query=query, database=c.database_production, user=c.user_production, password=c.password_production, host=c.host_production, port=c.port_production, verbose=logic_parameters.sql_debug)

        sql_importer.get_data()
        df  = sql_importer.data
        df['fail_reason'] = df['statuses']
        
        df['daily_status'] = df.statuses.apply(lambda x: 'FAIL' if x != ['TECHNICAL_OK'] else 'OK')
        self.df_daily_status = df

    def get_sensor_support_status(self):
        query = f"""
            SELECT serial_number as sensor_id, updated_at, status, work_type
            FROM work_order_line_items
            WHERE serial_number = '{self.sensor_id}'
            --AND status not in ('closed','completed')
            order by updated_at desc
            limit 1
        """
        if logic_parameters.sql_debug:
            print(query)

        sql_importer = SqlImporter(query=query, database=c.database_ruby_production, user=c.user_production, password=c.password_production, host=c.host_production, port=c.port_production, verbose=logic_parameters.sql_debug)

        sql_importer.get_data()
        sensor_support_status_dict = sql_importer.data.to_dict('records')
        if not sensor_support_status_dict:
            self.flag += '|empty SM support status'
            if self.debug:
                print(f"{self.sensor_id}::empty WOLI table")
            return({'status': '', 'updated_at': (dt.date.today()).strftime("%Y-%m-%d"),
                         'work_type': '', 'days_since_task_complete': -99})
        
        sensor_support_status_dict['days_since_task_complete'] = (dt.datetime.today().date() -
                                    sensor_support_status_dict['updated_at'].date()).days
        self.sensor_support_status = True
        return(sensor_support_status_dict)

    def display_app_link(self, env='app'):
        display(HTML("""<a href="%s">%s</a>"""  %(self.app_link, self.app_link)))

    def transform_date_to_local(self, df, col_ts='ts', col_local_date='local_time'):
        df['time'] = df[col_ts].apply(lambda x: dt.datetime.fromtimestamp(x/1000))
        if self.timezone:
            df[col_local_date] = pd.to_datetime(df['time']).dt.tz_localize(tz='UTC+0100').dt.tz_convert(self.timezone).dt.tz_localize(None)
        else: 
            df[col_local_date] = df['time']

    def transformer_remove_duplicates(self, df):
        df = df.sort_values(['local_time'], ascending=[True]).drop_duplicates(['local_time', 'depth_cm'], keep='first')

                            
    # return linear coefficient of SM values over time
    def probe_SM_trend(self, depth):
        _func_ = "probe_SM_trend"
        if self.debug:
            print(f"{_func_} started")
        
        x = self.df_sm_data[self.df_sm_data.depth_cm==depth].local_time.apply(date2num)
        y = self.df_sm_data[self.df_sm_data.depth_cm==depth].sm_val.rolling(3).mean()
        if not self.missing_data:
            try:
                z = np.polyfit(x,y,1,full=False)
            except Exception as e:
                if local_env:
                    print(f'pid {self.project_id}::{e}')
                else:
                    logging.info(f'pid {self.project_id}::{e}')
                z = ['NA']
            return(z[0])
        else:
            return('NA')

    #def find_sm_anomalies(self, debug=self.debug):
    #    pass

    def load_project_weather_data(self, future=14):
        if type(self.max_date) == str:
            max_date = dt.datetime.strptime(self.max_date, '%Y-%m-%d')
        else:
            max_date = self.max_date
        max_date += dt.timedelta(days=future)  # we need the future ET for the SM prediction

        query = f"""
                SELECT 
                    date local_date, 
                    TEMPERATURE_MIN min_temp, 
                    TEMPERATURE_MAX max_temp, 
                    RAIN_SUM rain, 
                    ET_SUM et
                FROM crosstab(
                        'SELECT 
                            wac.CALC_DATE,
                            concat(wac.DATA_TYPE,''_'',WAC.CALC_TYPE),
                            cast ( COALESCE(
                                MAX(CASE WHEN wac.IS_FORECAST = false 
                                    THEN (COALESCE (wac.CALC_PHYTECH_RESULT,wac.CALC_DAVIS_RESULT)) END),
                                MAX(CASE WHEN wac.IS_FORECAST = true 
                                    THEN (COALESCE (wac.CALC_NOAA_RESULT)) END),
                                MAX(CASE WHEN wac.IS_FORECAST = false
                                    THEN (COALESCE (wac.CALC_IBM_RESULT)) END),
                                MAX(CASE WHEN wac.IS_FORECAST = true
                                    THEN (COALESCE (wac.CALC_IBM_RESULT)) END)) as decimal(10,2))
                                FROM {DB_TABLES['weather_area_calcs']} wac
                            WHERE wac.area_id = {self.area_id}
                                AND WAC.CALC_DATE BETWEEN ''{self.min_date}'' AND ''{max_date}''
                                AND ( ( WAC.DATA_TYPE =''ET'' AND  WAC.CALC_TYPE =''SUM'')  
                                        OR 
                                    ( WAC.DATA_TYPE =''RAIN'' AND  WAC.CALC_TYPE =''SUM'') 
                                        OR 
                                    ( WAC.DATA_TYPE =''TEMPERATURE'' AND  WAC.CALC_TYPE =''MAX'') 
                                        OR 
                                    (WAC.DATA_TYPE =''TEMPERATURE'' AND  WAC.CALC_TYPE =''MIN'')
                                    )
                                AND WAC.CALC_SPAN = ''daily'' 
                            GROUP BY wac.CALC_DATE,wac.DATA_TYPE,WAC.CALC_TYPE
                            ORDER BY 1, 2',
                        'SELECT unnest(ARRAY[''TEMPERATURE_MIN'',''TEMPERATURE_MAX'',''RAIN_SUM'',''ET_SUM''])') 
                        AS res(date date, TEMPERATURE_MIN double precision, TEMPERATURE_MAX double precision, RAIN_SUM double precision, ET_SUM double precision)
                """
        sql_importer = SqlImporter(query=query, database=c.database_production, user=c.user_production, password=c.password_production, host=c.host_production, port=c.port_production, verbose=logic_parameters.sql_debug)

        sql_importer.get_data()
        self.df_weather_data = sql_importer.data


    def plot_sm_ts(self, preds=None, history=14):
        # plotly version
        fig = make_subplots(rows=1, cols=1, x_title='local time', )
        sm_col = 'sm_val'
        if len(self.multi_depths_sm) == 0:
            return

        for d, df in self.multi_depths_sm.items():
            df = df.sort_index().iloc[-history * 24:]
            fig.append_trace(go.Scatter(
                x=df.index, y=df[sm_col].values, opacity=1, name='depth=%dcm' % int(d), hovertemplate="%{x|%Y/%m/%d %H:%M:%S} value: %{y}"), row=1, col=1, )

        fig.update_layout(
            title="Soil Moisture Values",
            legend_title=None,
            hovermode='x unified'
        )

        if preds is not None:
            fig.append_trace(go.Scatter(
                x=preds.index, y=preds[0].values, opacity=1, name='prediction', hovertemplate="%{x|%Y/%m/%d %H:%M:%S} value: %{y}"), row=1, col=1, )

        fig.show()

        