## testing
import datetime as dt
from project_functions import aggregate_project_data,load_project_data
#from functions import find_not_responding_events,get_project_results,load_project_data,not_responding_logic
import logic_parameters

def test_project_data(p_id, debug=False, sql_debug=False):
    yesterday = (dt.date.today() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = (dt.date.today() - dt.timedelta(days=8)).strftime("%Y-%m-%d")
    try:
        project_data = load_project_data(project_id=p_id, min_date=start_date,
                      max_date=yesterday, min_depth=10, max_depth=91, debug=debug)
        print(project_data.app_link)
        if project_data.valid_project & debug:
            display(project_data.df_irrigation[project_data.df_irrigation.amount > logic_parameters.MIN_IRR_AMOUNT])
        if project_data.valid_project:
            project_df = aggregate_project_data(project_data, debug=debug)
            print(project_df)
            if debug:
                print(project_df['remarks'][0])
                print('SM_statistics: \n',project_data['SM_statistics'])
            return(project_df)
        else:
            return(f"project {p_id} is not valid for calculation")
    except Exception as e:
        print(e)
