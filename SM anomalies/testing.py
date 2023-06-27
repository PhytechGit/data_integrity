## testing
import datetime as dt
import common_db
import logic_parameters
import project_functions

def test_project_data(p_id, debug=logic_parameters.debug_, sql_debug=logic_parameters.sql_debug):
    yesterday = (dt.date.today() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = (dt.date.today() - dt.timedelta(days=8)).strftime("%Y-%m-%d")
    try:
        project_data = project_functions.load_project_data(project_id=p_id, min_date=start_date,
                      max_date=yesterday, min_depth=10, max_depth=91, debug=debug)
        
        if project_data:
            print(project_data.app_link)

            #if project_data.valid_project & debug:
            #    display(project_data.df_irrigation[project_data.df_irrigation.amount > logic_parameters.MIN_IRR_AMOUNT])
            if project_data.valid_project:
                project_df = project_functions.aggregate_project_data(project_data, debug=debug)
                if debug:
                    print('SM_statistics: \n',project_data['SM_statistics'])
                project_functions.write_results_to_db(project_df)
                return(project_df)
            else:
                print(f"project {p_id} is not valid for calculation")
    except Exception as e:
        print(e)
