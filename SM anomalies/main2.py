import os
import sys
import warnings
#import pandas as pd

#import logging
import common_db
import logic_parameters
#import project_functions
#from logic_parameters import debug_
#from testing import test_project_data

warnings.filterwarnings('ignore')

def main():
    #test_project_data(848547, debug=False)
    query = "select * from di_sm_not_responding_daily_results"
    sql_importer = SqlImporter(query=query, conn_str=os.environ['DATABASE_URL_DEV'], verbose=logic_parameters.sql_debug)
    sql_importer.get_data()
    return (sql_importer.data)
    
        
if __name__ == '__main__':
    main()
    
