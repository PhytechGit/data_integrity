import pandas as pd
import psycopg2
from sqlalchemy import create_engine



class SqlImporter: 
    def __init__(self, query=None, database=None, user=None, password=None, host=None, port=None, conn_str=None, verbose=False):
        self.query = query
        self.database = database
        self.connection = None
        self.cursor = None
        self.res = None
        self.data = pd.DataFrame([])
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.verbose = verbose
        self.conn_str = conn_str

        if self.conn_str:
            self.postgres_engine = create_engine(f"postgresql+psycopg2://{self.conn_str.replace('postgresql://', '')}")


        
    def get_postgress_conn(self):
        if self.conn_str is None:
            engine = psycopg2.connect(
            database=self.database,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port)
        else:
            if self.conn_str[0] == '\'':
                engine = psycopg2.connect(self.conn_str[1:-1])
            else:
                engine = psycopg2.connect(self.conn_str)
        return engine
    
    def set_connection(self):
        self.connection = self.get_postgress_conn()
        self.cursor = self.connection.cursor()
                
    def close_connection(self):
        if(self.connection):
            self.connection.close()
            self.cursor.close()
        if self.verbose:
            pass
            # print("PostgreSQL connection is closed")
    
    def get_data(self, commit=False):
        try:
            self.set_connection()
            df = pd.read_sql_query(self.query, self.connection)
            if commit:
                self.connection.commit()
                print("commited prediction")
            self.data = df
            table = self.query.lower().split('from ')[1].split(' ')[0]
            if self.verbose:
                print('Loaded table with %d lines from %s' % (len(self.data), table))
        except (Exception, psycopg2.Error) as error :
            if self.verbose:
                print ("Error while fetching data from PostgreSQL", error)

        finally:
            #closing database connection.
            self.close_connection()
         
    def execute_command(self):
        # used for executing command that require   commit (insert/update/delete data; createt table, etc.)
        try:
            self.set_connection()
            cursor = self.connection.cursor()
            cursor.execute(self.query) 
            self.connection.commit()
        except (Exception, psycopg2.Error) as error :
            print ("Error while fetching data from PostgreSQL", error)
        finally:
            self.close_connection()
            
    def execute_query(self):
        try:
            self.set_connection()
            cursor = self.connection.cursor()
            cursor.execute(self.query) 
            self.res = cursor.fetchall() 
        except (Exception, psycopg2.Error) as error :
            print ("Error while fetching data from PostgreSQL", error)
        finally:
            self.close_connection()


    def execute_values(self, iterable, query, commit=True):
        try:
            self.set_connection()
            with self.connection.cursor() as cursor:
                psycopg2.extras.execute_values(
                                    cursor,
                                    query,
                                    iterable, 
                                    page_size=1000)
                if commit:
                    self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            raise e