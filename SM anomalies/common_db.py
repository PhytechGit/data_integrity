MIN_SM_WEEKLY_DIFF = 10
MIN_SM_SATURATION = 25

DB_TABLES = {
    'projects_metadata_table': 'projects_metadata',
    'soil_sensors_metadata': 'soil_sensors_metadata',
    'phytoweb_projects': 'view_phytoweb_projects',
    'phytoweb_projects_s': 'view_phytoweb_projects_slims',
    'soil_measurements': 'soil_measurements',
    'weather_area_calcs': 'WEATHER_AREA_CALCS',
    'rl': 'project_refill_line',
    'results_table': 'project_soil_prediction',
    'project_irrigation_spans': 'project_irrigation_spans_v2'
}

ALLOWED_SM_TYPES  = (90, 91, 92, 98, 117, 118, 124, 127, 135, 137,)

def get_projects():
    import os
    from .sql_import_export import SqlImporter
    query = f"""
            SELECT DISTINCT project_id
            FROM {DB_TABLES['projects_metadata_table']}
            WHERE active=true
            """
    sql_importer = SqlImporter(
                        query, 
                        conn_str=os.environ['DATABASE_URL'])
    sql_importer.execute_query()
    return sql_importer.res