import cert_aws as c
from sql_import_export import SqlImporter

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
    'project_irrigation_spans': 'project_irrigation_spans_v2',
    'projects_hierachy': 'projects_hierachy'
}

ALLOWED_SM_TYPES  = (90, 91, 92, 98, 117, 118, 124, 127, 135, 137,)