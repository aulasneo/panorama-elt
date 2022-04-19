"""
Extract MySQL tables from an Open edX instance
"""
import yaml
import os

from course_structures_extractor.course_structures_extractor import CourseStructuresExtractor
from sql_tables_extractor.sql_extractor import SqlExtractor
from panorama_datalake.panorama_datalake import PanoramaDatalake

from panorama_logger.setup_logger import log


def load_settings(config_file: str) -> dict:
    with open(config_file, 'r') as f:
        yaml_settings = yaml.safe_load(f)

    return yaml_settings


def extract_and_load_sql_tables(force: bool = False):

    mysql_username = settings.get('mysql_username', 'root')
    mysql_password = settings.get('mysql_password')
    mysql_host = settings.get('mysql_host', '127.0.0.1')
    mysql_database = settings.get('mysql_database', 'edxapp')
    panorama_mysql_tables = [t.get('name') for t in settings.get('tables')]

    # This dicts defines which tables have partitions and static fields configurations (if present)
    # The interval is in MYSQL format
    table_partitions = {}
    table_fields = {}
    for table_setting in settings.get('tables'):
        partitions = table_setting.get('partitions')
        if partitions:
            table_partitions[table_setting.get('name')] = {
                'partition_fields': partitions.get('partition_fields'),
                'interval': partitions.get('interval'),
                'timestamp_field': partitions.get('timestamp_field'),
            }
        fields = table_setting.get('fields')
        if fields:
            table_fields[table_setting.get('name')] = fields

    sql_extractor = SqlExtractor(
        datalake=datalake,
        mysql_username=mysql_username,
        mysql_password=mysql_password,
        mysql_host=mysql_host,
        mysql_database=mysql_database,
        mysql_tables=panorama_mysql_tables,
        field_partitions=table_partitions,
        table_fields=table_fields
    )

    sql_extractor.extract_mysql_tables(force)


def extract_and_load_course_structures():

    # Extract course structures from MongoDB
    mongodb_username = settings.get('mongodb_username')
    mongodb_password = settings.get('mongodb_password')
    mongodb_host = settings.get('mongodb_host', '127.0.0.1')
    mongodb_database = settings.get('mongodb_database', 'edxapp')

    course_structures_extractor = CourseStructuresExtractor(
        datalake=datalake,
        mongodb_username=mongodb_username,
        mongodb_password=mongodb_password,
        mongodb_host=mongodb_host,
        mongodb_database=mongodb_database,
    )

    course_structures_extractor.extract_course_structures()


if __name__ == '__main__':

    settings = load_settings(os.getenv('PANORAMA_SETTINGS_FILE', default='panorama_settings.yaml'))

    panorama_raw_data_bucket = settings.get('panorama_raw_data_bucket')
    if not panorama_raw_data_bucket:
        log.error("panorama_raw_data_bucket must be set")
        exit(1)

    datalake_table_names = {}
    for table in settings.get('tables'):
        if 'datalake_table_name' in table:
            datalake_table_names[table.get('name')] = table.get('datalake_table_name')

    base_partitions = {}
    for base_partition in settings.get('base_partitions'):
        base_partitions[base_partition.get('key')] = base_partition.get('value')

    datalake = PanoramaDatalake(
        aws_access_key=settings.get('aws_access_key'),
        aws_secret_access_key=settings.get('aws_secret_access_key'),
        aws_region=settings.get('aws_region', 'us-east-1'),
        datalake_db=settings.get('datalake_database'),
        datalake_workgroup=settings.get('datalake_workgroup'),
        base_prefix=settings.get('base_prefix'),
        bucket=panorama_raw_data_bucket,
        base_partitions=base_partitions,
        datalake_table_names=datalake_table_names,
    )
    extract_and_load_sql_tables(force=True)
    extract_and_load_course_structures()
