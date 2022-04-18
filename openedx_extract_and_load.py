"""
Extract MySQL tables from an Open edX instance
"""
import yaml
import os

from course_structures_extractor.course_structures_extractor import CourseStructuresExtractor
from sql_tables_extractor.sql_extractor import SqlExtractor

from panorama_logger.setup_logger import log


def load_settings(config_file: str) -> dict:
    with open(config_file, 'r') as f:
        yaml_settings = yaml.safe_load(f)

    return yaml_settings


def extract_and_load_sql_tables():

    panorama_raw_data_bucket = settings.get('panorama_raw_data_bucket')
    if not panorama_raw_data_bucket:
        log.error("panorama_raw_data_bucket must be set")
        exit(1)

    aws_access_key = settings.get('aws_access_key')
    aws_secret_access_key = settings.get('aws_access_key')
    aws_region = settings.get('aws_region', 'us-east-1')

    mysql_username = settings.get('mysql_username', 'root')
    mysql_password = settings.get('mysql_password')
    mysql_host = settings.get('mysql_host', '127.0.0.1')
    mysql_database = settings.get('mysql_database', 'edxapp')
    panorama_mysql_tables = [table.get('name') for table in settings.get('tables')]

    # This dict defines which tables have partitions
    # The interval is in MYSQL format
    table_partitions = {}
    for table_setting in settings.get('tables'):
        partitions = table_setting.get('partitions')
        if partitions:
            table_partitions[table_setting.get('name')] = {
                'partition_fields': partitions.get('partition_fields'),
                'interval': partitions.get('interval'),
                'timestamp_field': partitions.get('timestamp_field'),
                'datalake_db': settings.get('datalake_database'),
                'datalake_table': table_setting.get('datalake_table_name'),
                'workgroup': settings.get('datalake_workgroup'),
            }

    base_partitions = {}
    for base_partition_setting in settings.get('base_partitions'):
        base_partitions[base_partition_setting.get('key')] = base_partition_setting.get('value')

    sql_extractor = SqlExtractor(
        panorama_raw_data_bucket=panorama_raw_data_bucket,
        aws_access_key=aws_access_key,
        aws_secret_access_key=aws_secret_access_key,
        aws_region=aws_region,
        mysql_username=mysql_username,
        mysql_password=mysql_password,
        mysql_host=mysql_host,
        mysql_database=mysql_database,
        panorama_mysql_tables=panorama_mysql_tables,
        table_partitions=table_partitions,
        base_partitions=base_partitions
    )

    sql_extractor.extract_mysql_tables()


def extract_and_load_course_structures():

    base_partitions = {}
    for base_partition_setting in settings.get('base_partitions'):
        base_partitions[base_partition_setting.get('key')] = base_partition_setting.get('value')

    panorama_raw_data_bucket = settings.get('panorama_raw_data_bucket')
    aws_access_key = settings.get('aws_access_key')
    aws_secret_access_key = settings.get('aws_access_key')

    # Extract course structures from MongoDB
    mongodb_username = settings.get('mongodb_username')
    mongodb_password = settings.get('mongodb_password')
    mongodb_host = settings.get('mongodb_host', '127.0.0.1')
    mongodb_database = settings.get('mongodb_database', 'edxapp')

    course_structures_extractor = CourseStructuresExtractor(
        aws_access_key=aws_access_key,
        aws_secret_access_key=aws_secret_access_key,
        panorama_s3_bucket=panorama_raw_data_bucket,
        mongodb_username=mongodb_username,
        mongodb_password=mongodb_password,
        mongodb_host=mongodb_host,
        mongodb_database=mongodb_database,
        base_partitions=base_partitions
    )

    course_structures_extractor.extract_course_structures()


if __name__ == '__main__':

    settings = load_settings(os.getenv('PANORAMA_SETTINGS_FILE', default='panorama_settings.yaml'))

    extract_and_load_sql_tables()
    extract_and_load_course_structures()
