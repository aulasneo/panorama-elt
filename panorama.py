"""
Extract MySQL tables from an Open edX instance.

Usage:
python panorama.py --help

"""
import yaml
import os

import click

from course_structures_extractor.course_structures_extractor import CourseStructuresExtractor
from sql_tables_extractor.sql_extractor import SqlExtractor
from panorama_datalake.panorama_datalake import PanoramaDatalake

from panorama_logger.setup_logger import log
from __about__ import __version__


def load_settings() -> dict:
    """
    Load config_file as settings
    :return: settings structure
    """
    try:
        with open(config_file, 'r') as f:
            yaml_settings = yaml.safe_load(f)
    except FileNotFoundError as e:
        log.error("No config file {} found".format(config_file))
        exit(1)

    return yaml_settings


def save_settings() -> None:
    """
    Save config_file from settings
    :return: settings structure
    """
    with open(config_file, 'w') as f:
        yaml.safe_dump(settings, f, sort_keys=False)


@click.group()
def cli():
    pass


@cli.command(help='Extracts and uploads all MySQL tables defined in the settings file')
@click.option('--force', is_flag=True, help='Force upload all partitions. False by default')
def upload_sql_tables(force):
    """
    Click command to run _upload_sql_tables
    :param force:
    :return:
    """
    _upload_sql_tables(force)


def _upload_sql_tables(force):
    """
    Query MySQL tables defined in the settings and uploads to the datalake
    :param force: boolean. Force a full dump for tables with incremental updates configured
    :return:
    """
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


@cli.command(help='Upload course structures only')
def upload_course_structures():
    """
    Click command to run _upload_course_structures
    :return:
    """
    _upload_course_structures()


def _upload_course_structures():
    """
    Connects to MongoDB to retrieve a list of Open edX modulestore blocks.
    It will create a csv file with the display name of each block and all it's parents
    :return:
    """
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


@cli.command(help='Uploads all tables defined in the configuration file plus the course structures table')
@click.option('--force', is_flag=True, help='Force upload all partitions. False by default')
def openedx_upload_all(force):
    """
    Convenience method for Open edX installations to upload all MySQL tables and the course structures
    :param force: boolean. Force a full dump for tables with incremental updates configured
    :return:
    """
    _upload_sql_tables(force)
    _upload_course_structures()


@cli.command(help='Creates datalake tables for all tables defined in the settings file')
def create_datalake_tables():
    """
    Click command to run _create_datalake_tables
    :return:
    """
    _create_datalake_tables()


def _create_datalake_tables():
    """
    Connect to Athena and create the table definition for the MySQL tables
    :return:
    """
    # Create tables for mysql
    for table_setting in settings.get('tables'):

        partitions = table_setting.get('partitions')
        if partitions:
            partition_fields = partitions.get('partition_fields')
        else:
            partition_fields = None

        fields = table_setting.get('fields')

        if fields:
            datalake.create_datalake_csv_table(
                table=table_setting.get('name'),
                fields=table_setting.get('fields'),
                field_partitions=partition_fields,
                datalake_table=table_setting.get('datalake_table_name')
            )

        else:
            log.warning("No fields defined for table {}. Skipping table creation".format(table_setting.get('name')))

    datalake.get_athena_executions()


@cli.command(help='Creates datalake table for the course structures')
def create_course_structures_datalake_table():
    """
    Click command to run _create_course_structures_datalake_table
    :return:
    """
    _create_course_structures_datalake_table()


def _create_course_structures_datalake_table():
    """
    Connect to Athena and create the table definition for the Open edX's course structures table
    :return:
    """
    # Create course_structures table

    fields = [
        'module_location',
        'course_id',
        'organization',
        'course_code',
        'course_edition',
        'parent',
        'block_type',
        'block_id',
        'display_name',
        'course_name',
        'chapter',
        'sequential',
        'vertical',
        'library',
        'component'
    ]
    datalake.create_datalake_csv_table(
        table='course_structures',
        fields=fields,
        datalake_table='course_structures_raw'
    )

    datalake.get_athena_executions()


@cli.command(help='Creates datalake tables for all tables defined in the settings file and the course structures')
def openedx_create_datalake_tables():
    """
    Convenience method to create all Open edX tables
    :return:
    """
    _create_datalake_tables()
    _create_course_structures_datalake_table()


@cli.command(help="Clears all table configurations and sets new tables from the comma-separated list")
@click.option('-t', '--tables', 'table_list_')
def set_tables(table_list_: str):
    new_tables_settings = []
    for table_name in table_list_.split(','):
        new_tables_settings.append({'name': table_name})

    settings['tables'] = new_tables_settings

    save_settings()

    click.echo("Tables updated".format(config_file))


@cli.command(help='Queries the SQL tables and updates the tables section of the settings file. Use with care.')
def set_tables_fields():
    """
    Query MySQL tables and get the field list from each table defined in the settings.
    Update the 'tables' settings with the list of fields retrieved
    * Not recommended for Open edX installations *
    :return:
    """
    mysql_username = settings.get('mysql_username', 'root')
    mysql_password = settings.get('mysql_password')
    mysql_host = settings.get('mysql_host', '127.0.0.1')
    mysql_database = settings.get('mysql_database', 'edxapp')

    panorama_mysql_tables = [t.get('name') for t in settings.get('tables')]

    sql_extractor = SqlExtractor(
        datalake=datalake,
        mysql_username=mysql_username,
        mysql_password=mysql_password,
        mysql_host=mysql_host,
        mysql_database=mysql_database,
        mysql_tables=panorama_mysql_tables,
    )

    table_fields = sql_extractor.get_all_fields()

    new_tables_settings = []
    for table_setting in settings.get('tables'):
        new_tables_setting = {}
        name = table_setting.get('name')
        for k, v in table_setting.items():
            new_tables_setting[k] = v
        new_tables_setting['fields'] = table_fields.get(name)

        new_tables_settings.append(new_tables_setting)

    settings['tables'] = new_tables_settings

    save_settings()

    click.echo("{} updated".format(config_file))


if __name__ == '__main__':

    # Load settings file
    config_file = os.getenv('PANORAMA_SETTINGS_FILE', default='panorama_settings.yaml')
    settings = load_settings()

    # S3 bucket where the tables will be uploaded.
    panorama_raw_data_bucket = settings.get('panorama_raw_data_bucket')
    if not panorama_raw_data_bucket:
        log.error("panorama_raw_data_bucket must be set")
        exit(1)

    # Datalake table names may differ from MySQL tables. As a convention, we add '_raw' to the table names
    datalake_table_names = {}
    for table in settings.get('tables'):
        if 'datalake_table_name' in table:
            datalake_table_names[table.get('name')] = table.get('datalake_table_name')

    # List of partitions common to all tables. For Open edX, it's set to {'lms': <LMS_HOST>}.
    base_partitions = {}
    if 'base_partitions' in settings:
        for base_partition in settings.get('base_partitions'):
            base_partitions[base_partition.get('key')] = base_partition.get('value')

    # Create the datalake object
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

    # Expect the CLI command
    cli()
