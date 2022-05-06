#! venv/bin/python
"""
Extract tables from datasources, load to a datalake and create primary transformations.

Usage:
panorama --help

"""
import logging

import yaml

import click

from course_structures_datasource.course_structures_datasource import CourseStructuresDatasource
from mysql_datasource.mysql_datasource import MySQLDatasource
from panorama_datalake.panorama_datalake import PanoramaDatalake

from panorama_logger.setup_logger import log
from __about__ import __version__

config_file = "panorama_settings.yaml"
datalake = PanoramaDatalake()
settings = {}
datalake_table_names = {}


def load_settings() -> dict:
    """
    Load config_file as settings
    :return: settings structure
    """
    try:
        with open(config_file, 'r') as f:
            yaml_settings = yaml.safe_load(f)
    except FileNotFoundError:
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
@click.version_option(version=__version__)
@click.option("--debug", is_flag=True, default=False, help="Enable debugging")
@click.option("--settings", 'file', help="Configuration file", default="panorama_settings.yaml")
def cli(debug, file):
    if debug:
        log.setLevel(logging.DEBUG)
    global config_file
    config_file = file


@cli.command(help='Extracts the data from the datasources and uploads to the datalake')
@click.option("--all", "-a", "all_", is_flag=True, default=False, help="Extract and load all tables of all datasource")
@click.option("--datasource", "-d", required=False, default=None, help="Extract and load only for this datasource")
@click.option("--tables", "-t", required=False, default=None, help="Comma separated list of tables to extract and load")
@click.option('--force', is_flag=True, help='Force upload all partitions. False by default', default=False)
def extract_and_load(all_, datasource, tables, force):
    """
    Click command to run _create_datalake_tables
    :return:
    """
    if all_:
        if tables:
            click.echo("--all and --table cannot be used together")
        else:
            if datasource:
                _extract_and_load(datasource=datasource, force=force)
            else:
                _extract_and_load(force=force)
    else:
        if datasource or tables:
            _extract_and_load(datasource=datasource, selected_tables=tables, force=force)
        else:
            click.echo("Either --all or --datasource or --table must be specified")


def _extract_and_load(datasource=None, selected_tables=None, force=False):
    """
    Query the datasources defined in the settings and uploads to the datalake
    :param force: boolean. Force a full dump for tables with incremental updates configured
    :return:
    """

    for ds_settings in settings.get('datasources'):
        datasource_type = ds_settings.get("type")

        if datasource and ds_settings.get("name") != datasource:
            continue

        if selected_tables:
            tables = [t.get('name') for t in ds_settings.get('tables') if t.get('name') in selected_tables.split(',')]
        else:
            tables = [t.get('name') for t in ds_settings.get('tables')]

        if datasource_type == 'mysql':

            # This dicts defines which tables have partitions and static fields configurations (if present)
            # The interval is in MYSQL format
            table_partitions = {}
            table_fields = {}
            for table_setting in ds_settings.get('tables'):
                partitions = table_setting.get('partitions')
                if partitions:
                    table_partitions[table_setting.get('name')] = {
                        'partition_fields': partitions.get('partition_fields'),
                        'interval': partitions.get('interval'),
                        'timestamp_field': partitions.get('timestamp_field'),
                    }
                fields = table_setting.get('fields')
                if fields:
                    table_fields[table_setting.get('name')] = [f.get("name") for f in fields]

            mysql_username = ds_settings.get('mysql_username', 'root')
            mysql_password = ds_settings.get('mysql_password')
            mysql_host = ds_settings.get('mysql_host', '127.0.0.1')
            mysql_database = ds_settings.get('mysql_database', 'edxapp')

            mysql_datasource = MySQLDatasource(
                datalake=datalake,
                mysql_username=mysql_username,
                mysql_password=mysql_password,
                mysql_host=mysql_host,
                mysql_database=mysql_database,
                mysql_tables=tables,
                field_partitions=table_partitions,
                table_fields=table_fields
            )

            mysql_datasource.extract_and_load(tables=','.join(tables), force=force)

        elif datasource_type == 'openedx_course_structures':

            # Extract course structures from MongoDB
            course_structures_datasource = get_course_structures_datasource(ds_settings)
            course_structures_datasource.extract_and_load()

        else:
            log.error("Datasource type {} not supported".format(datasource_type))


@cli.command(help='Creates datalake tables for all tables defined in the settings file. Table fields must be defined.')
@click.option("--all", "-a", "all_", is_flag=True, default=False, help="Create all tables of all datasource")
@click.option("--datasource", "-d", required=False, default=None, help="Create tables only for this datasource")
@click.option("--tables", "-t", required=False, default=None, help="Comma separated list of tables to create")
def create_datalake_tables(all_, datasource, tables):
    """
    Click command to run _create_datalake_tables
    :return:
    """
    if all_:
        if tables:
            click.echo("--all and --table cannot be used together")
        else:
            if datasource:
                _create_datalake_tables(datasource)
            else:
                _create_datalake_tables()
    else:
        if datasource or tables:
            _create_datalake_tables(datasource, tables)
        else:
            click.echo("Either --all or --datasource or --table must be specified")


def _create_datalake_tables(datasource=None, tables=None):
    """
    Connect to Athena and create the table definition for the MySQL tables
    :return:
    """
    # Create tables for all datasources
    for datasource_settings in settings.get('datasources'):
        if datasource and datasource_settings.get('name') != datasource:
            continue
        for table_setting in datasource_settings.get('tables'):
            if tables and table_setting.get('name') not in tables.split(','):
                continue

            partitions = table_setting.get('partitions')
            if partitions:
                partition_fields = partitions.get('partition_fields')
            else:
                partition_fields = None

            fields_and_types = table_setting.get('fields')

            if fields_and_types:
                fields = [f.get("name") for f in fields_and_types]
                log.info("Creating or updating datalake table for {}".format(table_setting.get('name')))
                datalake.create_datalake_table(
                    table=table_setting.get('name'),
                    fields=fields,
                    field_partitions=partition_fields,
                    datalake_table=table_setting.get('datalake_table_name')
                )

            else:
                log.warning("No fields defined for table {}. Skipping table creation".format(table_setting.get('name')))

    datalake.get_athena_executions()


@cli.command(help='Deletes datalake tables')
@click.option("--all", "-a", "all_", is_flag=True, default=False, help="Deletes all tables of all datasource")
@click.option("--datasource", "-d", required=False, default=None, help="Deletes tables only for this datasource")
@click.option("--tables", "-t", required=False, default=None, help="Comma separated list of tables to delete")
def drop_datalake_tables(all_, datasource, tables):
    """
    Click command to run _create_datalake_tables
    :return:
    """
    if all_:
        if tables:
            click.echo("--all and --table cannot be used together")
        else:
            if datasource:
                _drop_datalake_tables(datasource)
            else:
                _drop_datalake_tables()
    else:
        if datasource or tables:
            _drop_datalake_tables(datasource, tables)
        else:
            click.echo("Either --all or --datasource or --table must be specified")


def _drop_datalake_tables(datasource=None, tables=None):
    """
    Connect to Athena and delete the table definition for the MySQL tables
    :return:
    """
    # Create tables for all datasources
    base_prefix = settings.get('datalake').get('base_prefix')
    for datasource_settings in settings.get('datasources'):
        if datasource and datasource_settings.get('name') != datasource:
            continue
        for table_setting in datasource_settings.get('tables'):
            if tables and table_setting.get('name') not in tables.split(','):
                continue

            datalake_table = table_setting.get('datalake_table') or "{}_raw_{}".format(
                base_prefix, table_setting.get('name'))
            datalake_view = table_setting.get('datalake_table_view') or "{}_table_{}".format(
                base_prefix, table_setting.get('name'))

            log.info("Dropping {}".format(datalake_table))

            datalake.drop_datalake_table(datalake_table=datalake_table)
            datalake.drop_datalake_view(view=datalake_view)

    datalake.get_athena_executions()


@cli.command(help="Clears all table configurations and sets new tables from the comma-separated list")
@click.option('-t', '--tables', 'table_list_')
@click.option('-d', '--datasource', required=True)
def set_tables(table_list_: str, datasource: str):
    new_tables_settings = []
    for table_name in table_list_.split(','):
        new_tables_settings.append({'name': table_name})

    settings[datasource] = {'tables': new_tables_settings}

    save_settings()

    click.echo("Tables updated".format(config_file))


@cli.command(help='Creates views based on the tables defined. Tables must be created first.')
@click.option("--all", "-a", "all_", is_flag=True, default=False, help="Creates all table views of all datasource")
@click.option("--datasource", "-d", required=False, default=None, help="Creates table views only for this datasource")
@click.option("--tables", "-t", required=False, default=None, help="Comma separated list of tables views to create")
def create_table_views(all_, datasource, tables):
    """
    Click command to run _create_datalake_tables
    :return:
    """
    if all_:
        if tables:
            click.echo("--all and --table cannot be used together")
        else:
            if datasource:
                _create_table_view(datasource)
            else:
                _create_table_view()
    else:
        if datasource or tables:
            _create_table_view(datasource, tables)
        else:
            click.echo("Either --all or --datasource or --table must be specified")


def _create_table_view(datasource=None, tables=None):

    base_prefix = settings.get('datalake').get('base_prefix')
    for datasource_settings in settings.get('datasources'):
        if datasource and datasource_settings.get('name') != datasource:
            continue

        for table_setting in datasource_settings.get('tables'):

            table_name = table_setting.get('name')
            if tables and table_name not in tables.split(','):
                continue

            log.debug("Creating table view for table {} in datasource {}".format(
                table_name, datasource_settings.get('name')))

            view_name = table_setting.get('datalake_table_view')

            fields = table_setting.get('fields')
            if fields:
                datalake_table_name = datalake_table_names.get(table_name)

                if not view_name:
                    view_name = '{base_prefix}_table_{table_name}'.format(
                        base_prefix=base_prefix, table_name=table_name)

                log.info("Creating table view {}".format(view_name))
                datalake.create_table_view(datalake_table_name=datalake_table_name, view_name=view_name,
                                           fields=fields)
            else:
                log.warning("No fields defined for table {}".format(table_name))

    datalake.get_athena_executions()


def get_course_structures_datasource(ds_settings):
    mongodb_username = ds_settings.get('mongodb_username')
    mongodb_password = ds_settings.get('mongodb_password')
    mongodb_host = ds_settings.get('mongodb_host', '127.0.0.1')
    mongodb_database = ds_settings.get('mongodb_database', 'edxapp')

    course_structures_datasource = CourseStructuresDatasource(
        datalake=datalake,
        mongodb_username=mongodb_username,
        mongodb_password=mongodb_password,
        mongodb_host=mongodb_host,
        mongodb_database=mongodb_database,
    )

    return course_structures_datasource


@cli.command(help='Queries the SQL tables and updates the tables section of the settings file. Use with care.')
@click.option("--all", "-a", "all_", is_flag=True, default=False, help="Sets all table fields of all datasource")
@click.option("--datasource", "-d", required=False, default=None, help="Sets table fields only for this datasource")
@click.option("--tables", "-t", required=False, default=None, help="Comma separated list of tables to set fields")
def set_tables_fields(all_, datasource, tables):
    """
    Click command to run _create_datalake_tables
    :return:
    """
    if all_:
        if tables:
            click.echo("--all and --table cannot be used together")
        else:
            if datasource:
                _set_tables_fields(datasource)
            else:
                _set_tables_fields()
    else:
        if datasource or tables:
            _set_tables_fields(datasource, tables)
        else:
            click.echo("Either --all or --datasource or --table must be specified")


def _set_tables_fields(datasource=None, tables=None):
    """
    Query MySQL tables and get the field list from each table defined in the settings.
    Update the 'tables' settings with the list of fields retrieved
    * Not recommended for Open edX installations *
    :return:
    """

    for ds_settings in settings.get('datasources'):

        if datasource and datasource != ds_settings.get('name'):
            continue

        for table_settings in ds_settings.get('tables'):

            table_name = table_settings.get('name')
            if tables and table_name not in tables.split(','):
                continue

            log.debug("Setting fields for table {} in datasource {}".format(
                table_name, ds_settings.get('name')))

            if ds_settings.get('type') == 'mysql':

                mysql_username = ds_settings.get('mysql_username', 'root')
                mysql_password = ds_settings.get('mysql_password')
                mysql_host = ds_settings.get('mysql_host', '127.0.0.1')
                mysql_database = ds_settings.get('mysql_database', 'edxapp')

                mysql_datasource = MySQLDatasource(
                    datalake=datalake,
                    mysql_username=mysql_username,
                    mysql_password=mysql_password,
                    mysql_host=mysql_host,
                    mysql_database=mysql_database,
                    mysql_tables=tables,
                )

                table_fields = mysql_datasource.get_fields(table=table_name, force_query=True)

            elif ds_settings.get('type') == 'openedx_course_structures':

                course_structures_datasource = get_course_structures_datasource(ds_settings)

                table_fields = course_structures_datasource.get_fields(table=table_name)

            else:
                log.error("Unknown dataset type {}".format(ds_settings.get('type')))
                continue

            table_settings['fields'] = table_fields

    save_settings()

    click.echo("{} updated".format(config_file))


def init():

    # Load settings file
    global settings
    settings = load_settings()

    datalake_settings = settings.get('datalake')

    # S3 bucket where the tables will be uploaded.
    panorama_raw_data_bucket = datalake_settings.get('panorama_raw_data_bucket')
    if not panorama_raw_data_bucket:
        log.error("panorama_raw_data_bucket must be set")
        exit(1)

    # Datalake table names may differ from MySQL tables. As a convention, we add '_raw' to the table names
    global datalake_table_names
    datalake_table_names = {}
    for datasource_setting in settings.get('datasources'):
        for table in datasource_setting.get('tables'):
            if 'datalake_table_name' in table:
                datalake_table_names[table.get('name')] = table.get('datalake_table_name')
            else:
                datalake_table_names[table.get('name')] = "{base_prefix}_raw_{table_name}".format(
                    base_prefix=settings.get('datalake').get('base_prefix'),
                    table_name=table.get('name'))

    # List of partitions common to all tables. For Open edX, it's set to {'lms': <LMS_HOST>}.
    base_partitions = {}
    if 'base_partitions' in datalake_settings:
        for base_partition in datalake_settings.get('base_partitions'):
            base_partitions[base_partition.get('key')] = base_partition.get('value')

    # Create the datalake object
    global datalake
    datalake = PanoramaDatalake(
        aws_access_key=datalake_settings.get('aws_access_key'),
        aws_secret_access_key=datalake_settings.get('aws_secret_access_key'),
        aws_region=datalake_settings.get('aws_region', 'us-east-1'),
        datalake_db=datalake_settings.get('datalake_database'),
        datalake_workgroup=datalake_settings.get('datalake_workgroup'),
        base_prefix=datalake_settings.get('base_prefix'),
        bucket=panorama_raw_data_bucket,
        base_partitions=base_partitions,
        datalake_table_names=datalake_table_names,
    )

    # Expect the CLI command
    cli()


if __name__ == '__main__':
    init()
