"""
Extract tables from datasources, load to a datalake and create primary transformations.

Usage:
panorama --help

"""
import logging

import yaml

import click

from panorama_elt.course_structures_datasource.course_structures_datasource import CourseStructuresDatasource
from panorama_elt.csv_datasource.csv_datasource import CSVDatasource
from panorama_elt.mysql_datasource.mysql_datasource import MySQLDatasource
from panorama_elt.xls_datasource.xls_datasource import XLSDatasource

from panorama_elt.panorama_datalake.panorama_datalake import PanoramaDatalake

from panorama_elt.panorama_logger.setup_logger import log
from __about__ import __version__


def load_settings(config_file: str) -> dict:
    """
    Load config_file as settings.
    :return: settings structure
    """
    try:
        with open(config_file, 'r') as f:
            yaml_settings = yaml.safe_load(f)
    except FileNotFoundError:
        log.error("No config file {} found".format(config_file))
        exit(1)

    return yaml_settings


def save_settings(config_file, settings) -> None:
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
@click.pass_context
def cli(ctx, debug, file):

    # ensure that ctx.obj exists and is a dict (in case `cli()` is called
    # by means other than the `if` block below)
    ctx.ensure_object(dict)

    if debug:
        log.setLevel(logging.DEBUG)

    config_file = file
    # Load settings file
    settings = load_settings(config_file)

    datalake_settings = settings.get('datalake')

    # Create the datalake object
    datalake = PanoramaDatalake(datalake_settings)

    ctx.obj['datalake'] = datalake
    ctx.obj['config_file'] = config_file
    ctx.obj['settings'] = settings


def _get_datasource(datalake, ds_settings):

    ds_type = ds_settings.get('type')
    if ds_type == 'mysql':
        datasource = MySQLDatasource(datalake=datalake, datasource_settings=ds_settings)

    elif ds_type == 'openedx_course_structures':
        datasource = CourseStructuresDatasource(datalake=datalake, datasource_settings=ds_settings)

    elif ds_type == 'csv':
        datasource = CSVDatasource(datalake=datalake, datasource_settings=ds_settings)

    elif ds_type == 'xls':

        datasource = XLSDatasource(datalake=datalake, datasource_settings=ds_settings)

    else:
        log.error("Datasource type {} not supported".format(ds_type))
        exit(1)
        return

    return datasource


@cli.command(help='Extracts the data from the datasources and uploads to the datalake')
@click.option("--all", "-a", "all_", is_flag=True, default=False,
              help="Extract and load all tables of all datasource")
@click.option("--datasource", "-d", required=False, default=None,
              help="Extract and load only for this datasource")
@click.option("--tables", "-t", required=False, default=None,
              help="Comma separated list of tables to extract and load")
@click.option('--force', is_flag=True, help='Force upload all partitions. False by default', default=False)
@click.pass_context
def extract_and_load(ctx, all_, datasource, tables, force):
    """
    Click command to run _create_datalake_tables
    :return:
    """
    if all_:
        if tables:
            click.echo("--all and --table cannot be used together")
        else:
            if datasource:
                _extract_and_load(ctx, selected_datasource=datasource, force=force)
            else:
                _extract_and_load(ctx, force=force)
    else:
        if datasource or tables:
            _extract_and_load(ctx, selected_datasource=datasource, selected_tables=tables, force=force)
        else:
            click.echo("Either --all or --datasource or --table must be specified")


def _extract_and_load(ctx, selected_datasource=None, selected_tables=None, force=False):
    """
    Query the datasources defined in the settings and uploads to the datalake
    :param force: boolean. Force a full dump for tables with incremental updates configured
    :return:
    """
    settings = ctx.obj.get('settings')
    datalake = PanoramaDatalake(datalake_settings=settings.get('datalake'))

    for ds_settings in settings.get('datasources'):

        if selected_datasource and ds_settings.get("name") != selected_datasource:
            continue

        datasource = _get_datasource(datalake, ds_settings)

        datasource.extract_and_load(selected_tables=selected_tables, force=force)


@cli.command(help='Creates datalake tables for all tables defined in the settings file. '
                  'Table fields must be defined.')
@click.option("--all", "-a", "all_", is_flag=True, default=False,
              help="Create all tables of all datasource")
@click.option("--datasource", "-d", required=False, default=None,
              help="Create tables only for this datasource")
@click.option("--tables", "-t", required=False, default=None,
              help="Comma separated list of tables to create")
@click.pass_context
def create_datalake_tables(ctx, all_, datasource, tables):
    """
    Click command to run _create_datalake_tables
    :return:
    """
    if all_:
        if tables:
            click.echo("--all and --table cannot be used together")
        else:
            if datasource:
                _create_datalake_tables(ctx, datasource)
            else:
                _create_datalake_tables(ctx)
    else:
        if datasource or tables:
            _create_datalake_tables(ctx, datasource, tables)
        else:
            click.echo("Either --all or --datasource or --table must be specified")


def _create_datalake_tables(ctx, datasource=None, tables=None):
    """
    Connect to Athena and create the table definition for the MySQL tables
    :return:
    """
    settings = ctx.obj.get('settings')
    datalake = PanoramaDatalake(datalake_settings=settings.get('datalake'))

    base_prefix = settings.get('datalake').get('base_prefix')

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

                datalake_table_name = table_setting.get('datalake_table_name') or "{}_raw_{}".format(
                    base_prefix, table_setting.get('name'))

                log.info("Creating or updating datalake table for {}".format(table_setting.get('name')))
                datalake.create_datalake_table(
                    table=table_setting.get('name'),
                    fields=fields,
                    field_partitions=partition_fields,
                    datalake_table=datalake_table_name
                )

            else:
                log.warning("No fields defined for table {}. Skipping table creation".format(table_setting.get('name')))

    click.echo(datalake.get_athena_executions())


@cli.command(help='Deletes datalake tables')
@click.option("--all", "-a", "all_", is_flag=True, default=False,
              help="Deletes all tables of all datasource")
@click.option("--datasource", "-d", required=False, default=None,
              help="Deletes tables only for this datasource")
@click.option("--tables", "-t", required=False, default=None,
              help="Comma separated list of tables to delete")
@click.pass_context
def drop_datalake_tables(ctx, all_, datasource, tables):
    """
    Click command to run _create_datalake_tables
    :return:
    """
    if all_:
        if tables:
            click.echo("--all and --table cannot be used together")
        else:
            if datasource:
                _drop_datalake_tables(ctx, datasource)
            else:
                _drop_datalake_tables(ctx)
    else:
        if datasource or tables:
            _drop_datalake_tables(ctx, datasource, tables)
        else:
            click.echo("Either --all or --datasource or --table must be specified")


def _drop_datalake_tables(ctx, datasource=None, tables=None):
    """
    Connect to Athena and delete the table definition for the MySQL tables
    :return:
    """
    settings = ctx.obj.get('settings')
    datalake = PanoramaDatalake(datalake_settings=settings.get('datalake'))

    # Create tables for all datasources
    base_prefix = settings.get('datalake').get('base_prefix')
    for datasource_settings in settings.get('datasources'):
        if datasource and datasource_settings.get('name') != datasource:
            continue
        for table_setting in datasource_settings.get('tables'):
            if tables and table_setting.get('name') not in tables.split(','):
                continue

            datalake_table_name = table_setting.get('datalake_table_name') or "{}_raw_{}".format(
                base_prefix, table_setting.get('name'))
            datalake_view_name = table_setting.get('datalake_table_view') or "{}_table_{}".format(
                base_prefix, table_setting.get('name'))

            log.info("Dropping {}".format(datalake_table_name))
            datalake.drop_datalake_table(datalake_table=datalake_table_name)

            log.info("Dropping {}".format(datalake_view_name))
            datalake.drop_datalake_view(view=datalake_view_name)

    click.echo(datalake.get_athena_executions())


@cli.command(help='Creates views based on the tables defined. Tables must be created first.')
@click.option("--all", "-a", "all_", is_flag=True, default=False,
              help="Creates all table views of all datasource")
@click.option("--datasource", "-d", required=False, default=None,
              help="Creates table views only for this datasource")
@click.option("--tables", "-t", required=False, default=None,
              help="Comma separated list of tables views to create")
@click.pass_context
def create_table_views(ctx, all_, datasource, tables):
    """
    Click command to run _create_datalake_tables
    :return:
    """
    if all_:
        if tables:
            click.echo("--all and --table cannot be used together")
        else:
            if datasource:
                _create_table_view(ctx, datasource)
            else:
                _create_table_view(ctx)
    else:
        if datasource or tables:
            _create_table_view(ctx, datasource, tables)
        else:
            click.echo("Either --all or --datasource or --table must be specified")


def _create_table_view(ctx, datasource=None, tables=None):
    settings = ctx.obj.get('settings')
    datalake = PanoramaDatalake(datalake_settings=settings.get('datalake'))

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

            fields = table_setting.get('fields')
            if fields:
                datalake_table_name = table_setting.get('datalake_table_name') or "{}_raw_{}".format(
                    base_prefix, table_setting.get('name'))
                datalake_view_name = table_setting.get('datalake_table_view') or "{}_table_{}".format(
                    base_prefix, table_setting.get('name'))

                log.info("Creating table view {}".format(datalake_view_name))

                datalake.create_table_view(datalake_table_name=datalake_table_name, view_name=datalake_view_name,
                                           fields=fields)
            else:
                log.warning("No fields defined for table {}".format(table_name))

    click.echo(datalake.get_athena_executions())


@cli.command(help="Queries the datasource's tables and updates the tables section of the settings file. "
                  "Use with care.")
@click.option("--all", "-a", "all_", is_flag=True, default=False,
              help="Sets all table fields of all datasource")
@click.option("--datasource", "-d", required=False, default=None,
              help="Sets table fields only for this datasource")
@click.option("--tables", "-t", required=False, default=None,
              help="Comma separated list of tables to set fields")
@click.pass_context
def set_tables(ctx, all_, datasource, tables):
    """
    Click command to run _create_datalake_tables
    :return:
    """
    if all_:
        if tables:
            click.echo("--all and --table cannot be used together")
        else:
            if datasource:
                _set_tables(ctx, datasource)
            else:
                _set_tables(ctx)
    else:
        if datasource or tables:
            _set_tables(ctx, datasource, tables)
        else:
            click.echo("Either --all or --datasource or --table must be specified")


def _set_tables(ctx, datasource: str = None, tables: str = None) -> None:
    """
    Deletes the table settings and replaces with an empty dict containing only the table names
    returned by the datasource
    :param ctx: click context
    :param tables: Comma separated list of tables to create
    :param datasource: Datasource to operate on
    :return: None
    """
    settings = ctx.obj.get('settings')
    datalake = PanoramaDatalake(datalake_settings=settings.get('datalake'))
    config_file = ctx.obj.get('config_file')

    for ds_settings in settings.get('datasources'):

        if datasource and datasource != ds_settings.get('name'):
            continue

        datasource = _get_datasource(datalake, ds_settings)
        ds_tables = datasource.get_tables()

        if tables:
            selected_tables = tables.split(',')
            table_list = [t for t in ds_tables if t in selected_tables]
        else:
            table_list = ds_tables

        ds_settings['tables'] = [{'name': t} for t in table_list]

    save_settings(config_file=config_file, settings=settings)

    click.echo("{} updated".format(config_file))


@cli.command(help="Queries the datasource's tables and updates the fields of the tables in the settings file. "
                  "Use with care.")
@click.option("--all", "-a", "all_", is_flag=True, default=False,
              help="Sets all table fields of all datasource")
@click.option("--datasource", "-d", required=False, default=None,
              help="Sets table fields only for this datasource")
@click.option("--tables", "-t", required=False, default=None,
              help="Comma separated list of tables to set fields")
@click.pass_context
def set_tables_fields(ctx, all_, datasource, tables):
    """
    Click command to run _create_datalake_tables
    :return:
    """
    if all_:
        if tables:
            click.echo("--all and --table cannot be used together")
        else:
            if datasource:
                _set_tables_fields(ctx, datasource)
            else:
                _set_tables_fields(ctx)
    else:
        if datasource or tables:
            _set_tables_fields(ctx, datasource, tables)
        else:
            click.echo("Either --all or --datasource or --table must be specified")


def _set_tables_fields(ctx, datasource=None, tables=None):
    """
    Query MySQL tables and get the field list from each table defined in the settings.
    Update the 'tables' settings with the list of fields retrieved
    * Not recommended for Open edX installations *
    :return:
    """
    settings = ctx.obj.get('settings')
    datalake = PanoramaDatalake(datalake_settings=settings.get('datalake'))
    config_file = ctx.obj.get('config_file')

    for ds_settings in settings.get('datasources'):

        if datasource and datasource != ds_settings.get('name'):
            continue

        for table_settings in ds_settings.get('tables'):

            table_name = table_settings.get('name')
            if tables and table_name not in tables.split(','):
                continue

            log.debug("Setting fields for table {} in datasource {}".format(
                table_name, ds_settings.get('name')))

            datasource = _get_datasource(datalake, ds_settings)

            table_fields = datasource.get_fields(table=table_name, force_query=True)
            table_settings['fields'] = table_fields

    save_settings(config_file=config_file, settings=settings)

    click.echo("{} updated".format(config_file))


@cli.command(help="Test all connections")
@click.pass_context
def test_connections(ctx):
    settings = ctx.obj.get('settings')
    datalake_settings = settings.get('datalake')

    results = []

    click.echo("Testing datalake...")
    datalake = PanoramaDatalake(datalake_settings=datalake_settings)
    results.append(datalake.test_connections())

    for datasource_settings in settings.get('datasources'):
        click.echo("Testing {}...".format(datasource_settings.get('name')))

        datasource = _get_datasource(datalake, datasource_settings)
        results.append(datasource.test_connections())

    for r in results:
        for k, v in r.items():
            click.echo("{}: {}".format(k, v))


def main():
    cli(obj={})
