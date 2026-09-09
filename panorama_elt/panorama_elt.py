"""
Extract tables from datasources, load to a datalake and create primary transformations.

Usage:
panorama --help

"""
import logging
import sys
import re

import yaml

import click

from panorama_elt.course_structures_datasource.course_structures_datasource import CourseStructuresDatasource
from panorama_elt.csv_datasource.csv_datasource import CSVDatasource
from panorama_elt.mysql_datasource.mysql_datasource import MySQLDatasource, identifier
from panorama_elt.xls_datasource.xls_datasource import XLSDatasource

from panorama_elt.panorama_datalake.panorama_datalake import PanoramaDatalake

from panorama_elt.panorama_logger.setup_logger import log
from panorama_elt.__about__ import __version__


def load_settings(config_file: str) -> dict:
    """
    Load config_file as settings.
    :return: settings structure
    """
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            yaml_settings = yaml.safe_load(f)
    except FileNotFoundError:
        log.error("No config file {} found".format(config_file))
        sys.exit(1)

    validate_settings(yaml_settings)
    return yaml_settings


def validate_settings(settings):
    """Reject malformed configuration before constructing clients."""
    if not isinstance(settings, dict) or not isinstance(settings.get('datalake'), dict):
        raise click.ClickException('Settings must contain a datalake mapping')
    sources = settings.get('datasources')
    if not isinstance(sources, list) or not sources:
        raise click.ClickException('Settings must contain a nonempty datasources list')
    names = set()
    for source in sources:
        if not isinstance(source, dict) or not isinstance(source.get('name'), str):
            raise click.ClickException('Every datasource requires a name')
        if source['name'] in names or source.get('type') not in {
                'mysql', 'openedx_course_structures', 'csv', 'xls'}:
            raise click.ClickException('Duplicate datasource name or unsupported type')
        names.add(source['name'])
        if not isinstance(source.get('tables', []), list):
            raise click.ClickException('Datasource tables must be a list')
        for table in source.get('tables', []):
            if not isinstance(table, dict) or not isinstance(table.get('name'), str):
                raise click.ClickException('Every table requires a name')
            if source['type'] == 'mysql':
                try:
                    identifier(table['name'])
                    fields = table.get('fields', [])
                    if not isinstance(fields, list):
                        raise ValueError('Table fields must be a list')
                    for field in fields:
                        if not isinstance(field, dict):
                            raise ValueError('Fields must be mappings')
                        identifier(field.get('name'))
                    partitions = table.get('partitions') or {}
                    if not isinstance(partitions, dict):
                        raise ValueError('Partitions must be a mapping')
                    partition_fields = partitions.get('partition_fields', [])
                    if not isinstance(partition_fields, list):
                        raise ValueError('Partition fields must be a list')
                    interval = partitions.get('interval')
                    if interval:
                        identifier(partitions.get('timestamp_field'))
                        if not re.fullmatch(r'\d+\s+(SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|YEAR)', str(interval), re.I):
                            raise ValueError('Invalid incremental interval')
                    for field in partition_fields:
                        identifier(field)
                        if fields and field not in [item['name'] for item in fields]:
                            raise ValueError('Partition field must be included in fields')
                except (ValueError, TypeError) as exc:
                    raise click.ClickException(str(exc)) from exc


def save_settings(config_file, settings) -> None:
    """
    Save config_file from settings
    :return: settings structure
    """
    with open(config_file, 'w', encoding='utf-8') as f:
        yaml.safe_dump(settings, f, sort_keys=False)


@click.group()
@click.version_option(version=__version__)
@click.option("--debug", is_flag=True, default=False, help="Enable debugging")
@click.option("--settings", 'file', help="Configuration file", default="panorama_settings.yaml")
@click.pass_context
def cli(ctx, debug, file):
    """Panorama ELT command line entry point. Loads settings and the datalake into the context."""
    # ensure that ctx.obj exists and is a dict (in case `cli()` is called
    # by means other than the `if` block below)
    ctx.ensure_object(dict)

    if debug:
        log.setLevel(logging.DEBUG)

    config_file = file
    # Load settings file
    settings = load_settings(config_file)

    ctx.obj['config_file'] = config_file
    ctx.obj['settings'] = settings


def _get_datasource(datalake, ds_settings):
    """Instantiate the datasource object matching the configured datasource type."""
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
        sys.exit(1)

    context = click.get_current_context(silent=True)
    if context is not None and hasattr(datasource, 'close'):
        context.call_on_close(datasource.close)
    return datasource


def _selection_options(func):
    """Attach the shared --all/--datasource/--tables selectors to a command.

    Applied bottom-up so the rendered help order is --all, --datasource, --tables.
    """
    func = click.option("--tables", "-t", default=None,
                        help="Comma separated list of tables to operate on")(func)
    func = click.option("--datasource", "-d", default=None,
                        help="Operate only on this datasource")(func)
    func = click.option("--all", "-a", "all_", is_flag=True, default=False,
                        help="Operate on all tables of all datasources")(func)
    return func


def _dispatch(ctx, worker, all_, datasource, tables, **extra):
    """Validate the shared selector flags, then run the worker.

    Reproduces the guards every command used to repeat:
      * --all together with --tables is rejected
      * at least one of --all/--datasource/--tables must be given
    """
    if all_ and tables:
        raise click.UsageError("--all and --table cannot be used together")
    if all_ and datasource:
        raise click.UsageError("--all and --datasource cannot be used together")
    if not all_ and not (datasource or tables):
        raise click.UsageError("Either --all or --datasource or --table must be specified")
    sources = list(_iter_datasources(ctx.obj['settings'], datasource))
    if not sources:
        raise click.UsageError(f'Unknown datasource: {datasource}')
    if tables and worker is not _set_tables:
        known = {table['name'] for source in sources for table in source.get('tables', [])}
        missing = set(tables.split(',')) - known
        if missing:
            raise click.UsageError(f'Unknown tables: {sorted(missing)}')
    try:
        if 'datalake' not in ctx.obj:
            ctx.obj['datalake'] = PanoramaDatalake(ctx.obj['settings']['datalake'])
        worker(ctx, datasource=datasource, tables=tables, **extra)
    except Exception as exc:
        raise click.ClickException(str(exc)) from exc


def _iter_datasources(settings, datasource=None):
    """Yield datasource settings, optionally filtered by datasource name."""
    for ds_settings in settings.get('datasources'):
        if datasource and ds_settings.get('name') != datasource:
            continue
        yield ds_settings


def _iter_tables(settings, datasource=None, tables=None):
    """Yield (datasource_settings, table_setting) pairs matching the filters."""
    selected = tables.split(',') if tables else None
    for ds_settings in _iter_datasources(settings, datasource):
        for table_setting in ds_settings.get('tables'):
            if selected and table_setting.get('name') not in selected:
                continue
            yield ds_settings, table_setting


def _datalake_names(table_setting, base_prefix):
    """Return the (table, view) datalake names for a table setting, with defaults."""
    name = table_setting.get('datalake_s3_table') or table_setting.get('name')
    table_name = table_setting.get('datalake_table_name') or "{}_raw_{}".format(base_prefix, name)
    view_name = table_setting.get('datalake_table_view') or "{}_table_{}".format(base_prefix, name)
    return table_name, view_name


def _datalake_s3_table(table_setting):
    """Return the S3 table directory/name for a table setting."""
    return table_setting.get('datalake_s3_table') or table_setting.get('name')


@cli.command(help='Extracts the data from the datasources and uploads to the datalake')
@_selection_options
@click.option('--force', is_flag=True, default=False, help='Force upload all partitions. False by default')
@click.pass_context
def extract_and_load(ctx, all_, datasource, tables, force):
    """Click command to run _extract_and_load."""
    _dispatch(ctx, _extract_and_load, all_, datasource, tables, force=force)


def _extract_and_load(ctx, datasource=None, tables=None, force=False):
    """
    Query the datasources defined in the settings and uploads to the datalake
    :param force: boolean. Force a full dump for tables with incremental updates configured
    :return:
    """
    datalake = ctx.obj['datalake']
    failures = []
    for ds_settings in _iter_datasources(ctx.obj['settings'], datasource):
        datasource_obj = None
        try:
            datasource_obj = _get_datasource(datalake, ds_settings)
            datasource_obj.extract_and_load(selected_tables=tables, force=force)
        except Exception as exc:
            failures.append(f"{ds_settings['name']}: {exc}")
        finally:
            if datasource_obj is not None and hasattr(datasource_obj, 'close'):
                datasource_obj.close()
    try:
        datalake.get_athena_executions()
    except Exception as exc:
        failures.append(str(exc))
    if failures:
        raise RuntimeError('Incomplete extraction: ' + '; '.join(failures))


@cli.command(help='Creates datalake tables for all tables defined in the settings file. '
                  'Table fields must be defined.')
@_selection_options
@click.pass_context
def create_datalake_tables(ctx, all_, datasource, tables):
    """Click command to run _create_datalake_tables."""
    _dispatch(ctx, _create_datalake_tables, all_, datasource, tables)


def _create_datalake_tables(ctx, datasource=None, tables=None):
    """
    Connect to Athena and create the table definition for the MySQL tables
    :return:
    """
    settings = ctx.obj['settings']
    datalake = ctx.obj['datalake']
    base_prefix = settings.get('datalake').get('base_prefix')

    for _ds_settings, table_setting in _iter_tables(settings, datasource, tables):
        partitions = table_setting.get('partitions')
        partition_fields = partitions.get('partition_fields') if partitions else None

        fields_and_types = table_setting.get('fields')
        if fields_and_types:
            fields = [f.get("name") for f in fields_and_types]
            datalake_table_name, _view = _datalake_names(table_setting, base_prefix)

            log.info("Creating or updating datalake table for {}".format(table_setting.get('name')))
            datalake.create_datalake_table(
                table=_datalake_s3_table(table_setting),
                fields=fields,
                field_partitions=partition_fields,
                datalake_table=datalake_table_name
            )
        else:
            log.warning("No fields defined for table {}. Skipping table creation".format(table_setting.get('name')))

    click.echo(datalake.get_athena_executions())


@cli.command(help='Deletes datalake tables')
@_selection_options
@click.pass_context
def drop_datalake_tables(ctx, all_, datasource, tables):
    """Click command to run _drop_datalake_tables."""
    _dispatch(ctx, _drop_datalake_tables, all_, datasource, tables)


def _drop_datalake_tables(ctx, datasource=None, tables=None):
    """
    Connect to Athena and delete the table definition for the MySQL tables
    :return:
    """
    settings = ctx.obj['settings']
    datalake = ctx.obj['datalake']
    base_prefix = settings.get('datalake').get('base_prefix')

    for _ds_settings, table_setting in _iter_tables(settings, datasource, tables):
        datalake_table_name, datalake_view_name = _datalake_names(table_setting, base_prefix)

        log.info("Dropping {}".format(datalake_table_name))
        datalake.drop_datalake_table(datalake_table=datalake_table_name)

        log.info("Dropping {}".format(datalake_view_name))
        datalake.drop_datalake_view(view=datalake_view_name)

    click.echo(datalake.get_athena_executions())


@cli.command(help='Creates views based on the tables defined. Tables must be created first.')
@_selection_options
@click.pass_context
def create_table_views(ctx, all_, datasource, tables):
    """Click command to run _create_table_view."""
    _dispatch(ctx, _create_table_view, all_, datasource, tables)


def _create_table_view(ctx, datasource=None, tables=None):
    """Create an Athena view for every table matching the filters."""
    settings = ctx.obj['settings']
    datalake = ctx.obj['datalake']
    base_prefix = settings.get('datalake').get('base_prefix')

    for ds_settings, table_setting in _iter_tables(settings, datasource, tables):
        table_name = table_setting.get('name')
        log.debug("Creating table view for table {} in datasource {}".format(
            table_name, ds_settings.get('name')))

        fields = table_setting.get('fields')
        if fields:
            datalake_table_name, datalake_view_name = _datalake_names(table_setting, base_prefix)
            log.info("Creating table view {}".format(datalake_view_name))
            datalake.create_table_view(datalake_table_name=datalake_table_name, view_name=datalake_view_name,
                                       fields=fields)
        else:
            log.warning("No fields defined for table {}".format(table_name))

    click.echo(datalake.get_athena_executions())


@cli.command(help="Queries the datasource's tables and updates the tables section of the settings file. "
                  "Use with care.")
@_selection_options
@click.pass_context
def set_tables(ctx, all_, datasource, tables):
    """Click command to run _set_tables."""
    _dispatch(ctx, _set_tables, all_, datasource, tables)


def _set_tables(ctx, datasource=None, tables=None):
    """
    Deletes the table settings and replaces with an empty dict containing only the table names
    returned by the datasource
    :param ctx: click context
    :param tables: Comma separated list of tables to create
    :param datasource: Datasource to operate on
    :return: None
    """
    settings = ctx.obj['settings']
    datalake = ctx.obj['datalake']
    config_file = ctx.obj['config_file']

    selected = tables.split(',') if tables else None
    found = set()
    for ds_settings in _iter_datasources(settings, datasource):
        datasource_obj = _get_datasource(datalake, ds_settings)
        ds_tables = datasource_obj.get_tables()
        found.update(ds_tables)

        if selected:
            table_list = [t for t in ds_tables if t in selected]
        else:
            table_list = ds_tables

        ds_settings['tables'] = [{'name': t} for t in table_list]

    if selected and set(selected) - found:
        raise ValueError(f'Unknown source tables: {sorted(set(selected) - found)}')
    save_settings(config_file=config_file, settings=settings)

    click.echo("{} updated".format(config_file))


@cli.command(help="Queries the datasource's tables and updates the fields of the tables in the settings file. "
                  "Use with care.")
@_selection_options
@click.pass_context
def set_tables_fields(ctx, all_, datasource, tables):
    """Click command to run _set_tables_fields."""
    _dispatch(ctx, _set_tables_fields, all_, datasource, tables)


def _set_tables_fields(ctx, datasource=None, tables=None):
    """
    Query MySQL tables and get the field list from each table defined in the settings.
    Update the 'tables' settings with the list of fields retrieved
    * Not recommended for Open edX installations *
    :return:
    """
    settings = ctx.obj['settings']
    datalake = ctx.obj['datalake']
    config_file = ctx.obj['config_file']

    selected = tables.split(',') if tables else None
    for ds_settings in _iter_datasources(settings, datasource):
        datasource_obj = _get_datasource(datalake, ds_settings)

        for table_settings in ds_settings.get('tables'):
            table_name = table_settings.get('name')
            if selected and table_name not in selected:
                continue

            log.debug("Setting fields for table {} in datasource {}".format(
                table_name, ds_settings.get('name')))

            table_settings['fields'] = datasource_obj.get_fields(table=table_name, force_query=True)

    save_settings(config_file=config_file, settings=settings)

    click.echo("{} updated".format(config_file))


@cli.command(help="Test all connections")
@click.pass_context
def test_connections(ctx):
    """Test the datalake connection and every configured datasource connection."""
    datalake = PanoramaDatalake(ctx.obj['settings']['datalake'])

    results = []

    click.echo("Testing datalake...")
    results.append(datalake.test_connections())

    for datasource_settings in ctx.obj['settings'].get('datasources'):
        click.echo("Testing {}...".format(datasource_settings.get('name')))

        datasource_obj = _get_datasource(datalake, datasource_settings)
        results.append(datasource_obj.test_connections())

    for r in results:
        for k, v in r.items():
            click.echo("{}: {}".format(k, v))


def main():
    """Console-script entry point."""
    cli(obj={})
