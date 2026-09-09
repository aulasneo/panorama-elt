"""
Panorama Extract MySQL tables
Extracts a list of tables from a mysql database as csv files and uploads them to a s3 bucket.
Data can be partitioned by a list of base partitions and a set of fields.

"""
import datetime
import os
import csv
import re
import tempfile

import pymysql

from panorama_elt.panorama_datalake.panorama_datalake import PanoramaDatalake
from panorama_elt.panorama_logger.setup_logger import log


def save_rows(filename: str, fields: list, rows: iter) -> None:
    """
    Saves the result of a query as a csv file

    :param filename: filename to save (usually <table name>.csv)
    :param fields: list of field names
    :param rows: result of a query execution
    :return: None
    """

    log.debug("Saving {}".format(filename))

    # Preserve the existing Athena consumer wire format: backslash escaping,
    # literal CR/LF sequences and six-digit timestamp fractions. The historical
    # pre-3.10 workaround also doubles existing backslashes before CSV escaping;
    # removing it would change exported values and requires a consumer migration.
    def converted_rows():
        for row in rows:
            fields_list = []
            for field in row:
                if isinstance(field, str):
                    field = field.replace('\\', '\\\\').replace('\r', '\\r').replace('\n', '\\n')
                elif isinstance(field, datetime.datetime):
                    field = field.strftime('%Y-%m-%d %H:%M:%S.') + '%06d' % field.microsecond
                fields_list.append(field)
            yield fields_list

    try:
        with open(filename, 'w', encoding='utf-8') as f:
            write = csv.writer(f, doublequote=False, escapechar='\\')
            write.writerow(fields)
            write.writerows(converted_rows())
    finally:
        if hasattr(rows, 'close'):
            rows.close()


def identifier(value):
    """Quote a configured SQL identifier, rejecting expressions."""
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', value):
        raise ValueError(f'Invalid SQL identifier: {value!r}')
    return f'`{value}`'


class MySQLDatasource:
    """Extracts MySQL tables to CSV and uploads them to the datalake."""

    def __init__(
            self,
            datalake: PanoramaDatalake,
            datasource_settings: dict
    ):

        mysql_username = datasource_settings.get('mysql_username', 'root')
        mysql_password = datasource_settings.get('mysql_password')
        mysql_port = datasource_settings.get('mysql_port', 3306)
        mysql_host = datasource_settings.get('mysql_host', '127.0.0.1')
        mysql_database = datasource_settings.get('mysql_database', 'edxapp')

        try:
            conn = pymysql.connect(
                host=mysql_host,
                port=mysql_port,
                user=mysql_username,
                passwd=mysql_password,
                db=mysql_database
            )
            self.conn = conn
            self.cur = conn.cursor()

        except pymysql.err.OperationalError as e:
            raise RuntimeError('Unable to connect to MySQL') from e

        # This dicts defines which tables have partitions and static fields configurations (if present)
        # The interval is in MYSQL format
        self.field_partitions = {}
        self.table_fields = {}
        self.table_fields_settings = {}
        self.table_datalake_names = {}
        self.table_s3_tables = {}
        self.table_settings = datasource_settings.get('tables')

        if self.table_settings:
            for table_setting in self.table_settings:
                table_name = table_setting.get('name')
                partitions = table_setting.get('partitions')
                if partitions:
                    self.field_partitions[table_name] = {
                        'partition_fields': partitions.get('partition_fields'),
                        'interval': partitions.get('interval'),
                        'timestamp_field': partitions.get('timestamp_field'),
                    }
                fields = table_setting.get('fields')
                if fields:
                    self.table_fields[table_name] = [f.get("name") for f in fields]
                    self.table_fields_settings[table_name] = list(fields)
                if table_setting.get('datalake_table_name'):
                    self.table_datalake_names[table_name] = table_setting.get('datalake_table_name')
                if table_setting.get('datalake_s3_table'):
                    self.table_s3_tables[table_name] = table_setting.get('datalake_s3_table')

        self.datalake = datalake
        self.db = mysql_database
        self.closed = False

    def close(self):
        """Release database resources after the command."""
        if not self.closed:
            self.closed = True
            try:
                self.cur.close()
            finally:
                self.conn.close()

    def _upload_table_from_file(self, filename, table, field_partitions=None):
        """Upload a table file using optional datalake/S3 table overrides from settings."""
        upload_kwargs = {
            'filename': filename,
            'table': table,
            'update_partitions': True,
            's3_filename': f'{table}.csv',
        }
        if field_partitions:
            upload_kwargs['field_partitions'] = field_partitions

        s3_table = self.table_s3_tables.get(table)
        if s3_table:
            upload_kwargs['s3_table'] = s3_table
            upload_kwargs['s3_filename'] = "{}.csv".format(s3_table)

        datalake_table_name = self.table_datalake_names.get(table)
        if datalake_table_name:
            upload_kwargs['datalake_table_name'] = datalake_table_name

        self.datalake.upload_table_from_file(**upload_kwargs)

    def test_connections(self) -> dict:
        """
        Performs connections test
        :return: dict with test results
        """
        query = "SHOW DATABASES"
        self.cur.execute(query)
        r = self.cur.fetchall()

        if self.db in [x[0] for x in r]:
            results = {'MySQL': 'OK'}
        else:
            results = {'MySQL': 'DB not found'}
        return results

    def get_tables(self) -> list:
        """
        Returns the list of tables available in the database
        :return: list of sheet names
        """
        query = "SHOW TABLES"
        self.cur.execute(query)
        r = self.cur.fetchall()

        return [t[0] for t in r]

    def get_fields(self, table: str, force_query: bool = False) -> list:
        """
        Returns a list of fields of the table in the database using an existing mysql cursor

        :param table: table name
        :param force_query: (optional) if set to True, will query the db even if there is a definition set
        :return: list[str] of fields
        """

        # If the field list is declared in the settings file, return it.
        if self.table_fields and self.table_fields.get(table) and not force_query:
            return self.table_fields.get(table)

        fields_query = """
            select COLUMN_NAME, DATA_TYPE
            from INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = %s
            AND TABLE_SCHEMA = %s
            """

        log.debug("Querying mysql fields: {}".format(fields_query))

        self.cur.execute(fields_query, (table, self.db))
        fields = self.cur.fetchall()

        log.debug("Fields in table: {}".format(fields))

        fields_list = []
        for field in fields:
            fields_list.append({"name": field[0], "type": field[1]})

        return fields_list

    def get_rows(self, table: str, field_list: list = None,
                 where: str = None, distinct: bool = False, parameters=()) -> iter:
        """
        Returns the rows of the mysql table.

        :param distinct: if true, only distinct values will be returned
        :param table: name of the table
        :param field_list: (optional) list of fields to query. If omitted, all fields will be retrieved
        :param where: string to apply to the WHERE condition of the query, in mysql format
        :return:
        """

        constants = []
        if not field_list:
            log.warning("No field list provided for table '{}'. Using '*' to query all mysql fields.".format(table))
            fields_statement = '*'
        else:

            fields = self.table_fields_settings.get(table, [{'name': name} for name in field_list])
            field_statement_list = []
            for f in fields:
                # Omit fields not in the field list
                if f.get('name') in field_list:
                    # If a value key is set in the field configuration, set as a constant value for the query
                    if 'value' in f:
                        if f.get('value') is None:
                            field = "NULL as {}".format(identifier(f.get('name')))
                        else:
                            field = "%s as {}".format(identifier(f.get('name')))
                            constants.append(f.get('value'))
                    else:
                        field = identifier(f.get('name'))

                    field_statement_list.append(field)

            fields_statement = ','.join(field_statement_list)

        where_clause = 'where {}'.format(where) if where else ''

        query = 'select {prefix} {fields} from {table} {where_clause}'.format(
            prefix='distinct' if distinct else '',
            fields=fields_statement,
            table=identifier(table),
            where_clause=where_clause)

        log.debug("Querying mysql rows: {}".format(query))

        def rows():
            with self.conn.cursor(pymysql.cursors.SSCursor) as cursor:
                cursor.execute(query, tuple(constants) + tuple(parameters))
                while batch := cursor.fetchmany(1000):
                    yield from batch
        return rows()

    def _extract_and_load_table(self, table: str, force: bool = False):
        """Extract and upload one configured table."""
        with tempfile.TemporaryDirectory(prefix='panorama-') as directory:
            self._export_table(table, force, os.path.join(directory, 'export.csv'))

    def _export_table(self, table, force, filename):
        """Export in an isolated directory that is cleaned on every failure path."""
        fields = [field['name'] if isinstance(field, dict) else field for field in self.get_fields(table=table)]

        partitions = self.field_partitions.get(table)
        if partitions:

            # Process tables with partitions
            partition_fields = partitions.get('partition_fields')
            timestamp_field = partitions.get('timestamp_field')

            for partition_field in partition_fields:
                fields.remove(partition_field)

            # If there is an interval configured for the table, we do an incremental update.
            # Incremental updates work with partitions. We first query which partitions have records with changes in
            # the last interval configured. Then the full partition is updated.
            update_interval = partitions.get('interval')
            interval = None
            if update_interval:

                if force:
                    interval = None
                    log.info("Forcing a full dump")
                else:
                    # interval will be used in a where clause to query all the partitions with changes
                    if not re.fullmatch(r'\d+\s+(SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|YEAR)', str(update_interval), re.I):
                        raise ValueError('Invalid incremental interval')
                    interval = "{} >= date_sub(now(), interval {})".format(identifier(timestamp_field), update_interval)
                    log.debug("Doing incremental update of the last {}".format(update_interval))

            # Get a list of all distinct partition field values in the recordset within the last increment period
            values_list = list(self.get_rows(table=table, field_list=partition_fields, distinct=True, where=interval))
            log.info("{} partitions found to update".format(len(values_list)))

            # Now we need to make one query for each set of values representing partitions, with changes in the
            # last period.
            counter = 1
            for values in values_list:

                # Create a filter to match all partition fields with the values with changes in the interval
                where_clauses = []
                parameters = []
                for partition_field, value in zip(partition_fields, values):
                    where_clauses.append("{} <=> %s".format(identifier(partition_field)))
                    parameters.append(value)
                where_clause = " and ".join(where_clauses)

                log.info("Getting partition {}/{}".format(counter, len(values_list)))
                counter += 1

                # Query mysql table
                rows = self.get_rows(table=table, field_list=fields, where=where_clause, parameters=parameters)

                save_rows(filename=filename, fields=fields, rows=rows)

                field_partitions = {}
                for k, v in zip(partition_fields, values):
                    field_partitions[k] = v

                self._upload_table_from_file(filename=filename, table=table, field_partitions=field_partitions)

                os.remove(filename)

        else:

            # Process tables without field partitions
            rows = self.get_rows(table=table, field_list=fields)
            save_rows(filename=filename, fields=fields, rows=rows)

            self._upload_table_from_file(filename=filename, table=table)

            os.remove(filename)

    def extract_and_load(self, selected_tables: str = None, force: bool = False):
        """
        Extracts mysql tables and sends them to the datalake

        :param selected_tables: (optional) list of tables to extract and load
        :param force: Forces a full update of all the partitions
        :return:
        """
        failures = []
        for table in [item['name'] for item in self.table_settings or []]:

            if selected_tables and table not in selected_tables.split(','):
                continue

            log.info("Extracting {}".format(table))

            try:
                self._extract_and_load_table(table=table, force=force)
            except Exception as e:  # Finish independent tables, then report an incomplete run.
                failures.append(f'{table}: {e}')
                log.error("Skipping table '{}' after MySQL error: {}".format(table, e))
        if failures:
            raise RuntimeError('Incomplete extraction: ' + '; '.join(failures))
