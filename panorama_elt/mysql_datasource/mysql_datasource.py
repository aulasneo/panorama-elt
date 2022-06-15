"""
Panorama Extract MySQL tables
Extracts a list of tables from a mysql database as csv files and uploads them to a s3 bucket.
Data can be partitioned by a list of base partitions and a set of fields.

"""
import datetime
import os
import csv

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

    # As some fields may include double quotes, we need to set the csv writer to
    # use backslash as escape char and not double, double quotes.
    # Unfortunately, there are cases where there is field content which already
    # have escaped chars. There is a bug in csv writer by which it will not
    # escape preexistent escape chars (see https://bugs.python.org/issue12178)
    # This should be fixed by python 3.10. To keep compatibility with previous
    # versions, we unpack all the data and escape backslashes in all strings.
    # In python 3.10, the next block can be removed and use
    # write.writerows(rows) directly
    rows_list = []
    for row in rows:
        fields_list = []
        for field in row:
            if type(field) is str:
                field = field.replace('\\', '\\\\')
            elif isinstance(field, datetime.datetime):
                # When the seconds are zero, the microseconds are not displayed
                field = field.strftime('%Y-%m-%d %H:%M:%S.') + '%06d' % field.microsecond
            fields_list.append(field)
        rows_list.append(fields_list)

    with open(filename, 'w') as f:
        write = csv.writer(f, doublequote=False, escapechar='\\')
        write.writerow(fields)
        write.writerows(rows_list)


class MySQLDatasource:

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
            self.cur = conn.cursor()

        except pymysql.err.OperationalError as e:
            log.error(e)
            exit(1)

        # This dicts defines which tables have partitions and static fields configurations (if present)
        # The interval is in MYSQL format
        self.field_partitions = {}
        self.table_fields = {}
        table_settings = datasource_settings.get('tables')

        if table_settings:
            for table_setting in table_settings:
                partitions = table_setting.get('partitions')
                if partitions:
                    self.field_partitions[table_setting.get('name')] = {
                        'partition_fields': partitions.get('partition_fields'),
                        'interval': partitions.get('interval'),
                        'timestamp_field': partitions.get('timestamp_field'),
                    }
                fields = table_setting.get('fields')
                if fields:
                    self.table_fields[table_setting.get('name')] = [f.get("name") for f in fields]

        self.datalake = datalake
        self.db = mysql_database

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
            WHERE TABLE_NAME = "{table}"
            AND TABLE_SCHEMA = "{db}"
            """.format(
            table=table,
            db=self.db)

        log.debug("Querying mysql fields: {}".format(fields_query))

        self.cur.execute(fields_query)
        fields = self.cur.fetchall()

        log.debug("Fields in table: {}".format(fields))

        fields_list = []
        for field in fields:
            fields_list.append({"name": field[0], "type": field[1]})

        return fields_list

    def get_rows(self, table: str, field_list: list = None,
                 where: str = None, distinct: bool = False) -> iter:
        """
        Returns the rows of the mysql table.

        :param distinct: if true, only distinct values will be returned
        :param table: name of the table
        :param field_list: (optional) list of fields to query. If omitted, all fields will be retrieved
        :param where: string to apply to the WHERE condition of the query, in mysql format
        :return:
        """

        if not field_list:
            log.warning("No field list provided for table '{}'. Using '*' to query all mysql fields.".format(table))
        else:
            # Quote all field names
            field_list = ['`{}`'.format(f) for f in field_list]

        fields = '*' if not field_list else ','.join(field_list)
        where_clause = 'where {}'.format(where) if where else ''

        query = 'select {prefix} {fields} from {table} {where_clause}'.format(
            prefix='distinct' if distinct else '',
            fields=fields,
            table=table,
            where_clause=where_clause)

        log.debug("Querying mysql rows: {}".format(query))

        self.cur.execute(query)

        rows = self.cur.fetchall()

        return rows

    def extract_and_load(self, selected_tables: str = None, force: bool = False):
        """
        Extracts mysql tables and sends them to the datalake

        :param selected_tables: (optional) list of tables to extract and load
        :param force: Forces a full update of all the partitions
        :return:
        """
        for table in self.table_fields.keys():

            if selected_tables and table not in selected_tables.split(','):
                continue

            log.info("Extracting {}".format(table))

            fields = self.get_fields(table=table)

            filename = "{}.csv".format(table)

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
                        interval = "{} >= date_sub(now(), interval {})".format(timestamp_field, update_interval)
                        log.debug("Doing incremental update of the last {}".format(update_interval))

                # Get a list of all distinct partition field values in the recordset within the last increment period
                values_list = self.get_rows(table=table, field_list=partition_fields, distinct=True, where=interval)
                log.info("{} partitions found to update".format(len(values_list)))

                # Now we need to make one query for each set of values representing partitions, with changes in the
                # last period.
                counter = 1
                for values in values_list:

                    # Create a filter to match all partition fields with the values with changes in the interval
                    where_clauses = []
                    for partition_field, value in zip(partition_fields, values):
                        where_clauses.append("{} = '{}'".format(partition_field, value))
                    where_clause = " and ".join(where_clauses)

                    log.info("Getting partition {}/{}".format(counter, len(values_list)))
                    counter += 1

                    # Query mysql table
                    rows = self.get_rows(table=table, field_list=fields, where=where_clause)

                    save_rows(filename=filename, fields=fields, rows=rows)

                    field_partitions = {}
                    for k, v in zip(partition_fields, values):
                        field_partitions[k] = v

                    self.datalake.upload_table_from_file(filename=filename, table=table,
                                                         field_partitions=field_partitions,
                                                         update_partitions=True)

                    os.remove(filename)

            else:

                # Process tables without field partitions
                rows = self.get_rows(table=table, field_list=fields)
                save_rows(filename=filename, fields=fields, rows=rows)

                self.datalake.upload_table_from_file(filename=filename, table=table, update_partitions=True)

                os.remove(filename)
