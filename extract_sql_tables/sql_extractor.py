"""
Panorama Extract MySQL tables
Extracts a list of tables from a mysql database as csv files and uploads them to a s3 bucket.
Data can be partitioned by a list of base partitions and a set of fields.

"""
import os
from uuid import uuid4
import urllib.parse
import csv

import boto3
from botocore.exceptions import ClientError
import pymysql

from panorama_logger.setup_logger import log


class SqlExtractor:

    def __init__(
            self,
            panorama_raw_data_bucket: str,
            mysql_database: str,
            panorama_mysql_tables: str,
            aws_region: str = 'us-east-1',
            aws_access_key: str = None,
            aws_secret_access_key: str = None,
            mysql_username: str = None,
            mysql_password: str = None,
            mysql_host: str = 'localhost',
            table_partitions: str = None,
            base_partitions: str = None,
    ):

        session = boto3.Session(
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region
        )
        self.s3_client = session.client('s3')
        self.athena = session.client('athena')

        conn = pymysql.connect(
            host=mysql_host, port=3306,
            user=mysql_username,
            passwd=mysql_password,
            db=mysql_database
        )

        self.cur = conn.cursor()
        self.tables = panorama_mysql_tables.split(',')

        self.table_partitions = table_partitions
        self.base_partitions = base_partitions

        self.db = mysql_database
        self.panorama_raw_data_bucket = panorama_raw_data_bucket

        # This list is to store athena query executions
        self.executions = []

    def get_fields(self, table: str) -> list:
        """
        Returns a list of fields of the table in the database using an existing mysql cursor

        :param table: table name
        :return: list[str] of fields
        """

        fields_query = \
            'select COLUMN_NAME ' \
            'from INFORMATION_SCHEMA.COLUMNS ' \
            'WHERE TABLE_NAME = "{table}" ' \
            'AND TABLE_SCHEMA = "{db}"'.format(
                table=table,
                db=self.db)

        self.cur.execute(fields_query)
        fields = self.cur.fetchall()

        log.debug("Fields in table: {}".format(fields))

        return list(f[0] for f in fields)

    def get_rows(self, table: str, field_list: list = None,
                 where: str = None, distinct: bool = False) -> pymysql.cursors.Cursor:
        """
        Returns the rows of the mysql table.

        :param distinct: if true, only distinct values will be returned
        :param table: name of the table
        :param field_list: (optional) list of fields to query. If omitted, all fields will be retrieved
        :param where: string to apply to the WHERE condition of the query, in mysql format
        :return:
        """

        fields = '*' if not field_list else ','.join(field_list)
        where_clause = 'where {}'.format(where) if where else ''

        query = 'select {prefix} {fields} from {table} {where_clause}'.format(
            prefix='distinct' if distinct else '',
            fields=fields,
            table=table,
            where_clause=where_clause)

        self.cur.execute(query)

        rows = self.cur.fetchall()

        return rows

    def upload_rows(self, bucket: str, prefix: str, table: str,
                    fields: list, rows: pymysql.cursors.Cursor) -> None:
        """
        Saves the result of a query as a csv file and upload to s3.

        :param bucket: s3 bucket name
        :param prefix: path in the bucket to the file
        :param fields: list of field names
        :param table: name of the table. The file will have this name and csv extension
        :param rows: result of a query execution
        :return: None
        """

        filename = "{}.csv".format(table)
        key = os.path.join(prefix, filename)
        log.debug("Saving and uploading {} to {}/{}".format(filename, bucket, key))

        # As some fields may include doublequotes, we need to set the csv writer to
        # use backslash as escapechar and not double doublequotes.
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
                    fields_list.append(field.replace('\\', '\\\\'))
                else:
                    fields_list.append(field)
            rows_list.append(fields_list)

        with open(filename, 'w') as f:
            write = csv.writer(f, doublequote=False, escapechar='\\')
            write.writerow(fields)
            write.writerows(rows_list)

        try:
            self.s3_client.upload_file(filename, bucket, key)

        except ClientError as e:
            log.error(e)

    def query_athena(self, query: str, db: str, workgroup: str) -> None:
        """
        Sends a query to Athena and waits for a response
        :param db: database name
        :param workgroup: Athena workgroup
        :param query: SQL query
        :return: None
        """

        crt = str(uuid4())
        try:
            execution = self.athena.start_query_execution(
                QueryString=query,
                ClientRequestToken=crt,
                QueryExecutionContext={
                    'Database': db
                },
                WorkGroup=workgroup
            )
            self.executions.append(execution)

        except ClientError as e:
            log.error("boto3 error trying to execute athena query")
            log.error(e)
            return

    def get_athena_query_execution(self, execution):

        execution_id = execution.get('QueryExecutionId')
        response = self.athena.get_query_execution(QueryExecutionId=execution_id)
        state = response.get('QueryExecution').get('Status').get('State')

        return state

    def get_athena_executions(self):
        """
        Show the results of all athena executions
        :return: None
        """
        results = {}
        for execution in self.executions:
            result = self.get_athena_query_execution(execution)
            if result not in results:
                results[result] = 1
            else:
                results[result] += 1

        log.info("Summary of athena executions to update partitions statuses: {}".format(results))

    def extract_mysql_tables(self):

        for table in self.tables:
            log.info("Extracting {}".format(table))

            fields = self.get_fields(table=table)

            # Base prefix of the file in the S3 buckets. The first folder is the table name.
            # Next, the list of base partitions definitions for all tables in Hive format
            # The complete prefix will be the base prefix plus any specific partitions defined for the table
            base_prefix_list = [table]
            for key, value in self.base_partitions.items():
                base_prefix_list.append("{}={}".format(key, urllib.parse.quote(value)))
            base_prefix = "/".join(base_prefix_list)
            log.debug("Base prefix: {}".format(base_prefix))

            if table in self.table_partitions:

                # Process tables with partitions
                partition_fields = self.table_partitions.get(table).get('partition_fields')
                timestamp_field = self.table_partitions.get(table).get('timestamp_field')

                for partition_field in partition_fields:
                    fields.remove(partition_field)

                # If there is an interval configured for the table, we do an incremental update.
                # Incremental updates work with partitions. We first query which partitions have records with changes in
                # the last interval configured. Then the full partition is updated.
                update_interval = self.table_partitions.get(table).get('interval')
                interval = None
                if update_interval:

                    # If the folder with the base partition is empty, we do a full upload
                    try:
                        table_bucket = self.s3_client.list_objects_v2(
                            Bucket=self.panorama_raw_data_bucket,
                            Prefix=base_prefix)

                        if table_bucket.get('Contents'):
                            # interval will be used in a where clause to query all the partitions with changes
                            interval = "{} >= date_sub(now(), interval {})".format(timestamp_field, update_interval)
                            log.debug("Doing incremental update of the last {}".format(update_interval))
                        else:
                            interval = None
                            log.info("Update interval configured in {} but s3 base folder {} is empty. "
                                     "Doing a full dump".format(update_interval, base_prefix))

                    except self.s3_client.exceptions.NoSuchBucket as e:
                        log.critical("NoSuchBucket exception getting bucket: {}".format(e))
                        exit(1)

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

                    # Build the path to the csv file, including the partitions in Hive format
                    prefix_list = [base_prefix]
                    for partition_field, value in zip(partition_fields, values):
                        prefix_list.append("{}={}".format(partition_field, urllib.parse.quote(value)))
                    prefix = '/'.join(prefix_list)

                    log.info("Getting partition {}/{}: {}".format(counter, len(values_list), prefix))
                    counter += 1

                    # Query mysql table
                    rows = self.get_rows(table=table, field_list=fields, where=where_clause)

                    # Save and upload to s3
                    self.upload_rows(bucket=self.panorama_raw_data_bucket,
                                     prefix=prefix,
                                     table=table,
                                     fields=fields,
                                     rows=rows)

                    # Update partitions in the datalake
                    partitions = []
                    partitions_uri = []
                    for partition_field, value in self.base_partitions.items():
                        partitions.append("{} = '{}'".format(partition_field, value))
                        partitions_uri.append("{}={}".format(partition_field, urllib.parse.quote(value)))

                    for partition_field, value in zip(partition_fields, values):
                        partitions.append("{} = '{}'".format(partition_field, value))
                        partitions_uri.append("{}={}".format(partition_field, urllib.parse.quote(value)))

                    partitions_clause = ','.join(partitions)
                    location = 's3://{}/{}/{}/'.format(self.panorama_raw_data_bucket, table, '/'.join(partitions_uri))

                    # Use Athena to load the partitions
                    query = "ALTER TABLE {} ADD IF NOT EXISTS PARTITION ({}) LOCATION '{}'".format(
                        self.table_partitions.get(table).get('datalake_table'),
                        partitions_clause,
                        location
                    )

                    log.debug("Updating partitions with {}".format(query))
                    self.query_athena(
                        query=query,
                        db=self.table_partitions.get(table).get('datalake_db'),
                        workgroup=self.table_partitions.get(table).get('workgroup'))

            else:

                # Process tables without partitions (except for the lms)
                rows = self.get_rows(table=table)
                self.upload_rows(bucket=self.panorama_raw_data_bucket,
                                 prefix=base_prefix,
                                 table=table,
                                 fields=fields,
                                 rows=rows)

        self.get_athena_executions()
