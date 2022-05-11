"""
Utility class to manage aws datalake for Panorama analytics
"""
import urllib.parse
from uuid import uuid4

import boto3
import botocore
from botocore.exceptions import ClientError

from panorama_logger.setup_logger import log


class PanoramaDatalake:

    def __init__(self, datalake_settings: dict):
        """
        Connection to Panorama AWS datalake

        :param datalake_settings: dict with datalake settings
        """

        self.datalake_settings = datalake_settings

        # S3 bucket where the tables will be uploaded.
        self.panorama_raw_data_bucket = datalake_settings.get('panorama_raw_data_bucket')
        if not self.panorama_raw_data_bucket:
            log.error("panorama_raw_data_bucket must be set")
            exit(1)

        # List of partitions common to all tables. For Open edX, it's set to {'lms': <LMS_HOST>}.
        self.base_partitions = {}
        if 'base_partitions' in datalake_settings:
            for base_partition in datalake_settings.get('base_partitions'):
                self.base_partitions[base_partition.get('name')] = base_partition.get('value')

        # Base prefix (initial folder) for all datalake files
        self.base_prefix = datalake_settings.get('base_prefix')

        # List of athena executions to track
        self.executions = []

        if datalake_settings.get('aws_access_key'):
            log.debug("Creating boto3 session with access key {}".format(datalake_settings.get('aws_access_key')))
            session = boto3.Session(
                aws_access_key_id=datalake_settings.get('aws_access_key'),
                aws_secret_access_key=datalake_settings.get('aws_secret_access_key'),
                region_name=datalake_settings.get('aws_region', 'us-east-1')
            )
        else:
            log.debug("Creating boto3 session without access key")
            session = boto3.Session(region_name=datalake_settings.get('aws_region', 'us-east-1'))

        self.s3_client = session.client('s3')
        self.athena = session.client('athena', region_name=datalake_settings.get('aws_region', 'us-east-1'))

        self.datalake_db = datalake_settings.get('datalake_database')
        self.datalake_workgroup = datalake_settings.get('datalake_workgroup')

    def test_connections(self) -> dict:
        """
        Performs connections test
        :return: dict with test results
        """
        results = {}

        # Test upload to s3

        try:
            log.debug("Putting object PanoramaConnectionTest in bucket {}".format(self.panorama_raw_data_bucket))
            result1 = self.s3_client.put_object(Bucket=self.panorama_raw_data_bucket, Key='PanoramaConnectionTest')
            log.debug("Deletting object PanoramaConnectionTest in bucket {}".format(self.panorama_raw_data_bucket))
            result2 = self.s3_client.delete_object(Bucket=self.panorama_raw_data_bucket, Key='PanoramaConnectionTest')
            if result1 and result2:
                results['S3'] = 'OK'

        except botocore.exceptions.ClientError as e:
            results['S3'] = e.response.get('Error')

        # Test Athena
        query = "SHOW DATABASES"
        self.query_athena(query)
        result = self.get_athena_executions()

        if 'SUCCEEDED' in result and result.get('SUCCEEDED') == 1:
            r = self.athena.get_query_results(QueryExecutionId=self.executions[0]['QueryExecutionId'])

            rows = [row.get('Data') for row in r['ResultSet']['Rows']]

            db_list = [x[0]['VarCharValue'] for x in rows]
            if self.datalake_db in db_list:
                results['Athena'] = 'Ok'
            else:
                results['Athena'] = "Datalake database {} not found. Available databases: {}".format(
                    self.datalake_db, db_list)
        else:
            results['Athena'] = result

        return results

    def query_athena(self, query: str) -> None:
        """
        Sends a query to Athena and waits for a response
        :param query: SQL query
        :return: None
        """

        if not self.datalake_db or not self.datalake_workgroup:
            log.warning("Datalake db or workgroup not configured. Skipping datalake update.")
            return

        crt = str(uuid4())
        log.debug("Executing {}".format(query))
        try:
            execution = self.athena.start_query_execution(
                QueryString=query,
                ClientRequestToken=crt,
                QueryExecutionContext={
                    'Database': self.datalake_db
                },
                WorkGroup=self.datalake_workgroup
            )
            self.executions.append(execution)

        except ClientError as e:
            log.error("boto3 error trying to execute athena query {}".format(query))
            log.error(e)
            return

    def get_athena_query_execution(self, execution):

        execution_id = execution.get('QueryExecutionId')
        response = self.athena.get_query_execution(QueryExecutionId=execution_id)
        state = response.get('QueryExecution').get('Status').get('State')

        return state

    def get_athena_executions(self, max_iter: int = 20):
        """
        Show the results of all athena executions
        :return: None
        """
        results = {}
        i = max_iter
        while ('RUNNING' in results or 'QUEUED' in results or i == max_iter) and i > 0:
            results = {}
            i -= 1
            for execution in self.executions:
                result = self.get_athena_query_execution(execution)
                if result not in results:
                    results[result] = 1
                else:
                    results[result] += 1

            log.debug("Summary of athena executions: {}".format(results))

        return results

    def update_partitions(self, table, field_partitions: iter = None, datalake_table_name: str = None):
        """
        Updates the partitions of the table

        :param table: Original name of the table. Present in the s3 key
        :param field_partitions: (optional) field partitions if any
        :param datalake_table_name: (optional) datalake table name. If omitted, will look for the configuration setting.
            If there is none, <base_prefix>_raw_<table> will be used.
        :return:
        """
        # Update partitions in the datalake
        partitions = []
        partitions_uri = []
        if self.base_partitions:
            for partition_field, value in self.base_partitions.items():
                partitions.append("{} = '{}'".format(partition_field, value))
                partitions_uri.append("{}={}".format(partition_field, urllib.parse.quote(value)))

        if field_partitions:
            for partition_field, value in field_partitions.items():
                partitions.append("{} = '{}'".format(partition_field, value))
                partitions_uri.append("{}={}".format(partition_field, urllib.parse.quote(value)))

        partitions_clause = ','.join(partitions)
        location = 's3://{}/{}/{}/{}/'.format(
            self.panorama_raw_data_bucket,
            self.base_prefix,
            table,
            '/'.join(partitions_uri))

        # Use Athena to load the partitions
        if not datalake_table_name:
            datalake_table_name = "{base_prefix}_raw_{table}".format(base_prefix=self.base_prefix, table=table)
            log.debug("No table settings found for {}. Using default datalake name {}.".format(
                table, datalake_table_name))

        log.info("Updating partitions of {}".format(datalake_table_name))
        query = "ALTER TABLE {} ADD IF NOT EXISTS PARTITION ({}) LOCATION '{}'".format(
            datalake_table_name,
            partitions_clause,
            location
        )

        log.debug("Updating partitions with {}".format(query))
        self.query_athena(query)

    def upload_table_from_file(self, filename: str, table: str, field_partitions: iter = None,
                               update_partitions: bool = False) -> None:
        """
        Upload a file to S3 and -optionally- update the table partitions
        The complete path will be:
        s3://<bucket>/<base_prefix>/<table>/<base partitions>/<field partitions>/filename
        Bucket, base_prefix and base_partitions are set in the PanoramaDatalake instance

        :param filename: file to be uploaded
        :param table: table name represented in the file
        :param field_partitions: (optional) list of field name and value pairs to be represented as partitions in Hive
            format <field_name>=<value>
        :param update_partitions: (optional). If set to True, will call update partition on this object. Default: False
        :return: None
        """

        # Base prefix of the file in the S3 buckets. If there is a base_prefix configured, then we start from there.
        # Otherwise, we start from the root of the bucket. The next folder is the table name.
        # Next, the list of base partitions definitions for all tables in Hive format
        # The complete prefix will be the base prefix plus any specific partitions defined for the table
        # The complete path will be:
        #
        if self.base_prefix:
            prefix_list = [self.base_prefix, table]
        else:
            prefix_list = [table]

        if self.base_partitions:
            for key, value in self.base_partitions.items():
                prefix_list.append("{}={}".format(key, urllib.parse.quote(value)))

        if field_partitions:
            for key, value in field_partitions.items():
                prefix_list.append("{}={}".format(key, urllib.parse.quote(value)))

        prefix_list.append(filename)

        key = "/".join(prefix_list)

        log.info("Uploading to {}".format(key))
        self.s3_client.upload_file(filename, self.panorama_raw_data_bucket, key)

        if update_partitions and (self.base_partitions or field_partitions):
            self.update_partitions(table=table, field_partitions=field_partitions)

    def create_datalake_table(self, table: str, fields: list,
                              datalake_table: str = None, field_partitions: list = None) -> None:
        """
        Run an Athena query to create the csv table in the database

        :param table: original table name, included in the s3 path
        :param datalake_table: (optional) datalake table name. If omitted, <base_prefix>_raw_<table> name will be used
        :param fields: list of fields
        :param field_partitions: list of fields to partition
        :return: None
        """

        if not datalake_table:
            datalake_table = "{base_prefix}_raw_{table}".format(base_prefix=self.base_prefix, table=table)

        # Remove partition fields from the field list
        if field_partitions:
            for partition_field in field_partitions:
                fields.remove(partition_field)

        # Build the list of fields
        fields_definitions_list = []
        for field in fields:
            fields_definitions_list.append('`{field}` string'.format(field=field))
        fields_definitions = ','.join(fields_definitions_list)

        # Build the partition section
        partitions_definitions_list = []
        if self.base_partitions:
            for base_partition in self.base_partitions:
                partitions_definitions_list.append('`{partition_field}` string'.format(partition_field=base_partition))
        if field_partitions:
            for field_partitions in field_partitions:
                partitions_definitions_list.append(
                    '`{partition_field}` string'.format(partition_field=field_partitions))
        if partitions_definitions_list:

            partitions_definitions = ','.join(partitions_definitions_list)
            partitions_section = """
                PARTITIONED BY ( 
                    {partitions_definitions}
                  )
            """.format(partitions_definitions=partitions_definitions)
        else:
            partitions_section = ''

        # The location of the table doesn't include the partitions subdirectories
        location = 's3://{bucket}/{prefix}/{table}/'.format(
            bucket=self.panorama_raw_data_bucket,
            prefix=self.base_prefix,
            table=table,
        )

        query = """
            CREATE EXTERNAL TABLE IF NOT EXISTS `{datalake_table}`(
                {fields_definitions}
              )
            {partitions_section}
            ROW FORMAT SERDE 
              'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
            WITH SERDEPROPERTIES ( 
              'escapeChar'='\\\\', 
              'quoteChar'='\\"', 
              'separatorChar'=',') 
            STORED AS INPUTFORMAT 
              'org.apache.hadoop.mapred.TextInputFormat' 
            OUTPUTFORMAT 
              'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
            LOCATION
              '{location}'
            TBLPROPERTIES (
              'areColumnsQuoted'='false', 
              'classification'='csv', 
              'columnsOrdered'='true', 
              'compressionType'='none', 
              'delimiter'=',', 
              'skip.header.line.count'='1', 
              'typeOfData'='file')
        """.format(
            datalake_table=datalake_table,
            fields_definitions=fields_definitions,
            partitions_section=partitions_section,
            location=location
        )

        log.debug("Creating datalake table {} with {}".format(table, query))
        self.query_athena(query=query)

    def drop_datalake_table(self, datalake_table: str):
        """
        Deletes a table from the datalake catalog
        :param datalake_table: name of the table in the datalake
        :return:
        """

        query = """
            DROP TABLE `{datalake_table}`
            """.format(datalake_table=datalake_table)
        self.query_athena(query=query)

    def drop_datalake_view(self, view: str):
        """
        Deletes a table from the datalake catalog
        :param view: name of the table in the datalake
        :return:
        """

        query = """
            DROP VIEW "{view}"
            """.format(view=view)
        self.query_athena(query=query)

    def create_table_view(self, datalake_table_name: str, view_name: str, fields: list):

        fields_definition = []

        if self.datalake_settings.get('base_partitions'):
            fields += self.datalake_settings.get('base_partitions')

        for field in fields:
            field_type = field.get('type').upper()

            # Numeric types
            if field_type in ['INT', 'TINYINT', 'SMALLINT', 'MEDIUMINT', 'BIGINT']:
                fields_definition.append('TRY_CAST("{field}" AS BIGINT) "{field}"'.format(field=field.get('name')))
            elif field_type in ['FLOAT', 'DOUBLE', 'DECIMAL']:
                fields_definition.append('TRY_CAST("{field}" AS DOUBLE) "{field}"'.format(field=field.get('name')))

            # Datetime types
            elif field_type == 'DATETIME':
                fields_definition.append('TRY("date_parse"("{field}", \'%Y-%m-%d %H:%i:%s.%f\')) "{field}"'.format(
                    field=field.get('name')))
            elif field_type == 'DATE':
                fields_definition.append('TRY("date_parse"("{field}", \'%Y-%m-%d\')) "{field}"'.format(
                    field=field.get('name')))
            elif field_type == 'TIMESTAMP':
                fields_definition.append('TRY("date_parse"("{field}", \'%Y-%m-%d %H:%i:%s\')) "{field}"'.format(
                    field=field.get('name')))
            elif field_type == 'TIME':
                fields_definition.append('TRY("date_parse"("{field}", \'%H:%i:%s\')) "{field}"'.format(
                    field=field.get('name')))
            elif field_type == 'YEAR':
                fields_definition.append('TRY("date_parse"("{field}", \'%Y\')) "{field}"'.format(
                    field=field.get('name')))

            # String types
            elif field_type in ['CHAR', 'VARCHAR', 'BLOB', 'TEXT', 'TINYBLOB', 'TINYTEXT', 'MEDIUMBLOB', 'MEDIUMTEXT',
                                'LONGBLOB', 'LONGTEXT', 'ENUM', 'STRING']:
                fields_definition.append('NULLIF("{field}", \'NULL\') "{field}"'.format(field=field.get('name')))

            # Other custom types:
            else:
                fields_definition.append('TRY_CAST("{field}" AS {field_type}) "{field}"'.format(
                    field=field.get('name'), field_type=field_type))

        query = """CREATE OR REPLACE VIEW "{view_name}" AS
        SELECT {fields_definition} 
        FROM "{database}"."{table_name}"
        """.format(view_name=view_name,
                   fields_definition=','.join(fields_definition),
                   database=self.datalake_db,
                   table_name=datalake_table_name
                   )
        self.query_athena(query)
