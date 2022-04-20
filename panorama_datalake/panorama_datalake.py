"""
Utility class to manage aws datalake for Panorama analytics
"""
import urllib.parse
from uuid import uuid4

import boto3
from botocore.exceptions import ClientError

from panorama_logger.setup_logger import log


class PanoramaDatalake:

    def __init__(self,
                 aws_access_key: str = None,
                 aws_secret_access_key: str = None,
                 aws_region: str = None,
                 datalake_db: str = None,
                 datalake_workgroup: str = None,
                 base_prefix: str = None,
                 bucket: str = None,
                 base_partitions: dict = None,
                 datalake_table_names: dict = None,
                 ):
        """
        Connection to Panorama AWS datalake

        :param aws_access_key: (optional) aws credentials. If unset, will try to use a default profile or assume a role
        :param aws_secret_access_key:
        :param aws_region: (optional)
        :param datalake_db: Datalake database name
        :param datalake_workgroup: Athena workgroup
        :param base_prefix: Prefix prepended to the S3 path
        :param bucket: S3 bucket
        :param base_partitions: dict with fixed partitions in the form { <field>: <value>, ...}
        :param datalake_table_names: Dict with the name of the datalake table for each table, in the form
            { <table name>: <datalake table name>, ...}
            If no datalake name is specified, the same table name will be used.


        """

        self.base_partitions = base_partitions
        self.base_prefix = base_prefix
        self.panorama_raw_data_bucket = bucket
        self.executions = []
        self.datalake_workgroup = None
        self.datalake_db = None

        self.datalake_table_names = datalake_table_names

        session = boto3.Session(
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region
        )
        self.s3_client = session.client('s3')
        self.athena = session.client('athena')

        self.datalake_db = datalake_db
        self.datalake_workgroup = datalake_workgroup

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

            log.info("Summary of athena executions: {}".format(results))

    def update_partitions(self, table, field_partitions: iter = None, datalake_table_name: str = None):
        """
        Updates the partitions of the table

        :param table: Original name of the table. Present in the s3 key
        :param field_partitions: (optional) field partitions if any
        :param datalake_table_name: (optional) datalake table name. If omitted, will look for the configuration setting.
            If there is none, the original table name will be used.
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
            if table not in self.datalake_table_names:
                log.warning("No table settings found for {}. Using defaults.".format(table))
                datalake_table_name = table
            else:
                datalake_table_name = self.datalake_table_names.get(table)

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
        log.debug("Base prefix: {}".format(prefix_list))

        self.s3_client.upload_file(filename, self.panorama_raw_data_bucket, key)

        if update_partitions:
            self.update_partitions(table=table, field_partitions=field_partitions)

    def create_datalake_csv_table(self, table: str, fields: list,
                                  datalake_table: str = None, field_partitions: list = None) -> None:
        """
        Run an Athena query to create the csv table in the database

        :param table: original table name, included in the s3 path
        :param datalake_table: (optional) datalake table name. If omitted, the table name will be used
        :param fields: list of fields
        :param field_partitions: list of fields to partition
        :return: None
        """

        if not datalake_table:
            datalake_table = table

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
                partitions_definitions_list.append('`{partition_field}` string'.format(partition_field=field_partitions))
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
        location = 's3://{prefix}/{bucket}/{table}/'.format(
            prefix=self.base_prefix,
            bucket=self.panorama_raw_data_bucket,
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
