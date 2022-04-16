"""
Extract MySQL tables from an Open edX instance, taking the parameters from environment variables

    PANORAMA_RAW_DATA_BUCKET: S3 bucket to upload the csv files
    PANORAMA_AWS_ACCESS_KEY: AWS access key. If omitted, will try to use the local credentials or instance role
    PANORAMA_AWS_SECRET_ACCESS_KEY: AWS secret
    PANORAMA_AWS_REGION: AWS region required for Athena queries. Default='us-east-1'
    PANORAMA_MYSQL_USERNAME: default='root'
    PANORAMA_MYSQL_PASSWORD
    PANORAMA_MYSQL_HOST: default='127.0.0.1'
    PANORAMA_MYSQL_DATABASE: default='edxapp'
    PANORAMA_MYSQL_TABLES: comma separated list of tables to query.
        Default=auth_user,student_courseenrollment,auth_userprofile,student_courseaccessrole, \
            course_overviews_courseoverview,courseware_studentmodule,grades_persistentcoursegrade,\
            student_manualenrollmentaudit,student_courseenrollmentallowed,certificates_generatedcertificate
    PANORAMA_BASE_PARTITIONS: (Optional) Base partition to apply to all tables, in JSON format.
        Each partition will result in a folder to be created in the form "<key>=<value>" in the Hive partition format.
        If left empty, no partition will be applied.
        For Open edX, please set to '{"lms": "<value of LMS_BASE>"}
    PANORAMA_TABLE_PARTITIONS: (Optional) Partitions to apply based on the values of certain table fields.
        This allows to make incremental updates.
        Only partitions with changes in the latest interval defined will be updated.

        For Open edX, the recommended setting is:
        {"courseware_studentmodule": {"partition_fields": ["course_id"], "interval": "2 hour", \
        "timestamp_field": "modified", "datalake_db": "panorama", "datalake_table": "courseware_studentmodule_raw", \
        "workgroup": "panorama"}}'
        This will partition the courseware_studentmodule by the course_id field.

        Format:
        {
            <table name>: {
                "partition_fields": [
                    <field to partition>,
                    ...
                ],
                "interval": <interval in mysql format. E.g.:"2 hour">,
                "timestamp_field": <(optional) field name in date format to make incremental updates. E.g.:"modified">,
                "datalake_db": <(optional) Datalake db name. E.g.:"panorama">,
                "datalake_table": <(optional) name of the table in the datalake. E.g.:"courseware_studentmodule_raw">,
                "workgroup": <(optional) Athena workgroup to update partitions in the datalake. E.g.:"panorama">
            },
            ...
        }

    PANORAMA_MONGODB_USERNAME: (optional)
    PANORAMA_MONGODB_PASSWORD: (optional)
    PANORAMA_MONGODB_HOST (default='127.0.0.1')
    PANORAMA_MONGODB_DATABASE (default='edxapp')

"""
import json
import os

from course_structures_extractor.course_structures_extractor import CourseStructuresExtractor
from sql_tables_extractor.sql_extractor import SqlExtractor

from panorama_logger.setup_logger import log


def openedx_extract_and_load():
    panorama_raw_data_bucket = os.getenv('PANORAMA_RAW_DATA_BUCKET')
    aws_access_key = os.getenv('PANORAMA_AWS_ACCESS_KEY')
    aws_secret_access_key = os.getenv('PANORAMA_AWS_SECRET_ACCESS_KEY')
    aws_region = os.getenv('PANORAMA_AWS_REGION', default='us-east-1')
    mysql_username = os.getenv('PANORAMA_MYSQL_USERNAME', default='root')
    mysql_password = os.getenv('PANORAMA_MYSQL_PASSWORD')
    mysql_host = os.getenv('PANORAMA_MYSQL_HOST', default='127.0.0.1')
    mysql_database = os.getenv('PANORAMA_MYSQL_DATABASE', default='edxapp')
    panorama_mysql_tables = os.getenv('PANORAMA_MYSQL_TABLES', default="auth_user,"
                                                                       "student_courseenrollment,"
                                                                       "auth_userprofile,"
                                                                       "student_courseaccessrole,"
                                                                       "course_overviews_courseoverview,"
                                                                       "courseware_studentmodule,"
                                                                       "grades_persistentcoursegrade,"
                                                                       "student_manualenrollmentaudit,"
                                                                       "student_courseenrollmentallowed,"
                                                                       "certificates_generatedcertificate"
                                      )

    # This dict defines which tables have partitions
    # The interval is in MYSQL format
    default_table_partitions = {
        'courseware_studentmodule': {
            'partition_fields': [
                'course_id',
                # 'student_id'
            ],
            'interval': '2 hour',
            'timestamp_field': 'modified',
            'datalake_db': 'panorama',
            'datalake_table': 'courseware_studentmodule_raw',
            'workgroup': 'panorama'
        }
    }

    table_partitions = default_table_partitions
    if os.getenv('PANORAMA_TABLE_PARTITIONS'):
        try:
            table_partitions = json.loads(os.getenv('PANORAMA_TABLE_PARTITIONS'))
        except json.decoder.JSONDecodeError as e:
            log.error(
                "JSON error {} decoding PANORAMA_TABLE_PARTITIONS={}".format(e, os.getenv('PANORAMA_TABLE_PARTITIONS')))
            exit(1)

    base_partitions = {}
    if os.getenv('PANORAMA_BASE_PARTITIONS'):
        try:
            base_partitions = json.loads(os.getenv('PANORAMA_BASE_PARTITIONS'))
        except json.decoder.JSONDecodeError as e:
            log.error(
                "JSON error {} decoding PANORAMA_BASE_PARTITIONS={}".format(e, os.getenv('PANORAMA_BASE_PARTITIONS')))
            exit(1)

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

    # Extract course structures from MongoDB
    mongodb_username = os.getenv('PANORAMA_MONGODB_USERNAME')
    mongodb_password = os.getenv('PANORAMA_MONGODB_PASSWORD')
    mongodb_host = os.getenv('PANORAMA_MONGODB_HOST', default='127.0.0.1')
    mongodb_database = os.getenv('PANORAMA_MONGODB_DATABASE', default='edxapp')

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
    openedx_extract_and_load()
