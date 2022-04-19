"""
Extract MySQL tables from an Open edX instance
"""
import yaml
import os

from panorama_datalake.panorama_datalake import PanoramaDatalake

from panorama_logger.setup_logger import log


def load_settings(config_file: str) -> dict:
    with open(config_file, 'r') as f:
        yaml_settings = yaml.safe_load(f)

    return yaml_settings


if __name__ == '__main__':

    settings = load_settings(os.getenv('PANORAMA_SETTINGS_FILE', default='panorama_settings.yaml'))

    panorama_raw_data_bucket = settings.get('panorama_raw_data_bucket')
    if not panorama_raw_data_bucket:
        log.error("panorama_raw_data_bucket must be set")
        exit(1)

    datalake_table_names = {}
    for table in settings.get('tables'):
        if 'datalake_table_name' in table:
            datalake_table_names[table.get('name')] = table.get('datalake_table_name')

    base_partitions = {}
    for base_partition in settings.get('base_partitions'):
        base_partitions[base_partition.get('key')] = base_partition.get('value')

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
