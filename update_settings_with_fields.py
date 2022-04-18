"""
Extract MySQL tables from an Open edX instance
"""
import yaml
import os

from course_structures_extractor.course_structures_extractor import CourseStructuresExtractor
from sql_tables_extractor.sql_extractor import SqlExtractor

from panorama_logger.setup_logger import log


def load_settings(config_file: str) -> dict:
    with open(config_file, 'r') as f:
        yaml_settings = yaml.safe_load(f)

    return yaml_settings


def save_settings(yaml_settings: dict, config_file: str) -> None:
    with open(config_file, 'w') as f:
        yaml.safe_dump(yaml_settings, f, sort_keys=False)


def update_settings(yaml_settings: dict):

    mysql_username = settings.get('mysql_username', 'root')
    mysql_password = settings.get('mysql_password')
    mysql_host = settings.get('mysql_host', '127.0.0.1')
    mysql_database = settings.get('mysql_database', 'edxapp')

    panorama_mysql_tables = [table.get('name') for table in settings.get('tables')]

    sql_extractor = SqlExtractor(
        mysql_username=mysql_username,
        mysql_password=mysql_password,
        mysql_host=mysql_host,
        mysql_database=mysql_database,
        panorama_mysql_tables=panorama_mysql_tables,
    )

    table_fields = sql_extractor.get_all_fields()

    new_tables_settings = []
    for table_setting in settings.get('tables'):
        new_tables_setting = {}
        name = table_setting.get('name')
        for k, v in table_setting.items():
            new_tables_setting[k] = v
        new_tables_setting['fields'] = table_fields.get(name)

        new_tables_settings.append(new_tables_setting)

    settings['tables'] = new_tables_settings


if __name__ == '__main__':

    filename = os.getenv('PANORAMA_SETTINGS_FILE', default='panorama_settings.yaml')
    settings = load_settings(filename)

    update_settings(settings)

    save_settings(yaml_settings=settings, config_file=filename)

