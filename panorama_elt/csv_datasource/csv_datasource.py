"""
Panorama uploads a local csv file
This datasource doesn't allow field partitions. Only one file at a time.

"""
import os
import csv
from pathlib import Path

from panorama_elt.panorama_datalake.panorama_datalake import PanoramaDatalake
from panorama_elt.panorama_logger.setup_logger import log


class CSVDatasource:
    """
    Settings required:
    - table: only one table, corresponding to the file
    - location: path to the local file
    """

    def __init__(
            self,
            datalake: PanoramaDatalake,
            datasource_settings: dict
    ):

        table_settings = datasource_settings.get('tables')
        self.table_fields = {}
        self.table_s3_tables = {}
        self.table_datalake_names = {}
        if table_settings:
            for table_setting in table_settings:
                table_name = table_setting.get('name')
                fields = table_setting.get('fields')
                if fields:
                    self.table_fields[table_name] = [f.get("name") for f in fields]
                if table_setting.get('datalake_s3_table'):
                    self.table_s3_tables[table_name] = table_setting.get('datalake_s3_table')
                if table_setting.get('datalake_table_name'):
                    self.table_datalake_names[table_name] = table_setting.get('datalake_table_name')

        self.location = datasource_settings.get('location')
        self.datalake = datalake

    def test_connections(self) -> dict:
        """
        Performs connections test
        :return: dict with test results
        """
        path = Path(self.location)

        results = {'CSV': 'OK' if path.is_file() else 'File {} not found'.format(self.location)}

        return results

    def get_tables(self) -> list:
        """
        Returns the base name of the filename as a list, as the only table possible
        :return: list of sheet names
        """

        return [os.path.basename(os.path.splitext(self.location)[0])]

    def get_fields(self, table: str, force_query: bool = False) -> list:
        """
        Returns a list of fields of the table based on the first row of the csv file.
        All types are assumed to be string.

        :param table: table name
        :param force_query: (optional) if set to True, will query the db even if there is a definition set
        :return: list[str] of fields
        """

        # If the field list is declared in the settings file, return it.
        if self.table_fields and self.table_fields.get(table) and not force_query:
            return self.table_fields.get(table)

        with open(self.location, mode='r', encoding='utf-8') as file:
            csv_file = csv.reader(file)
            fields = next(csv_file)

        log.debug("Fields in table: {}".format(fields))

        fields_list = []
        for field in fields:
            fields_list.append({"name": field, "type": 'string'})

        return fields_list

    def extract_and_load(self, selected_tables: str = None, force: bool = False):  # pylint: disable=unused-argument
        """
        Upload the file to the datalake

        :param selected_tables: (optional) list of tables to extract and load
        :param force: Forces a full update of all the partitions
        :return:
        """

        table = next(iter(self.table_fields), self.get_tables()[0])

        if selected_tables and table not in selected_tables.split(','):
            return

        upload_kwargs = {
            'filename': self.location,
            'table': table,
            'update_partitions': False,
        }

        s3_table = self.table_s3_tables.get(table)
        if s3_table:
            upload_kwargs['s3_table'] = s3_table
            upload_kwargs['s3_filename'] = "{}.csv".format(s3_table)

        datalake_table_name = self.table_datalake_names.get(table)
        if datalake_table_name:
            upload_kwargs['datalake_table_name'] = datalake_table_name

        self.datalake.upload_table_from_file(**upload_kwargs)
