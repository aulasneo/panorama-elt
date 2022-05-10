"""
Panorama uploads a local csv file
This datasource doesn't allow field partitions. Only one file at a time.

"""
import os
import csv

import pymysql

from panorama_datalake.panorama_datalake import PanoramaDatalake
from panorama_logger.setup_logger import log


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

        self.table_fields = {}
        for table_setting in datasource_settings.get('tables'):
            fields = table_setting.get('fields')
            if fields:
                self.table_fields[table_setting.get('name')] = [f.get("name") for f in fields]

        self.location = datasource_settings.get('location')
        self.datalake = datalake

    def test_connections(self) -> dict:
        """
        Performs connections test
        :return: dict with test results
        """
        from pathlib import Path
        path = Path(self.location)

        results = {'CSV': 'OK' if path.is_file() else 'File not found'}

        return results

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

        with open(self.location, mode='r') as file:
            csv_file = csv.reader(file)
            fields = next(csv_file)

        log.debug("Fields in table: {}".format(fields))

        fields_list = []
        for field in fields:
            fields_list.append({"name": field, "type": 'string'})

        return fields_list

    def extract_and_load(self, selected_tables: str = None, force: bool = False):
        """
        Upload the file to the datalake

        :param selected_tables: (optional) list of tables to extract and load
        :param force: Forces a full update of all the partitions
        :return:
        """

        table = list(self.table_fields.keys())[0]

        self.datalake.upload_table_from_file(filename=self.location, table=table, update_partitions=False)
