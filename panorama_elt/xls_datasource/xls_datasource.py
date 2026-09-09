"""
Panorama Excel datasource
This datasource doesn't allow field partitions.
It will create a table for each sheet, using the sheet name. Each sheet must have data in a tabular format
Do not leave empty rows or columns.
The first row must have the field names.
"""
import csv
import os
import tempfile
from pathlib import Path

import openpyxl

from panorama_elt.panorama_datalake.panorama_datalake import PanoramaDatalake
from panorama_elt.panorama_logger.setup_logger import log


class XLSDatasource:
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
        self.table_s3_tables = {}
        self.table_datalake_names = {}
        table_settings = datasource_settings.get('tables')
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

        results = {'XLS': 'OK' if path.is_file() else 'File {} not found'.format(self.location)}

        return results

    def get_tables(self) -> list:
        """
        Returns the list of sheet names, as a list of tables
        :return: list of sheet names
        """
        workbook = openpyxl.load_workbook(self.location, read_only=True)
        sheet_names = workbook.sheetnames
        workbook.close()
        return sheet_names

    def get_fields(self, table: str, force_query: bool = False) -> list:
        """
        Returns a list of fields of the table based on the first row of the specified sheet in the Excel file.
        All types are assumed to be string.

        :param table: table name
        :param force_query: (optional) if set to True, will query the db even if there is a definition set
        :return: list[str] of fields
        """

        # If the field list is declared in the settings file, return it.
        if self.table_fields and self.table_fields.get(table) and not force_query:
            return self.table_fields.get(table)

        workbook = openpyxl.load_workbook(self.location, read_only=True)
        sheet = workbook[table]
        fields = []

        colnum = 1
        value = sheet.cell(row=1, column=1).value
        while value:
            fields.append(value)
            colnum += 1
            value = sheet.cell(row=1, column=colnum).value

        workbook.close()

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

        workbook = openpyxl.load_workbook(self.location, read_only=True)
        try:
            with tempfile.TemporaryDirectory(prefix='panorama-xls-') as directory:
                self._export_workbook(workbook, selected_tables, directory)
        finally:
            workbook.close()

    def _export_workbook(self, workbook, selected_tables, directory):
        """Stream worksheet rows into temporary CSV files."""
        table_names = self.table_fields.keys() or workbook.sheetnames
        for table in table_names:
            if selected_tables and table not in selected_tables.split(','):
                continue

            fields = self.table_fields.get(table) or [f.get('name') for f in self.get_fields(table)]
            sheet = workbook[table]

            # Save the dataset in a csv file
            filename = os.path.join(directory, 'export.csv')
            with open(filename, 'w', encoding='utf-8') as f:
                write = csv.writer(f, doublequote=False, escapechar='\\')
                write.writerow(fields)
                for row in sheet.iter_rows(min_row=2, max_col=len(fields), values_only=True):
                    if all(value is None for value in row):
                        break
                    write.writerow(row)

            upload_kwargs = {
                'filename': filename,
                'table': table,
                'update_partitions': True,
                's3_filename': f'{table}.csv',
            }

            s3_table = self.table_s3_tables.get(table)
            if s3_table:
                upload_kwargs['s3_table'] = s3_table
                upload_kwargs['s3_filename'] = "{}.csv".format(s3_table)

            datalake_table_name = self.table_datalake_names.get(table)
            if datalake_table_name:
                upload_kwargs['datalake_table_name'] = datalake_table_name

            self.datalake.upload_table_from_file(**upload_kwargs)

            os.remove(filename)
