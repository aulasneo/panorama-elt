import importlib
import sys
import types
import unittest


def install_fake_aws_modules():
    fake_boto3 = types.ModuleType("boto3")
    fake_botocore = types.ModuleType("botocore")
    fake_exceptions = types.ModuleType("botocore.exceptions")

    class FakeClientError(Exception):
        pass

    class FakeSession:
        def client(self, *_args, **_kwargs):
            return types.SimpleNamespace()

    fake_boto3.Session = lambda **_kwargs: FakeSession()
    fake_botocore.exceptions = types.SimpleNamespace(ClientError=FakeClientError)
    fake_exceptions.ClientError = FakeClientError

    sys.modules["boto3"] = fake_boto3
    sys.modules["botocore"] = fake_botocore
    sys.modules["botocore.exceptions"] = fake_exceptions


class CsvDatasourceTests(unittest.TestCase):
    def setUp(self):
        self.original_modules = {
            name: sys.modules.get(name)
            for name in (
                "boto3",
                "botocore",
                "botocore.exceptions",
                "panorama_elt.csv_datasource.csv_datasource",
                "panorama_elt.panorama_datalake.panorama_datalake",
            )
        }
        install_fake_aws_modules()

    def tearDown(self):
        for name, module in self.original_modules.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module

    def test_get_tables_returns_single_item_list(self):
        from panorama_elt.csv_datasource.csv_datasource import CSVDatasource

        datasource = CSVDatasource(
            datalake=object(),
            datasource_settings={"location": "/tmp/enrollments.csv"},
        )

        self.assertEqual(datasource.get_tables(), ["enrollments"])

    def test_extract_and_load_uses_basename_when_fields_are_not_preconfigured(self):
        from panorama_elt.csv_datasource.csv_datasource import CSVDatasource

        class FakeDatalake:
            def __init__(self):
                self.calls = []

            def upload_table_from_file(self, **kwargs):
                self.calls.append(kwargs)

        datalake = FakeDatalake()
        datasource = CSVDatasource(
            datalake=datalake,
            datasource_settings={"location": "/tmp/enrollments.csv"},
        )

        datasource.extract_and_load()

        self.assertEqual(
            datalake.calls,
            [{
                "filename": "/tmp/enrollments.csv",
                "table": "enrollments",
                "update_partitions": False,
            }],
        )


class XlsDatasourceTests(unittest.TestCase):
    def setUp(self):
        self.original_modules = {
            name: sys.modules.get(name)
            for name in (
                "boto3",
                "botocore",
                "botocore.exceptions",
                "openpyxl",
                "panorama_elt.xls_datasource.xls_datasource",
                "panorama_elt.panorama_datalake.panorama_datalake",
            )
        }
        install_fake_aws_modules()

    def tearDown(self):
        for name, module in self.original_modules.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module

    def test_uses_modern_openpyxl_sheet_accessors(self):
        workbook_instances = []

        class FakeSheet:
            def cell(self, row, column):
                values = {
                    (1, 1): "username",
                    (1, 2): None,
                    (2, 1): "alice",
                    (3, 1): None,
                }
                return types.SimpleNamespace(value=values.get((row, column)))

        class FakeWorkbook:
            def __init__(self):
                self.sheetnames = ["Sheet1"]
                self.closed = False
                self.sheet_accesses = []

            def __getitem__(self, key):
                self.sheet_accesses.append(key)
                return FakeSheet()

            def close(self):
                self.closed = True

        fake_openpyxl = types.ModuleType("openpyxl")

        def load_workbook(_location):
            workbook = FakeWorkbook()
            workbook_instances.append(workbook)
            return workbook

        fake_openpyxl.load_workbook = load_workbook
        sys.modules["openpyxl"] = fake_openpyxl

        module = importlib.import_module("panorama_elt.xls_datasource.xls_datasource")
        datasource = module.XLSDatasource(
            datalake=object(),
            datasource_settings={"location": "/tmp/data.xlsx"},
        )

        self.assertEqual(datasource.get_tables(), ["Sheet1"])
        self.assertEqual(datasource.get_fields("Sheet1"), [{"name": "username", "type": "string"}])
        self.assertEqual(workbook_instances[0].sheet_accesses, [])
        self.assertEqual(workbook_instances[1].sheet_accesses, ["Sheet1"])
        self.assertTrue(workbook_instances[0].closed)
        self.assertTrue(workbook_instances[1].closed)


class DatalakeViewTests(unittest.TestCase):
    def setUp(self):
        self.original_modules = {
            name: sys.modules.get(name)
            for name in ("boto3", "botocore", "botocore.exceptions")
        }
        install_fake_aws_modules()

    def tearDown(self):
        for name, module in self.original_modules.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module
        sys.modules.pop("panorama_elt.panorama_datalake.panorama_datalake", None)

    def test_create_table_view_does_not_mutate_fields_argument(self):
        module = importlib.import_module("panorama_elt.panorama_datalake.panorama_datalake")
        datalake = module.PanoramaDatalake(
            {
                "panorama_raw_data_bucket": "bucket",
                "datalake_database": "panorama",
                "datalake_workgroup": "wg",
                "base_partitions": [{"name": "lms", "type": "string"}],
            }
        )
        queries = []
        datalake.query_athena = queries.append

        fields = [{"name": "username", "type": "varchar"}]
        datalake.create_table_view("raw_users", "table_users", fields)

        self.assertEqual(fields, [{"name": "username", "type": "varchar"}])
        self.assertEqual(len(queries), 1)
        self.assertIn('"lms"', queries[0])


if __name__ == "__main__":
    unittest.main()
