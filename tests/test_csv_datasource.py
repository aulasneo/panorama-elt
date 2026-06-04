"""Tests for the CSV datasource covering field inference and connection checks."""
from panorama_elt.csv_datasource.csv_datasource import CSVDatasource


class FakeDatalake:
    def __init__(self):
        self.uploads = []

    def upload_table_from_file(self, **kwargs):
        self.uploads.append(kwargs)


def test_get_tables_uses_basename():
    ds = CSVDatasource(datalake=FakeDatalake(), datasource_settings={"location": "/data/enrollments.csv"})
    assert ds.get_tables() == ["enrollments"]


def test_get_fields_infers_from_header(tmp_path):
    csv_file = tmp_path / "data.csv"
    csv_file.write_text("id,name,email\n1,alice,a@x.com\n")

    ds = CSVDatasource(datalake=FakeDatalake(), datasource_settings={"location": str(csv_file)})
    assert ds.get_fields("data") == [
        {"name": "id", "type": "string"},
        {"name": "name", "type": "string"},
        {"name": "email", "type": "string"},
    ]


def test_get_fields_returns_configured_definition(tmp_path):
    ds = CSVDatasource(
        datalake=FakeDatalake(),
        datasource_settings={
            "location": str(tmp_path / "missing.csv"),
            "tables": [{"name": "data", "fields": [{"name": "id"}, {"name": "name"}]}],
        },
    )
    # configured fields are returned without reading the (absent) file
    assert ds.get_fields("data") == ["id", "name"]


def test_test_connections_reports_presence(tmp_path):
    present = tmp_path / "there.csv"
    present.write_text("a\n")
    ds_ok = CSVDatasource(datalake=FakeDatalake(), datasource_settings={"location": str(present)})
    assert ds_ok.test_connections() == {"CSV": "OK"}

    ds_missing = CSVDatasource(datalake=FakeDatalake(), datasource_settings={"location": str(tmp_path / "nope.csv")})
    assert ds_missing.test_connections()["CSV"].startswith("File ")


def test_extract_and_load_uploads_file():
    datalake = FakeDatalake()
    ds = CSVDatasource(datalake=datalake, datasource_settings={"location": "/data/enrollments.csv"})
    ds.extract_and_load()
    assert datalake.uploads == [{
        "filename": "/data/enrollments.csv",
        "table": "enrollments",
        "update_partitions": False,
    }]


def test_extract_and_load_respects_selected_tables():
    datalake = FakeDatalake()
    ds = CSVDatasource(datalake=datalake, datasource_settings={"location": "/data/enrollments.csv"})
    ds.extract_and_load(selected_tables="other_table")
    assert datalake.uploads == []
