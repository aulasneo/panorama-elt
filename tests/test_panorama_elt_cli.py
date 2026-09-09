"""Tests for the panorama_elt CLI helpers and command dispatch."""
import pytest
import yaml
from click.testing import CliRunner

from panorama_elt import panorama_elt
from panorama_elt.csv_datasource.csv_datasource import CSVDatasource
from panorama_elt.xls_datasource.xls_datasource import XLSDatasource


def test_load_settings_round_trip(tmp_path):
    config = tmp_path / "settings.yaml"
    settings = {"datalake": {"base_prefix": "p"}, "datasources": [{"name": "a", "type": "csv"}]}
    config.write_text(yaml.safe_dump(settings))
    assert panorama_elt.load_settings(str(config)) == settings


def test_load_settings_missing_file_exits():
    with pytest.raises(SystemExit):
        panorama_elt.load_settings("/no/such/file.yaml")


def test_save_settings_writes_yaml(tmp_path):
    config = tmp_path / "out.yaml"
    panorama_elt.save_settings(str(config), {"a": 1, "b": [1, 2]})
    assert yaml.safe_load(config.read_text()) == {"a": 1, "b": [1, 2]}


def test_get_datasource_dispatch_csv_and_xls():
    csv_ds = panorama_elt._get_datasource(object(), {"type": "csv", "location": "/x.csv"})
    assert isinstance(csv_ds, CSVDatasource)

    xls_ds = panorama_elt._get_datasource(object(), {"type": "xls", "location": "/x.xlsx"})
    assert isinstance(xls_ds, XLSDatasource)


def test_get_datasource_unsupported_exits():
    with pytest.raises(SystemExit):
        panorama_elt._get_datasource(object(), {"type": "unknown"})


def _write_settings(tmp_path):
    config = tmp_path / "settings.yaml"
    config.write_text(yaml.safe_dump({
        "datalake": {
            "panorama_raw_data_bucket": "test-bucket",
            "base_prefix": "panorama",
            "datalake_database": "panorama",
            "datalake_workgroup": "primary",
        },
        "datasources": [
            {"name": "files", "type": "csv", "location": "/data/enrollments.csv",
             "tables": [{"name": "enrollments"}]},
        ],
    }))
    return str(config)


def test_cli_requires_a_selector(patch_boto3, tmp_path):
    config = _write_settings(tmp_path)
    result = CliRunner().invoke(panorama_elt.cli, ["--settings", config, "extract-and-load"])
    assert result.exit_code == 2
    assert "Either --all or --datasource or --table must be specified" in result.output


def test_cli_extract_and_load_all(patch_boto3, tmp_path):
    config = _write_settings(tmp_path)
    result = CliRunner().invoke(panorama_elt.cli, ["--settings", config, "extract-and-load", "--all"])
    assert result.exit_code == 0, result.output
    # the command reuses the datalake built by the cli group (single boto3 session)
    assert len(patch_boto3) >= 1
    upload_calls = [call for session in patch_boto3 for call in session.s3.uploaded]
    # the csv datasource uploads using the source file path as the object filename
    assert ("/data/enrollments.csv", "test-bucket", "panorama/enrollments//data/enrollments.csv") in upload_calls


def test_cli_version(patch_boto3):
    result = CliRunner().invoke(panorama_elt.cli, ["--version"])
    assert result.exit_code == 0
    assert panorama_elt.__version__ in result.output


@pytest.mark.parametrize("command", [
    "extract-and-load",
    "create-datalake-tables",
    "drop-datalake-tables",
    "create-table-views",
    "set-tables",
    "set-tables-fields",
])
def test_cli_commands_require_a_selector(patch_boto3, tmp_path, command):
    config = _write_settings(tmp_path)
    result = CliRunner().invoke(panorama_elt.cli, ["--settings", config, command])
    assert result.exit_code == 2
    assert "Either --all or --datasource or --table must be specified" in result.output


def test_cli_command_rejects_all_with_tables(patch_boto3, tmp_path):
    config = _write_settings(tmp_path)
    result = CliRunner().invoke(
        panorama_elt.cli, ["--settings", config, "create-datalake-tables", "--all", "--tables", "x"])
    assert result.exit_code == 2
    assert "--all and --table cannot be used together" in result.output


def test_cli_test_connections_end_to_end(patch_boto3, monkeypatch, tmp_path):
    # avoid the real 1s poll sleep inside get_athena_executions
    import panorama_elt.panorama_datalake.panorama_datalake as dl_mod
    monkeypatch.setattr(dl_mod.time, "sleep", lambda _seconds: None)

    csv_file = tmp_path / "data.csv"
    csv_file.write_text("id\n")
    config = tmp_path / "settings.yaml"
    config.write_text(yaml.safe_dump({
        "datalake": {
            "panorama_raw_data_bucket": "test-bucket",
            "base_prefix": "panorama",
            "datalake_database": "panorama",
            "datalake_workgroup": "primary",
        },
        "datasources": [
            {"name": "files", "type": "csv", "location": str(csv_file), "tables": [{"name": "data"}]},
        ],
    }))

    result = CliRunner().invoke(panorama_elt.cli, ["--settings", str(config), "test-connections"])
    assert result.exit_code == 0, result.output
    assert "Testing datalake..." in result.output
    assert "S3: OK" in result.output
    assert "Athena: Ok" in result.output
    assert "Testing files..." in result.output
    assert "CSV: OK" in result.output
