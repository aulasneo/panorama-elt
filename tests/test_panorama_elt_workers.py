"""Unit tests for the panorama_elt CLI helpers and worker functions.

These exercise the worker logic directly (without the click layer) using a fake
datalake and a monkeypatched ``_get_datasource``.
"""
import types

import yaml
import click
import pytest

from panorama_elt import panorama_elt


class FakeDatalake:
    def __init__(self):
        self.created = []
        self.dropped_tables = []
        self.dropped_views = []
        self.views = []

    def create_datalake_table(self, **kwargs):
        self.created.append(kwargs)

    def drop_datalake_table(self, **kwargs):
        self.dropped_tables.append(kwargs)

    def drop_datalake_view(self, **kwargs):
        self.dropped_views.append(kwargs)

    def create_table_view(self, **kwargs):
        self.views.append(kwargs)

    def get_athena_executions(self):
        return {"SUCCEEDED": 1}


class FakeDatasource:
    """Records extract calls and returns canned tables/fields."""

    def __init__(self, name):
        self.name = name
        self.extract_calls = []

    def extract_and_load(self, selected_tables=None, force=False):
        self.extract_calls.append((selected_tables, force))

    def get_tables(self):
        return ["t_" + self.name]

    def get_fields(self, table, force_query=False):
        return [{"name": "f_" + table, "type": "string"}]


def make_ctx(settings, datalake=None, config_file="settings.yaml"):
    return types.SimpleNamespace(obj={
        "settings": settings,
        "datalake": datalake if datalake is not None else FakeDatalake(),
        "config_file": config_file,
    })


SETTINGS = {
    "datalake": {"base_prefix": "panorama"},
    "datasources": [
        {"name": "a", "type": "csv", "tables": [{"name": "t1"}, {"name": "t2"}]},
        {"name": "b", "type": "csv", "tables": [{"name": "t3"}]},
    ],
}


# --- _dispatch -------------------------------------------------------------

def test_dispatch_rejects_all_with_tables(capsys):
    called = []
    with pytest.raises(click.UsageError, match='cannot be used together'):
        panorama_elt._dispatch(None, lambda *a, **k: called.append(1), all_=True, datasource=None, tables="t")
    assert called == []


def test_dispatch_requires_a_selector(capsys):
    called = []
    with pytest.raises(click.UsageError, match='must be specified'):
        panorama_elt._dispatch(None, lambda *a, **k: called.append(1), all_=False, datasource=None, tables=None)
    assert called == []


def test_dispatch_forwards_selectors_and_extras():
    received = {}

    def worker(ctx, datasource=None, tables=None, **extra):
        received.update(ctx=ctx, datasource=datasource, tables=tables, extra=extra)

    ctx = make_ctx(SETTINGS)
    panorama_elt._dispatch(ctx, worker, all_=False, datasource="a", tables="t1,t2", force=True)
    assert received == {"ctx": ctx, "datasource": "a", "tables": "t1,t2", "extra": {"force": True}}


def test_dispatch_all_runs_worker():
    received = {}

    def worker(ctx, datasource=None, tables=None, **extra):
        received.update(datasource=datasource, tables=tables)

    panorama_elt._dispatch(make_ctx(SETTINGS), worker, all_=True, datasource=None, tables=None)
    assert received == {"datasource": None, "tables": None}


# --- iteration helpers -----------------------------------------------------

def test_iter_datasources_all_and_filtered():
    assert [d["name"] for d in panorama_elt._iter_datasources(SETTINGS)] == ["a", "b"]
    assert [d["name"] for d in panorama_elt._iter_datasources(SETTINGS, "b")] == ["b"]


def test_iter_tables_all():
    pairs = [(d["name"], t["name"]) for d, t in panorama_elt._iter_tables(SETTINGS)]
    assert pairs == [("a", "t1"), ("a", "t2"), ("b", "t3")]


def test_iter_tables_filtered_by_table():
    pairs = [(d["name"], t["name"]) for d, t in panorama_elt._iter_tables(SETTINGS, tables="t1,t3")]
    assert pairs == [("a", "t1"), ("b", "t3")]


def test_iter_tables_filtered_by_datasource_and_table():
    pairs = [(d["name"], t["name"]) for d, t in panorama_elt._iter_tables(SETTINGS, datasource="a", tables="t2")]
    assert pairs == [("a", "t2")]


# --- _datalake_names -------------------------------------------------------

def test_datalake_names_defaults():
    assert panorama_elt._datalake_names({"name": "users"}, "panorama") == (
        "panorama_raw_users", "panorama_table_users")


def test_datalake_names_overrides():
    table = {"name": "users", "datalake_table_name": "X", "datalake_table_view": "Y"}
    assert panorama_elt._datalake_names(table, "panorama") == ("X", "Y")


def test_datalake_names_default_to_s3_table_name():
    table = {"name": "mdl_course", "datalake_s3_table": "course"}
    assert panorama_elt._datalake_names(table, "moodle") == ("moodle_raw_course", "moodle_table_course")


# --- workers ---------------------------------------------------------------

def test_create_datalake_tables_builds_each_and_skips_fieldless():
    settings = {"datalake": {"base_prefix": "panorama"}, "datasources": [
        {"name": "a", "tables": [
            {"name": "users", "fields": [{"name": "id"}, {"name": "email"}],
             "partitions": {"partition_fields": ["email"]}},
            {"name": "empty"},
        ]},
    ]}
    dl = FakeDatalake()
    panorama_elt._create_datalake_tables(make_ctx(settings, dl))
    assert dl.created == [{
        "table": "users",
        "fields": ["id", "email"],
        "field_partitions": ["email"],
        "datalake_table": "panorama_raw_users",
    }]


def test_create_datalake_tables_uses_s3_table_for_location_and_default_name():
    settings = {"datalake": {"base_prefix": "moodle"}, "datasources": [
        {"name": "a", "tables": [
            {"name": "mdl_course", "datalake_s3_table": "course", "fields": [{"name": "id"}]},
        ]},
    ]}
    dl = FakeDatalake()
    panorama_elt._create_datalake_tables(make_ctx(settings, dl))
    assert dl.created == [{
        "table": "course",
        "fields": ["id"],
        "field_partitions": None,
        "datalake_table": "moodle_raw_course",
    }]


def test_drop_datalake_tables_drops_table_and_view():
    settings = {"datalake": {"base_prefix": "panorama"}, "datasources": [
        {"name": "a", "tables": [{"name": "users"}]},
    ]}
    dl = FakeDatalake()
    panorama_elt._drop_datalake_tables(make_ctx(settings, dl))
    assert dl.dropped_tables == [{"datalake_table": "panorama_raw_users"}]
    assert dl.dropped_views == [{"view": "panorama_table_users"}]


def test_create_table_view_builds_view_and_skips_fieldless():
    settings = {"datalake": {"base_prefix": "panorama"}, "datasources": [
        {"name": "a", "tables": [
            {"name": "users", "fields": [{"name": "id", "type": "int"}]},
            {"name": "nofields"},
        ]},
    ]}
    dl = FakeDatalake()
    panorama_elt._create_table_view(make_ctx(settings, dl))
    assert dl.views == [{
        "datalake_table_name": "panorama_raw_users",
        "view_name": "panorama_table_users",
        "fields": [{"name": "id", "type": "int"}],
    }]


def test_extract_and_load_runs_every_datasource(monkeypatch):
    settings = {"datalake": {}, "datasources": [{"name": "a"}, {"name": "b"}]}
    created = {}

    def fake_get(_datalake, ds_settings):
        ds = FakeDatasource(ds_settings["name"])
        created[ds_settings["name"]] = ds
        return ds

    monkeypatch.setattr(panorama_elt, "_get_datasource", fake_get)
    panorama_elt._extract_and_load(make_ctx(settings), tables="t1", force=True)

    assert created["a"].extract_calls == [("t1", True)]
    assert created["b"].extract_calls == [("t1", True)]


def test_set_tables_processes_all_datasources(monkeypatch, tmp_path):
    # Regression: previously only the first datasource was processed.
    settings = {"datalake": {}, "datasources": [{"name": "a"}, {"name": "b"}]}
    monkeypatch.setattr(panorama_elt, "_get_datasource", lambda _dl, s: FakeDatasource(s["name"]))
    config = tmp_path / "s.yaml"
    panorama_elt._set_tables(make_ctx(settings, config_file=str(config)))

    assert settings["datasources"][0]["tables"] == [{"name": "t_a"}]
    assert settings["datasources"][1]["tables"] == [{"name": "t_b"}]
    # changes are persisted to disk
    assert yaml.safe_load(config.read_text())["datasources"][1]["tables"] == [{"name": "t_b"}]


def test_set_tables_filters_by_table_name(monkeypatch, tmp_path):
    settings = {"datalake": {}, "datasources": [{"name": "a"}]}

    class DS(FakeDatasource):
        def get_tables(self):
            return ["keep", "drop"]

    monkeypatch.setattr(panorama_elt, "_get_datasource", lambda _dl, s: DS("a"))
    panorama_elt._set_tables(make_ctx(settings, config_file=str(tmp_path / "s.yaml")), tables="keep")
    assert settings["datasources"][0]["tables"] == [{"name": "keep"}]


def test_set_tables_fields_processes_all_datasources(monkeypatch, tmp_path):
    # Regression: previously only the first datasource was processed.
    settings = {"datalake": {}, "datasources": [
        {"name": "a", "tables": [{"name": "t1"}]},
        {"name": "b", "tables": [{"name": "t2"}]},
    ]}
    monkeypatch.setattr(panorama_elt, "_get_datasource", lambda _dl, s: FakeDatasource(s["name"]))
    panorama_elt._set_tables_fields(make_ctx(settings, config_file=str(tmp_path / "s.yaml")))

    assert settings["datasources"][0]["tables"][0]["fields"] == [{"name": "f_t1", "type": "string"}]
    assert settings["datasources"][1]["tables"][0]["fields"] == [{"name": "f_t2", "type": "string"}]


def test_set_tables_fields_skips_unselected_tables(monkeypatch, tmp_path):
    settings = {"datalake": {}, "datasources": [
        {"name": "a", "tables": [{"name": "t1"}, {"name": "t2"}]},
    ]}
    monkeypatch.setattr(panorama_elt, "_get_datasource", lambda _dl, s: FakeDatasource("a"))
    panorama_elt._set_tables_fields(make_ctx(settings, config_file=str(tmp_path / "s.yaml")), tables="t1")

    assert settings["datasources"][0]["tables"][0]["fields"] == [{"name": "f_t1", "type": "string"}]
    assert "fields" not in settings["datasources"][0]["tables"][1]
