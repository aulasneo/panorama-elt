"""Tests for the MySQL datasource: CSV serialisation and cursor-driven queries."""
import datetime
import types

import pymysql
import pytest


class FakeCursor:
    """Records executed SQL and returns queued fetchall results in order."""

    def __init__(self, results=None):
        self.results = list(results or [])
        self.queries = []

    def execute(self, query):
        self.queries.append(query)

    def fetchall(self):
        return self.results.pop(0) if self.results else []


class ErrorOnceCursor(FakeCursor):
    """Raises a MySQL error on the first query, then behaves like FakeCursor."""

    def __init__(self, results=None):
        super().__init__(results=results)
        self.raise_next = True

    def execute(self, query):
        if self.raise_next:
            self.raise_next = False
            raise pymysql.err.ProgrammingError("missing table")
        super().execute(query)


class FakeDatalake:
    def __init__(self):
        self.uploads = []

    def upload_table_from_file(self, **kwargs):
        self.uploads.append(kwargs)


def make_datasource(monkeypatch, settings, cursor):
    conn = types.SimpleNamespace(cursor=lambda: cursor)
    monkeypatch.setattr(pymysql, "connect", lambda **_kwargs: conn)
    from panorama_elt.mysql_datasource.mysql_datasource import MySQLDatasource
    return MySQLDatasource(datalake=FakeDatalake(), datasource_settings=settings)


def test_init_exits_on_connection_error(monkeypatch):
    def boom(**_kwargs):
        raise pymysql.err.OperationalError("no db")

    monkeypatch.setattr(pymysql, "connect", boom)
    from panorama_elt.mysql_datasource.mysql_datasource import MySQLDatasource
    with pytest.raises(SystemExit):
        MySQLDatasource(datalake=None, datasource_settings={})


def test_save_rows_escapes_and_formats(tmp_path):
    from panorama_elt.mysql_datasource.mysql_datasource import save_rows

    target = tmp_path / "out.csv"
    rows = [
        ("back\\slash", "line\nbreak", datetime.datetime(2022, 5, 1, 12, 0, 0)),
    ]
    save_rows(str(target), ["a", "b", "c"], rows)

    content = target.read_text()
    lines = content.splitlines()
    # The embedded real newline must be escaped, so the row stays on a single line.
    assert len(lines) == 2
    assert lines[0] == "a,b,c"
    assert "2022-05-01 12:00:00.000000" in lines[1]
    # the backslash workaround doubles backslashes rather than emitting a raw newline
    assert "\\\\" in lines[1]
    assert "slash" in lines[1] and "break" in lines[1]


def test_get_tables(monkeypatch):
    cursor = FakeCursor(results=[[("users",), ("enrollments",)]])
    ds = make_datasource(monkeypatch, {}, cursor)
    assert ds.get_tables() == ["users", "enrollments"]
    assert cursor.queries == ["SHOW TABLES"]


def test_get_fields_returns_cached_definition(monkeypatch):
    settings = {"tables": [{"name": "users", "fields": [{"name": "id"}, {"name": "email"}]}]}
    cursor = FakeCursor()
    ds = make_datasource(monkeypatch, settings, cursor)
    assert ds.get_fields("users") == ["id", "email"]
    # cached path must not hit the database
    assert cursor.queries == []


def test_get_fields_queries_information_schema(monkeypatch):
    cursor = FakeCursor(results=[[("id", "int"), ("email", "varchar")]])
    ds = make_datasource(monkeypatch, {"mysql_database": "edxapp"}, cursor)
    fields = ds.get_fields("users", force_query=True)
    assert fields == [{"name": "id", "type": "int"}, {"name": "email", "type": "varchar"}]
    assert "INFORMATION_SCHEMA.COLUMNS" in cursor.queries[0]
    assert 'TABLE_NAME = "users"' in cursor.queries[0]


def test_get_rows_without_field_list_selects_star(monkeypatch):
    cursor = FakeCursor(results=[[(1,)]])
    ds = make_datasource(monkeypatch, {}, cursor)
    rows = ds.get_rows("users")
    assert rows == [(1,)]
    query = cursor.queries[0]
    assert "*" in query and "from users" in query


def test_get_rows_builds_field_statements_with_constants(monkeypatch):
    settings = {"tables": [{"name": "users", "fields": [
        {"name": "id"},
        {"name": "source", "value": "edx", "type": "VARCHAR"},
        {"name": "active", "value": 1, "type": "INT"},
        {"name": "empty", "value": None},
    ]}]}
    cursor = FakeCursor(results=[[]])
    ds = make_datasource(monkeypatch, settings, cursor)

    ds.get_rows("users", field_list=["id", "source", "active", "empty"], where="id > 0", distinct=True)
    query = cursor.queries[0]
    assert "select distinct" in query
    assert "`id`" in query
    assert "'edx' as `source`" in query
    assert "1 as `active`" in query
    assert "NULL as `empty`" in query
    assert "where id > 0" in query


def test_test_connections(monkeypatch):
    cursor = FakeCursor(results=[[("edxapp",), ("mysql",)]])
    ds = make_datasource(monkeypatch, {"mysql_database": "edxapp"}, cursor)
    assert ds.test_connections() == {"MySQL": "OK"}

    cursor2 = FakeCursor(results=[[("other",)]])
    ds2 = make_datasource(monkeypatch, {"mysql_database": "edxapp"}, cursor2)
    assert ds2.test_connections() == {"MySQL": "DB not found"}


def test_extract_and_load_without_partitions(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    settings = {"tables": [{"name": "users", "fields": [{"name": "id"}, {"name": "email"}]}]}
    # one fetchall result for the get_rows call
    cursor = FakeCursor(results=[[(1, "a@x.com"), (2, "b@x.com")]])
    ds = make_datasource(monkeypatch, settings, cursor)

    ds.extract_and_load()

    assert ds.datalake.uploads == [{
        "filename": "users.csv",
        "table": "users",
        "update_partitions": True,
    }]
    # the temporary csv is cleaned up after upload
    assert not (tmp_path / "users.csv").exists()


def test_extract_and_load_uses_configured_s3_table(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    settings = {"tables": [{
        "name": "mdl_course",
        "datalake_s3_table": "course",
        "fields": [{"name": "id"}, {"name": "fullname"}],
    }]}
    cursor = FakeCursor(results=[[(1, "Course")]])
    ds = make_datasource(monkeypatch, settings, cursor)

    ds.extract_and_load()

    assert "from mdl_course" in cursor.queries[0]
    assert ds.datalake.uploads == [{
        "filename": "mdl_course.csv",
        "table": "mdl_course",
        "update_partitions": True,
        "s3_table": "course",
        "s3_filename": "course.csv",
    }]
    assert not (tmp_path / "mdl_course.csv").exists()


def test_extract_and_load_skips_unselected_tables(monkeypatch):
    settings = {"tables": [{"name": "users", "fields": [{"name": "id"}]}]}
    cursor = FakeCursor()
    ds = make_datasource(monkeypatch, settings, cursor)
    ds.extract_and_load(selected_tables="other")
    assert ds.datalake.uploads == []


def test_extract_and_load_logs_mysql_errors_and_continues(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    settings = {"tables": [
        {"name": "missing", "fields": [{"name": "id"}]},
        {"name": "users", "fields": [{"name": "id"}, {"name": "email"}]},
    ]}
    cursor = ErrorOnceCursor(results=[[(1, "a@x.com")]])
    ds = make_datasource(monkeypatch, settings, cursor)

    ds.extract_and_load()

    assert ds.datalake.uploads == [{
        "filename": "users.csv",
        "table": "users",
        "update_partitions": True,
    }]
    assert not (tmp_path / "missing.csv").exists()
    assert not (tmp_path / "users.csv").exists()


def test_extract_and_load_with_partitions(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    settings = {"tables": [{
        "name": "events",
        "fields": [{"name": "id"}, {"name": "org"}, {"name": "created"}],
        "partitions": {"partition_fields": ["org"], "interval": "1 day", "timestamp_field": "created"},
    }]}
    # fetchall order: distinct partition values, then the rows for each partition
    cursor = FakeCursor(results=[
        [("edX",), ("mit",)],
        [(1, "2024-01-01")],
        [(2, "2024-01-02")],
    ])
    ds = make_datasource(monkeypatch, settings, cursor)

    ds.extract_and_load()

    assert ds.datalake.uploads == [
        {"filename": "events.csv", "table": "events",
         "field_partitions": {"org": "edX"}, "update_partitions": True},
        {"filename": "events.csv", "table": "events",
         "field_partitions": {"org": "mit"}, "update_partitions": True},
    ]
    # the incremental query uses the configured interval on the timestamp field
    assert any("created >= date_sub(now(), interval 1 day)" in q for q in cursor.queries)
    # the per-partition csv is cleaned up after each upload
    assert not (tmp_path / "events.csv").exists()


def test_extract_and_load_with_partitions_force_does_full_dump(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    settings = {"tables": [{
        "name": "events",
        "fields": [{"name": "id"}, {"name": "org"}],
        "partitions": {"partition_fields": ["org"], "interval": "1 day", "timestamp_field": "created"},
    }]}
    cursor = FakeCursor(results=[[("edX",)], [(1,)]])
    ds = make_datasource(monkeypatch, settings, cursor)

    ds.extract_and_load(force=True)

    # forcing a full dump means no incremental interval clause is used
    assert not any("date_sub" in q for q in cursor.queries)
    assert ds.datalake.uploads[0]["field_partitions"] == {"org": "edX"}
