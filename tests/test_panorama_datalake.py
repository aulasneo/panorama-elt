"""Tests for PanoramaDatalake query building and S3 key construction."""
import pytest


def test_init_parses_base_partitions_and_prefix(make_datalake):
    datalake, _ = make_datalake({
        "base_partitions": [{"name": "lms", "value": "example.com"}],
        "base_prefix": "openedx",
    })
    assert datalake.base_partitions == {"lms": "example.com"}
    assert datalake.base_prefix == "openedx"
    assert datalake.panorama_raw_data_bucket == "test-bucket"


def test_init_with_access_key_creates_session(patch_boto3):
    from panorama_elt.panorama_datalake.panorama_datalake import PanoramaDatalake

    PanoramaDatalake({
        "panorama_raw_data_bucket": "b",
        "aws_access_key": "AKIA",
        "aws_secret_access_key": "secret",
        "aws_region": "eu-west-1",
    })
    assert patch_boto3, "boto3.Session should have been created"


def test_init_without_bucket_exits(patch_boto3):
    from panorama_elt.panorama_datalake.panorama_datalake import PanoramaDatalake

    with pytest.raises(SystemExit):
        PanoramaDatalake({})


def test_query_athena_skips_without_db_or_workgroup(patch_boto3):
    from panorama_elt.panorama_datalake.panorama_datalake import PanoramaDatalake

    datalake = PanoramaDatalake({"panorama_raw_data_bucket": "b"})
    datalake.query_athena("SHOW DATABASES")
    assert datalake.executions == []


def test_query_athena_records_execution(make_datalake):
    datalake, _ = make_datalake(capture_queries=False)
    datalake.query_athena("SELECT 1")
    assert len(datalake.executions) == 1
    assert datalake.athena.started[0]["QueryString"] == "SELECT 1"


def test_update_partitions_builds_alter_table_query(make_datalake):
    datalake, queries = make_datalake({
        "base_partitions": [{"name": "lms", "value": "example.com"}],
    })
    datalake.update_partitions(table="enrollments", field_partitions={"org": "edX"})

    assert len(queries) == 1
    query = queries[0]
    assert "ALTER TABLE panorama_raw_enrollments ADD IF NOT EXISTS PARTITION" in query
    assert "lms = 'example.com'" in query
    assert "org = 'edX'" in query
    assert "s3://test-bucket/panorama/enrollments/lms=example.com/org=edX/" in query


def test_update_partitions_uses_explicit_table_name(make_datalake):
    datalake, queries = make_datalake({
        "base_partitions": [{"name": "lms", "value": "example.com"}],
    })
    datalake.update_partitions(table="enrollments", datalake_table_name="custom_table")
    assert "ALTER TABLE custom_table" in queries[0]


def test_upload_table_from_file_builds_key_and_uploads(make_datalake):
    datalake, _ = make_datalake({
        "base_partitions": [{"name": "lms", "value": "example.com"}],
    })
    datalake.upload_table_from_file(filename="enrollments.csv", table="enrollments")

    assert datalake.s3_client.uploaded == [(
        "enrollments.csv",
        "test-bucket",
        "panorama/enrollments/lms=example.com/enrollments.csv",
    )]


def test_upload_table_from_file_without_base_prefix(patch_boto3):
    from panorama_elt.panorama_datalake.panorama_datalake import PanoramaDatalake

    datalake = PanoramaDatalake({"panorama_raw_data_bucket": "test-bucket"})
    datalake.upload_table_from_file(filename="data.csv", table="t")
    assert datalake.s3_client.uploaded[0][2] == "t/data.csv"


def test_upload_table_from_file_updates_partitions_when_requested(make_datalake):
    datalake, _ = make_datalake({
        "base_partitions": [{"name": "lms", "value": "example.com"}],
    })
    calls = []
    datalake.update_partitions = lambda **kwargs: calls.append(kwargs)

    datalake.upload_table_from_file(
        filename="enrollments.csv", table="enrollments",
        field_partitions={"org": "edX"}, update_partitions=True,
    )
    assert calls == [{"table": "enrollments", "field_partitions": {"org": "edX"}}]


def test_upload_table_from_file_skips_partition_update_without_partitions(patch_boto3):
    from panorama_elt.panorama_datalake.panorama_datalake import PanoramaDatalake

    datalake = PanoramaDatalake({"panorama_raw_data_bucket": "test-bucket"})
    calls = []
    datalake.update_partitions = lambda **kwargs: calls.append(kwargs)

    datalake.upload_table_from_file(filename="d.csv", table="t", update_partitions=True)
    assert calls == []


def test_create_datalake_table_removes_partition_fields(make_datalake):
    datalake, queries = make_datalake({
        "base_partitions": [{"name": "lms", "value": "example.com"}],
    })
    fields = ["id", "name", "created"]
    datalake.create_datalake_table(
        table="users", fields=fields, field_partitions=["created"],
    )

    query = queries[0]
    assert "`id` string" in query
    assert "`name` string" in query
    assert "PARTITIONED BY" in query
    assert "`created` string" in query  # appears in partition section
    assert "`lms` string" in query
    # 'created' was removed from the column list and only kept as a partition
    assert query.count("`created` string") == 1


def test_create_datalake_table_without_partitions(make_datalake):
    datalake, queries = make_datalake()
    datalake.create_datalake_table(table="t", fields=["a"], datalake_table="explicit")
    query = queries[0]
    assert "`explicit`" in query
    assert "PARTITIONED BY" not in query


def test_drop_table_and_view(make_datalake):
    datalake, queries = make_datalake()
    datalake.drop_datalake_table("panorama_raw_users")
    datalake.drop_datalake_view("panorama_table_users")
    assert "DROP TABLE `panorama_raw_users`" in queries[0]
    assert 'DROP VIEW "panorama_table_users"' in queries[1]


@pytest.mark.parametrize("mysql_type,expected", [
    ("BIGINT", 'TRY_CAST("n" AS BIGINT) "n"'),
    ("DOUBLE", 'TRY_CAST("n" AS DOUBLE) "n"'),
    ("DATETIME", 'TRY("date_parse"("n"'),
    ("JSON", 'NULLIF("n"'),
    ("VARCHAR", 'NULLIF("n"'),
    ("CUSTOMTYPE", 'TRY_CAST("n" AS CUSTOMTYPE) "n"'),
])
def test_create_table_view_type_mapping(make_datalake, mysql_type, expected):
    datalake, queries = make_datalake()
    datalake.create_table_view(
        datalake_table_name="raw", view_name="v",
        fields=[{"name": "n", "type": mysql_type}],
    )
    assert expected in queries[0]
    assert 'CREATE OR REPLACE VIEW "v"' in queries[0]


def test_create_table_view_appends_base_partitions(make_datalake):
    datalake, queries = make_datalake({
        "base_partitions": [{"name": "lms", "type": "string"}],
    })
    datalake.create_table_view(
        datalake_table_name="raw", view_name="v",
        fields=[{"name": "id", "type": "INT"}],
    )
    assert '"lms"' in queries[0]
    assert '"id"' in queries[0]


def test_get_athena_executions_counts_states(make_datalake, monkeypatch):
    import panorama_elt.panorama_datalake.panorama_datalake as mod
    monkeypatch.setattr(mod.time, "sleep", lambda _seconds: None)

    datalake, _ = make_datalake(capture_queries=False)
    datalake.executions = [{"QueryExecutionId": "1"}, {"QueryExecutionId": "2"}]
    datalake.get_athena_query_execution = lambda execution: "SUCCEEDED"

    results = datalake.get_athena_executions(max_iter=3)
    assert results == {"SUCCEEDED": 2}
