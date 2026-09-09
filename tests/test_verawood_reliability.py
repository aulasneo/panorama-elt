"""Failure, streaming and escaping contracts for scheduled extraction."""
import csv
import logging
from pathlib import Path
from types import SimpleNamespace

import click
import pytest
from botocore.exceptions import ClientError

from panorama_elt import panorama_elt
from panorama_elt.mysql_datasource.mysql_datasource import identifier, save_rows
from test_mysql_datasource import FakeCursor, make_datasource
from test_course_structures_datasource import make_cs


@pytest.mark.parametrize('value', [0, False, '', "O'Reilly", None])
def test_sql_constants_preserve_values(monkeypatch, value):
    cursor = FakeCursor(results=[[]])
    ds = make_datasource(monkeypatch, {'tables': [{'name': 't', 'fields': [
        {'name': 'value', 'value': value}]}]}, cursor)
    list(ds.get_rows('t', ['value']))
    assert cursor.parameters[0] == (() if value is None else (value,))
    assert ('NULL as' in cursor.queries[0]) == (value is None)


@pytest.mark.parametrize('name', ['x; DROP TABLE x', 'a`b', 'a.b', '', None])
def test_identifiers_reject_sql(name):
    with pytest.raises(ValueError):
        identifier(name)


def test_large_export_streams_without_fetchall(monkeypatch, tmp_path):
    cursor = FakeCursor(results=[[(number,) for number in range(10001)]])
    cursor.fetchall = lambda: pytest.fail('Row exports must not buffer all rows')
    ds = make_datasource(monkeypatch, {'tables': [{'name': 't', 'fields': [{'name': 'id'}]}]}, cursor)
    target = tmp_path / 'data.csv'
    save_rows(target, ['id'], ds.get_rows('t', ['id']))
    assert len(target.read_text().splitlines()) == 10002
    assert cursor.closed
    ds.close()


def test_csv_wire_contract(tmp_path):
    target = tmp_path / 'data.csv'
    values = ['Unicode ñ', 'quote"', 'slash\\', 'line\r\nbreak', 0, False, None]
    save_rows(target, list('abcdefg'), iter([values]))
    with target.open() as stream:
        rows = list(csv.reader(stream, doublequote=False, escapechar='\\'))
    assert rows[1] == ['Unicode ñ', 'quote"', 'slash\\\\', 'line\\r\\nbreak', '0', 'False', '']
    assert len(target.read_text().splitlines()) == 2


def test_partition_values_bound_and_fields_not_mutated(monkeypatch):
    settings = {'tables': [{'name': 't', 'fields': [{'name': 'id'}, {'name': 'org'}],
                           'partitions': {'partition_fields': ['org']}}]}
    cursor = FakeCursor(results=[[('O\'Reilly',), (None,)], [(1,)], [(2,)], [], []])
    ds = make_datasource(monkeypatch, settings, cursor)
    ds.extract_and_load()
    ds.extract_and_load()
    assert ds.table_fields['t'] == ['id', 'org']
    assert cursor.parameters[1:3] == [("O'Reilly",), (None,)]
    assert '`org` <=> %s' in cursor.queries[1]


def test_upload_failure_cleans_temporary_file(monkeypatch):
    cursor = FakeCursor(results=[[(1,)]])
    ds = make_datasource(monkeypatch, {'tables': [{'name': 't', 'fields': [{'name': 'id'}]}]}, cursor)
    files = []

    def fail(**kwargs):
        files.append(Path(kwargs['filename']))
        raise RuntimeError('S3 unavailable')
    ds.datalake.upload_table_from_file = fail
    with pytest.raises(RuntimeError, match='S3 unavailable'):
        ds.extract_and_load()
    assert files and not files[0].exists()


@pytest.mark.parametrize('state', ['FAILED', 'CANCELLED', 'RUNNING', 'QUEUED'])
def test_athena_failures_and_timeout_are_not_success(make_datalake, monkeypatch, state):
    dl, _ = make_datalake(capture_queries=False)
    dl.query_athena('SELECT 1')
    dl.athena.get_query_execution = lambda **kw: {'QueryExecution': {'Status': {
        'State': state, 'StateChangeReason': 'test reason'}}}
    monkeypatch.setattr('panorama_elt.panorama_datalake.panorama_datalake.time.sleep', lambda _: None)
    with pytest.raises(RuntimeError, match='test reason'):
        dl.get_athena_executions(max_iter=2)


def test_athena_submission_failure_propagates(make_datalake):
    dl, _ = make_datalake(capture_queries=False)

    def fail(**kwargs):
        raise ClientError({'Error': {'Code': 'AccessDeniedException'}}, 'StartQueryExecution')
    dl.athena.start_query_execution = fail
    with pytest.raises(ClientError):
        dl.query_athena('SELECT 1')


def test_s3_partition_and_athena_literals_agree(make_datalake):
    dl, queries = make_datalake()
    dl.upload_table_from_file('t.csv', 't', field_partitions={'org': "O'Reilly", 'n': 0, 'missing': None},
                              update_partitions=True)
    assert dl.s3_client.uploaded[0][2] == (
        'panorama/t/org=O%27Reilly/n=0/missing=__HIVE_DEFAULT_PARTITION__/t.csv')
    assert "org = 'O''Reilly'" in queries[0]
    assert 'org=O%27Reilly/n=0/missing=__HIVE_DEFAULT_PARTITION__/' in queries[0]


@pytest.mark.parametrize('settings', [
    None, [], {}, {'datalake': {}, 'datasources': {}},
    {'datalake': {}, 'datasources': [{'name': 'x', 'type': 'unknown'}]},
    {'datalake': {}, 'datasources': [{'name': 'x', 'type': 'csv', 'tables': ['bad']}]},
])
def test_malformed_settings_fail(settings):
    with pytest.raises(click.ClickException):
        panorama_elt.validate_settings(settings)


@pytest.mark.parametrize('selectors', [
    {'datasource': 'missing', 'tables': None}, {'datasource': 'x', 'tables': 'missing'},
])
def test_unknown_selectors_never_run_worker(selectors):
    ctx = SimpleNamespace(obj={'settings': {'datasources': [
        {'name': 'x', 'tables': [{'name': 'known'}]}]}})
    with pytest.raises(click.UsageError):
        panorama_elt._dispatch(ctx, lambda *a, **k: pytest.fail('worker called'), False, **selectors)


def test_independent_sources_finish_close_and_check_athena(monkeypatch):
    events = []

    class Source:
        def extract_and_load(self, **kwargs):
            events.append('extract')
            raise RuntimeError('table missing')

        def close(self):
            events.append('close')
    monkeypatch.setattr(panorama_elt, '_get_datasource', lambda *args: Source())
    dl = SimpleNamespace(get_athena_executions=lambda: events.append('athena'))
    ctx = SimpleNamespace(obj={'datalake': dl, 'settings': {'datasources': [{'name': 'a'}, {'name': 'b'}]}})
    with pytest.raises(RuntimeError, match='table missing'):
        panorama_elt._extract_and_load(ctx)
    assert events == ['extract', 'close', 'extract', 'close', 'athena']


def test_debug_logs_omit_database_passwords(monkeypatch, caplog):
    with caplog.at_level(logging.DEBUG):
        make_cs(monkeypatch, settings={
            'mongodb_password': 'mongo-sentinel', 'mysql_host': 'localhost', 'mysql_password': 'mysql-sentinel',
        })
    assert 'mongo-sentinel' not in caplog.text
    assert 'mysql-sentinel' not in caplog.text


def test_zero_problem_weight_does_not_load_definition(monkeypatch):
    ds = make_cs(monkeypatch)
    active = {'course-v1:o+c+r': {'published_branch': 'b', 'org': 'o', 'course': 'c', 'run': 'r'}}
    structure = {'b': {'blocks': [
        {'block_id': 'root', 'block_type': 'course', 'fields': {}},
        {'block_id': 'p', 'block_type': 'problem', 'fields': {'weight': 0}},
    ]}}
    blocks = ds.get_blocks(structure, active)
    assert blocks['block-v1:o+c+r+type@problem+block@p']['weight'] == 0
    structure['b']['blocks'][1]['fields']['weight'] = None
    structure['b']['blocks'][1]['definition'] = '0' * 24
    with pytest.raises(RuntimeError, match='Missing problem definition'):
        ds.get_blocks(structure, active)


@pytest.mark.parametrize('table', [
    {'name': 't', 'fields': 'invalid'},
    {'name': 't', 'fields': ['invalid']},
    {'name': 't', 'partitions': ['invalid']},
    {'name': 't', 'partitions': {'partition_fields': 'invalid'}},
    {'name': 't', 'partitions': {'interval': '1 day; drop table t', 'timestamp_field': 'created'}},
    {'name': 't', 'fields': [{'name': 'id'}], 'partitions': {'partition_fields': ['missing']}},
])
def test_mysql_config_rejected_before_connecting(table):
    with pytest.raises(click.ClickException):
        panorama_elt.validate_settings({'datalake': {}, 'datasources': [
            {'name': 'source', 'type': 'mysql', 'tables': [table]},
        ]})


def test_valid_mysql_partition_configuration():
    panorama_elt.validate_settings({'datalake': {}, 'datasources': [
        {'name': 'source', 'type': 'mysql', 'tables': [
            {'name': 'events', 'fields': [{'name': 'id'}, {'name': 'org'}],
             'partitions': {'partition_fields': ['org'], 'interval': '1 day', 'timestamp_field': 'created'}},
        ]},
    ]})


def test_real_xlsx_stream_and_failure_cleanup(tmp_path):
    import openpyxl
    from panorama_elt.xls_datasource.xls_datasource import XLSDatasource

    source = tmp_path / 'source.xlsx'
    workbook = openpyxl.Workbook()
    workbook.active.append(['id', 'name'])
    workbook.active.append([0, 'Niño'])
    workbook.save(source)
    workbook.close()
    paths = []

    def upload(**kwargs):
        path = Path(kwargs['filename'])
        paths.append(path)
        assert path.read_text().splitlines() == ['id,name', '0,Niño']
        assert kwargs['s3_filename'] == 'Sheet.csv'
        raise RuntimeError('upload unavailable')

    datasource = XLSDatasource(SimpleNamespace(upload_table_from_file=upload), {'location': str(source)})
    with pytest.raises(RuntimeError, match='upload unavailable'):
        datasource.extract_and_load()
    assert paths and not paths[0].exists()


def test_mysql_rsa_authentication_crypto_compatibility():
    from cryptography.hazmat.primitives.asymmetric import padding, rsa
    from cryptography.hazmat.primitives import hashes, serialization
    from pymysql._auth import sha2_rsa_encrypt

    private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public = private.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
    password, salt = b"special:'@\\credential", b'01234567890123456789'
    encrypted = sha2_rsa_encrypt(password, salt, public)
    decrypted = private.decrypt(encrypted, padding.OAEP(
        mgf=padding.MGF1(algorithm=hashes.SHA1()), algorithm=hashes.SHA1(), label=None))
    assert decrypted == bytes(value ^ salt[index % len(salt)] for index, value in enumerate(password + b'\0'))
