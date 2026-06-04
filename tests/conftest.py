"""Shared pytest fixtures and fakes for the Panorama ELT test suite."""
import boto3
import pytest


class FakeS3Client:
    """Records S3 calls so tests can assert on uploads without touching AWS."""

    def __init__(self):
        self.uploaded = []
        self.put_objects = []
        self.deleted = []

    def upload_file(self, filename, bucket, key):
        self.uploaded.append((filename, bucket, key))

    def put_object(self, **kwargs):
        self.put_objects.append(kwargs)
        return {"ok": True}

    def delete_object(self, **kwargs):
        self.deleted.append(kwargs)
        return {"ok": True}


class FakeAthenaClient:
    """Athena stub that reports queries as SUCCEEDED and returns the configured database."""

    def __init__(self, databases=("panorama",)):
        self.started = []
        self.databases = list(databases)

    def start_query_execution(self, **kwargs):
        self.started.append(kwargs)
        return {"QueryExecutionId": "exec-{}".format(len(self.started))}

    def get_query_execution(self, **_kwargs):
        return {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}

    def get_query_results(self, **_kwargs):
        rows = [{"Data": [{"VarCharValue": name}]} for name in self.databases]
        return {"ResultSet": {"Rows": rows}}


class FakeSession:
    """Replacement for boto3.Session that hands back fake clients."""

    def __init__(self, **_kwargs):
        self.s3 = FakeS3Client()
        self.athena = FakeAthenaClient()

    def client(self, service, **_kwargs):
        if service == "s3":
            return self.s3
        if service == "athena":
            return self.athena
        raise AssertionError("Unexpected client requested: {}".format(service))


@pytest.fixture
def patch_boto3(monkeypatch):
    """Patch boto3.Session so PanoramaDatalake can be instantiated offline."""
    created = []

    def factory(**kwargs):
        session = FakeSession(**kwargs)
        created.append(session)
        return session

    monkeypatch.setattr(boto3, "Session", factory)
    return created


@pytest.fixture
def make_datalake(patch_boto3):
    """Factory returning a PanoramaDatalake whose query_athena calls are captured.

    Returns a tuple of (datalake, queries_list). The Athena call path is replaced
    with a list-append so tests can assert on the SQL that would be issued.
    """
    from panorama_elt.panorama_datalake.panorama_datalake import PanoramaDatalake

    def _make(settings=None, capture_queries=True):
        base = {
            "panorama_raw_data_bucket": "test-bucket",
            "base_prefix": "panorama",
            "datalake_database": "panorama",
            "datalake_workgroup": "primary",
        }
        if settings:
            base.update(settings)
        datalake = PanoramaDatalake(base)
        queries = []
        if capture_queries:
            # query_athena is called both positionally and as query=... across the
            # codebase, so accept either form.
            datalake.query_athena = lambda query: queries.append(query)
        return datalake, queries

    return _make
