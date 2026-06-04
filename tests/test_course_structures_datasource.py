"""Tests for the Open edX course structures datasource logic."""
import types

import pymysql

import panorama_elt.course_structures_datasource.course_structures_datasource as mod


class FakeFind:
    """Mimics a pymongo cursor: iterable and index-accessible."""

    def __init__(self, docs):
        self.docs = list(docs)

    def __iter__(self):
        return iter(self.docs)

    def __getitem__(self, index):
        return self.docs[index]


class FakeCollection:
    def __init__(self, docs=None):
        self._docs = list(docs or [])

    def find(self, _query=None):
        return FakeFind(self._docs)


class FakeModulestore:
    def __init__(self, structures=None, active_versions=None, definitions=None):
        self.structures = FakeCollection(structures)
        self.active_versions = FakeCollection(active_versions)
        self.definitions = FakeCollection(definitions)


class FakeMongoDB:
    def __init__(self, modulestore=None):
        self.modulestore = modulestore or FakeModulestore()

    def get_collection(self, name):
        return getattr(self, name, None)


class FakeCursor:
    def __init__(self, rows=None):
        self.rows = list(rows or [])

    def execute(self, _query):
        pass

    def fetchall(self):
        return self.rows


def make_cs(monkeypatch, settings=None, mongodb=None, rows=None):
    settings = settings or {}
    mongodb = mongodb or FakeMongoDB()

    class FakeClient:
        def __getitem__(self, _name):
            return mongodb

    monkeypatch.setattr(mod, "MongoClient", lambda **_kwargs: FakeClient())
    if settings.get("mysql_host"):
        conn = types.SimpleNamespace(cursor=lambda: FakeCursor(rows))
        monkeypatch.setattr(pymysql, "connect", lambda **_kwargs: conn)

    return mod.CourseStructuresDatasource(datalake=None, datasource_settings=settings)


def test_get_tables(monkeypatch):
    ds = make_cs(monkeypatch)
    assert ds.get_tables() == ["course_structures"]


def test_test_connections_ok(monkeypatch):
    ds = make_cs(monkeypatch)
    assert ds.test_connections() == {"MongoDB": "OK"}


def test_get_structures_keys_by_id(monkeypatch):
    structures = [{"_id": "BR1", "blocks": []}, {"_id": "BR2", "blocks": []}]
    ds = make_cs(monkeypatch, mongodb=FakeMongoDB(FakeModulestore(structures=structures)))

    active_versions = {"c1": {"published_branch": "BR1"}, "c2": {"published_branch": "BR2"}}
    result = ds.get_structures(active_versions)
    assert set(result.keys()) == {"BR1", "BR2"}
    assert result["BR1"]["_id"] == "BR1"


def test_get_active_versions_mongodb(monkeypatch):
    docs = [{
        "versions": {"published-branch": "BR1"},
        "org": "edX", "course": "DemoX", "run": "2024",
    }]
    ds = make_cs(monkeypatch, mongodb=FakeMongoDB(FakeModulestore(active_versions=docs)))

    result = ds.get_active_versions_mongodb()
    assert "course-v1:edX+DemoX+2024" in result
    entry = result["course-v1:edX+DemoX+2024"]
    assert entry["published_branch"] == "BR1"
    assert entry["org"] == "edX"


def test_get_active_versions_from_mysql(monkeypatch):
    rows = [("0" * 24, "course-v1:edX+DemoX+2024"), ("1" * 24, "library-v1:edX+lib")]
    ds = make_cs(monkeypatch, settings={"mysql_host": "db"}, rows=rows)

    result = ds.get_active_versions()
    # only the course (not the library) is kept
    assert list(result.keys()) == ["course-v1:edX+DemoX+2024"]
    entry = result["course-v1:edX+DemoX+2024"]
    assert entry["org"] == "edX"
    assert entry["course"] == "DemoX"
    assert entry["run"] == "2024"


def _course_structure():
    course_id = "course-v1:edX+DemoX+2024"
    structure = {"blocks": [
        {"block_id": "course", "block_type": "course",
         "fields": {"display_name": "Demo", "children": [["chapter", "ch1"], ["problem", "p1"]]}},
        {"block_id": "ch1", "block_type": "chapter",
         "fields": {"display_name": "Chapter 1", "children": []}},
        {"block_id": "p1", "block_type": "problem",
         "fields": {"display_name": "Problem 1", "children": [], "weight": 3}},
    ]}
    return course_id, structure


def test_get_blocks_builds_module_locations_and_parents(monkeypatch):
    course_id, structure = _course_structure()
    ds = make_cs(monkeypatch)

    active_versions = {course_id: {
        "published_branch": "BR1", "org": "edX", "course": "DemoX", "run": "2024",
    }}
    blocks = ds.get_blocks(course_structures={"BR1": structure}, active_versions=active_versions)

    chapter_loc = "block-v1:edX+DemoX+2024+type@chapter+block@ch1"
    problem_loc = "block-v1:edX+DemoX+2024+type@problem+block@p1"

    assert course_id in blocks
    assert chapter_loc in blocks
    # course root block keeps the bare course id as its location
    assert blocks[course_id]["block_type"] == "course"
    # parents are filled in from the tree
    assert blocks[chapter_loc]["parent"] == course_id
    assert blocks[chapter_loc]["course"] == "Demo"
    # an explicit weight on a problem is preserved without a definitions lookup
    assert blocks[problem_loc]["weight"] == 3
    # leaf component gets its display name as component_name
    assert blocks[problem_loc]["component_name"] == "Problem 1"


def test_get_blocks_counts_problem_weight_from_definition(monkeypatch):
    course_id = "course-v1:edX+DemoX+2024"
    structure = {"blocks": [
        {"block_id": "course", "block_type": "course",
         "fields": {"display_name": "Demo", "children": []}},
        {"block_id": "p1", "block_type": "problem", "definition": "0" * 24,
         "fields": {"display_name": "P", "children": [], "weight": None}},
    ]}
    definitions = [{"fields": {"data": "<numericalresponse/><choiceresponse/>"}}]
    ds = make_cs(monkeypatch, mongodb=FakeMongoDB(FakeModulestore(definitions=definitions)))

    active_versions = {course_id: {
        "published_branch": "BR1", "org": "edX", "course": "DemoX", "run": "2024",
    }}
    blocks = ds.get_blocks(course_structures={"BR1": structure}, active_versions=active_versions)

    problem_loc = "block-v1:edX+DemoX+2024+type@problem+block@p1"
    # two response tags in the definition data -> weight of 2
    assert blocks[problem_loc]["weight"] == 2


def test_get_blocks_skips_missing_structure(monkeypatch):
    ds = make_cs(monkeypatch)
    active_versions = {"course-v1:edX+DemoX+2024": {"published_branch": "BR_missing"}}
    blocks = ds.get_blocks(course_structures={}, active_versions=active_versions)
    assert blocks == {}


def test_extract_and_load_writes_and_uploads(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    _course_id, structure = _course_structure()
    active_versions_doc = [{
        "versions": {"published-branch": "BR1"},
        "org": "edX", "course": "DemoX", "run": "2024",
    }]
    structures = [dict(structure, _id="BR1")]
    mongodb = FakeMongoDB(FakeModulestore(structures=structures, active_versions=active_versions_doc))

    uploads = []
    ds = make_cs(monkeypatch, mongodb=mongodb)
    ds.datalake = types.SimpleNamespace(
        upload_table_from_file=lambda **kwargs: uploads.append(kwargs))

    ds.extract_and_load()

    assert uploads == [{
        "filename": "course_structures.csv",
        "table": "course_structures",
        "update_partitions": True,
    }]
    # the temporary csv is cleaned up after upload
    assert not (tmp_path / "course_structures.csv").exists()


def test_extract_and_load_skips_unselected_table(monkeypatch):
    ds = make_cs(monkeypatch)
    called = []
    ds.get_active_versions_mongodb = lambda: called.append("queried")
    ds.extract_and_load(selected_tables="other_table")
    assert called == []


def test_extract_and_load_returns_without_active_versions(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    ds = make_cs(monkeypatch)
    ds.get_active_versions_mongodb = lambda: {}
    uploads = []
    ds.datalake = types.SimpleNamespace(
        upload_table_from_file=lambda **kwargs: uploads.append(kwargs))

    ds.extract_and_load()
    assert uploads == []
    assert not (tmp_path / "course_structures.csv").exists()
