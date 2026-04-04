import os
import tempfile
import pytest
from workflows.tasks import TaskStore


@pytest.fixture
def ts():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    store = TaskStore(path)
    yield store
    store.close()
    os.unlink(path)


def test_create_and_get(ts):
    task = ts.create(name="My Task", description="Do stuff")
    assert task["name"] == "My Task"
    assert task["description"] == "Do stuff"
    assert task["status"] == "pending"
    assert task["task_id"]

    got = ts.get(task["task_id"])
    assert got["name"] == "My Task"


def test_list(ts):
    ts.create(name="A")
    ts.create(name="B")
    tasks = ts.list()
    assert len(tasks) == 2
    names = {t["name"] for t in tasks}
    assert names == {"A", "B"}


def test_update(ts):
    task = ts.create(name="Old", labels={"env": "dev"})
    updated = ts.update(
        task["task_id"], name="New", labels={"env": "prod", "team": "core"}
    )
    assert updated["name"] == "New"
    assert updated["labels"] == {"env": "prod", "team": "core"}


def test_update_status(ts):
    task = ts.create(name="Task")
    assert task["status"] == "pending"
    updated = ts.update(task["task_id"], status="finished")
    assert updated["status"] == "finished"


def test_update_needs_input(ts):
    task = ts.create(name="Task")
    assert task["needs_input"] is False
    updated = ts.update(task["task_id"], needs_input=True)
    assert updated["needs_input"] is True


def test_update_color(ts):
    task = ts.create(name="Card")
    updated = ts.update(task["task_id"], color="blue")
    assert updated["color"] == "blue"


def test_delete(ts):
    task = ts.create(name="Gone")
    ts.delete(task["task_id"])
    assert ts.list() == []
    with pytest.raises(KeyError):
        ts.get(task["task_id"])


def test_find_by_prefix(ts):
    task = ts.create(name="Findable")
    prefix = task["task_id"][:6]
    found = ts.find_by_prefix(prefix)
    assert found["task_id"] == task["task_id"]


def test_find_by_prefix_not_found(ts):
    with pytest.raises(KeyError):
        ts.find_by_prefix("nonexistent")


def test_labels_default_empty(ts):
    task = ts.create(name="No Labels")
    assert task["labels"] == {}


def test_needs_input_default_false(ts):
    task = ts.create(name="No Input")
    assert task["needs_input"] is False
