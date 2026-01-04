# Tests for class_to_sql Base helper.
# Tests set CLASS_TO_SQL_DB to a temporary file to avoid interfering with real DB.
import os
from pathlib import Path

import pytest

from piorm.main import Base


@pytest.fixture(autouse=True)
def ensure_tmp_db(tmp_path, monkeypatch):
    db_file = tmp_path / "test_peppermint.db"
    monkeypatch.setenv("CLASS_TO_SQL_DB", str(db_file))
    # ensure directory exists
    db_file.parent.mkdir(parents=True, exist_ok=True)
    yield


def test_create_read_update_delete():
    b = Base()
    meta = {
        'id': int,
        'name': str,
        'meta': dict,
        'super': {
            'primary_key': ['id'],
            'defaults': {}
        }
    }
    # create table
    Base.create_table('users', meta)

    # create
    b.callback_create('users', {'id': 1, 'name': 'Bob', 'meta': {}}, meta)

    # read
    row = b.callback_read('users', {'id': 1}, meta)
    assert row is not None
    # id should be first column and name second (original code used simple ordering)
    assert row[0] == 1
    assert row[1] == 'Bob'

    # update
    b.callback_update('users', {'id': 1, 'name': 'Bobby', 'meta': {}}, meta)
    row2 = b.callback_read('users', {'id': 1}, meta)
    assert row2 is not None
    assert row2[1] == 'Bobby'

    # delete
    b.callback_delete('users', {'id': 1}, meta)
    row3 = b.callback_read('users', {'id': 1}, meta)
    assert row3 is None
