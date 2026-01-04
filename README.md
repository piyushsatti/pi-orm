# class-to-sql

Tiny helper to map simple Python class-like metadata to SQLite tables. This repo contains
a minimal, easy-to-read implementation suitable for small projects and examples.

Quickstart
----------

1. (Optional) set the database location:

```bash
export CLASS_TO_SQL_DB=$(pwd)/db/peppermint.db
```

2. Run the example or import in Python (ensure `src/` is on `PYTHONPATH` or install editable):

```bash
python examples/basic.py
```

or in Python:

```py
from class_to_sql.base_dataclass import Base

base = Base()
meta = {
	'id': int,
	'name': str,
	'super': {'primary_key': ['id'], 'defaults': {}}
}
Base.create_table('users', meta)
base.callback_create('users', {'id': 1, 'name': 'Alice'}, meta)
```

Development
-----------

Create a virtual environment and install test deps:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install black ruff
```

Run linter/formatter checks and tests:

```bash
ruff check .
black --check .
pytest -q
```

Notes
-----

- By default the package writes the SQLite DB to `db/peppermint.db` in the repository root when `CLASS_TO_SQL_DB` is not set.
- The implementation uses parameterized SQL for DML to reduce injection risks. DDL still interpolates table/column names and should be used with trusted metadata.
- Tests use a temporary DB file and set `CLASS_TO_SQL_DB` to keep runs hermetic.
