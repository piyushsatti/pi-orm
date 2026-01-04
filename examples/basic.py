# Minimal example showing usage of Base from `class_to_sql`.
# Run this example after adding `src/` to PYTHONPATH or installing the package.
import os
from piorm.main import Base

# Optionally override location with environment variable
# os.environ['CLASS_TO_SQL_DB'] = '/tmp/example_peppermint.db'

base = Base()

meta = {
    'id': int,
    'name': str,
    'super': {
        'primary_key': ['id'],
        'defaults': {}
    }
}

# Create table and perform a simple insert/read
Base.create_table('users', meta)
base.callback_create('users', {'id': 1, 'name': 'Alice'}, meta)
row = base.callback_read('users', {'id': 1}, meta)
print('Read row:', row)
