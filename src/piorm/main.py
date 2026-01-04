"""Refactored Base DB helper moved into package `class_to_sql`.

Purpose: lazily create sqlite3 connection from env var `CLASS_TO_SQL_DB` or
fallback to `<repo-root>/db/peppermint.db`. Provides basic CRUD helpers mirroring
the original API but using parameterized SQL for DML operations.
"""
from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


class Base:
    """Base DB helper. Lazily opens sqlite connection and provides CRUD helpers.

    Keep method names compatible with the original top-level implementation
    so existing call sites can be migrated with minimal friction.
    """

    _conn: Optional[sqlite3.Connection] = None

    @classmethod
    def get_conn(cls) -> sqlite3.Connection:
        """Return a shared sqlite3.Connection, creating it if needed.

        Behavior:
        - If env var `CLASS_TO_SQL_DB` is set, use that path (expanded).
        - Otherwise use repository root `/db/peppermint.db`.
        """
        if cls._conn is not None:
            return cls._conn
        env_path = os.getenv("CLASS_TO_SQL_DB")
        if env_path:
            db_path = Path(env_path).expanduser().resolve()
            # Ensure parent dir exists
            db_path.parent.mkdir(parents=True, exist_ok=True)
        else:
            repo_root = Path(__file__).resolve().parents[2]
            db_dir = repo_root / "db"
            db_dir.mkdir(parents=True, exist_ok=True)
            db_path = db_dir / "peppermint.db"
        cls._conn = sqlite3.connect(str(db_path))
        return cls._conn

    def __init__(self) -> None:
        pass

    @staticmethod
    def create_table(table_name: str, meta_data: Dict[str, Any]) -> None:
        """Create a table using metadata (keeps DDL behavior of original).

        meta_data must include a `super` key with `primary_key` (list) and
        `defaults` (dict) keys to be compatible with original shape.
        """
        try:
            q = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
            for k in meta_data.keys():
                if k in ["super", "meta_data", "table_name"]:
                    continue
                if meta_data[k] == int:
                    if k in meta_data["super"]["defaults"].keys():
                        q += f"{k} INT DEFAULT {meta_data['super']['defaults'][k]},\n"
                    else:
                        q += f"{k} INT,\n"
                elif meta_data[k] in (str, dict):
                    if k in meta_data["super"]["defaults"].keys():
                        q += f"{k} VARCHAR DEFAULT '{meta_data['super']['defaults'][k]}',\n"
                    else:
                        q += f"{k} VARCHAR,\n"
            out = ""
            for i, prim in enumerate(meta_data["super"]["primary_key"]):
                if len(meta_data["super"]["primary_key"]) == 1:
                    out += f"{prim}"
                elif i == len(meta_data["super"]["primary_key"]) - 1:
                    out += f"{prim}"
                else:
                    out += f"{prim},"
            q += f"PRIMARY KEY ({out})\n)"
            Base.manage_table(q, class_name=table_name)
        except Exception:
            raise

    @staticmethod
    def delete_table(table_name: str) -> None:
        try:
            q = f"DROP TABLE {table_name}"
            Base.manage_table(q, class_name=table_name)
        except Exception:
            raise

    @staticmethod
    def read_all(table_name: str) -> Optional[Tuple[Any, ...]]:
        try:
            q = f"SELECT * FROM {table_name}"
            return Base.read_table(q, class_name=table_name)
        except Exception:
            raise

    # CRUD using parameterized queries
    def callback_create(self, table_name: str, data: Dict[str, Any], meta_data: Dict[str, Any]) -> None:
        try:
            self.check_primary_keys(data, meta_data)
            cols: List[str] = []
            placeholders: List[str] = []
            params: List[Any] = []
            for k in data.keys():
                if data[k] in ["", -1, {}]:
                    continue
                cols.append(k)
                placeholders.append("?")
                if meta_data[k] == dict:
                    params.append(self.json_to_str(data[k]))
                else:
                    params.append(data[k])
            if not cols:
                return
            q = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({', '.join(placeholders)})"
            Base.manage_table(q, class_name=table_name, params=params)
        except Exception:
            raise

    def callback_read(self, table_name: str, data: Dict[str, Any], meta_data: Dict[str, Any]) -> Optional[List[Any]]:
        try:
            condition, params = self.get_primary_key_condition(data, meta_data)
            q = f"SELECT * FROM {table_name} WHERE {condition}"
            row = Base.read_table(q, class_name=table_name, params=params)
            if row is None:
                return None
            result: List[Any] = []
            dict_mask: List[bool] = []
            for k in meta_data.keys():
                if k in ["super", "meta_data", "table_name"]:
                    continue
                dict_mask.append(meta_data[k] == dict)
            for i, val in enumerate(row):
                if i < len(dict_mask) and dict_mask[i]:
                    result.append(self.str_to_json(val))
                else:
                    result.append(val)
            return result
        except Exception:
            raise

    def callback_update(self, table_name: str, data: Dict[str, Any], meta_data: Dict[str, Any]) -> None:
        try:
            condition, where_params = self.get_primary_key_condition(data, meta_data)
            assignments: List[str] = []
            params: List[Any] = []
            for k in data.keys():
                if k in meta_data["super"]["primary_key"]:
                    continue
                if meta_data[k] == dict:
                    assignments.append(f"{k} = ?")
                    params.append(self.json_to_str(data[k]))
                else:
                    assignments.append(f"{k} = ?")
                    params.append(data[k])
            if not assignments:
                return
            q = f"UPDATE {table_name} SET {', '.join(assignments)} WHERE {condition}"
            Base.manage_table(q, class_name=table_name, params=params + where_params)
        except Exception:
            raise

    def callback_delete(self, table_name: str, data: Dict[str, Any], meta_data: Dict[str, Any]) -> None:
        try:
            condition, params = self.get_primary_key_condition(data, meta_data)
            q = f"DELETE FROM {table_name} WHERE {condition}"
            Base.manage_table(q, class_name=table_name, params=params)
        except Exception:
            raise

    # Helpers
    @classmethod
    def manage_table(cls, q: str, class_name: str = "", params: Optional[Iterable[Any]] = None) -> None:
        try:
            conn = cls.get_conn()
            cur = conn.cursor()
            if params:
                cur.execute(q, tuple(params))
            else:
                cur.execute(q)
            conn.commit()
        except Exception:
            raise

    @classmethod
    def read_table(cls, q: str, class_name: str = "", params: Optional[Iterable[Any]] = None) -> Optional[Tuple[Any, ...]]:
        try:
            conn = cls.get_conn()
            cur = conn.cursor()
            if params:
                cur.execute(q, tuple(params))
            else:
                cur.execute(q)
            return cur.fetchone()
        except Exception:
            raise

    @staticmethod
    def str_to_json(a: Optional[str]) -> Optional[dict]:
        try:
            if a is None:
                return None
            return json.loads(a)
        except Exception:
            return None

    @staticmethod
    def json_to_str(a: Any) -> str:
        return json.dumps(a)

    def check_primary_keys(self, data: Dict[str, Any], meta_data: Dict[str, Any]) -> None:
        for k in meta_data["super"]["primary_key"]:
            if data.get(k) is None:
                raise AssertionError(f"Error: {k} key is primary and None")

    def get_primary_key_condition(self, data: Dict[str, Any], meta_data: Dict[str, Any]) -> Tuple[str, List[Any]]:
        """Return (condition_sql, params_list) using ? placeholders."""
        condition_parts: List[str] = []
        params: List[Any] = []
        for k in meta_data["super"]["primary_key"]:
            condition_parts.append(f"{k} = ?")
            if meta_data[k] == dict:
                params.append(self.json_to_str(data[k]))
            else:
                params.append(data[k])
        condition = " AND ".join(condition_parts)
        return condition, params
