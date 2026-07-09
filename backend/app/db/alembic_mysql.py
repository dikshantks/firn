"""MySQL-safe Alembic helpers.

SQLAlchemy emits ``IF NOT EXISTS`` / ``IF EXISTS`` for index DDL, but MySQL only
supports those clauses for tables — not standalone ``CREATE INDEX`` or
``DROP INDEX`` statements. Use these helpers in migrations instead of
``op.create_index(..., if_not_exists=True)`` or
``op.drop_index(..., if_exists=True)``.
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import inspect


def _table_indexes(table_name: str) -> set[str]:
    return {idx["name"] for idx in inspect(op.get_bind()).get_indexes(table_name)}


def create_index_if_not_exists(
    index_name: str,
    table_name: str,
    columns: list[str],
    *,
    unique: bool = False,
    **kwargs: object,
) -> None:
    """Create an index when it is missing."""
    if index_name not in _table_indexes(table_name):
        op.create_index(index_name, table_name, columns, unique=unique, **kwargs)


def drop_index_if_exists(index_name: str, table_name: str, **kwargs: object) -> None:
    """Drop an index when it exists."""
    if index_name in _table_indexes(table_name):
        op.drop_index(index_name, table_name=table_name, **kwargs)
