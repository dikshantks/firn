#!/bin/sh
set -eu

if [ "${FERN_USE_DB:-false}" != "true" ]; then
  echo "FERN_USE_DB is not true; skipping Alembic migrations"
  exit 0
fi

python - <<'PY'
import subprocess
import sys

from sqlalchemy import create_engine, inspect, text

from app.config import settings

if not settings.database_url:
    raise SystemExit("DATABASE_URL is required when FERN_USE_DB=true")

REQUIRED_TABLES = frozenset(
    {"catalog_registry", "catalog_summary", "table_health", "jobs"}
)

engine = create_engine(settings.database_url, pool_pre_ping=True, future=True)
inspector = inspect(engine)
tables = {name.lower() for name in inspector.get_table_names()}

if "alembic_version" in tables:
    with engine.connect() as connection:
        version = connection.execute(
            text("SELECT version_num FROM alembic_version LIMIT 1")
        ).scalar_one_or_none()
    if version:
        print(f"Alembic schema already tracked at revision {version}")
        sys.exit(0)

existing = REQUIRED_TABLES & tables
if existing == REQUIRED_TABLES:
    print("Control-plane tables already exist; stamping Alembic head")
    subprocess.run(["alembic", "stamp", "head"], check=True)
elif existing:
    print(
        f"Partial control-plane schema detected "
        f"({len(existing)}/{len(REQUIRED_TABLES)} tables); resuming migration"
    )

subprocess.run(["alembic", "upgrade", "head"], check=True)
PY
