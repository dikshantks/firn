"""create control plane tables

Revision ID: 0001_create_control_plane_tables
Revises: None
Create Date: 2026-05-24
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from app.db.alembic_mysql import create_index_if_not_exists, drop_index_if_exists

revision: str = "0001_create_control_plane_tables"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "catalog_registry",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("type", sa.String(length=32), nullable=False),
        sa.Column("config_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
        mysql_charset="utf8mb4",
        mysql_engine="InnoDB",
        if_not_exists=True,
    )
    op.create_table(
        "catalog_summary",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("catalog", sa.String(length=128), nullable=False),
        sa.Column("total_tables", sa.Integer(), nullable=False),
        sa.Column("healthy_tables", sa.Integer(), nullable=False),
        sa.Column("warning_tables", sa.Integer(), nullable=False),
        sa.Column("critical_tables", sa.Integer(), nullable=False),
        sa.Column("tables_needing_snapshot_expiration", sa.Integer(), nullable=False),
        sa.Column("tables_needing_compaction", sa.Integer(), nullable=False),
        sa.Column("tables_needing_manifest_rewrite", sa.Integer(), nullable=False),
        sa.Column("tables_with_delete_files", sa.Integer(), nullable=False),
        sa.Column("total_wasted_storage_gb", sa.Float(), nullable=False),
        sa.Column("scan_mode", sa.String(length=16), nullable=False),
        sa.Column("scanned_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("catalog"),
        mysql_charset="utf8mb4",
        mysql_engine="InnoDB",
        if_not_exists=True,
    )
    op.create_table(
        "table_health",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("catalog", sa.String(length=128), nullable=False),
        sa.Column("namespace", sa.String(length=255), nullable=False),
        sa.Column("table_name", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("health_score", sa.Integer(), nullable=False),
        sa.Column("total_snapshots", sa.Integer(), nullable=False),
        sa.Column("total_data_files", sa.Integer(), nullable=False),
        sa.Column("total_delete_files", sa.Integer(), nullable=False),
        sa.Column("small_files_count", sa.Integer(), nullable=False),
        sa.Column("total_size_gb", sa.Float(), nullable=False),
        sa.Column("avg_file_size_mb", sa.Float(), nullable=False),
        sa.Column("oldest_snapshot_age_days", sa.Float(), nullable=True),
        sa.Column("days_since_last_write", sa.Float(), nullable=True),
        sa.Column("issues_count", sa.Integer(), nullable=False),
        sa.Column("warnings_count", sa.Integer(), nullable=False),
        sa.Column("recommendations_json", sa.JSON(), nullable=False),
        sa.Column("scan_mode", sa.String(length=16), nullable=False),
        sa.Column("scanned_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("catalog", "namespace", "table_name", name="uq_table_health_name"),
        mysql_charset="utf8mb4",
        mysql_engine="InnoDB",
        if_not_exists=True,
    )
    create_index_if_not_exists(
        "idx_table_health_catalog_status",
        "table_health",
        ["catalog", "status"],
    )
    create_index_if_not_exists(
        "idx_table_health_catalog_scanned",
        "table_health",
        ["catalog", "scanned_at"],
    )
    op.create_table(
        "jobs",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("type", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("progress", sa.Integer(), nullable=False),
        sa.Column("message", sa.String(length=255), nullable=True),
        sa.Column("payload_json", sa.JSON(), nullable=True),
        sa.Column("catalog", sa.String(length=128), nullable=True),
        sa.Column("owner_replica", sa.String(length=64), nullable=True),
        sa.Column("heartbeat_at", sa.DateTime(), nullable=True),
        sa.Column("result_json", sa.JSON(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        mysql_charset="utf8mb4",
        mysql_engine="InnoDB",
        if_not_exists=True,
    )
    create_index_if_not_exists(
        "idx_jobs_status_heartbeat",
        "jobs",
        ["status", "heartbeat_at"],
    )
    create_index_if_not_exists(
        "idx_jobs_catalog_created",
        "jobs",
        ["catalog", "created_at"],
    )


def downgrade() -> None:
    drop_index_if_exists("idx_jobs_catalog_created", "jobs")
    drop_index_if_exists("idx_jobs_status_heartbeat", "jobs")
    op.drop_table("jobs")
    drop_index_if_exists("idx_table_health_catalog_scanned", "table_health")
    drop_index_if_exists("idx_table_health_catalog_status", "table_health")
    op.drop_table("table_health")
    op.drop_table("catalog_summary")
    op.drop_table("catalog_registry")
