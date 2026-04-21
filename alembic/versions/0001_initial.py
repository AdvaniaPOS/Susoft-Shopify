"""initial schema

Revision ID: 0001_initial
Revises:
Create Date: 2026-04-21

Creates the full baseline schema for the Susoft-Shopify integration:
tenants, product_mappings, sync_logs, dead_letter_queue, webhook_events,
integration_queue. Field names mirror app/db/models.py exactly.
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


# Reusable enum definitions
sync_type_enum = postgresql.ENUM(
    "ORDER", "STOCK", "PRODUCT", "CUSTOMER",
    name="synctype", create_type=False,
)
sync_direction_enum = postgresql.ENUM(
    "SHOPIFY_TO_SUSOFT", "SUSOFT_TO_SHOPIFY",
    name="syncdirection", create_type=False,
)
sync_status_enum = postgresql.ENUM(
    "PENDING", "PROCESSING", "SUCCESS", "FAILED", "RETRYING",
    name="syncstatus", create_type=False,
)
task_status_enum = postgresql.ENUM(
    "QUEUED", "PROCESSING", "COMPLETED", "FAILED", "DEAD",
    name="taskstatus", create_type=False,
)
webhook_source_enum = postgresql.ENUM(
    "SHOPIFY", "SUSOFT",
    name="webhooksource", create_type=False,
)


def upgrade() -> None:
    bind = op.get_bind()

    # Create enum types first
    sync_type_enum.create(bind, checkfirst=True)
    sync_direction_enum.create(bind, checkfirst=True)
    sync_status_enum.create(bind, checkfirst=True)
    task_status_enum.create(bind, checkfirst=True)
    webhook_source_enum.create(bind, checkfirst=True)

    # ---------- tenants ----------
    op.create_table(
        "tenants",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("slug", sa.String(100), nullable=False, unique=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),

        sa.Column("susoft_api_url", sa.String(500), nullable=False),
        sa.Column("susoft_api_key_encrypted", sa.Text(), nullable=False),
        sa.Column("susoft_integration_id", sa.String(255), nullable=False),
        sa.Column("susoft_webhook_secret_encrypted", sa.Text(), nullable=True),

        sa.Column("shopify_shop_url", sa.String(500), nullable=False),
        sa.Column("shopify_access_token_encrypted", sa.Text(), nullable=False),
        sa.Column("shopify_api_key_encrypted", sa.Text(), nullable=True),
        sa.Column("shopify_api_secret_encrypted", sa.Text(), nullable=True),
        sa.Column("shopify_webhook_secret_encrypted", sa.Text(), nullable=True),
        sa.Column("shopify_default_location_id", sa.String(255), nullable=True),

        sa.Column("safety_stock_default", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("sync_enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("sync_interval_seconds", sa.Integer(), nullable=False, server_default="300"),
        sa.Column("stock_sync_direction", sa.String(30), nullable=False, server_default="susoft_to_shopify"),

        sa.Column("last_sync_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_order_sync_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_stock_sync_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_susoft_heartbeat", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_shopify_heartbeat", sa.DateTime(timezone=True), nullable=True),

        sa.Column("alert_email", sa.String(255), nullable=True),
        sa.Column("alert_slack_channel", sa.String(100), nullable=True),

        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_tenants_slug", "tenants", ["slug"], unique=True)
    op.create_index("ix_tenants_shopify_shop_url", "tenants", ["shopify_shop_url"])

    # ---------- product_mappings ----------
    op.create_table(
        "product_mappings",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False),

        sa.Column("sku", sa.String(255), nullable=False),
        sa.Column("susoft_product_id", sa.String(255), nullable=False),
        sa.Column("susoft_barcode", sa.String(255), nullable=True),
        sa.Column("susoft_location_id", sa.String(255), nullable=True),

        sa.Column("shopify_product_id", sa.String(255), nullable=False),
        sa.Column("shopify_variant_id", sa.String(255), nullable=False),
        sa.Column("shopify_inventory_item_id", sa.String(255), nullable=True),
        sa.Column("shopify_location_id", sa.String(255), nullable=True),

        sa.Column("current_susoft_stock", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("current_shopify_stock", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("safety_stock", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("use_safety_stock", sa.Boolean(), nullable=False, server_default=sa.false()),

        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("last_synced_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),

        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),

        sa.UniqueConstraint("tenant_id", "sku", name="uix_tenant_sku"),
    )
    op.create_index("ix_product_mappings_tenant_id", "product_mappings", ["tenant_id"])
    op.create_index("ix_product_mappings_sku", "product_mappings", ["sku"])
    op.create_index("ix_product_mappings_susoft_product_id", "product_mappings", ["susoft_product_id"])
    op.create_index("ix_product_mappings_shopify_product_id", "product_mappings", ["shopify_product_id"])
    op.create_index("ix_product_mappings_shopify_variant_id", "product_mappings", ["shopify_variant_id"])
    op.create_index("ix_product_mapping_tenant_susoft", "product_mappings", ["tenant_id", "susoft_product_id"])
    op.create_index("ix_product_mapping_tenant_shopify", "product_mappings", ["tenant_id", "shopify_product_id"])

    # ---------- sync_logs ----------
    op.create_table(
        "sync_logs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False),

        sa.Column("sync_type", sync_type_enum, nullable=False),
        sa.Column("direction", sync_direction_enum, nullable=False),
        sa.Column("status", sync_status_enum, nullable=False),

        sa.Column("external_id", sa.String(255), nullable=True),
        sa.Column("sku", sa.String(255), nullable=True),
        sa.Column("shopify_order_id", sa.String(255), nullable=True),
        sa.Column("susoft_order_id", sa.String(255), nullable=True),

        sa.Column("source_payload", postgresql.JSONB, nullable=True),
        sa.Column("response_payload", postgresql.JSONB, nullable=True),

        sa.Column("previous_stock", sa.Integer(), nullable=True),
        sa.Column("new_stock", sa.Integer(), nullable=True),

        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("error_code", sa.String(100), nullable=True),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),

        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("duration_ms", sa.Integer(), nullable=True),

        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_sync_logs_tenant_id", "sync_logs", ["tenant_id"])
    op.create_index("ix_sync_logs_sync_type", "sync_logs", ["sync_type"])
    op.create_index("ix_sync_logs_direction", "sync_logs", ["direction"])
    op.create_index("ix_sync_logs_status", "sync_logs", ["status"])
    op.create_index("ix_sync_logs_external_id", "sync_logs", ["external_id"])
    op.create_index("ix_sync_logs_sku", "sync_logs", ["sku"])
    op.create_index("ix_sync_log_tenant_created", "sync_logs", ["tenant_id", "created_at"])
    op.create_index("ix_sync_log_status_created", "sync_logs", ["status", "created_at"])
    op.create_index("ix_sync_log_type_direction", "sync_logs", ["sync_type", "direction"])

    # ---------- dead_letter_queue ----------
    op.create_table(
        "dead_letter_queue",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=True),

        sa.Column("task_name", sa.String(255), nullable=False),
        sa.Column("payload", postgresql.JSONB, nullable=True),
        sa.Column("error_message", sa.Text(), nullable=False),
        sa.Column("traceback", sa.Text(), nullable=True),

        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_retry_at", sa.DateTime(timezone=True), nullable=True),

        sa.Column("alerted", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("alert_sent_at", sa.DateTime(timezone=True), nullable=True),

        sa.Column("resolved", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolved_by", sa.String(255), nullable=True),
        sa.Column("resolution_notes", sa.Text(), nullable=True),

        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_dead_letter_queue_tenant_id", "dead_letter_queue", ["tenant_id"])
    op.create_index("ix_dead_letter_queue_task_name", "dead_letter_queue", ["task_name"])
    op.create_index("ix_dead_letter_queue_alerted", "dead_letter_queue", ["alerted"])
    op.create_index("ix_dead_letter_queue_resolved", "dead_letter_queue", ["resolved"])
    op.create_index("ix_dlq_tenant_resolved", "dead_letter_queue", ["tenant_id", "resolved"])
    op.create_index("ix_dlq_alerted_created", "dead_letter_queue", ["alerted", "created_at"])

    # ---------- webhook_events ----------
    op.create_table(
        "webhook_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False),

        sa.Column("source", webhook_source_enum, nullable=False),
        sa.Column("event_type", sa.String(100), nullable=False),
        sa.Column("external_id", sa.String(255), nullable=True),

        sa.Column("headers", postgresql.JSONB, nullable=True),
        sa.Column("payload", postgresql.JSONB, nullable=False),

        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("processed", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("celery_task_id", sa.String(255), nullable=True),
        sa.Column("signature_valid", sa.Boolean(), nullable=True),

        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_webhook_events_tenant_id", "webhook_events", ["tenant_id"])
    op.create_index("ix_webhook_events_source", "webhook_events", ["source"])
    op.create_index("ix_webhook_events_event_type", "webhook_events", ["event_type"])
    op.create_index("ix_webhook_events_external_id", "webhook_events", ["external_id"])
    op.create_index("ix_webhook_events_processed", "webhook_events", ["processed"])
    op.create_index("ix_webhook_tenant_processed", "webhook_events", ["tenant_id", "processed"])
    op.create_index("ix_webhook_source_type", "webhook_events", ["source", "event_type"])

    # ---------- integration_queue ----------
    op.create_table(
        "integration_queue",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False),

        sa.Column("task_type", sync_type_enum, nullable=False),
        sa.Column("direction", sync_direction_enum, nullable=False),

        sa.Column("external_reference", sa.String(255), nullable=False),
        sa.Column("sku", sa.String(255), nullable=True),
        sa.Column("payload", postgresql.JSONB, nullable=False),

        sa.Column("status", task_status_enum, nullable=False, server_default="QUEUED"),

        sa.Column("priority", sa.Integer(), nullable=False, server_default="5"),
        sa.Column("scheduled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),

        sa.Column("celery_task_id", sa.String(255), nullable=True),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("max_retries", sa.Integer(), nullable=False, server_default="3"),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("alerted", sa.Boolean(), nullable=False, server_default=sa.false()),

        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_integration_queue_tenant_id", "integration_queue", ["tenant_id"])
    op.create_index("ix_integration_queue_task_type", "integration_queue", ["task_type"])
    op.create_index("ix_integration_queue_external_reference", "integration_queue", ["external_reference"])
    op.create_index("ix_integration_queue_status", "integration_queue", ["status"])
    op.create_index("ix_queue_tenant_status", "integration_queue", ["tenant_id", "status"])
    op.create_index("ix_queue_priority", "integration_queue", ["priority", "created_at"])
    op.create_index("ix_queue_scheduled", "integration_queue", ["scheduled_at", "status"])


def downgrade() -> None:
    op.drop_table("integration_queue")
    op.drop_table("webhook_events")
    op.drop_table("dead_letter_queue")
    op.drop_table("sync_logs")
    op.drop_table("product_mappings")
    op.drop_table("tenants")

    bind = op.get_bind()
    webhook_source_enum.drop(bind, checkfirst=True)
    task_status_enum.drop(bind, checkfirst=True)
    sync_status_enum.drop(bind, checkfirst=True)
    sync_direction_enum.drop(bind, checkfirst=True)
    sync_type_enum.drop(bind, checkfirst=True)
