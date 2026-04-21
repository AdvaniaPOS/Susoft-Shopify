"""add order-flow columns to tenants

Revision ID: 0002_tenant_order_flow
Revises: 0001_initial
Create Date: 2026-04-21

Adds columns controlling the post-Susoft order flow on Shopify:

* ``close_orders_after_susoft`` — close Shopify order once Susoft accepted it
* ``shopify_synced_tag`` — tag added on success
* ``shopify_failed_tag`` — tag added on failure
"""

from alembic import op
import sqlalchemy as sa


revision = "0002_tenant_order_flow"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "tenants",
        sa.Column(
            "close_orders_after_susoft",
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
    )
    op.add_column(
        "tenants",
        sa.Column(
            "shopify_synced_tag",
            sa.String(length=64),
            nullable=False,
            server_default="susoft-synced",
        ),
    )
    op.add_column(
        "tenants",
        sa.Column(
            "shopify_failed_tag",
            sa.String(length=64),
            nullable=False,
            server_default="susoft-failed",
        ),
    )


def downgrade() -> None:
    op.drop_column("tenants", "shopify_failed_tag")
    op.drop_column("tenants", "shopify_synced_tag")
    op.drop_column("tenants", "close_orders_after_susoft")
