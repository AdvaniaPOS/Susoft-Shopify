"""
Database Models
================
SQLAlchemy ORM models for the Susoft-Shopify integration.

Conventions:
- UUID primary keys
- All tenant-scoped tables include tenant_id with index
- Timestamps via TimestampMixin
- Sensitive credentials stored encrypted (Fernet)

Field names here are the source of truth and must stay consistent with
``app/db/repositories.py``, ``app/api/admin.py``, ``app/api/webhooks.py``,
``app/workers/tasks.py`` and ``app/workers/scheduled_tasks.py``.
"""

import uuid
from datetime import datetime
from typing import Optional, List
from enum import Enum

from sqlalchemy import (
    String, Text, Boolean, Integer, DateTime,
    ForeignKey, Index, UniqueConstraint,
    Enum as SQLEnum, func
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.dialects.postgresql import UUID, JSONB

from app.core.database import Base


# ===================
# Enums
# ===================

class SyncType(str, Enum):
    """Type of synchronization operation."""
    ORDER = "order"
    STOCK = "stock"
    PRODUCT = "product"
    CUSTOMER = "customer"


class SyncDirection(str, Enum):
    """Direction of a sync operation."""
    SHOPIFY_TO_SUSOFT = "shopify_to_susoft"
    SUSOFT_TO_SHOPIFY = "susoft_to_shopify"


class SyncSource(str, Enum):
    """Source system that initiated a sync (legacy/audit)."""
    SHOPIFY = "shopify"
    SUSOFT = "susoft"
    MANUAL = "manual"
    SCHEDULED = "scheduled"


class SyncStatus(str, Enum):
    """Status of a sync operation."""
    PENDING = "pending"
    PROCESSING = "processing"
    SUCCESS = "success"
    FAILED = "failed"
    RETRYING = "retrying"


class TaskStatus(str, Enum):
    """Status of a queued task."""
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD = "dead"


# Alias kept for backward compatibility with imports in repositories/tasks.
IntegrationQueueStatus = TaskStatus


class WebhookSource(str, Enum):
    """Source of an incoming webhook."""
    SHOPIFY = "shopify"
    SUSOFT = "susoft"


# ===================
# Mixins
# ===================

class TimestampMixin:
    """Mixin for created_at and updated_at timestamps."""

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False
    )


# ===================
# Tenant
# ===================

class Tenant(TimestampMixin, Base):
    """A customer/tenant in the multi-tenant integration."""

    __tablename__ = "tenants"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )

    # Basic info
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    slug: Mapped[str] = mapped_column(String(100), unique=True, nullable=False, index=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # Susoft configuration
    susoft_api_url: Mapped[str] = mapped_column(String(500), nullable=False)
    susoft_api_key_encrypted: Mapped[str] = mapped_column(Text, nullable=False)
    susoft_integration_id: Mapped[str] = mapped_column(String(255), nullable=False)
    # Susoft's numeric shop id used in order payloads (Order.shopId,
    # Payment.shopId, Payment.issuedShopId). DIFFERENT from
    # ``susoft_integration_id`` which is the API login user.
    # Falls back to ``susoft_integration_id`` for backwards compatibility
    # if not set.
    susoft_shop_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    # Optional POS register / cash desk number used by Susoft POS endpoints.
    susoft_pos_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    susoft_webhook_secret_encrypted: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Shopify configuration
    shopify_shop_url: Mapped[str] = mapped_column(String(500), nullable=False, index=True)
    shopify_access_token_encrypted: Mapped[str] = mapped_column(Text, nullable=False)
    shopify_api_key_encrypted: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    shopify_api_secret_encrypted: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    shopify_webhook_secret_encrypted: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    shopify_default_location_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # Sync settings
    safety_stock_default: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    sync_enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    sync_interval_seconds: Mapped[int] = mapped_column(Integer, default=300, nullable=False)
    stock_sync_direction: Mapped[str] = mapped_column(
        String(30), default="susoft_to_shopify", nullable=False
    )

    # Order-flow settings (Shopify -> Susoft)
    # When True, after a Shopify order has been successfully created in Susoft
    # we tag the Shopify order and CLOSE it (POST /orders/{id}/close.json) so
    # nobody fulfills it manually in Shopify-admin. Susoft is the system of
    # record for picking/sending. Defaults to True (recommended).
    close_orders_after_susoft: Mapped[bool] = mapped_column(
        Boolean, default=True, nullable=False
    )
    # Tag added to Shopify order after a successful Susoft create.
    shopify_synced_tag: Mapped[str] = mapped_column(
        String(64), default="susoft-synced", nullable=False
    )
    # Tag added to Shopify order when Susoft create fails (after retries).
    shopify_failed_tag: Mapped[str] = mapped_column(
        String(64), default="susoft-failed", nullable=False
    )

    # Health / heartbeats
    last_sync_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_order_sync_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_stock_sync_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_susoft_heartbeat: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_shopify_heartbeat: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    # Notifications
    alert_email: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    alert_slack_channel: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # Relationships
    product_mappings: Mapped[List["ProductMapping"]] = relationship(
        "ProductMapping", back_populates="tenant", cascade="all, delete-orphan"
    )
    sync_logs: Mapped[List["SyncLog"]] = relationship(
        "SyncLog", back_populates="tenant", cascade="all, delete-orphan"
    )
    dead_letter_queue: Mapped[List["DeadLetterQueue"]] = relationship(
        "DeadLetterQueue", back_populates="tenant", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<Tenant {self.name} ({self.slug})>"


# ===================
# ProductMapping
# ===================

class ProductMapping(TimestampMixin, Base):
    """Maps a SKU between Susoft and Shopify."""

    __tablename__ = "product_mappings"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False, index=True
    )

    # Identifiers
    sku: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    susoft_product_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    susoft_barcode: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    susoft_location_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    shopify_product_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    shopify_variant_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    shopify_inventory_item_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    shopify_location_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # Stock state
    current_susoft_stock: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    current_shopify_stock: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    safety_stock: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    use_safety_stock: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    # Sync state
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    last_synced_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)

    tenant: Mapped["Tenant"] = relationship("Tenant", back_populates="product_mappings")

    __table_args__ = (
        UniqueConstraint("tenant_id", "sku", name="uix_tenant_sku"),
        Index("ix_product_mapping_tenant_susoft", "tenant_id", "susoft_product_id"),
        Index("ix_product_mapping_tenant_shopify", "tenant_id", "shopify_product_id"),
    )

    def get_effective_stock_for_shopify(self) -> int:
        """Stock to expose in Shopify, accounting for safety stock."""
        if self.use_safety_stock:
            return max(0, self.current_susoft_stock - self.safety_stock)
        return self.current_susoft_stock

    def __repr__(self) -> str:
        return f"<ProductMapping SKU={self.sku}>"


# ===================
# SyncLog
# ===================

class SyncLog(TimestampMixin, Base):
    """Audit log for every sync operation."""

    __tablename__ = "sync_logs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False, index=True
    )

    sync_type: Mapped[SyncType] = mapped_column(SQLEnum(SyncType), nullable=False, index=True)
    direction: Mapped[SyncDirection] = mapped_column(SQLEnum(SyncDirection), nullable=False, index=True)
    status: Mapped[SyncStatus] = mapped_column(SQLEnum(SyncStatus), nullable=False, index=True)

    # References
    external_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)
    sku: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)
    shopify_order_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    susoft_order_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # Payloads
    source_payload: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    response_payload: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

    # Stock delta
    previous_stock: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    new_stock: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Error info
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error_code: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    # Timing
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    duration_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    tenant: Mapped["Tenant"] = relationship("Tenant", back_populates="sync_logs")

    __table_args__ = (
        Index("ix_sync_log_tenant_created", "tenant_id", "created_at"),
        Index("ix_sync_log_status_created", "status", "created_at"),
        Index("ix_sync_log_type_direction", "sync_type", "direction"),
    )

    def __repr__(self) -> str:
        return f"<SyncLog {self.sync_type.value} {self.direction.value}: {self.status.value}>"


# ===================
# DeadLetterQueue
# ===================

class DeadLetterQueue(TimestampMixin, Base):
    """Tasks that failed after max retries."""

    __tablename__ = "dead_letter_queue"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    tenant_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=True, index=True
    )

    task_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    payload: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

    error_message: Mapped[str] = mapped_column(Text, nullable=False)
    traceback: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    retry_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    last_retry_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    alerted: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False, index=True)
    alert_sent_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    resolved: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False, index=True)
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    resolved_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    resolution_notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    tenant: Mapped[Optional["Tenant"]] = relationship("Tenant", back_populates="dead_letter_queue")

    __table_args__ = (
        Index("ix_dlq_tenant_resolved", "tenant_id", "resolved"),
        Index("ix_dlq_alerted_created", "alerted", "created_at"),
    )

    def __repr__(self) -> str:
        return f"<DeadLetterQueue {self.task_name} resolved={self.resolved}>"


# ===================
# WebhookEvent
# ===================

class WebhookEvent(TimestampMixin, Base):
    """Incoming webhook payloads (for replay/debug)."""

    __tablename__ = "webhook_events"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False, index=True
    )

    source: Mapped[WebhookSource] = mapped_column(SQLEnum(WebhookSource), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    external_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)

    headers: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)

    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    processed: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False, index=True)
    processed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    celery_task_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    signature_valid: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)

    __table_args__ = (
        Index("ix_webhook_tenant_processed", "tenant_id", "processed"),
        Index("ix_webhook_source_type", "source", "event_type"),
    )

    def __repr__(self) -> str:
        return f"<WebhookEvent {self.source.value}:{self.event_type}>"


# ===================
# IntegrationQueue
# ===================

class IntegrationQueue(TimestampMixin, Base):
    """Persistent queue table mirroring Celery tasks."""

    __tablename__ = "integration_queue"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False, index=True
    )

    task_type: Mapped[SyncType] = mapped_column(SQLEnum(SyncType), nullable=False, index=True)
    direction: Mapped[SyncDirection] = mapped_column(SQLEnum(SyncDirection), nullable=False)

    external_reference: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    sku: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)

    status: Mapped[TaskStatus] = mapped_column(
        SQLEnum(TaskStatus), default=TaskStatus.QUEUED, nullable=False, index=True
    )

    priority: Mapped[int] = mapped_column(Integer, default=5, nullable=False)
    scheduled_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    celery_task_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    max_retries: Mapped[int] = mapped_column(Integer, default=3, nullable=False)
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    alerted: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    __table_args__ = (
        Index("ix_queue_tenant_status", "tenant_id", "status"),
        Index("ix_queue_priority", "priority", "created_at"),
        Index("ix_queue_scheduled", "scheduled_at", "status"),
    )

    def __repr__(self) -> str:
        return f"<IntegrationQueue {self.task_type.value} {self.direction.value}: {self.status.value}>"
