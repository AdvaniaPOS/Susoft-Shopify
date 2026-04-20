"""
Database Models
================
SQLAlchemy ORM models for the Susoft-Shopify integration.

All models follow these conventions:
- Use UUID primary keys for security (no sequential IDs exposed)
- Include tenant_id for multi-tenancy isolation
- Include created_at/updated_at timestamps
- Use appropriate indexes for query performance
- Store sensitive data encrypted (API keys, tokens)

Entity Relationships:
- Tenant: The top-level entity representing a customer
- ProductMapping: Links products between Susoft and Shopify
- SyncLog: Audit trail of all sync operations
- DeadLetterQueue: Failed tasks for manual retry
- WebhookEvent: Incoming webhook payloads for debugging
"""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Optional, List
from enum import Enum

from sqlalchemy import (
    String, Text, Boolean, Integer, DateTime, 
    ForeignKey, Index, Numeric, UniqueConstraint,
    Enum as SQLEnum, JSON, func
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


class SyncSource(str, Enum):
    """Source system of the sync operation."""
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
    DEAD = "dead"  # Moved to DLQ


# ===================
# Mixin Classes
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
# Tenant Model
# ===================

class Tenant(TimestampMixin, Base):
    """
    Represents a customer/tenant in the multi-tenant system.
    
    Each tenant has their own Susoft and Shopify configurations.
    All other models reference a tenant for data isolation.
    """
    __tablename__ = "tenants"
    
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )
    
    # Basic Info
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    slug: Mapped[str] = mapped_column(String(100), unique=True, nullable=False, index=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    
    # Susoft Configuration
    susoft_base_url: Mapped[str] = mapped_column(String(500), nullable=False)
    susoft_shop_url_key: Mapped[str] = mapped_column(String(255), nullable=False)
    susoft_api_key_encrypted: Mapped[str] = mapped_column(Text, nullable=False)
    susoft_webhook_token: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    
    # Shopify Configuration
    shopify_shop_url: Mapped[str] = mapped_column(String(500), nullable=False)
    shopify_api_key_encrypted: Mapped[str] = mapped_column(Text, nullable=False)
    shopify_api_secret_encrypted: Mapped[str] = mapped_column(Text, nullable=False)
    shopify_access_token_encrypted: Mapped[str] = mapped_column(Text, nullable=False)
    shopify_webhook_secret_encrypted: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Sync Settings
    default_safety_stock: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    sync_enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    stock_sync_direction: Mapped[str] = mapped_column(
        String(20), 
        default="susoft_to_shopify",
        nullable=False
    )  # susoft_to_shopify, bidirectional
    
    # Health/Status
    last_susoft_heartbeat: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_shopify_heartbeat: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_successful_sync: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    
    # Notification Settings
    alert_email: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    alert_slack_channel: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    
    # Relationships
    product_mappings: Mapped[List["ProductMapping"]] = relationship(
        "ProductMapping",
        back_populates="tenant",
        cascade="all, delete-orphan"
    )
    sync_logs: Mapped[List["SyncLog"]] = relationship(
        "SyncLog",
        back_populates="tenant",
        cascade="all, delete-orphan"
    )
    dead_letter_queue: Mapped[List["DeadLetterQueue"]] = relationship(
        "DeadLetterQueue",
        back_populates="tenant",
        cascade="all, delete-orphan"
    )
    
    def __repr__(self) -> str:
        return f"<Tenant {self.name} ({self.slug})>"


# ===================
# Product Mapping Model
# ===================

class ProductMapping(TimestampMixin, Base):
    """
    Maps products between Susoft and Shopify.
    
    Uses SKU as the primary linking field (as specified in requirements).
    Supports Shopify variants linked to Susoft simple products.
    """
    __tablename__ = "product_mappings"
    
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )
    
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    
    # Product Identifiers
    sku: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    
    # Susoft IDs
    susoft_product_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    susoft_barcode: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    
    # Shopify IDs
    shopify_product_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    shopify_variant_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    shopify_inventory_item_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    
    # Stock Management
    current_stock_susoft: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    current_stock_shopify: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    safety_stock_level: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    use_safety_stock: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    
    # Sync State
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    last_stock_sync: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    stock_sync_locked: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    
    # Version for optimistic locking
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    
    # Relationship
    tenant: Mapped["Tenant"] = relationship("Tenant", back_populates="product_mappings")
    
    __table_args__ = (
        # Ensure unique SKU per tenant
        UniqueConstraint("tenant_id", "sku", name="uix_tenant_sku"),
        # Composite indexes for common queries
        Index("ix_product_mapping_tenant_susoft", "tenant_id", "susoft_product_id"),
        Index("ix_product_mapping_tenant_shopify", "tenant_id", "shopify_product_id"),
    )
    
    def __repr__(self) -> str:
        return f"<ProductMapping SKU={self.sku} Susoft={self.susoft_product_id} Shopify={self.shopify_variant_id}>"
    
    def get_effective_stock_for_shopify(self) -> int:
        """
        Calculate the stock to display in Shopify, accounting for safety stock.
        
        Returns:
            Stock level to set in Shopify.
        """
        if self.use_safety_stock:
            return max(0, self.current_stock_susoft - self.safety_stock_level)
        return self.current_stock_susoft


# ===================
# Sync Log Model
# ===================

class SyncLog(TimestampMixin, Base):
    """
    Audit log for all synchronization operations.
    
    Records every stock change and order transfer with full payload
    for debugging and compliance purposes.
    """
    __tablename__ = "sync_logs"
    
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )
    
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    
    # Operation Details
    sync_type: Mapped[SyncType] = mapped_column(SQLEnum(SyncType), nullable=False, index=True)
    source: Mapped[SyncSource] = mapped_column(SQLEnum(SyncSource), nullable=False, index=True)
    status: Mapped[SyncStatus] = mapped_column(SQLEnum(SyncStatus), nullable=False, index=True)
    
    # Reference IDs
    external_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)
    sku: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)
    shopify_order_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    susoft_order_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    
    # Payload (stored as JSONB for PostgreSQL query capabilities)
    request_payload: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    response_payload: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    
    # Stock Change Details (for stock syncs)
    stock_before: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    stock_after: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    stock_change_reason: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    
    # Error Information
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error_code: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    
    # Timing
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    duration_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    
    # Relationship
    tenant: Mapped["Tenant"] = relationship("Tenant", back_populates="sync_logs")
    
    __table_args__ = (
        Index("ix_sync_log_tenant_created", "tenant_id", "created_at"),
        Index("ix_sync_log_status_created", "status", "created_at"),
        Index("ix_sync_log_type_source", "sync_type", "source"),
    )
    
    def __repr__(self) -> str:
        return f"<SyncLog {self.sync_type.value} from {self.source.value}: {self.status.value}>"


# ===================
# Dead Letter Queue Model
# ===================

class DeadLetterQueue(TimestampMixin, Base):
    """
    Dead Letter Queue for failed tasks.
    
    Tasks that fail after max retries are moved here for manual
    inspection and retry. Includes full context for debugging.
    """
    __tablename__ = "dead_letter_queue"
    
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )
    
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    
    # Task Information
    task_id: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    task_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    task_args: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    task_kwargs: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    
    # Queue Information
    queue_name: Mapped[str] = mapped_column(String(100), nullable=False)
    
    # Error Information
    error_type: Mapped[str] = mapped_column(String(255), nullable=False)
    error_message: Mapped[str] = mapped_column(Text, nullable=False)
    error_traceback: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    
    # Status
    status: Mapped[TaskStatus] = mapped_column(
        SQLEnum(TaskStatus),
        default=TaskStatus.DEAD,
        nullable=False,
        index=True
    )
    
    # Manual Retry Tracking
    last_retry_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    resolved_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    resolution_notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Notifications
    alert_sent: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    alert_sent_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    
    # Relationship
    tenant: Mapped["Tenant"] = relationship("Tenant", back_populates="dead_letter_queue")
    
    __table_args__ = (
        Index("ix_dlq_tenant_status", "tenant_id", "status"),
        Index("ix_dlq_alert_sent", "alert_sent", "created_at"),
    )
    
    def __repr__(self) -> str:
        return f"<DeadLetterQueue {self.task_name} - {self.status.value}>"


# ===================
# Webhook Event Model
# ===================

class WebhookEvent(TimestampMixin, Base):
    """
    Stores incoming webhook events for debugging and replay.
    
    All webhooks are logged before processing, allowing us to
    replay events if needed and debug issues.
    """
    __tablename__ = "webhook_events"
    
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )
    
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    
    # Event Information
    source: Mapped[SyncSource] = mapped_column(SQLEnum(SyncSource), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    event_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, unique=True)
    
    # Request Details
    headers: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    
    # Processing Status
    processed: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False, index=True)
    processed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    celery_task_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    
    # Validation
    signature_valid: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    
    __table_args__ = (
        Index("ix_webhook_tenant_processed", "tenant_id", "processed"),
        Index("ix_webhook_source_type", "source", "event_type"),
    )
    
    def __repr__(self) -> str:
        return f"<WebhookEvent {self.source.value}:{self.event_type}>"


# ===================
# Integration Queue Model
# ===================

class IntegrationQueue(TimestampMixin, Base):
    """
    Queue table for pending integration tasks.
    
    Used alongside Celery for persistent task tracking and
    to provide visibility into queue status via admin panel.
    """
    __tablename__ = "integration_queue"
    
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )
    
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    
    # Task Details
    task_type: Mapped[SyncType] = mapped_column(SQLEnum(SyncType), nullable=False, index=True)
    direction: Mapped[str] = mapped_column(String(50), nullable=False)  # shopify_to_susoft, susoft_to_shopify
    
    # Reference
    external_reference: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    sku: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    
    # Payload
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    
    # Status
    status: Mapped[TaskStatus] = mapped_column(
        SQLEnum(TaskStatus),
        default=TaskStatus.QUEUED,
        nullable=False,
        index=True
    )
    
    # Timing
    priority: Mapped[int] = mapped_column(Integer, default=5, nullable=False)  # 1=highest, 10=lowest
    scheduled_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    
    # Celery Integration
    celery_task_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    
    # Retry Information
    retry_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    max_retries: Mapped[int] = mapped_column(Integer, default=3, nullable=False)
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    __table_args__ = (
        Index("ix_queue_tenant_status", "tenant_id", "status"),
        Index("ix_queue_priority", "priority", "created_at"),
        Index("ix_queue_scheduled", "scheduled_at", "status"),
    )
    
    def __repr__(self) -> str:
        return f"<IntegrationQueue {self.task_type.value} {self.direction}: {self.status.value}>"
