"""
Repository Pattern for Tenant-Aware Database Access
====================================================
Provides a clean interface for database operations with automatic
tenant isolation. All queries automatically filter by tenant_id.
"""

from typing import TypeVar, Generic, Optional, List, Type
from uuid import UUID
from datetime import datetime, timedelta

from sqlalchemy import select, update, delete, func, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import (
    Base, Tenant, ProductMapping, SyncLog, DeadLetterQueue,
    WebhookEvent, IntegrationQueue, SyncStatus, TaskStatus,
    IntegrationQueueStatus, WebhookSource, SyncType, SyncDirection
)


T = TypeVar("T", bound=Base)


class BaseRepository(Generic[T]):
    """
    Base repository with common CRUD operations.
    
    All methods that access tenant-specific data require tenant_id
    to ensure proper data isolation.
    """
    
    def __init__(self, session: AsyncSession, model: Type[T]):
        self.session = session
        self.model = model
    
    async def get_by_id(self, id: UUID, tenant_id: Optional[UUID] = None) -> Optional[T]:
        """Get entity by ID, optionally filtered by tenant."""
        query = select(self.model).where(self.model.id == id)
        
        if tenant_id and hasattr(self.model, "tenant_id"):
            query = query.where(self.model.tenant_id == tenant_id)
        
        result = await self.session.execute(query)
        return result.scalar_one_or_none()
    
    async def create(self, entity: T) -> T:
        """Create a new entity."""
        self.session.add(entity)
        await self.session.flush()
        await self.session.refresh(entity)
        return entity
    
    async def update(self, entity: T) -> T:
        """Update an existing entity."""
        await self.session.merge(entity)
        await self.session.flush()
        return entity
    
    async def delete(self, entity: T) -> None:
        """Delete an entity."""
        await self.session.delete(entity)
        await self.session.flush()


class TenantRepository(BaseRepository[Tenant]):
    """Repository for Tenant operations."""
    
    def __init__(self, session: AsyncSession):
        super().__init__(session, Tenant)
    
    async def get_by_id(self, tenant_id: str) -> Optional[Tenant]:
        """Get tenant by ID."""
        query = select(Tenant).where(Tenant.id == UUID(tenant_id) if isinstance(tenant_id, str) else tenant_id)
        result = await self.session.execute(query)
        return result.scalar_one_or_none()
    
    async def get_by_slug(self, slug: str) -> Optional[Tenant]:
        """Get tenant by slug."""
        query = select(Tenant).where(Tenant.slug == slug)
        result = await self.session.execute(query)
        return result.scalar_one_or_none()
    
    async def get_by_shopify_shop(self, shop_url: str) -> Optional[Tenant]:
        """Get tenant by Shopify shop URL."""
        # Normalize shop URL
        normalized = shop_url.replace("https://", "").replace("http://", "").rstrip("/")
        query = select(Tenant).where(Tenant.shopify_shop_url == normalized)
        result = await self.session.execute(query)
        return result.scalar_one_or_none()
    
    async def get_all(self) -> List[Tenant]:
        """Get all tenants."""
        query = select(Tenant).order_by(Tenant.created_at.desc())
        result = await self.session.execute(query)
        return list(result.scalars().all())
    
    async def get_all_active(self) -> List[Tenant]:
        """Get all active tenants."""
        query = select(Tenant).where(Tenant.is_active == True)
        result = await self.session.execute(query)
        return list(result.scalars().all())
    
    async def get_active_tenants(self) -> List[Tenant]:
        """Get all active tenants (alias)."""
        return await self.get_all_active()
    
    async def get_tenants_with_sync_enabled(self) -> List[Tenant]:
        """Get tenants with sync enabled."""
        query = select(Tenant).where(
            and_(Tenant.is_active == True, Tenant.sync_enabled == True)
        )
        result = await self.session.execute(query)
        return list(result.scalars().all())
    
    async def get_stale_tenants(self, cutoff_time: datetime) -> List[Tenant]:
        """Get tenants that haven't synced since cutoff time."""
        query = select(Tenant).where(
            and_(
                Tenant.is_active == True,
                (Tenant.last_sync_at == None) | (Tenant.last_sync_at < cutoff_time)
            )
        )
        result = await self.session.execute(query)
        return list(result.scalars().all())
    
    async def create(
        self,
        name: str,
        susoft_api_url: str,
        susoft_api_key_encrypted: str,
        susoft_integration_id: str,
        susoft_webhook_secret_encrypted: str,
        shopify_shop_url: str,
        shopify_access_token_encrypted: str,
        shopify_api_key_encrypted: Optional[str] = None,
        shopify_api_secret_encrypted: Optional[str] = None,
        shopify_default_location_id: Optional[str] = None,
        sync_interval_seconds: int = 300,
        safety_stock_default: int = 0,
        susoft_shop_id: Optional[str] = None,
        susoft_pos_id: Optional[str] = None,
    ) -> Tenant:
        """Create a new tenant."""
        from uuid import uuid4
        from slugify import slugify
        
        tenant = Tenant(
            id=uuid4(),
            name=name,
            slug=slugify(name),
            susoft_api_url=susoft_api_url,
            susoft_api_key_encrypted=susoft_api_key_encrypted,
            susoft_integration_id=susoft_integration_id,
            susoft_shop_id=susoft_shop_id,
            susoft_pos_id=susoft_pos_id,
            susoft_webhook_secret_encrypted=susoft_webhook_secret_encrypted,
            shopify_shop_url=shopify_shop_url.replace("https://", "").replace("http://", "").rstrip("/"),
            shopify_access_token_encrypted=shopify_access_token_encrypted,
            shopify_api_key_encrypted=shopify_api_key_encrypted,
            shopify_api_secret_encrypted=shopify_api_secret_encrypted,
            shopify_default_location_id=shopify_default_location_id,
            sync_interval_seconds=sync_interval_seconds,
            safety_stock_default=safety_stock_default,
            is_active=True
        )
        
        self.session.add(tenant)
        await self.session.commit()
        await self.session.refresh(tenant)
        
        return tenant
    
    async def set_active(self, tenant_id: str, is_active: bool) -> bool:
        """Set tenant active status."""
        result = await self.session.execute(
            update(Tenant)
            .where(Tenant.id == UUID(tenant_id))
            .values(is_active=is_active)
        )
        await self.session.commit()
        return result.rowcount > 0
    
    async def update_heartbeat(
        self, 
        tenant_id: str, 
        direction: str
    ) -> None:
        """Update last heartbeat timestamp for a tenant."""
        values = {"last_sync_at": datetime.utcnow()}
        
        if direction == "order_sync":
            values["last_order_sync_at"] = datetime.utcnow()
        elif direction == "stock_sync":
            values["last_stock_sync_at"] = datetime.utcnow()
        
        await self.session.execute(
            update(Tenant)
            .where(Tenant.id == UUID(tenant_id))
            .values(**values)
        )
        await self.session.commit()


class ProductMappingRepository(BaseRepository[ProductMapping]):
    """Repository for ProductMapping operations."""
    
    def __init__(self, session: AsyncSession):
        super().__init__(session, ProductMapping)
    
    async def get_by_sku(
        self, 
        tenant_id: UUID, 
        sku: str
    ) -> Optional[ProductMapping]:
        """Get product mapping by SKU for a tenant."""
        query = select(ProductMapping).where(
            and_(
                ProductMapping.tenant_id == tenant_id,
                ProductMapping.sku == sku,
                ProductMapping.is_active == True
            )
        )
        result = await self.session.execute(query)
        return result.scalar_one_or_none()
    
    async def get_by_susoft_id(
        self, 
        tenant_id: UUID, 
        susoft_id: str
    ) -> Optional[ProductMapping]:
        """Get product mapping by Susoft product ID."""
        query = select(ProductMapping).where(
            and_(
                ProductMapping.tenant_id == tenant_id,
                ProductMapping.susoft_product_id == susoft_id,
                ProductMapping.is_active == True
            )
        )
        result = await self.session.execute(query)
        return result.scalar_one_or_none()
    
    async def get_by_shopify_variant_id(
        self, 
        tenant_id: UUID, 
        variant_id: str
    ) -> Optional[ProductMapping]:
        """Get product mapping by Shopify variant ID."""
        query = select(ProductMapping).where(
            and_(
                ProductMapping.tenant_id == tenant_id,
                ProductMapping.shopify_variant_id == variant_id,
                ProductMapping.is_active == True
            )
        )
        result = await self.session.execute(query)
        return result.scalar_one_or_none()
    
    async def get_all_for_tenant(
        self, 
        tenant_id: UUID,
        active_only: bool = True
    ) -> List[ProductMapping]:
        """Get all product mappings for a tenant."""
        tid = UUID(tenant_id) if isinstance(tenant_id, str) else tenant_id
        query = select(ProductMapping).where(ProductMapping.tenant_id == tid)
        
        if active_only:
            query = query.where(ProductMapping.is_active == True)
        
        result = await self.session.execute(query)
        return list(result.scalars().all())
    
    async def get_active_mappings(self, tenant_id: str) -> List[ProductMapping]:
        """Get all active mappings for a tenant."""
        return await self.get_all_for_tenant(tenant_id, active_only=True)
    
    async def create(
        self,
        tenant_id: str,
        sku: str,
        susoft_product_id: str,
        shopify_product_id: str,
        shopify_variant_id: str,
        shopify_inventory_item_id: str,
        susoft_location_id: Optional[str] = None,
        shopify_location_id: Optional[str] = None,
        safety_stock: int = 0
    ) -> ProductMapping:
        """Create a new product mapping."""
        from uuid import uuid4
        
        mapping = ProductMapping(
            id=uuid4(),
            tenant_id=UUID(tenant_id),
            sku=sku,
            susoft_product_id=susoft_product_id,
            shopify_product_id=shopify_product_id,
            shopify_variant_id=shopify_variant_id,
            shopify_inventory_item_id=shopify_inventory_item_id,
            susoft_location_id=susoft_location_id,
            shopify_location_id=shopify_location_id,
            safety_stock=safety_stock,
            is_active=True
        )
        
        self.session.add(mapping)
        await self.session.commit()
        await self.session.refresh(mapping)
        
        return mapping
    
    async def delete(self, mapping_id: str, tenant_id: str) -> bool:
        """Delete a product mapping."""
        result = await self.session.execute(
            delete(ProductMapping)
            .where(
                and_(
                    ProductMapping.id == UUID(mapping_id),
                    ProductMapping.tenant_id == UUID(tenant_id)
                )
            )
        )
        await self.session.commit()
        return result.rowcount > 0
    
    async def update_stock_with_version(
        self,
        mapping_id: UUID,
        new_susoft_stock: int,
        new_shopify_stock: int,
        expected_version: int
    ) -> bool:
        """
        Update stock with optimistic locking.
        
        Returns True if update succeeded, False if version mismatch.
        """
        mid = UUID(mapping_id) if isinstance(mapping_id, str) else mapping_id
        result = await self.session.execute(
            update(ProductMapping)
            .where(
                and_(
                    ProductMapping.id == mid,
                    ProductMapping.version == expected_version
                )
            )
            .values(
                current_susoft_stock=new_susoft_stock,
                current_shopify_stock=new_shopify_stock,
                last_synced_at=datetime.utcnow(),
                version=expected_version + 1
            )
        )
        await self.session.commit()
        return result.rowcount > 0


class SyncLogRepository(BaseRepository[SyncLog]):
    """Repository for SyncLog operations."""
    
    def __init__(self, session: AsyncSession):
        super().__init__(session, SyncLog)
    
    async def create(
        self,
        tenant_id: str,
        sync_type: SyncType,
        direction: SyncDirection,
        external_id: str,
        source_payload: dict,
        status: SyncStatus,
        previous_stock: Optional[int] = None,
        new_stock: Optional[int] = None
    ) -> SyncLog:
        """Create a new sync log entry."""
        from uuid import uuid4
        
        log = SyncLog(
            id=uuid4(),
            tenant_id=UUID(tenant_id) if isinstance(tenant_id, str) else tenant_id,
            sync_type=sync_type,
            direction=direction,
            external_id=external_id,
            source_payload=source_payload,
            status=status,
            previous_stock=previous_stock,
            new_stock=new_stock,
            created_at=datetime.utcnow()
        )
        
        self.session.add(log)
        await self.session.commit()
        await self.session.refresh(log)
        
        return log
    
    async def get_by_external_id(
        self,
        tenant_id: str,
        external_id: str
    ) -> Optional[SyncLog]:
        """Get sync log by external ID."""
        tid = UUID(tenant_id) if isinstance(tenant_id, str) else tenant_id
        query = select(SyncLog).where(
            and_(
                SyncLog.tenant_id == tid,
                SyncLog.external_id == external_id
            )
        ).order_by(SyncLog.created_at.desc()).limit(1)

        result = await self.session.execute(query)
        return result.scalars().first()
    
    async def update_status(
        self,
        sync_log_id: UUID,
        status: SyncStatus,
        response_payload: Optional[dict] = None,
        error_message: Optional[str] = None
    ) -> bool:
        """Update sync log status."""
        sid = UUID(sync_log_id) if isinstance(sync_log_id, str) else sync_log_id
        values = {
            "status": status,
            "completed_at": datetime.utcnow()
        }
        
        if response_payload:
            values["response_payload"] = response_payload
        if error_message:
            values["error_message"] = error_message
        
        result = await self.session.execute(
            update(SyncLog)
            .where(SyncLog.id == sid)
            .values(**values)
        )
        await self.session.commit()
        return result.rowcount > 0
    
    async def get_for_tenant(
        self,
        tenant_id: str,
        limit: int = 100,
        status: Optional[SyncStatus] = None,
        sync_type: Optional[str] = None
    ) -> List[SyncLog]:
        """Get sync logs for a tenant."""
        tid = UUID(tenant_id) if isinstance(tenant_id, str) else tenant_id
        query = select(SyncLog).where(SyncLog.tenant_id == tid)
        
        if status:
            query = query.where(SyncLog.status == status)
        if sync_type:
            query = query.where(SyncLog.sync_type == sync_type)
        
        query = query.order_by(SyncLog.created_at.desc()).limit(limit)
        
        result = await self.session.execute(query)
        return list(result.scalars().all())
    
    async def get_count_since(self, since: datetime) -> int:
        """Count all sync operations since a given time."""
        query = select(func.count(SyncLog.id)).where(
            SyncLog.created_at >= since
        )
        result = await self.session.execute(query)
        return result.scalar() or 0
    
    async def get_failed_count_since(
        self,
        tenant_id: Optional[str],
        since: datetime
    ) -> int:
        """Count failed sync operations since a given time."""
        if tenant_id:
            tid = UUID(tenant_id) if isinstance(tenant_id, str) else tenant_id
            query = select(func.count(SyncLog.id)).where(
                and_(
                    SyncLog.tenant_id == tid,
                    SyncLog.status == SyncStatus.FAILED,
                    SyncLog.created_at >= since
                )
            )
        else:
            query = select(func.count(SyncLog.id)).where(
                and_(
                    SyncLog.status == SyncStatus.FAILED,
                    SyncLog.created_at >= since
                )
            )
        result = await self.session.execute(query)
        return result.scalar() or 0
    
    async def delete_old_logs(self, cutoff_date: datetime) -> int:
        """Delete sync logs older than cutoff date."""
        result = await self.session.execute(
            delete(SyncLog).where(SyncLog.created_at < cutoff_date)
        )
        await self.session.commit()
        return result.rowcount


class DeadLetterQueueRepository(BaseRepository[DeadLetterQueue]):
    """Repository for DeadLetterQueue operations."""
    
    def __init__(self, session: AsyncSession):
        super().__init__(session, DeadLetterQueue)
    
    async def create(
        self,
        task_name: str,
        tenant_id: Optional[str],
        payload: dict,
        error_message: str,
        traceback: Optional[str] = None
    ) -> DeadLetterQueue:
        """Create a new DLQ entry."""
        from uuid import uuid4
        
        dlq_entry = DeadLetterQueue(
            id=uuid4(),
            tenant_id=UUID(tenant_id) if tenant_id else None,
            task_name=task_name,
            payload=payload,
            error_message=error_message,
            traceback=traceback,
            retry_count=0,
            alerted=False,
            resolved=False,
            created_at=datetime.utcnow()
        )
        
        self.session.add(dlq_entry)
        await self.session.commit()
        await self.session.refresh(dlq_entry)
        
        return dlq_entry
    
    async def get_by_id(self, dlq_id: str) -> Optional[DeadLetterQueue]:
        """Get DLQ entry by ID."""
        did = UUID(dlq_id) if isinstance(dlq_id, str) else dlq_id
        query = select(DeadLetterQueue).where(DeadLetterQueue.id == did)
        result = await self.session.execute(query)
        return result.scalar_one_or_none()
    
    async def get_items(
        self,
        tenant_id: Optional[str] = None,
        unresolved_only: bool = True,
        limit: int = 100
    ) -> List[DeadLetterQueue]:
        """Get DLQ items with filters."""
        query = select(DeadLetterQueue)
        
        if unresolved_only:
            query = query.where(DeadLetterQueue.resolved == False)
        
        if tenant_id:
            tid = UUID(tenant_id) if isinstance(tenant_id, str) else tenant_id
            query = query.where(DeadLetterQueue.tenant_id == tid)
        
        query = query.order_by(DeadLetterQueue.created_at.desc()).limit(limit)
        
        result = await self.session.execute(query)
        return list(result.scalars().all())
    
    async def get_unalerted_items(self) -> List[DeadLetterQueue]:
        """Get DLQ entries that haven't triggered an alert."""
        query = select(DeadLetterQueue).where(
            and_(
                DeadLetterQueue.resolved == False,
                DeadLetterQueue.alerted == False
            )
        )
        result = await self.session.execute(query)
        return list(result.scalars().all())
    
    async def mark_alerted(self, dlq_id: str) -> None:
        """Mark a DLQ entry as alerted."""
        did = UUID(dlq_id) if isinstance(dlq_id, str) else dlq_id
        await self.session.execute(
            update(DeadLetterQueue)
            .where(DeadLetterQueue.id == did)
            .values(alerted=True)
        )
        await self.session.commit()
    
    async def mark_resolved(self, dlq_id: str) -> bool:
        """Mark a DLQ entry as resolved."""
        did = UUID(dlq_id) if isinstance(dlq_id, str) else dlq_id
        result = await self.session.execute(
            update(DeadLetterQueue)
            .where(DeadLetterQueue.id == did)
            .values(resolved=True, resolved_at=datetime.utcnow())
        )
        await self.session.commit()
        return result.rowcount > 0
    
    async def mark_retried(self, dlq_id: str) -> None:
        """Mark a DLQ entry as retried."""
        did = UUID(dlq_id) if isinstance(dlq_id, str) else dlq_id
        await self.session.execute(
            update(DeadLetterQueue)
            .where(DeadLetterQueue.id == did)
            .values(
                retry_count=DeadLetterQueue.retry_count + 1,
                last_retry_at=datetime.utcnow()
            )
        )
        await self.session.commit()
    
    async def get_total_count(self) -> int:
        """Get total count of DLQ items."""
        query = select(func.count(DeadLetterQueue.id))
        result = await self.session.execute(query)
        return result.scalar() or 0
    
    async def get_unresolved_count(self, tenant_id: Optional[str] = None) -> int:
        """Get count of unresolved DLQ items."""
        query = select(func.count(DeadLetterQueue.id)).where(
            DeadLetterQueue.resolved == False
        )
        
        if tenant_id:
            tid = UUID(tenant_id) if isinstance(tenant_id, str) else tenant_id
            query = query.where(DeadLetterQueue.tenant_id == tid)
        
        result = await self.session.execute(query)
        return result.scalar() or 0


class IntegrationQueueRepository(BaseRepository[IntegrationQueue]):
    """Repository for IntegrationQueue operations."""
    
    def __init__(self, session: AsyncSession):
        super().__init__(session, IntegrationQueue)
    
    async def get_pending_count(self, tenant_id: str) -> int:
        """Get count of pending tasks for a tenant."""
        tid = UUID(tenant_id) if isinstance(tenant_id, str) else tenant_id
        query = select(func.count(IntegrationQueue.id)).where(
            and_(
                IntegrationQueue.tenant_id == tid,
                IntegrationQueue.status.in_([TaskStatus.QUEUED, TaskStatus.PROCESSING])
            )
        )
        result = await self.session.execute(query)
        return result.scalar() or 0
    
    async def get_stuck_tasks(self, minutes: int = 5) -> List[IntegrationQueue]:
        """Get tasks that have been processing for too long."""
        threshold = datetime.utcnow() - timedelta(minutes=minutes)
        
        query = select(IntegrationQueue).where(
            and_(
                IntegrationQueue.status == TaskStatus.PROCESSING,
                IntegrationQueue.started_at < threshold
            )
        )
        result = await self.session.execute(query)
        return list(result.scalars().all())
    
    async def get_stuck_items(self, cutoff_time: datetime) -> List[IntegrationQueue]:
        """Get items stuck since cutoff time."""
        query = select(IntegrationQueue).where(
            and_(
                IntegrationQueue.status == TaskStatus.PROCESSING,
                IntegrationQueue.started_at < cutoff_time
            )
        )
        result = await self.session.execute(query)
        return list(result.scalars().all())
    
    async def mark_alerted(self, queue_id: UUID) -> None:
        """Mark a queue item as alerted."""
        await self.session.execute(
            update(IntegrationQueue)
            .where(IntegrationQueue.id == queue_id)
            .values(alerted=True)
        )
        await self.session.commit()


class WebhookEventRepository(BaseRepository[WebhookEvent]):
    """Repository for WebhookEvent operations."""
    
    def __init__(self, session: AsyncSession):
        super().__init__(session, WebhookEvent)
    
    async def create(
        self,
        tenant_id: str,
        source: WebhookSource,
        event_type: str,
        external_id: str,
        payload: dict,
        headers: Optional[dict] = None,
        processed: bool = False
    ) -> WebhookEvent:
        """Create a new webhook event record."""
        from uuid import uuid4
        
        event = WebhookEvent(
            id=uuid4(),
            tenant_id=UUID(tenant_id) if isinstance(tenant_id, str) else tenant_id,
            source=source,
            event_type=event_type,
            external_id=external_id,
            payload=payload,
            headers=headers or {},
            processed=processed,
            received_at=datetime.utcnow()
        )
        
        self.session.add(event)
        await self.session.commit()
        await self.session.refresh(event)
        
        return event
    
    async def mark_processed(self, event_id: UUID) -> bool:
        """Mark webhook event as processed."""
        result = await self.session.execute(
            update(WebhookEvent)
            .where(WebhookEvent.id == event_id)
            .values(processed=True, processed_at=datetime.utcnow())
        )
        await self.session.commit()
        return result.rowcount > 0
