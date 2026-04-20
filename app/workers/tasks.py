"""
Celery Tasks
=============
Core async tasks for Susoft-Shopify synchronization.

All tasks:
- Use distributed Redis locks to prevent concurrent processing
- Implement retry with exponential backoff
- Move to DLQ after max retries exceeded
- Track all operations in sync_log for audit
"""

import asyncio
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import uuid

from celery import Task
from celery.exceptions import MaxRetriesExceededError
import structlog
import redis

from app.workers.celery_app import celery_app
from app.core.config import settings
from app.core.database import get_session_context
from app.db.models import (
    SyncType, SyncStatus, SyncDirection, IntegrationQueueStatus
)
from app.db.repositories import (
    TenantRepository,
    ProductMappingRepository,
    SyncLogRepository,
    DeadLetterQueueRepository,
    IntegrationQueueRepository
)
from app.services.susoft_client import create_susoft_client, SusoftAPIError
from app.services.shopify_client import create_shopify_client, ShopifyAPIError


logger = structlog.get_logger()

# Redis client for distributed locks
redis_client = redis.Redis.from_url(settings.redis_url)


class BaseTaskWithRetry(Task):
    """Base task with automatic retry and DLQ handling."""
    
    autoretry_for = (Exception,)
    retry_backoff = True
    retry_backoff_max = 60
    retry_jitter = True
    max_retries = settings.alert_max_retries
    
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Handle task failure - move to DLQ after all retries exhausted."""
        asyncio.get_event_loop().run_until_complete(
            self._move_to_dlq(task_id, args, kwargs, str(exc), einfo)
        )
    
    async def _move_to_dlq(
        self,
        task_id: str,
        args: tuple,
        kwargs: dict,
        error_message: str,
        einfo: Any
    ):
        """Move failed task to dead letter queue."""
        async with get_session_context() as session:
            dlq_repo = DeadLetterQueueRepository(session)
            
            # Extract tenant_id from args or kwargs
            tenant_id = kwargs.get("tenant_id")
            if not tenant_id and args:
                tenant_id = args[0] if isinstance(args[0], str) else None
            
            await dlq_repo.create(
                task_name=self.name,
                tenant_id=tenant_id,
                payload={
                    "args": list(args),
                    "kwargs": kwargs,
                    "task_id": task_id
                },
                error_message=error_message,
                traceback=str(einfo) if einfo else None
            )
            
            logger.error(
                "Task moved to DLQ",
                task_name=self.name,
                task_id=task_id,
                tenant_id=tenant_id,
                error=error_message
            )


@celery_app.task(base=BaseTaskWithRetry, bind=True)
def process_shopify_order(
    self,
    tenant_id: str,
    order_data: Dict[str, Any],
    webhook_event_id: Optional[str] = None
):
    """
    Process a Shopify order webhook and create order in Susoft.
    
    Flow:
    1. Acquire distributed lock for this order
    2. Check idempotency (has this order been processed?)
    3. Map Shopify line items to Susoft products via SKU
    4. Create order in Susoft with alternativeId for idempotency
    5. Log the sync operation
    
    Args:
        tenant_id: UUID of the tenant
        order_data: Shopify order webhook payload
        webhook_event_id: Optional webhook event ID for tracking
    """
    order_id = order_data.get("id")
    order_name = order_data.get("name", f"#{order_id}")
    
    # Distributed lock key
    lock_key = f"order_lock:{tenant_id}:{order_id}"
    lock = redis_client.lock(lock_key, timeout=120)  # 2 min timeout
    
    if not lock.acquire(blocking=False):
        logger.warning(
            "Order already being processed, will retry",
            tenant_id=tenant_id,
            order_id=order_id
        )
        raise self.retry(countdown=5)
    
    try:
        # Run async processing
        asyncio.get_event_loop().run_until_complete(
            _process_shopify_order_async(
                task=self,
                tenant_id=tenant_id,
                order_data=order_data,
                webhook_event_id=webhook_event_id
            )
        )
    finally:
        try:
            lock.release()
        except Exception:
            pass  # Lock may have expired


async def _process_shopify_order_async(
    task: Task,
    tenant_id: str,
    order_data: Dict[str, Any],
    webhook_event_id: Optional[str]
):
    """Async implementation of Shopify order processing."""
    order_id = str(order_data.get("id"))
    order_name = order_data.get("name", f"#{order_id}")
    
    async with get_session_context() as session:
        tenant_repo = TenantRepository(session)
        mapping_repo = ProductMappingRepository(session)
        sync_log_repo = SyncLogRepository(session)
        queue_repo = IntegrationQueueRepository(session)
        
        # Get tenant
        tenant = await tenant_repo.get_by_id(tenant_id)
        if not tenant or not tenant.is_active:
            logger.warning("Tenant not found or inactive", tenant_id=tenant_id)
            return
        
        # Check if already processed (idempotency)
        existing_log = await sync_log_repo.get_by_external_id(
            tenant_id=tenant_id,
            external_id=f"shopify_order_{order_id}"
        )
        if existing_log and existing_log.status == SyncStatus.SUCCESS:
            logger.info(
                "Order already processed, skipping",
                tenant_id=tenant_id,
                order_id=order_id
            )
            return
        
        # Create sync log entry
        sync_log = await sync_log_repo.create(
            tenant_id=tenant_id,
            sync_type=SyncType.ORDER,
            direction=SyncDirection.SHOPIFY_TO_SUSOFT,
            external_id=f"shopify_order_{order_id}",
            source_payload=order_data,
            status=SyncStatus.PROCESSING
        )
        
        try:
            # Create Susoft client
            susoft_client = create_susoft_client(
                base_url=tenant.susoft_api_url,
                api_key_encrypted=tenant.susoft_api_key_encrypted,
                integration_id=tenant.susoft_integration_id
            )
            
            async with susoft_client:
                # Build Susoft order
                susoft_order = await _build_susoft_order(
                    tenant_id=tenant_id,
                    order_data=order_data,
                    mapping_repo=mapping_repo
                )
                
                # Create order in Susoft with idempotency
                # Use alternativeId = "SHOPIFY-{order_id}" as discussed
                susoft_order["alternativeId"] = f"SHOPIFY-{order_id}"
                
                result = await susoft_client.create_order(susoft_order)
                
                # Update sync log as success
                await sync_log_repo.update_status(
                    sync_log_id=sync_log.id,
                    status=SyncStatus.SUCCESS,
                    response_payload=result
                )
                
                # Update tenant heartbeat
                await tenant_repo.update_heartbeat(
                    tenant_id=tenant_id,
                    direction="order_sync"
                )
                
                logger.info(
                    "Order synced to Susoft",
                    tenant_id=tenant_id,
                    shopify_order=order_name,
                    susoft_order_id=result.get("uuid")
                )
                
        except SusoftAPIError as e:
            await sync_log_repo.update_status(
                sync_log_id=sync_log.id,
                status=SyncStatus.FAILED,
                error_message=str(e)
            )
            raise
        except Exception as e:
            await sync_log_repo.update_status(
                sync_log_id=sync_log.id,
                status=SyncStatus.FAILED,
                error_message=str(e)
            )
            raise


async def _build_susoft_order(
    tenant_id: str,
    order_data: Dict[str, Any],
    mapping_repo: ProductMappingRepository
) -> Dict[str, Any]:
    """Build a Susoft order from Shopify order data."""
    
    # Extract customer info
    customer = order_data.get("customer", {})
    shipping = order_data.get("shipping_address", {})
    
    # Build customer for Susoft
    susoft_customer = {
        "name": f"{customer.get('first_name', '')} {customer.get('last_name', '')}".strip(),
        "email": customer.get("email"),
        "phone": customer.get("phone"),
        "address": {
            "street": shipping.get("address1", ""),
            "street2": shipping.get("address2", ""),
            "city": shipping.get("city", ""),
            "zip": shipping.get("zip", ""),
            "country": shipping.get("country_code", "NO")
        }
    }
    
    # Build line items
    lines = []
    for item in order_data.get("line_items", []):
        sku = item.get("sku")
        
        if not sku:
            logger.warning(
                "Skipping line item without SKU",
                variant_id=item.get("variant_id")
            )
            continue
        
        # Look up Susoft product by SKU
        mapping = await mapping_repo.get_by_sku(tenant_id, sku)
        
        if not mapping:
            logger.warning(
                "No product mapping found for SKU",
                tenant_id=tenant_id,
                sku=sku
            )
            # Create line with just SKU, let Susoft handle lookup
            lines.append({
                "sku": sku,
                "quantity": item.get("quantity", 1),
                "unitPrice": float(item.get("price", 0)),
                "description": item.get("name")
            })
        else:
            lines.append({
                "productUuid": mapping.susoft_product_id,
                "sku": sku,
                "quantity": item.get("quantity", 1),
                "unitPrice": float(item.get("price", 0)),
                "description": item.get("name")
            })
    
    # Build the order
    susoft_order = {
        "customer": susoft_customer,
        "lines": lines,
        "orderNumber": order_data.get("name", ""),
        "note": order_data.get("note", ""),
        "currency": order_data.get("currency", "NOK"),
        "totalPrice": float(order_data.get("total_price", 0)),
        "source": "shopify"
    }
    
    return susoft_order


@celery_app.task(base=BaseTaskWithRetry, bind=True)
def process_susoft_stock_change(
    self,
    tenant_id: str,
    stock_data: Dict[str, Any],
    webhook_event_id: Optional[str] = None
):
    """
    Process Susoft stock change webhook and update Shopify inventory.
    
    Flow:
    1. Acquire distributed lock for this product
    2. Look up product mapping by Susoft product ID or SKU
    3. Calculate available stock (total - safety stock)
    4. Update Shopify inventory level
    5. Log the sync operation
    
    Args:
        tenant_id: UUID of the tenant
        stock_data: Susoft stock change payload
        webhook_event_id: Optional webhook event ID
    """
    product_uuid = stock_data.get("productUuid") or stock_data.get("uuid")
    sku = stock_data.get("sku")
    
    # Distributed lock key
    lock_key = f"stock_lock:{tenant_id}:{product_uuid or sku}"
    lock = redis_client.lock(lock_key, timeout=60)
    
    if not lock.acquire(blocking=False):
        logger.warning(
            "Stock update already being processed, will retry",
            tenant_id=tenant_id,
            product_uuid=product_uuid
        )
        raise self.retry(countdown=3)
    
    try:
        asyncio.get_event_loop().run_until_complete(
            _process_susoft_stock_change_async(
                task=self,
                tenant_id=tenant_id,
                stock_data=stock_data,
                webhook_event_id=webhook_event_id
            )
        )
    finally:
        try:
            lock.release()
        except Exception:
            pass


async def _process_susoft_stock_change_async(
    task: Task,
    tenant_id: str,
    stock_data: Dict[str, Any],
    webhook_event_id: Optional[str]
):
    """Async implementation of stock change processing."""
    product_uuid = stock_data.get("productUuid") or stock_data.get("uuid")
    sku = stock_data.get("sku")
    new_quantity = stock_data.get("quantity", 0)
    location_id = stock_data.get("locationId")
    
    async with get_session_context() as session:
        tenant_repo = TenantRepository(session)
        mapping_repo = ProductMappingRepository(session)
        sync_log_repo = SyncLogRepository(session)
        
        # Get tenant
        tenant = await tenant_repo.get_by_id(tenant_id)
        if not tenant or not tenant.is_active:
            logger.warning("Tenant not found or inactive", tenant_id=tenant_id)
            return
        
        # Find product mapping
        mapping = None
        if product_uuid:
            mapping = await mapping_repo.get_by_susoft_id(tenant_id, product_uuid)
        if not mapping and sku:
            mapping = await mapping_repo.get_by_sku(tenant_id, sku)
        
        if not mapping:
            logger.warning(
                "No product mapping found for stock update",
                tenant_id=tenant_id,
                product_uuid=product_uuid,
                sku=sku
            )
            return
        
        # Calculate available quantity (subtract safety stock)
        safety_stock = mapping.safety_stock or 0
        available_quantity = max(0, int(new_quantity) - safety_stock)
        
        previous_quantity = mapping.current_shopify_stock
        
        # Create sync log
        sync_log = await sync_log_repo.create(
            tenant_id=tenant_id,
            sync_type=SyncType.STOCK,
            direction=SyncDirection.SUSOFT_TO_SHOPIFY,
            external_id=f"stock_{product_uuid}_{datetime.now(timezone.utc).isoformat()}",
            source_payload=stock_data,
            status=SyncStatus.PROCESSING,
            previous_stock=previous_quantity,
            new_stock=available_quantity
        )
        
        try:
            # Create Shopify client
            shopify_client = create_shopify_client(
                shop_url=tenant.shopify_shop_url,
                access_token_encrypted=tenant.shopify_access_token_encrypted
            )
            
            async with shopify_client:
                # Find the Shopify location ID
                shopify_location_id = mapping.shopify_location_id
                
                if not shopify_location_id:
                    # Use default location from tenant
                    shopify_location_id = tenant.shopify_default_location_id
                
                if not shopify_location_id:
                    raise ValueError("No Shopify location configured")
                
                # Update inventory in Shopify
                result = await shopify_client.set_inventory_level(
                    inventory_item_id=mapping.shopify_inventory_item_id,
                    location_id=shopify_location_id,
                    available=available_quantity
                )
                
                # Update mapping with new stock level
                await mapping_repo.update_stock_with_version(
                    mapping_id=mapping.id,
                    new_susoft_stock=int(new_quantity),
                    new_shopify_stock=available_quantity,
                    expected_version=mapping.version
                )
                
                # Update sync log as success
                await sync_log_repo.update_status(
                    sync_log_id=sync_log.id,
                    status=SyncStatus.SUCCESS,
                    response_payload=result
                )
                
                # Update tenant heartbeat
                await tenant_repo.update_heartbeat(
                    tenant_id=tenant_id,
                    direction="stock_sync"
                )
                
                logger.info(
                    "Stock synced to Shopify",
                    tenant_id=tenant_id,
                    sku=mapping.sku,
                    previous=previous_quantity,
                    new=available_quantity
                )
                
        except ShopifyAPIError as e:
            await sync_log_repo.update_status(
                sync_log_id=sync_log.id,
                status=SyncStatus.FAILED,
                error_message=str(e)
            )
            raise
        except Exception as e:
            await sync_log_repo.update_status(
                sync_log_id=sync_log.id,
                status=SyncStatus.FAILED,
                error_message=str(e)
            )
            raise


@celery_app.task(base=BaseTaskWithRetry, bind=True)
def sync_stock_to_shopify(
    self,
    tenant_id: str,
    product_mappings: Optional[list] = None
):
    """
    Bulk sync stock levels from Susoft to Shopify.
    
    Can be triggered manually or by scheduled task for reconciliation.
    
    Args:
        tenant_id: UUID of the tenant
        product_mappings: Optional list of specific mappings to sync.
                         If None, syncs all active mappings.
    """
    asyncio.get_event_loop().run_until_complete(
        _sync_stock_to_shopify_async(
            task=self,
            tenant_id=tenant_id,
            product_mappings=product_mappings
        )
    )


async def _sync_stock_to_shopify_async(
    task: Task,
    tenant_id: str,
    product_mappings: Optional[list]
):
    """Async implementation of bulk stock sync."""
    async with get_session_context() as session:
        tenant_repo = TenantRepository(session)
        mapping_repo = ProductMappingRepository(session)
        sync_log_repo = SyncLogRepository(session)
        
        tenant = await tenant_repo.get_by_id(tenant_id)
        if not tenant or not tenant.is_active:
            return
        
        # Get mappings to sync
        if product_mappings:
            mappings = product_mappings
        else:
            mappings = await mapping_repo.get_active_mappings(tenant_id)
        
        if not mappings:
            logger.info("No mappings to sync", tenant_id=tenant_id)
            return
        
        # Create clients
        susoft_client = create_susoft_client(
            base_url=tenant.susoft_api_url,
            api_key_encrypted=tenant.susoft_api_key_encrypted,
            integration_id=tenant.susoft_integration_id
        )
        
        shopify_client = create_shopify_client(
            shop_url=tenant.shopify_shop_url,
            access_token_encrypted=tenant.shopify_access_token_encrypted
        )
        
        async with susoft_client, shopify_client:
            # Fetch all stock from Susoft
            susoft_stock = await susoft_client.get_all_stock()
            
            # Build lookup by product UUID
            stock_lookup = {
                item.get("productUuid"): item.get("quantity", 0)
                for item in susoft_stock
            }
            
            # Prepare bulk updates
            updates = []
            for mapping in mappings:
                susoft_qty = stock_lookup.get(mapping.susoft_product_id, 0)
                safety_stock = mapping.safety_stock or 0
                available = max(0, susoft_qty - safety_stock)
                
                shopify_location_id = (
                    mapping.shopify_location_id or 
                    tenant.shopify_default_location_id
                )
                
                if shopify_location_id:
                    updates.append({
                        "inventory_item_id": mapping.shopify_inventory_item_id,
                        "location_id": shopify_location_id,
                        "available": available
                    })
            
            if updates:
                # Use bulk update for efficiency
                await shopify_client.bulk_set_inventory_levels(updates)
                
                logger.info(
                    "Bulk stock sync completed",
                    tenant_id=tenant_id,
                    count=len(updates)
                )


@celery_app.task
def retry_dlq_item(dlq_item_id: str):
    """
    Retry a specific dead letter queue item.
    
    Args:
        dlq_item_id: UUID of the DLQ item to retry
    """
    asyncio.get_event_loop().run_until_complete(
        _retry_dlq_item_async(dlq_item_id)
    )


async def _retry_dlq_item_async(dlq_item_id: str):
    """Async implementation of DLQ retry."""
    async with get_session_context() as session:
        dlq_repo = DeadLetterQueueRepository(session)
        
        dlq_item = await dlq_repo.get_by_id(dlq_item_id)
        if not dlq_item:
            logger.warning("DLQ item not found", dlq_item_id=dlq_item_id)
            return
        
        # Get the original task
        task_name = dlq_item.task_name
        payload = dlq_item.payload
        
        # Re-queue the task
        if task_name == "app.workers.tasks.process_shopify_order":
            process_shopify_order.apply_async(
                args=payload.get("args", []),
                kwargs=payload.get("kwargs", {})
            )
        elif task_name == "app.workers.tasks.process_susoft_stock_change":
            process_susoft_stock_change.apply_async(
                args=payload.get("args", []),
                kwargs=payload.get("kwargs", {})
            )
        
        # Mark DLQ item as retried
        await dlq_repo.mark_retried(dlq_item_id)
        
        logger.info("DLQ item requeued", dlq_item_id=dlq_item_id, task=task_name)
