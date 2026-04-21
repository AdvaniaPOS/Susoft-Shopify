"""
Scheduled Tasks
================
Periodic tasks run by Celery Beat for maintenance and monitoring.

Tasks:
- check_stuck_tasks: Find and alert on stuck tasks (>5 min in processing)
- send_dlq_alerts: Send notifications for unalerted DLQ items
- check_tenant_heartbeats: Monitor tenant integration health
- daily_stock_reconciliation: Full stock sync to catch any drift
"""

import asyncio
from datetime import datetime, timezone, timedelta

import structlog

from app.workers.celery_app import celery_app
from app.workers.tasks import sync_stock_to_shopify, sync_products_to_shopify, redis_client
from app.core.config import settings
from app.core.database import get_session_context
from app.db.repositories import (
    TenantRepository,
    IntegrationQueueRepository,
    DeadLetterQueueRepository,
    SyncLogRepository
)
from app.db.models import IntegrationQueueStatus, SyncStatus
from app.utils.notifier import (
    send_slack_notification,
    send_telegram_notification,
    format_dlq_alert,
    format_stuck_task_alert,
    format_tenant_offline_alert
)


logger = structlog.get_logger()


@celery_app.task
def check_stuck_tasks():
    """
    Check for tasks stuck in processing state for too long.
    
    Alert threshold is configurable via settings.alert_queue_timeout_minutes.
    """
    asyncio.get_event_loop().run_until_complete(
        _check_stuck_tasks_async()
    )


async def _check_stuck_tasks_async():
    """Async implementation of stuck task checker."""
    threshold = timedelta(minutes=settings.alert_queue_timeout_minutes)
    cutoff_time = datetime.now(timezone.utc) - threshold
    
    async with get_session_context() as session:
        queue_repo = IntegrationQueueRepository(session)
        sync_repo = SyncLogRepository(session)
        
        # Find stuck queue items
        stuck_items = await queue_repo.get_stuck_items(cutoff_time)
        
        if not stuck_items:
            logger.debug("No stuck tasks found")
            return
        
        logger.warning(
            "Found stuck tasks",
            count=len(stuck_items),
            threshold_minutes=settings.alert_queue_timeout_minutes
        )
        
        # Group by tenant for alerting
        by_tenant = {}
        for item in stuck_items:
            tenant_id = str(item.tenant_id)
            if tenant_id not in by_tenant:
                by_tenant[tenant_id] = []
            by_tenant[tenant_id].append(item)
        
        # Send alerts
        for tenant_id, items in by_tenant.items():
            message = format_stuck_task_alert(
                tenant_id=tenant_id,
                stuck_tasks=items,
                threshold_minutes=settings.alert_queue_timeout_minutes
            )
            
            await send_slack_notification(message)
            await send_telegram_notification(message)
            
            # Mark items as alerted
            for item in items:
                await queue_repo.mark_alerted(item.id)


@celery_app.task
def send_dlq_alerts():
    """
    Send alerts for dead letter queue items that haven't been alerted yet.
    
    Groups items by tenant and sends summary notification.
    """
    asyncio.get_event_loop().run_until_complete(
        _send_dlq_alerts_async()
    )


async def _send_dlq_alerts_async():
    """Async implementation of DLQ alerter."""
    async with get_session_context() as session:
        dlq_repo = DeadLetterQueueRepository(session)
        
        # Get unalerted items
        unalerted = await dlq_repo.get_unalerted_items()
        
        if not unalerted:
            logger.debug("No new DLQ items to alert")
            return
        
        logger.warning(
            "Found unalerted DLQ items",
            count=len(unalerted)
        )
        
        # Group by tenant
        by_tenant = {}
        for item in unalerted:
            tenant_id = str(item.tenant_id) if item.tenant_id else "unknown"
            if tenant_id not in by_tenant:
                by_tenant[tenant_id] = []
            by_tenant[tenant_id].append(item)
        
        # Send alerts
        for tenant_id, items in by_tenant.items():
            message = format_dlq_alert(
                tenant_id=tenant_id,
                dlq_items=items
            )
            
            await send_slack_notification(message)
            await send_telegram_notification(message)
            
            # Mark items as alerted
            for item in items:
                await dlq_repo.mark_alerted(item.id)


@celery_app.task
def check_tenant_heartbeats():
    """
    Check tenant integration health via heartbeat timestamps.
    
    Alerts if a tenant hasn't synced in too long, indicating
    possible integration issues.
    """
    asyncio.get_event_loop().run_until_complete(
        _check_tenant_heartbeats_async()
    )


async def _check_tenant_heartbeats_async():
    """Async implementation of heartbeat checker."""
    # Alert if no heartbeat in 30 minutes
    threshold = timedelta(minutes=30)
    cutoff_time = datetime.now(timezone.utc) - threshold
    
    async with get_session_context() as session:
        tenant_repo = TenantRepository(session)
        
        # Get active tenants with stale heartbeats
        stale_tenants = await tenant_repo.get_stale_tenants(cutoff_time)
        
        if not stale_tenants:
            logger.debug("All tenants healthy")
            return
        
        logger.warning(
            "Found tenants with stale heartbeats",
            count=len(stale_tenants)
        )
        
        for tenant in stale_tenants:
            message = format_tenant_offline_alert(
                tenant_name=tenant.name,
                tenant_id=str(tenant.id),
                last_heartbeat=tenant.last_sync_at
            )
            
            await send_slack_notification(message)
            await send_telegram_notification(message)


@celery_app.task
def daily_stock_reconciliation():
    """
    Perform daily full stock reconciliation for all active tenants.
    
    This catches any stock drift between systems that may have
    occurred due to failed webhooks or timing issues.
    """
    asyncio.get_event_loop().run_until_complete(
        _daily_stock_reconciliation_async()
    )


async def _daily_stock_reconciliation_async():
    """Async implementation of daily reconciliation."""
    logger.info("Starting daily stock reconciliation")
    
    async with get_session_context() as session:
        tenant_repo = TenantRepository(session)
        
        # Get all active tenants
        active_tenants = await tenant_repo.get_all_active()
        
        for tenant in active_tenants:
            # Queue bulk sync for each tenant
            sync_stock_to_shopify.apply_async(
                kwargs={"tenant_id": str(tenant.id)},
                queue="stock"
            )
        
        logger.info(
            "Queued daily reconciliation for tenants",
            count=len(active_tenants)
        )


@celery_app.task
def schedule_tenant_stock_syncs():
    """
    Queue stock sync for tenants that are due based on sync_interval_seconds.

    This is the main automation loop for stock sync between Susoft and Shopify.
    """
    asyncio.get_event_loop().run_until_complete(
        _schedule_tenant_stock_syncs_async()
    )


def _to_utc(dt: datetime) -> datetime:
    """Normalize naive/aware datetimes to UTC for safe comparisons."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


async def _schedule_tenant_stock_syncs_async():
    """Async implementation of interval-based tenant stock scheduler."""
    now = datetime.now(timezone.utc)

    async with get_session_context() as session:
        tenant_repo = TenantRepository(session)

        tenants = await tenant_repo.get_tenants_with_sync_enabled()
        queued_count = 0

        for tenant in tenants:
            interval = max(60, int(getattr(tenant, "sync_interval_seconds", 300) or 300))

            last_stock_sync = getattr(tenant, "last_stock_sync_at", None)
            last_sync = getattr(tenant, "last_sync_at", None)
            reference_time = last_stock_sync or last_sync

            if reference_time:
                reference_time = _to_utc(reference_time)
                age_seconds = (now - reference_time).total_seconds()
                if age_seconds < interval:
                    continue

            sync_stock_to_shopify.apply_async(
                kwargs={"tenant_id": str(tenant.id)},
                queue="stock"
            )
            queued_count += 1

        if queued_count:
            logger.info(
                "Queued interval-based stock sync tasks",
                tenant_count=queued_count
            )


# Default cadence for product attribute sync (name/price/category/VAT).
# Susoft master data doesn't change as often as stock, so 30 min is a
# reasonable balance between freshness and API load.
PRODUCT_SYNC_INTERVAL_SECONDS = 30 * 60


@celery_app.task
def schedule_tenant_product_syncs():
    """
    Queue product attribute syncs for tenants whose last run is older than
    ``PRODUCT_SYNC_INTERVAL_SECONDS``. Throttling state is kept in Redis
    so we don't need a database migration for ``last_product_sync_at``.
    """
    asyncio.get_event_loop().run_until_complete(
        _schedule_tenant_product_syncs_async()
    )


async def _schedule_tenant_product_syncs_async():
    now = datetime.now(timezone.utc)

    async with get_session_context() as session:
        tenant_repo = TenantRepository(session)
        tenants = await tenant_repo.get_tenants_with_sync_enabled()
        queued_count = 0

        for tenant in tenants:
            tenant_id = str(tenant.id)
            try:
                last_raw = redis_client.get(f"product_sync:last:{tenant_id}")
            except Exception:
                last_raw = None

            if last_raw:
                try:
                    last_dt = datetime.fromisoformat(last_raw.decode("utf-8"))
                    if last_dt.tzinfo is None:
                        last_dt = last_dt.replace(tzinfo=timezone.utc)
                    if (now - last_dt).total_seconds() < PRODUCT_SYNC_INTERVAL_SECONDS:
                        continue
                except Exception:
                    pass  # malformed marker, run anyway

            sync_products_to_shopify.apply_async(
                kwargs={"tenant_id": tenant_id},
                queue="products",
            )
            queued_count += 1

        if queued_count:
            logger.info(
                "Queued product attribute sync tasks",
                tenant_count=queued_count,
            )


@celery_app.task
def cleanup_old_sync_logs():
    """
    Clean up old sync logs to prevent database bloat.
    
    Retains logs for the configured retention period.
    """
    asyncio.get_event_loop().run_until_complete(
        _cleanup_old_sync_logs_async()
    )


async def _cleanup_old_sync_logs_async():
    """Async implementation of log cleanup."""
    # Keep logs for 90 days
    retention_days = 90
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=retention_days)
    
    async with get_session_context() as session:
        sync_repo = SyncLogRepository(session)
        
        deleted_count = await sync_repo.delete_old_logs(cutoff_date)
        
        if deleted_count > 0:
            logger.info(
                "Cleaned up old sync logs",
                deleted_count=deleted_count,
                retention_days=retention_days
            )
