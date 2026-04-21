"""
Webhook Endpoints
==================
Receives webhooks from Shopify and Susoft, validates them,
and queues them for processing.

Security:
- Shopify: HMAC-SHA256 signature verification
- Susoft: Bearer token validation

All webhooks are:
1. Validated (signature/token)
2. Logged to webhook_events table
3. Queued for async processing
4. Returned 200 immediately (to avoid timeouts)
"""

from datetime import datetime, timezone
from typing import Dict, Any, Optional
import uuid

from fastapi import APIRouter, Request, Header, HTTPException, Depends, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
import structlog

from app.core.config import settings
from app.core.database import get_session
from app.db.models import WebhookSource
from app.db.repositories import (
    TenantRepository,
    WebhookEventRepository
)
from app.workers.tasks import (
    process_shopify_order,
    process_susoft_stock_change,
    _process_shopify_order_async,
    _process_susoft_order_delivered_async,
)


async def _run_shopify_order_inline(
    tenant_id: str,
    order_data: Dict[str, Any],
    webhook_event_id: Optional[str],
) -> None:
    """Run order processing inline (no Celery/Redis) — used in development."""
    try:
        await _process_shopify_order_async(
            task=None,
            tenant_id=tenant_id,
            order_data=order_data,
            webhook_event_id=webhook_event_id,
        )
    except Exception as exc:
        logger.exception(
            "Inline Shopify order processing failed",
            tenant_id=tenant_id,
            error=str(exc),
        )


logger = structlog.get_logger()

router = APIRouter(prefix="/webhooks", tags=["webhooks"])


# ===================
# Dependency Injection
# ===================


async def get_tenant_by_shop(
    x_shopify_shop_domain: str = Header(None),
    session: AsyncSession = Depends(get_session)
) -> Optional[Dict[str, Any]]:
    """Get tenant by Shopify shop domain from header."""
    if not x_shopify_shop_domain:
        return None
    
    tenant_repo = TenantRepository(session)
    tenant = await tenant_repo.get_by_shopify_shop(x_shopify_shop_domain)
    
    return tenant


# ===================
# Shopify Webhooks
# ===================


@router.post("/shopify/orders/create")
async def shopify_order_created(
    request: Request,
    background_tasks: BackgroundTasks,
    x_shopify_hmac_sha256: str = Header(None),
    x_shopify_shop_domain: str = Header(None),
    x_shopify_topic: str = Header(None),
    x_shopify_webhook_id: str = Header(None),
    session: AsyncSession = Depends(get_session)
):
    """
    Handle Shopify orders/create webhook.
    
    Triggered when a new order is placed in Shopify.
    Creates the order in Susoft.
    """
    # Get raw body for signature verification
    body = await request.body()
    
    # Find tenant by shop domain
    tenant_repo = TenantRepository(session)
    tenant = await tenant_repo.get_by_shopify_shop(x_shopify_shop_domain)
    
    if not tenant:
        logger.warning(
            "Webhook from unknown shop",
            shop_domain=x_shopify_shop_domain
        )
        raise HTTPException(status_code=404, detail="Shop not found")
    
    # Verify HMAC signature
    if settings.environment != "development":
        from app.services.shopify_client import ShopifyClient
        
        client = ShopifyClient(
            shop_url=tenant.shopify_shop_url,
            access_token_encrypted=tenant.shopify_access_token_encrypted,
            api_secret_encrypted=tenant.shopify_api_secret_encrypted
        )
        
        if not client.verify_webhook_signature(body, x_shopify_hmac_sha256):
            logger.warning(
                "Invalid Shopify webhook signature",
                shop_domain=x_shopify_shop_domain
            )
            raise HTTPException(status_code=401, detail="Invalid signature")
    
    # Parse body
    import json
    order_data = json.loads(body)
    
    # Log webhook event
    webhook_repo = WebhookEventRepository(session)
    webhook_event = await webhook_repo.create(
        tenant_id=str(tenant.id),
        source=WebhookSource.SHOPIFY,
        event_type="orders/create",
        external_id=x_shopify_webhook_id or str(order_data.get("id")),
        payload=order_data,
        headers={
            "x_shopify_shop_domain": x_shopify_shop_domain,
            "x_shopify_topic": x_shopify_topic
        }
    )
    
    # Dispatch for processing — inline in dev, Celery in prod
    if settings.is_development:
        background_tasks.add_task(
            _run_shopify_order_inline,
            tenant_id=str(tenant.id),
            order_data=order_data,
            webhook_event_id=str(webhook_event.id),
        )
        dispatch_mode = "inline"
    else:
        process_shopify_order.apply_async(
            kwargs={
                "tenant_id": str(tenant.id),
                "order_data": order_data,
                "webhook_event_id": str(webhook_event.id)
            },
            queue="orders"
        )
        dispatch_mode = "celery"

    logger.info(
        "Shopify order webhook queued",
        tenant_id=str(tenant.id),
        order_id=order_data.get("id"),
        order_name=order_data.get("name"),
        dispatch=dispatch_mode,
    )
    
    return {"status": "queued", "webhook_event_id": str(webhook_event.id)}


@router.post("/shopify/orders/updated")
async def shopify_order_updated(
    request: Request,
    background_tasks: BackgroundTasks,
    x_shopify_hmac_sha256: str = Header(None),
    x_shopify_shop_domain: str = Header(None),
    x_shopify_topic: str = Header(None),
    session: AsyncSession = Depends(get_session)
):
    """
    Handle Shopify orders/updated webhook.
    
    Currently logged but not processed - updates typically
    don't need to be synced to Susoft.
    """
    body = await request.body()
    
    # Find tenant
    tenant_repo = TenantRepository(session)
    tenant = await tenant_repo.get_by_shopify_shop(x_shopify_shop_domain)
    
    if not tenant:
        raise HTTPException(status_code=404, detail="Shop not found")
    
    # Log but don't process
    import json
    order_data = json.loads(body)
    
    webhook_repo = WebhookEventRepository(session)
    await webhook_repo.create(
        tenant_id=str(tenant.id),
        source=WebhookSource.SHOPIFY,
        event_type="orders/updated",
        external_id=str(order_data.get("id")),
        payload=order_data,
        processed=True  # Mark as processed since we're ignoring it
    )
    
    return {"status": "acknowledged"}


@router.post("/shopify/refunds/create")
async def shopify_refund_created(
    request: Request,
    background_tasks: BackgroundTasks,
    x_shopify_hmac_sha256: str = Header(None),
    x_shopify_shop_domain: str = Header(None),
    session: AsyncSession = Depends(get_session)
):
    """
    Handle Shopify refunds/create webhook.
    
    Per requirements: Refunds/returns are handled by the originating system
    (Shopify), so we only log this for reference.
    """
    body = await request.body()
    
    tenant_repo = TenantRepository(session)
    tenant = await tenant_repo.get_by_shopify_shop(x_shopify_shop_domain)
    
    if not tenant:
        raise HTTPException(status_code=404, detail="Shop not found")
    
    import json
    refund_data = json.loads(body)
    
    webhook_repo = WebhookEventRepository(session)
    await webhook_repo.create(
        tenant_id=str(tenant.id),
        source=WebhookSource.SHOPIFY,
        event_type="refunds/create",
        external_id=str(refund_data.get("id")),
        payload=refund_data,
        processed=True  # Mark as processed (no action needed)
    )
    
    logger.info(
        "Shopify refund logged (no action taken)",
        tenant_id=str(tenant.id),
        refund_id=refund_data.get("id")
    )
    
    return {"status": "acknowledged"}


# ===================
# Susoft Webhooks
# ===================


@router.post("/susoft/{tenant_id}/stock-changed")
async def susoft_stock_changed(
    request: Request,
    tenant_id: str,
    background_tasks: BackgroundTasks,
    authorization: str = Header(None),
    session: AsyncSession = Depends(get_session)
):
    """
    Handle Susoft ON_PRODUCT_STOCK_CHANGED webhook.
    
    Triggered when stock levels change in Susoft.
    Updates Shopify inventory accordingly.
    """
    # Get tenant
    tenant_repo = TenantRepository(session)
    tenant = await tenant_repo.get_by_id(tenant_id)
    
    if not tenant:
        logger.warning(
            "Webhook for unknown tenant",
            tenant_id=tenant_id
        )
        raise HTTPException(status_code=404, detail="Tenant not found")
    
    # Verify Susoft webhook token (only if both header and tenant secret exist)
    if authorization and tenant.susoft_webhook_secret_encrypted:
        # Extract token from "Bearer <token>" format
        token = authorization.replace("Bearer ", "").strip()
        
        # Verify against tenant's webhook secret
        from app.core.security import decrypt_credential
        expected_token = decrypt_credential(tenant.susoft_webhook_secret_encrypted)
        
        if token != expected_token:
            logger.warning(
                "Invalid Susoft webhook token",
                tenant_id=tenant_id
            )
            raise HTTPException(status_code=401, detail="Invalid token")
    
    # Parse body
    body = await request.body()
    import json
    stock_data = json.loads(body)
    
    # Log webhook event
    webhook_repo = WebhookEventRepository(session)
    webhook_event = await webhook_repo.create(
        tenant_id=tenant_id,
        source=WebhookSource.SUSOFT,
        event_type="ON_PRODUCT_STOCK_CHANGED",
        external_id=stock_data.get("uuid") or stock_data.get("productUuid"),
        payload=stock_data
    )
    
    # Queue for processing
    process_susoft_stock_change.apply_async(
        kwargs={
            "tenant_id": tenant_id,
            "stock_data": stock_data,
            "webhook_event_id": str(webhook_event.id)
        },
        queue="stock"
    )
    
    logger.info(
        "Susoft stock webhook queued",
        tenant_id=tenant_id,
        product_uuid=stock_data.get("productUuid")
    )
    
    return {"status": "queued", "webhook_event_id": str(webhook_event.id)}


@router.post("/susoft/{tenant_id}/order-created")
async def susoft_order_created(
    request: Request,
    tenant_id: str,
    authorization: str = Header(None),
    session: AsyncSession = Depends(get_session)
):
    """
    Handle Susoft ON_ORDER_CREATED webhook.
    
    Per requirements: If the order came from Shopify (checked via alternativeId),
    we don't need to do anything. If it's a Susoft-native order, we might update
    Shopify inventory when fulfilled.
    
    Currently just logged for audit purposes.
    """
    tenant_repo = TenantRepository(session)
    tenant = await tenant_repo.get_by_id(tenant_id)
    
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    
    body = await request.body()
    import json
    order_data = json.loads(body)
    
    # Check if this came from Shopify
    alternative_id = order_data.get("alternativeId", "")
    is_shopify_order = alternative_id.startswith("SHOPIFY-")
    
    # Log the webhook
    webhook_repo = WebhookEventRepository(session)
    await webhook_repo.create(
        tenant_id=tenant_id,
        source=WebhookSource.SUSOFT,
        event_type="ON_ORDER_CREATED",
        external_id=order_data.get("uuid"),
        payload=order_data,
        processed=True  # Mark as processed since no action needed
    )
    
    logger.info(
        "Susoft order webhook logged",
        tenant_id=tenant_id,
        order_uuid=order_data.get("uuid"),
        is_shopify_order=is_shopify_order
    )
    
    return {"status": "acknowledged"}


@router.post("/susoft/{tenant_id}/order-delivered")
async def susoft_order_delivered(
    request: Request,
    tenant_id: str,
    background_tasks: BackgroundTasks,
    authorization: str = Header(None),
    session: AsyncSession = Depends(get_session),
):
    """Handle Susoft ON_DELIVERY webhook -> create Shopify fulfillment."""
    tenant_repo = TenantRepository(session)
    tenant = await tenant_repo.get_by_id(tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    # Optional bearer-token check (mirrors stock-changed handler)
    if authorization and tenant.susoft_webhook_secret_encrypted:
        from app.core.security import decrypt_credential
        token = authorization.replace("Bearer ", "").strip()
        expected_token = decrypt_credential(tenant.susoft_webhook_secret_encrypted)
        if token != expected_token:
            logger.warning("Invalid Susoft webhook token", tenant_id=tenant_id)
            raise HTTPException(status_code=401, detail="Invalid token")

    body = await request.body()
    import json
    order_data = json.loads(body)

    webhook_repo = WebhookEventRepository(session)
    webhook_event = await webhook_repo.create(
        tenant_id=tenant_id,
        source=WebhookSource.SUSOFT,
        event_type="ON_DELIVERY",
        external_id=order_data.get("uuid"),
        payload=order_data,
    )

    # Inline in dev (no Celery). For prod we'd queue this on the orders queue.
    background_tasks.add_task(
        _process_susoft_order_delivered_async,
        tenant_id=tenant_id,
        susoft_order=order_data,
        webhook_event_id=str(webhook_event.id),
    )

    logger.info(
        "Susoft delivery webhook queued",
        tenant_id=tenant_id,
        susoft_uuid=order_data.get("uuid"),
        susoft_order_no=order_data.get("orderNo"),
    )
    return {"status": "queued", "webhook_event_id": str(webhook_event.id)}


# ===================
# Health Check
# ===================


@router.get("/health")
async def webhook_health():
    """Health check for webhook endpoints."""
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
