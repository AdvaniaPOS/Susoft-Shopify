"""
Admin API
==========
Administrative endpoints for managing tenants, viewing sync status,
and handling dead letter queue items.

All admin endpoints require authentication via API key.
"""

from datetime import datetime, timezone, timedelta
from typing import Optional, List
import uuid

from fastapi import APIRouter, Depends, HTTPException, Header, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel, Field, field_validator
import structlog

from app.core.config import settings
from app.core.security import encrypt_credential, hash_password, verify_password
from app.core.database import get_session
from app.db.models import SyncStatus, SyncDirection
from app.db.repositories import (
    TenantRepository,
    ProductMappingRepository,
    SyncLogRepository,
    DeadLetterQueueRepository,
    IntegrationQueueRepository
)
from app.services.shopify_webhooks import reconcile_tenant_webhooks
from app.workers.tasks import sync_stock_to_shopify, retry_dlq_item


logger = structlog.get_logger()


async def _reconcile_webhooks_for_tenant(tenant) -> Optional[dict]:
    """Best-effort Shopify webhook reconciliation. Returns result dict or None.

    Skips silently when ``webhook_base_url`` is not configured. All errors are
    swallowed and logged - failure to register webhooks must not break tenant
    administration.
    """
    base_url = settings.webhook_base_url
    if not base_url or not settings.auto_register_webhooks:
        return None
    try:
        result = await reconcile_tenant_webhooks(tenant, base_url=base_url)
        logger.info(
            "Shopify webhooks reconciled",
            tenant_id=str(tenant.id),
            **result.to_dict()["summary"],
        )
        return result.to_dict()
    except Exception as exc:  # noqa: BLE001 - best effort
        logger.exception(
            "Shopify webhook reconciliation failed",
            tenant_id=str(tenant.id),
            error=str(exc),
        )
        return {"error": str(exc)}

router = APIRouter(prefix="/admin", tags=["admin"])


# ===================
# Authentication
# ===================


async def verify_admin_key(
    request: Request,
    x_admin_api_key: str = Header(None),
) -> bool:
    """Verify admin access via either an X-Admin-Api-Key header or a valid
    portal session cookie set by /portal/login."""
    # Allow access if a portal session is present (browser users via /portal)
    try:
        session = request.session  # provided by SessionMiddleware
    except AssertionError:
        session = None
    if session and session.get("portal_user"):
        return True

    configured = settings.admin_api_key
    if configured is None:
        raise HTTPException(
            status_code=503,
            detail="Admin API is disabled: ADMIN_API_KEY is not configured."
        )

    if not x_admin_api_key:
        raise HTTPException(
            status_code=401,
            detail="Missing X-Admin-Api-Key header"
        )

    expected = configured.get_secret_value()
    # Constant-time comparison to avoid timing oracles.
    import hmac
    if not hmac.compare_digest(x_admin_api_key, expected):
        raise HTTPException(
            status_code=403,
            detail="Invalid admin API key"
        )

    return True


# ===================
# Request/Response Models
# ===================


class TenantCreate(BaseModel):
    """Request model for creating a tenant."""
    name: str = Field(..., description="Tenant display name")
    
    # Susoft credentials
    susoft_api_url: str = Field(..., description="Susoft API base URL")
    susoft_api_key: str = Field(..., description="Susoft API key (will be encrypted)")
    susoft_integration_id: str = Field(..., description="Susoft integration ID")
    susoft_webhook_secret: str = Field(..., description="Secret for Susoft webhooks")
    
    # Shopify credentials
    shopify_shop_url: str = Field(..., description="Shopify shop URL (myshop.myshopify.com)")
    shopify_access_token: str = Field(..., description="Shopify access token (will be encrypted)")
    shopify_api_key: Optional[str] = Field(None, description="Shopify API key")
    shopify_api_secret: Optional[str] = Field(None, description="Shopify API secret")
    shopify_default_location_id: Optional[str] = Field(None, description="Default Shopify location ID")
    
    # Settings
    sync_interval_seconds: int = Field(default=300, description="Sync interval in seconds")
    safety_stock_default: int = Field(default=0, description="Default safety stock")


class TenantResponse(BaseModel):
    """Response model for tenant data."""
    id: str
    name: str
    is_active: bool
    susoft_api_url: str
    susoft_integration_id: str
    shopify_shop_url: str
    shopify_default_location_id: Optional[str]
    sync_interval_seconds: int
    safety_stock_default: int
    last_sync_at: Optional[datetime]
    last_order_sync_at: Optional[datetime]
    last_stock_sync_at: Optional[datetime]
    created_at: datetime

    model_config = {"from_attributes": True}

    @field_validator("id", mode="before")
    @classmethod
    def _uuid_to_str(cls, v):
        return str(v) if v is not None else v


class TenantStatus(BaseModel):
    """Response model for tenant status overview."""
    id: str
    name: str
    is_active: bool
    is_healthy: bool
    last_sync_at: Optional[datetime]
    minutes_since_last_sync: Optional[float]
    pending_tasks: int
    failed_tasks_24h: int
    dlq_items: int

    @field_validator("id", mode="before")
    @classmethod
    def _uuid_to_str(cls, v):
        return str(v) if v is not None else v


class ProductMappingCreate(BaseModel):
    """Request model for creating a product mapping."""
    sku: str
    susoft_product_id: str
    shopify_product_id: str
    shopify_variant_id: str
    shopify_inventory_item_id: str
    susoft_location_id: Optional[str] = None
    shopify_location_id: Optional[str] = None
    safety_stock: int = 0


class ProductMappingResponse(BaseModel):
    """Response model for product mapping."""
    id: str
    sku: str
    susoft_product_id: str
    shopify_product_id: str
    shopify_variant_id: str
    shopify_inventory_item_id: str
    safety_stock: int
    current_susoft_stock: Optional[int]
    current_shopify_stock: Optional[int]
    is_active: bool
    last_synced_at: Optional[datetime]

    model_config = {"from_attributes": True}

    @field_validator("id", mode="before")
    @classmethod
    def _uuid_to_str(cls, v):
        return str(v) if v is not None else v


class SyncLogResponse(BaseModel):
    """Response model for sync log entry."""
    id: str
    sync_type: str
    direction: str
    status: str
    external_id: Optional[str]
    error_message: Optional[str]
    previous_stock: Optional[int]
    new_stock: Optional[int]
    created_at: datetime
    completed_at: Optional[datetime]

    model_config = {"from_attributes": True}

    @field_validator("id", mode="before")
    @classmethod
    def _uuid_to_str(cls, v):
        return str(v) if v is not None else v


class DLQItemResponse(BaseModel):
    """Response model for dead letter queue item."""
    id: str
    tenant_id: Optional[str] = None
    task_name: str
    error_message: str
    traceback: Optional[str] = None
    payload: Optional[dict] = None
    retry_count: int
    alerted: bool
    resolved: bool
    created_at: datetime
    last_retry_at: Optional[datetime]

    model_config = {"from_attributes": True}

    @field_validator("id", "tenant_id", mode="before")
    @classmethod
    def _uuid_to_str(cls, v):
        return str(v) if v is not None else v

    @field_validator("id", mode="before")
    @classmethod
    def _uuid_to_str(cls, v):
        return str(v) if v is not None else v


class DashboardStats(BaseModel):
    """Response model for dashboard statistics."""
    total_tenants: int
    active_tenants: int
    healthy_tenants: int
    total_dlq_items: int
    unresolved_dlq_items: int
    syncs_today: int
    failed_syncs_today: int
    success_rate: float


# ===================
# Tenant Management
# ===================


@router.post("/tenants", response_model=TenantResponse)
async def create_tenant(
    tenant_data: TenantCreate,
    session: AsyncSession = Depends(get_session),
    _: bool = Depends(verify_admin_key)
):
    """Create a new tenant with credentials."""
    tenant_repo = TenantRepository(session)
    
    # Check if shop URL already exists
    existing = await tenant_repo.get_by_shopify_shop(tenant_data.shopify_shop_url)
    if existing:
        raise HTTPException(
            status_code=400,
            detail="Tenant with this Shopify shop already exists"
        )
    
    # Encrypt sensitive credentials
    tenant = await tenant_repo.create(
        name=tenant_data.name,
        susoft_api_url=tenant_data.susoft_api_url,
        susoft_api_key_encrypted=encrypt_credential(tenant_data.susoft_api_key),
        susoft_integration_id=tenant_data.susoft_integration_id,
        susoft_webhook_secret_encrypted=encrypt_credential(tenant_data.susoft_webhook_secret),
        shopify_shop_url=tenant_data.shopify_shop_url,
        shopify_access_token_encrypted=encrypt_credential(tenant_data.shopify_access_token),
        shopify_api_key_encrypted=encrypt_credential(tenant_data.shopify_api_key) if tenant_data.shopify_api_key else None,
        shopify_api_secret_encrypted=encrypt_credential(tenant_data.shopify_api_secret) if tenant_data.shopify_api_secret else None,
        shopify_default_location_id=tenant_data.shopify_default_location_id,
        sync_interval_seconds=tenant_data.sync_interval_seconds,
        safety_stock_default=tenant_data.safety_stock_default
    )
    
    logger.info("Tenant created", tenant_id=str(tenant.id), name=tenant_data.name)

    # Best-effort: register Shopify webhooks for the new tenant.
    await _reconcile_webhooks_for_tenant(tenant)

    return TenantResponse.model_validate(tenant)


@router.get("/tenants", response_model=List[TenantResponse])
async def list_tenants(
    include_inactive: bool = False,
    session: AsyncSession = Depends(get_session),
    _: bool = Depends(verify_admin_key)
):
    """List all tenants."""
    tenant_repo = TenantRepository(session)
    
    if include_inactive:
        tenants = await tenant_repo.get_all()
    else:
        tenants = await tenant_repo.get_all_active()
    
    return [TenantResponse.model_validate(t) for t in tenants]


@router.get("/tenants/{tenant_id}", response_model=TenantResponse)
async def get_tenant(
    tenant_id: str,
    session: AsyncSession = Depends(get_session),
    _: bool = Depends(verify_admin_key)
):
    """Get a specific tenant."""
    tenant_repo = TenantRepository(session)
    tenant = await tenant_repo.get_by_id(tenant_id)
    
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    
    return TenantResponse.model_validate(tenant)


@router.get("/tenants/{tenant_id}/status", response_model=TenantStatus)
async def get_tenant_status(
    tenant_id: str,
    session: AsyncSession = Depends(get_session),
    _: bool = Depends(verify_admin_key)
):
    """Get tenant status and health metrics."""
    tenant_repo = TenantRepository(session)
    sync_repo = SyncLogRepository(session)
    dlq_repo = DeadLetterQueueRepository(session)
    queue_repo = IntegrationQueueRepository(session)
    
    tenant = await tenant_repo.get_by_id(tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    
    # Calculate health
    minutes_since_sync = None
    is_healthy = True
    
    if tenant.last_sync_at:
        delta = datetime.now(timezone.utc) - tenant.last_sync_at
        minutes_since_sync = delta.total_seconds() / 60
        is_healthy = minutes_since_sync < 30  # Unhealthy if no sync in 30 min
    else:
        is_healthy = False
    
    # Get counts
    pending_tasks = await queue_repo.get_pending_count(tenant_id)
    failed_24h = await sync_repo.get_failed_count_since(
        tenant_id,
        datetime.now(timezone.utc) - timedelta(hours=24)
    )
    dlq_count = await dlq_repo.get_unresolved_count(tenant_id)
    
    return TenantStatus(
        id=str(tenant.id),
        name=tenant.name,
        is_active=tenant.is_active,
        is_healthy=is_healthy,
        last_sync_at=tenant.last_sync_at,
        minutes_since_last_sync=minutes_since_sync,
        pending_tasks=pending_tasks,
        failed_tasks_24h=failed_24h,
        dlq_items=dlq_count
    )


@router.patch("/tenants/{tenant_id}/activate")
async def activate_tenant(
    tenant_id: str,
    session: AsyncSession = Depends(get_session),
    _: bool = Depends(verify_admin_key)
):
    """Activate a tenant."""
    tenant_repo = TenantRepository(session)
    
    updated = await tenant_repo.set_active(tenant_id, True)
    if not updated:
        raise HTTPException(status_code=404, detail="Tenant not found")
    
    return {"status": "activated"}


@router.patch("/tenants/{tenant_id}/deactivate")
async def deactivate_tenant(
    tenant_id: str,
    session: AsyncSession = Depends(get_session),
    _: bool = Depends(verify_admin_key)
):
    """Deactivate a tenant."""
    tenant_repo = TenantRepository(session)
    
    updated = await tenant_repo.set_active(tenant_id, False)
    if not updated:
        raise HTTPException(status_code=404, detail="Tenant not found")
    
    return {"status": "deactivated"}


# ===================
# Shopify Webhook Registration
# ===================


@router.post("/tenants/{tenant_id}/webhooks/register")
async def register_tenant_webhooks(
    tenant_id: str,
    session: AsyncSession = Depends(get_session),
    _: bool = Depends(verify_admin_key)
):
    """Reconcile (idempotently create / update) Shopify webhooks for a tenant.

    Requires ``WEBHOOK_BASE_URL`` to be configured. Safe to call repeatedly -
    webhooks already pointing to the correct address are kept as-is.
    """
    if not settings.webhook_base_url:
        raise HTTPException(
            status_code=400,
            detail="webhook_base_url is not configured; set WEBHOOK_BASE_URL to enable.",
        )

    tenant_repo = TenantRepository(session)
    tenant = await tenant_repo.get_by_id(tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    from app.services.shopify_webhooks import reconcile_tenant_webhooks
    result = await reconcile_tenant_webhooks(
        tenant,
        base_url=settings.webhook_base_url,
    )
    return result.to_dict()


@router.get("/tenants/{tenant_id}/webhooks")
async def list_tenant_webhooks(
    tenant_id: str,
    session: AsyncSession = Depends(get_session),
    _: bool = Depends(verify_admin_key)
):
    """List Shopify webhook subscriptions currently registered on the tenant's shop."""
    tenant_repo = TenantRepository(session)
    tenant = await tenant_repo.get_by_id(tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    from app.services.shopify_client import ShopifyClient
    client = ShopifyClient(
        shop_url=tenant.shopify_shop_url,
        access_token_encrypted=tenant.shopify_access_token_encrypted,
        api_key_encrypted=getattr(tenant, "shopify_api_key_encrypted", None),
        api_secret_encrypted=getattr(tenant, "shopify_api_secret_encrypted", None),
    )
    async with client:
        webhooks = await client.list_webhooks()

    return {
        "tenant_id": tenant_id,
        "shop_url": tenant.shopify_shop_url,
        "configured_base_url": settings.webhook_base_url,
        "count": len(webhooks),
        "webhooks": webhooks,
    }


# ===================
# Product Mappings
# ===================


@router.post("/tenants/{tenant_id}/mappings", response_model=ProductMappingResponse)
async def create_product_mapping(
    tenant_id: str,
    mapping_data: ProductMappingCreate,
    session: AsyncSession = Depends(get_session),
    _: bool = Depends(verify_admin_key)
):
    """Create a product mapping."""
    tenant_repo = TenantRepository(session)
    mapping_repo = ProductMappingRepository(session)
    
    # Verify tenant exists
    tenant = await tenant_repo.get_by_id(tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    
    # Check for duplicate SKU
    existing = await mapping_repo.get_by_sku(tenant_id, mapping_data.sku)
    if existing:
        raise HTTPException(
            status_code=400,
            detail=f"Mapping for SKU {mapping_data.sku} already exists"
        )
    
    mapping = await mapping_repo.create(
        tenant_id=tenant_id,
        sku=mapping_data.sku,
        susoft_product_id=mapping_data.susoft_product_id,
        shopify_product_id=mapping_data.shopify_product_id,
        shopify_variant_id=mapping_data.shopify_variant_id,
        shopify_inventory_item_id=mapping_data.shopify_inventory_item_id,
        susoft_location_id=mapping_data.susoft_location_id,
        shopify_location_id=mapping_data.shopify_location_id,
        safety_stock=mapping_data.safety_stock
    )
    
    return ProductMappingResponse.model_validate(mapping)


@router.get("/tenants/{tenant_id}/mappings", response_model=List[ProductMappingResponse])
async def list_product_mappings(
    tenant_id: str,
    session: AsyncSession = Depends(get_session),
    _: bool = Depends(verify_admin_key)
):
    """List all product mappings for a tenant."""
    mapping_repo = ProductMappingRepository(session)
    
    mappings = await mapping_repo.get_all_for_tenant(tenant_id)
    
    return [ProductMappingResponse.model_validate(m) for m in mappings]


@router.delete("/tenants/{tenant_id}/mappings/{mapping_id}")
async def delete_product_mapping(
    tenant_id: str,
    mapping_id: str,
    session: AsyncSession = Depends(get_session),
    _: bool = Depends(verify_admin_key)
):
    """Delete a product mapping."""
    mapping_repo = ProductMappingRepository(session)
    
    deleted = await mapping_repo.delete(mapping_id, tenant_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Mapping not found")
    
    return {"status": "deleted"}


# ===================
# Sync Operations
# ===================


@router.post("/tenants/{tenant_id}/sync-stock")
async def trigger_stock_sync(
    tenant_id: str,
    session: AsyncSession = Depends(get_session),
    _: bool = Depends(verify_admin_key)
):
    """Trigger a full stock sync for a tenant."""
    tenant_repo = TenantRepository(session)
    
    tenant = await tenant_repo.get_by_id(tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    
    # Queue the sync task
    sync_stock_to_shopify.apply_async(
        kwargs={"tenant_id": tenant_id},
        queue="stock"
    )
    
    logger.info("Manual stock sync triggered", tenant_id=tenant_id)
    
    return {"status": "queued"}


@router.post("/tenants/{tenant_id}/test-connection")
async def test_tenant_connection(
    tenant_id: str,
    session: AsyncSession = Depends(get_session),
    _: bool = Depends(verify_admin_key),
):
    """Live-ping Susoft and Shopify APIs for a tenant. Returns ok/error per side."""
    from app.services.shopify_client import create_shopify_client
    from app.services.susoft_client import create_susoft_client

    tenant_repo = TenantRepository(session)
    tenant = await tenant_repo.get_by_id(tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    result: Dict[str, Any] = {"susoft": {"ok": False}, "shopify": {"ok": False}}

    # Susoft
    try:
        susoft_client = create_susoft_client(
            base_url=tenant.susoft_api_url,
            api_key_encrypted=tenant.susoft_api_key_encrypted,
            integration_id=tenant.susoft_integration_id,
        )
        async with susoft_client:
            healthy = await susoft_client.health_check()
            result["susoft"] = {"ok": bool(healthy), "shop_url_key": tenant.susoft_integration_id}
    except Exception as exc:  # noqa: BLE001
        result["susoft"] = {"ok": False, "error": str(exc)}

    # Shopify
    try:
        shopify_client = create_shopify_client(
            shop_url=tenant.shopify_shop_url,
            access_token_encrypted=tenant.shopify_access_token_encrypted,
        )
        async with shopify_client:
            info = await shopify_client.get_shop_info()
            result["shopify"] = {
                "ok": True,
                "shop_name": info.get("name"),
                "domain": info.get("myshopify_domain") or info.get("domain"),
            }
    except Exception as exc:  # noqa: BLE001
        result["shopify"] = {"ok": False, "error": str(exc)}

    return result


@router.post("/tenants/{tenant_id}/rebootstrap-mappings")
async def rebootstrap_mappings(
    tenant_id: str,
    match_key: str = Query(default="barcode", pattern="^(barcode|id|external_ref)$"),
    safety_stock: int = Query(default=0, ge=0),
    deactivate_unmatched: bool = Query(default=True),
    session: AsyncSession = Depends(get_session),
    _: bool = Depends(verify_admin_key),
):
    """Rebuild ProductMapping rows for a tenant from live Susoft + Shopify data.

    For every Susoft product the SKU/barcode is matched against Shopify variant
    SKUs. New mappings are created; existing mappings (matched by Susoft id)
    are updated to point at the matched Shopify variant. Optionally,
    unmatched existing mappings are deactivated so they no longer pull stock.
    """
    from app.services.shopify_client import create_shopify_client
    from app.services.susoft_client import create_susoft_client
    from app.db.models import ProductMapping
    from sqlalchemy import select, update as sa_update

    tenant_repo = TenantRepository(session)
    mapping_repo = ProductMappingRepository(session)

    tenant = await tenant_repo.get_by_id(tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    susoft_client = create_susoft_client(
        base_url=tenant.susoft_api_url,
        api_key_encrypted=tenant.susoft_api_key_encrypted,
        integration_id=tenant.susoft_integration_id,
    )
    shopify_client = create_shopify_client(
        shop_url=tenant.shopify_shop_url,
        access_token_encrypted=tenant.shopify_access_token_encrypted,
    )

    def _norm(s):
        if s is None:
            return None
        s = str(s).strip().lower()
        return s or None

    def _susoft_match_value(p):
        if match_key == "barcode":
            return _norm(p.get("barcode"))
        if match_key == "id":
            return _norm(p.get("id") or p.get("productId"))
        return _norm(p.get("externalRefId"))

    async with susoft_client, shopify_client:
        susoft_products = await susoft_client.get_all_products()
        shopify_products = await shopify_client.get_all_products(fields="id,title,variants")
        default_loc = tenant.shopify_default_location_id

        # Build Shopify SKU -> variant index
        shop_index = {}
        for prod in shopify_products:
            for var in prod.get("variants") or []:
                sku = _norm(var.get("sku"))
                if sku and sku not in shop_index:
                    shop_index[sku] = {
                        "product_id": str(prod.get("id")),
                        "variant_id": str(var.get("id")),
                        "inventory_item_id": str(var.get("inventory_item_id")),
                    }

        # Build Susoft id -> match value
        susoft_by_id = {}
        for p in susoft_products:
            pid = p.get("id") or p.get("productId")
            if pid is None:
                continue
            susoft_by_id[str(pid)] = (_susoft_match_value(p), p)

        existing = await mapping_repo.get_all_for_tenant(tenant.id, active_only=False)
        existing_by_susoft = {m.susoft_product_id: m for m in existing}

        created = 0
        updated = 0
        deactivated = 0
        unmatched_susoft = 0
        matched_ids: set[str] = set()

        for sus_id, (mval, prod) in susoft_by_id.items():
            if not mval:
                continue
            shop_match = shop_index.get(mval)
            if not shop_match:
                unmatched_susoft += 1
                continue
            matched_ids.add(sus_id)
            mapping = existing_by_susoft.get(sus_id)
            if mapping:
                mapping.sku = mval
                mapping.shopify_product_id = shop_match["product_id"]
                mapping.shopify_variant_id = shop_match["variant_id"]
                mapping.shopify_inventory_item_id = shop_match["inventory_item_id"]
                if not mapping.shopify_location_id:
                    mapping.shopify_location_id = default_loc
                mapping.is_active = True
                updated += 1
            else:
                await mapping_repo.create(
                    tenant_id=str(tenant.id),
                    sku=mval,
                    susoft_product_id=sus_id,
                    shopify_product_id=shop_match["product_id"],
                    shopify_variant_id=shop_match["variant_id"],
                    shopify_inventory_item_id=shop_match["inventory_item_id"],
                    shopify_location_id=default_loc,
                    safety_stock=safety_stock,
                )
                created += 1

        if deactivate_unmatched:
            for m in existing:
                if m.is_active and m.susoft_product_id not in matched_ids:
                    m.is_active = False
                    deactivated += 1

        await session.commit()

    logger.info(
        "Rebootstrap complete",
        tenant_id=tenant_id,
        created=created,
        updated=updated,
        deactivated=deactivated,
    )
    return {
        "status": "ok",
        "match_key": match_key,
        "susoft_products": len(susoft_by_id),
        "shopify_skus": len(shop_index),
        "created": created,
        "updated": updated,
        "deactivated_existing": deactivated,
        "unmatched_susoft": unmatched_susoft,
    }


@router.get("/tenants/{tenant_id}/diagnose-stock")
async def diagnose_stock(
    tenant_id: str,
    only_mismatch: bool = Query(default=False),
    session: AsyncSession = Depends(get_session),
    _: bool = Depends(verify_admin_key),
):
    """Compare Susoft live stock vs Shopify per-location stock for every active mapping."""
    from app.services.shopify_client import create_shopify_client
    from app.services.susoft_client import create_susoft_client

    tenant_repo = TenantRepository(session)
    mapping_repo = ProductMappingRepository(session)

    tenant = await tenant_repo.get_by_id(tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    mappings = await mapping_repo.get_active_mappings(tenant_id)
    susoft_client = create_susoft_client(
        base_url=tenant.susoft_api_url,
        api_key_encrypted=tenant.susoft_api_key_encrypted,
        integration_id=tenant.susoft_integration_id,
    )
    shopify_client = create_shopify_client(
        shop_url=tenant.shopify_shop_url,
        access_token_encrypted=tenant.shopify_access_token_encrypted,
    )

    rows: list[dict] = []
    summary = {"inspected": len(mappings), "no_susoft": 0, "no_shopify": 0, "drift": 0, "ok": 0}

    async with susoft_client, shopify_client:
        susoft_products = await susoft_client.get_all_products()
        stock_lookup: dict[str, int] = {}
        for item in susoft_products:
            pid = item.get("id") or item.get("productId")
            if pid is None:
                continue
            stock_obj = item.get("stock") or {}
            qty = stock_obj.get("stock", 0) if isinstance(stock_obj, dict) else stock_obj
            try:
                stock_lookup[str(pid)] = int(qty or 0)
            except (TypeError, ValueError):
                stock_lookup[str(pid)] = 0

        locations = await shopify_client.get_locations()
        location_ids = [str(loc.get("id")) for loc in locations if loc.get("id")]
        location_names = {str(loc.get("id")): loc.get("name", "?") for loc in locations}

        inv_lookup: dict[str, dict[str, int]] = {}
        inv_ids = [str(m.shopify_inventory_item_id) for m in mappings if m.shopify_inventory_item_id]
        for i in range(0, len(inv_ids), 50):
            chunk = inv_ids[i : i + 50]
            levels = await shopify_client.get_inventory_levels(chunk, location_ids=location_ids or None)
            for lvl in levels:
                iid = str(lvl.get("inventory_item_id"))
                lid = str(lvl.get("location_id"))
                inv_lookup.setdefault(iid, {})[lid] = int(lvl.get("available") or 0)

        for m in mappings:
            sus_id = str(m.susoft_product_id)
            live_sus = stock_lookup.get(sus_id)
            safety = m.safety_stock or 0
            expected = max(0, (live_sus or 0) - safety) if live_sus is not None else None
            inv_at = inv_lookup.get(str(m.shopify_inventory_item_id), {})
            live_shop = sum(inv_at.values()) if inv_at else None
            per_loc = {location_names.get(lid, lid): qty for lid, qty in inv_at.items()}

            if live_sus is None:
                status = "no_susoft"
                summary["no_susoft"] += 1
            elif live_shop is None:
                status = "no_shopify"
                summary["no_shopify"] += 1
            elif expected != live_shop:
                status = "drift"
                summary["drift"] += 1
            else:
                status = "ok"
                summary["ok"] += 1

            if only_mismatch and status == "ok":
                continue

            rows.append({
                "sku": m.sku,
                "susoft_id": sus_id,
                "live_susoft": live_sus,
                "db_susoft": m.current_susoft_stock,
                "db_shopify": m.current_shopify_stock,
                "live_shopify_total": live_shop,
                "live_shopify_per_location": per_loc,
                "safety_stock": safety,
                "expected_shopify": expected,
                "status": status,
                "diff": (live_shop - expected) if (live_shop is not None and expected is not None) else None,
            })

    return {"tenant_id": tenant_id, "summary": summary, "locations": location_names, "rows": rows}


@router.post("/tenants/{tenant_id}/orders/{shopify_order_id}/close-after-susoft")
async def close_order_after_susoft(
    tenant_id: str,
    shopify_order_id: str,
    susoft_uuid: Optional[str] = None,
    session: AsyncSession = Depends(get_session),
    _: bool = Depends(verify_admin_key),
):
    """Manually replay the post-Susoft success actions on a Shopify order.

    Useful when the original webhook task succeeded in Susoft but the close /
    tag step failed (network blip, Shopify rate-limit, etc.). Tags the order,
    appends a Susoft note and closes it (if the tenant has the close flag on).
    """
    from app.workers.tasks import _post_susoft_success_actions  # local import

    tenant_repo = TenantRepository(session)
    tenant = await tenant_repo.get_by_id(tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    fake_result: Dict[str, Any] = {"uuid": susoft_uuid} if susoft_uuid else {}
    await _post_susoft_success_actions(
        tenant=tenant,
        shopify_order_id=shopify_order_id,
        shopify_order_name=f"#{shopify_order_id}",
        susoft_result=fake_result,
    )
    return {"status": "ok", "tenant_id": tenant_id, "order_id": shopify_order_id}


@router.get("/tenants/{tenant_id}/sync-logs", response_model=List[SyncLogResponse])
async def get_sync_logs(
    tenant_id: str,
    limit: int = Query(default=50, le=500),
    status: Optional[str] = None,
    sync_type: Optional[str] = None,
    session: AsyncSession = Depends(get_session),
    _: bool = Depends(verify_admin_key)
):
    """Get sync logs for a tenant."""
    sync_repo = SyncLogRepository(session)
    
    logs = await sync_repo.get_for_tenant(
        tenant_id=tenant_id,
        limit=limit,
        status=SyncStatus(status) if status else None,
        sync_type=sync_type
    )
    
    return [SyncLogResponse.model_validate(log) for log in logs]


# ===================
# Dead Letter Queue
# ===================


@router.get("/dlq", response_model=List[DLQItemResponse])
async def list_dlq_items(
    tenant_id: Optional[str] = None,
    unresolved_only: bool = True,
    limit: int = Query(default=50, le=500),
    session: AsyncSession = Depends(get_session),
    _: bool = Depends(verify_admin_key)
):
    """List dead letter queue items."""
    dlq_repo = DeadLetterQueueRepository(session)
    
    items = await dlq_repo.get_items(
        tenant_id=tenant_id,
        unresolved_only=unresolved_only,
        limit=limit
    )
    
    return [DLQItemResponse.model_validate(item) for item in items]


@router.post("/dlq/{dlq_id}/retry")
async def retry_dlq(
    dlq_id: str,
    session: AsyncSession = Depends(get_session),
    _: bool = Depends(verify_admin_key)
):
    """Retry a dead letter queue item."""
    dlq_repo = DeadLetterQueueRepository(session)
    
    item = await dlq_repo.get_by_id(dlq_id)
    if not item:
        raise HTTPException(status_code=404, detail="DLQ item not found")
    
    # Queue the retry
    retry_dlq_item.apply_async(args=[dlq_id])
    
    logger.info("DLQ retry queued", dlq_id=dlq_id)
    
    return {"status": "retry_queued"}


@router.post("/dlq/{dlq_id}/resolve")
async def resolve_dlq(
    dlq_id: str,
    session: AsyncSession = Depends(get_session),
    _: bool = Depends(verify_admin_key)
):
    """Mark a DLQ item as resolved without retrying."""
    dlq_repo = DeadLetterQueueRepository(session)
    
    updated = await dlq_repo.mark_resolved(dlq_id)
    if not updated:
        raise HTTPException(status_code=404, detail="DLQ item not found")
    
    return {"status": "resolved"}


# ===================
# Dashboard
# ===================


@router.get("/dashboard", response_model=DashboardStats)
async def get_dashboard_stats(
    session: AsyncSession = Depends(get_session),
    _: bool = Depends(verify_admin_key)
):
    """Get dashboard statistics."""
    tenant_repo = TenantRepository(session)
    sync_repo = SyncLogRepository(session)
    dlq_repo = DeadLetterQueueRepository(session)
    
    # Tenant stats
    all_tenants = await tenant_repo.get_all()
    active_tenants = [t for t in all_tenants if t.is_active]
    
    # Calculate healthy tenants
    threshold = datetime.now(timezone.utc) - timedelta(minutes=30)
    healthy_tenants = [
        t for t in active_tenants
        if t.last_sync_at and t.last_sync_at > threshold
    ]
    
    # DLQ stats
    total_dlq = await dlq_repo.get_total_count()
    unresolved_dlq = await dlq_repo.get_unresolved_count()
    
    # Sync stats for today
    today_start = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    syncs_today = await sync_repo.get_count_since(today_start)
    failed_today = await sync_repo.get_failed_count_since(None, today_start)
    
    success_rate = 0.0
    if syncs_today > 0:
        success_rate = ((syncs_today - failed_today) / syncs_today) * 100
    
    return DashboardStats(
        total_tenants=len(all_tenants),
        active_tenants=len(active_tenants),
        healthy_tenants=len(healthy_tenants),
        total_dlq_items=total_dlq,
        unresolved_dlq_items=unresolved_dlq,
        syncs_today=syncs_today,
        failed_syncs_today=failed_today,
        success_rate=round(success_rate, 1)
    )


# ===================
# Health
# ===================


@router.get("/health")
async def admin_health():
    """Health check for admin API."""
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


# ===================
# Application Logs
# ===================

from app.core.logging_service import get_log_buffer, get_file_writer, LogLevel


class AppLogResponse(BaseModel):
    """Response model for application log entry."""
    timestamp: str
    level: str
    message: str
    logger_name: str
    tenant_id: Optional[str] = None
    extra: Optional[dict] = None
    traceback: Optional[str] = None


@router.get("/app-logs", response_model=List[AppLogResponse])
async def get_application_logs(
    limit: int = Query(default=100, le=500),
    level: Optional[str] = None,
    tenant_id: Optional[str] = None,
    search: Optional[str] = None,
    _: bool = Depends(verify_admin_key)
):
    """
    Get recent application logs from memory buffer.
    
    These are real-time logs from the running application.
    """
    buffer = get_log_buffer()
    
    log_level = LogLevel(level) if level else None
    entries = await buffer.get_recent(
        limit=limit,
        level=log_level,
        tenant_id=tenant_id,
        search=search
    )
    
    return [
        AppLogResponse(
            timestamp=e.timestamp,
            level=e.level,
            message=e.message,
            logger_name=e.logger_name,
            tenant_id=e.tenant_id,
            extra=e.extra,
            traceback=e.traceback
        )
        for e in entries
    ]


@router.get("/app-logs/dates")
async def get_log_dates(
    _: bool = Depends(verify_admin_key)
):
    """Get list of dates with available log files."""
    writer = get_file_writer()
    dates = writer.get_available_dates()
    return {"dates": dates}


@router.get("/app-logs/file/{date}", response_model=List[AppLogResponse])
async def get_logs_from_file(
    date: str,
    limit: int = Query(default=500, le=2000),
    _: bool = Depends(verify_admin_key)
):
    """
    Get logs from a specific date's log file.
    
    Date format: YYYY-MM-DD
    """
    writer = get_file_writer()
    entries = await writer.read_logs(date=date, limit=limit)
    
    return [
        AppLogResponse(
            timestamp=e.timestamp,
            level=e.level,
            message=e.message,
            logger_name=e.logger_name,
            tenant_id=e.tenant_id,
            extra=e.extra,
            traceback=e.traceback
        )
        for e in entries
    ]


# ===================
# Connection Testing
# ===================


class ConnectionTestResult(BaseModel):
    """Result of a connection test."""
    susoft_connected: bool
    susoft_error: Optional[str] = None
    shopify_connected: bool
    shopify_error: Optional[str] = None
    tested_at: str


@router.post("/tenants/{tenant_id}/test-connection", response_model=ConnectionTestResult)
async def test_tenant_connections(
    tenant_id: str,
    session: AsyncSession = Depends(get_session),
    _: bool = Depends(verify_admin_key)
):
    """
    Test Susoft and Shopify API connections for a tenant.
    """
    from app.services.susoft_client import SusoftClient
    from app.services.shopify_client import ShopifyClient
    from app.core.security import decrypt_credential
    
    tenant_repo = TenantRepository(session)
    tenant = await tenant_repo.get_by_id(tenant_id)
    
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    
    result = ConnectionTestResult(
        susoft_connected=False,
        shopify_connected=False,
        tested_at=datetime.now(timezone.utc).isoformat()
    )
    
    # Test Susoft
    try:
        susoft_client = SusoftClient(
            base_url=tenant.susoft_api_url,
            api_key=decrypt_credential(tenant.susoft_api_key_encrypted)
        )
        await susoft_client.health_check()
        result.susoft_connected = True
    except Exception as e:
        result.susoft_error = str(e)
    
    # Test Shopify
    try:
        shopify_client = ShopifyClient(
            shop_url=tenant.shopify_shop_url,
            access_token=decrypt_credential(tenant.shopify_access_token_encrypted)
        )
        await shopify_client.health_check()
        result.shopify_connected = True
    except Exception as e:
        result.shopify_error = str(e)
    
    return result
