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
                    mapping_repo=mapping_repo,
                    susoft_shop_id=tenant.susoft_integration_id
                )
                
                # Create order in Susoft with idempotency.
                # SusoftClient.create_order sets alternativeId/uuid from
                # shopify_order_id internally.
                # use_pos_endpoint=None -> auto: paid orders go to /order/pos
                # (deducts stock immediately, like an aPOS sale); unpaid
                # orders go to /order (waits for invoicing). create_order
                # has built-in fallback from /order/pos -> /order if POS fails.
                result = await susoft_client.create_order(
                    order_data=susoft_order,
                    shopify_order_id=order_id,
                    use_pos_endpoint=None,
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
                    direction="order_sync"
                )
                
                logger.info(
                    "Order synced to Susoft",
                    tenant_id=tenant_id,
                    shopify_order=order_name,
                    susoft_order_id=result.get("uuid")
                )

                # Mark + close Shopify order so nobody fulfills it manually
                await _post_susoft_success_actions(
                    tenant=tenant,
                    shopify_order_id=order_id,
                    shopify_order_name=order_name,
                    susoft_result=result,
                )

        except SusoftAPIError as e:
            await sync_log_repo.update_status(
                sync_log_id=sync_log.id,
                status=SyncStatus.FAILED,
                error_message=str(e)
            )
            await _post_susoft_failure_actions(
                tenant=tenant,
                shopify_order_id=order_id,
                shopify_order_name=order_name,
                error_message=str(e),
            )
            raise
        except Exception as e:
            await sync_log_repo.update_status(
                sync_log_id=sync_log.id,
                status=SyncStatus.FAILED,
                error_message=str(e)
            )
            await _post_susoft_failure_actions(
                tenant=tenant,
                shopify_order_id=order_id,
                shopify_order_name=order_name,
                error_message=str(e),
            )
            raise


async def _post_susoft_success_actions(
    tenant,
    shopify_order_id: str,
    shopify_order_name: str,
    susoft_result: Dict[str, Any],
) -> None:
    """Tag, annotate and (optionally) close the Shopify order after a successful
    Susoft create.

    Best-effort: any failure here is logged but does NOT raise — the Susoft
    order has already been created, so we must not retry the whole task and
    risk a duplicate. Operators can re-run the post-actions manually via the
    admin endpoint if needed.
    """
    try:
        shopify_client = create_shopify_client(
            shop_url=tenant.shopify_shop_url,
            access_token_encrypted=tenant.shopify_access_token_encrypted,
        )
        async with shopify_client:
            susoft_uuid = susoft_result.get("uuid") or susoft_result.get("id") or ""
            # Only treat orderNo as real if Susoft actually returned one.
            # Falling back to alternativeId would mislead — alternativeId is
            # OUR id (e.g. SHOPIFY-12054937108844), not a Susoft orderNo.
            real_order_no = susoft_result.get("orderNo")
            susoft_order_no = real_order_no or ""
            alt_id = susoft_result.get("alternativeId") or ""
            is_stub = bool(susoft_result.get("duplicate")) and not real_order_no
            tag = getattr(tenant, "shopify_synced_tag", None) or "susoft-synced"
            tags_to_add = [tag]
            # Shopify tags are limited to 40 characters; full UUIDs (36 chars)
            # plus a "susoft-id-" prefix exceed that. Prefer the short numeric
            # orderNo if present, otherwise fall back to the first 8 chars of
            # the UUID for traceability.
            if susoft_order_no:
                short_ref = str(susoft_order_no)
            elif susoft_uuid:
                short_ref = str(susoft_uuid).split("-")[0]
            else:
                short_ref = ""
            if short_ref:
                tags_to_add.append(f"susoft-id-{short_ref}")
            if is_stub:
                # Flag so we can audit which orders need manual verification.
                tags_to_add.append("susoft-needs-verify")

            await shopify_client.add_order_tags(shopify_order_id, tags_to_add)

            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            if is_stub:
                # We don't actually know if the order is in Susoft. Be honest
                # in the note so staff can verify manually.
                note_line = (
                    f"[Susoft] {ts} — sync uncertain: Susoft returned "
                    f"duplicate without order data (alternativeId={alt_id}). "
                    f"Please verify in Susoft."
                )
            else:
                note_parts = [f"[Susoft] {ts} — order created in Susoft"]
                if susoft_order_no:
                    note_parts.append(f"orderNo={susoft_order_no}")
                if susoft_uuid:
                    note_parts.append(f"uuid={susoft_uuid}")
                note_line = note_parts[0] if len(note_parts) == 1 else (
                    note_parts[0] + " (" + ", ".join(note_parts[1:]) + ")"
                )
            await shopify_client.append_order_note(shopify_order_id, note_line)

            should_close = getattr(tenant, "close_orders_after_susoft", True)
            if should_close:
                await shopify_client.close_order(shopify_order_id)
                logger.info(
                    "Shopify order closed after Susoft sync",
                    shopify_order=shopify_order_name,
                    susoft_uuid=susoft_uuid,
                    susoft_order_no=susoft_order_no,
                )
    except Exception as exc:  # noqa: BLE001 - best-effort, never re-raise
        logger.warning(
            "post_susoft_success_actions failed (non-fatal)",
            shopify_order=shopify_order_name,
            error=str(exc),
        )


async def _post_susoft_failure_actions(
    tenant,
    shopify_order_id: str,
    shopify_order_name: str,
    error_message: str,
) -> None:
    """Tag and annotate the Shopify order after Susoft create failed.

    Does NOT close the order — operator needs to investigate and either
    re-trigger the sync or fulfill manually in Shopify. Best-effort: never
    re-raises so the original Celery exception propagates cleanly.
    """
    try:
        shopify_client = create_shopify_client(
            shop_url=tenant.shopify_shop_url,
            access_token_encrypted=tenant.shopify_access_token_encrypted,
        )
        async with shopify_client:
            tag = getattr(tenant, "shopify_failed_tag", None) or "susoft-failed"
            await shopify_client.add_order_tags(shopify_order_id, [tag])

            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            note_line = f"[Susoft] {ts} — sync FAILED: {error_message[:300]}"
            await shopify_client.append_order_note(shopify_order_id, note_line)
    except Exception as exc:  # noqa: BLE001 - best-effort, never re-raise
        logger.warning(
            "post_susoft_failure_actions failed (non-fatal)",
            shopify_order=shopify_order_name,
            error=str(exc),
        )


# ---------------------------------------------------------------------------
# Susoft ON_DELIVERY -> Shopify fulfillment
# ---------------------------------------------------------------------------


def _extract_shopify_order_id(susoft_order: Dict[str, Any]) -> Optional[str]:
    """Pull the Shopify order id out of a Susoft order's ``alternativeId``.

    We set ``alternativeId = "SHOPIFY-{order_id}"`` when creating the order in
    Susoft, so reverse-mapping is just stripping the prefix.
    """
    alt = (susoft_order.get("alternativeId") or "").strip()
    if not alt.upper().startswith("SHOPIFY-"):
        return None
    rest = alt.split("-", 1)[1]
    # ``test_order_flow.py`` appends a timestamp suffix (SHOPIFY-{id}-{HHMMSS})
    # in dev. Take only the leading digits.
    head = rest.split("-", 1)[0]
    return head or None


async def _process_susoft_order_delivered_async(
    tenant_id: str,
    susoft_order: Dict[str, Any],
    webhook_event_id: Optional[str],
) -> None:
    """Fulfill the matching Shopify order when Susoft fires ON_DELIVERY.

    Best-effort: any failure here is logged. The Susoft side has already
    shipped, so we never want to bubble exceptions up and trigger retries
    that could double-fulfill in Shopify.
    """
    susoft_uuid = susoft_order.get("uuid") or ""
    susoft_order_no = susoft_order.get("orderNo") or ""
    tracking_number = susoft_order.get("trackingNumber") or ""

    shopify_order_id = _extract_shopify_order_id(susoft_order)
    if not shopify_order_id:
        logger.info(
            "ON_DELIVERY for non-Shopify order (no SHOPIFY- alternativeId); ignoring",
            tenant_id=tenant_id,
            susoft_uuid=susoft_uuid,
            susoft_order_no=susoft_order_no,
        )
        return

    async with get_session_context() as session:
        tenant_repo = TenantRepository(session)
        sync_log_repo = SyncLogRepository(session)
        tenant = await tenant_repo.get_by_id(tenant_id)
        if not tenant or not tenant.is_active:
            logger.warning(
                "ON_DELIVERY: tenant missing/inactive",
                tenant_id=tenant_id,
            )
            return

        # Idempotency: skip if we already fulfilled this Susoft order.
        external_id = f"susoft_delivery_{susoft_uuid or susoft_order_no}"
        existing = await sync_log_repo.get_by_external_id(
            tenant_id=tenant_id, external_id=external_id
        )
        if existing and existing.status == SyncStatus.SUCCESS:
            logger.info(
                "ON_DELIVERY already processed; skipping",
                tenant_id=tenant_id,
                susoft_uuid=susoft_uuid,
                shopify_order_id=shopify_order_id,
            )
            return

        sync_log = await sync_log_repo.create(
            tenant_id=tenant_id,
            sync_type=SyncType.ORDER,
            direction=SyncDirection.SUSOFT_TO_SHOPIFY,
            external_id=external_id,
            source_payload=susoft_order,
            status=SyncStatus.PROCESSING,
        )

        shopify_client = create_shopify_client(
            shop_url=tenant.shopify_shop_url,
            access_token_encrypted=tenant.shopify_access_token_encrypted,
        )
        try:
            async with shopify_client:
                # 1. Tag + note (best-effort, even if fulfillment later fails)
                tag = "susoft-shipped"
                tags_to_add = [tag]
                if susoft_order_no:
                    tags_to_add.append(f"susoft-shipped-{susoft_order_no}")
                try:
                    await shopify_client.add_order_tags(
                        shopify_order_id, tags_to_add
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "add_order_tags (delivery) failed",
                        shopify_order_id=shopify_order_id,
                        error=str(exc),
                    )

                ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                note_parts = [f"[Susoft] {ts} — order shipped"]
                if susoft_order_no:
                    note_parts.append(f"orderNo={susoft_order_no}")
                if tracking_number:
                    note_parts.append(f"tracking={tracking_number}")
                note_line = note_parts[0] + (
                    " (" + ", ".join(note_parts[1:]) + ")"
                    if len(note_parts) > 1
                    else ""
                )
                try:
                    await shopify_client.append_order_note(
                        shopify_order_id, note_line
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "append_order_note (delivery) failed",
                        shopify_order_id=shopify_order_id,
                        error=str(exc),
                    )

                # 2. Create fulfillment for every fulfillment_order on this order.
                fulfillment_orders = await shopify_client.list_fulfillment_orders(
                    shopify_order_id
                )
                created_fulfillments = []
                for fo in fulfillment_orders:
                    if (fo.get("status") or "").lower() == "closed":
                        continue
                    fo_id = fo.get("id")
                    if not fo_id:
                        continue
                    fulfillment = await shopify_client.create_fulfillment(
                        fulfillment_order_id=str(fo_id),
                        tracking_number=tracking_number or None,
                        notify_customer=True,
                    )
                    created_fulfillments.append(fulfillment.get("id"))

                logger.info(
                    "Susoft ON_DELIVERY -> Shopify fulfillment created",
                    tenant_id=tenant_id,
                    shopify_order_id=shopify_order_id,
                    susoft_uuid=susoft_uuid,
                    susoft_order_no=susoft_order_no,
                    tracking_number=tracking_number or None,
                    fulfillments=created_fulfillments,
                )

                await sync_log_repo.update_status(
                    sync_log_id=sync_log.id,
                    status=SyncStatus.SUCCESS,
                    response_payload={
                        "shopify_order_id": shopify_order_id,
                        "fulfillments": created_fulfillments,
                        "tracking_number": tracking_number or None,
                    },
                )
                await tenant_repo.update_heartbeat(
                    tenant_id=tenant_id, direction="fulfillment_sync"
                )
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "ON_DELIVERY processing failed",
                tenant_id=tenant_id,
                shopify_order_id=shopify_order_id,
                error=str(exc),
            )
            await sync_log_repo.update_status(
                sync_log_id=sync_log.id,
                status=SyncStatus.FAILED,
                error_message=str(exc),
            )


async def _build_susoft_order(
    tenant_id: str,
    order_data: Dict[str, Any],
    mapping_repo: ProductMappingRepository,
    susoft_shop_id: str
) -> Dict[str, Any]:
    """Build a Susoft order from Shopify order data."""

    def _parse_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _map_gateway_to_payment_type(gateways: Any) -> str:
        names = [str(g).lower() for g in (gateways or [])]
        joined = " ".join(names)

        if "vipps" in joined:
            return "VIPPS"
        if "klarna" in joined:
            return "KLARNA"
        if "stripe" in joined:
            return "STRIPE"
        if "sumup" in joined:
            return "SUMUP"
        if "nets" in joined:
            return "NETS_EASY"
        return "TERMINAL"
    
    # Extract customer info
    customer = order_data.get("customer", {}) or {}
    shipping = order_data.get("shipping_address", {}) or {}
    billing = order_data.get("billing_address", {}) or {}

    # Build customer for Susoft (per Susoft Customer/Address schema).
    # `lastName` is required; fall back to a sensible default for anonymous orders.
    # Customer (the buyer/account holder) — used for the customer record.
    cust_first = (customer.get("first_name") or "").strip()
    cust_last = (customer.get("last_name") or "").strip()
    # Address recipient — may differ from buyer (e.g. gift, ship-to-other).
    # Prefer shipping address name; fall back to billing, then customer.
    addr_src = shipping or billing
    addr_first = (
        addr_src.get("first_name")
        or cust_first
        or billing.get("first_name")
        or ""
    ).strip()
    addr_last = (
        addr_src.get("last_name")
        or cust_last
        or billing.get("last_name")
        or ""
    ).strip()
    if not addr_last:
        addr_last = addr_first or "Shopify Customer"
        addr_first = "" if addr_last == addr_first else addr_first
    # Customer record name (separate from shipping name).
    if not cust_last:
        cust_last = cust_first or addr_last or "Shopify Customer"
        cust_first = "" if cust_last == cust_first else cust_first

    susoft_address = {
        "addressLine1": addr_src.get("address1", "") or "",
        "addressLine2": addr_src.get("address2", "") or "",
        "city": addr_src.get("city", "") or "",
        "zipCode": addr_src.get("zip", "") or "",
        "countryCode": addr_src.get("country_code", "NO") or "NO",
        "name": (f"{addr_first} {addr_last}".strip() or addr_last),
        "email": customer.get("email") or order_data.get("email"),
        "mobilePhone": customer.get("phone") or addr_src.get("phone"),
    }
    susoft_customer = {
        "firstName": cust_first,
        "lastName": cust_last,
        "displayName": f"{cust_first} {cust_last}".strip() or cust_last,
        "address": susoft_address,
    }

    # Build line items (per Susoft OrderLine schema: nested `product`, `text`, no `sku`/`productUuid`)
    lines = []
    next_line_no = 1
    for item in order_data.get("line_items", []):
        sku = item.get("sku")

        if not sku:
            logger.warning(
                "Skipping line item without SKU",
                variant_id=item.get("variant_id")
            )
            continue

        mapping = await mapping_repo.get_by_sku(tenant_id, sku)

        product_ref: Dict[str, Any] = {"barcode": sku}
        if mapping and mapping.susoft_product_id:
            product_ref["id"] = mapping.susoft_product_id
        else:
            logger.warning(
                "No product mapping found for SKU; sending barcode only",
                tenant_id=tenant_id,
                sku=sku,
            )

        qty = item.get("quantity", 1) or 1
        unit_price = _parse_float(item.get("price", 0))
        line_discount = _parse_float(item.get("total_discount", 0))
        line_total = round(unit_price * qty - line_discount, 2)
        lines.append({
            "lineNo": next_line_no,
            "product": product_ref,
            "barcode": sku,
            "quantity": qty,
            "qtyOrdered": qty,
            "qtyDelivered": qty,
            "producedQty": qty,
            "unitPrice": unit_price,
            "price": unit_price,
            "total": line_total,
            "discountAmount": line_discount,
            "text": item.get("name"),
        })
        next_line_no += 1

    # Susoft API currently ignores shippingAmount/shippingName on create,
    # so we optionally model shipping as an explicit order line instead.
    shipping_lines = order_data.get("shipping_lines", []) or []
    shipping_amount = sum(_parse_float(line.get("price", 0)) for line in shipping_lines)
    if shipping_amount <= 0:
        shipping_amount = _parse_float(
            (((order_data.get("total_shipping_price_set") or {}).get("shop_money") or {}).get("amount")),
            0.0,
        )
    shipping_name = ", ".join(
        str(line.get("title") or "Frakt")
        for line in shipping_lines
        if line.get("title")
    )

    shipping_sku = settings.shopify_shipping_sku
    logger.info(
        "Shipping detection",
        tenant_id=tenant_id,
        shopify_order=order_data.get("name"),
        shipping_lines_count=len(shipping_lines),
        shipping_amount=shipping_amount,
        shipping_sku=shipping_sku,
        total_shipping_price_set=order_data.get("total_shipping_price_set"),
    )
    if shipping_amount > 0 and shipping_sku:
        shipping_mapping = await mapping_repo.get_by_sku(tenant_id, shipping_sku)
        shipping_product: Dict[str, Any] = {"barcode": shipping_sku}
        if shipping_mapping and shipping_mapping.susoft_product_id:
            shipping_product["id"] = shipping_mapping.susoft_product_id
        else:
            logger.warning(
                "Shipping SKU has no mapping; sending shipping line with barcode only",
                tenant_id=tenant_id,
                shipping_sku=shipping_sku,
            )

        lines.append({
            "lineNo": next_line_no,
            "product": shipping_product,
            "barcode": shipping_sku,
            "quantity": 1,
            "qtyOrdered": 1,
            "qtyDelivered": 1,
            "producedQty": 1,
            "unitPrice": shipping_amount,
            "price": shipping_amount,
            "total": shipping_amount,
            "text": shipping_name or "Frakt",
        })
        next_line_no += 1
    
    # Build the order (per Susoft Order schema)
    # Susoft expects local time without offset; use Europe/Oslo so timestamps
    # match what users see in the Susoft UI.
    try:
        from zoneinfo import ZoneInfo
        _OSLO = ZoneInfo("Europe/Oslo")
    except Exception:  # pragma: no cover - fallback
        _OSLO = timezone.utc
    now_dt = datetime.now(_OSLO)
    now_iso_dt = now_dt.strftime("%Y-%m-%dT%H:%M:%S.000")
    susoft_order = {
        "shopId": susoft_shop_id,
        "orderDateTime": now_iso_dt,
        "pickupDateTime": now_iso_dt,
        "deliveryDate": now_iso_dt,
        "invoiceDate": now_iso_dt,
        "invoiceDueDate": now_iso_dt,
        "customer": susoft_customer,
        "deliveryAddress": susoft_address,
        "invoiceAddress": susoft_address,
        "lines": lines,
        "customerReference": order_data.get("name", ""),
        "note": order_data.get("note", "") or "",
        "currencyCode": order_data.get("currency", "NOK"),
    }

    financial_status = str(order_data.get("financial_status", "")).lower()
    if financial_status in {"paid", "partially_paid"}:
        total_amount = _parse_float(order_data.get("total_price", 0))
        payment_type = _map_gateway_to_payment_type(order_data.get("payment_gateway_names", []))
        now_iso = now_iso_dt

        susoft_order["payments"] = [{
            "paymentType": payment_type,
            "amount": total_amount,
            "currencyAmount": total_amount,
            "currency": order_data.get("currency", "NOK"),
            "rate": 1.0,
            "orderNo": 0,
            "shopId": susoft_shop_id,
            "issuedShopId": susoft_shop_id,
            "transactionId": f"shopify-{order_data.get('id')}",
            "paymentDateTime": now_iso,
            "number": str(order_data.get("id", "")),
            "note": "Betalt i Shopify"
        }]
        susoft_order["isForInvoicing"] = False
    else:
        susoft_order["isForInvoicing"] = True
    
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

        # Skip mappings whose Shopify inventory_item_id is not a real numeric
        # ID (e.g. "shipping" placeholder for FRAKT). Shopify will reject
        # these and they have no real on_hand to update.
        iid = (mapping.shopify_inventory_item_id or "").strip()
        if not iid.isdigit():
            logger.info(
                "Skipping stock update for non-numeric inventory_item_id",
                tenant_id=tenant_id,
                sku=mapping.sku,
                inventory_item_id=iid,
            )
            return

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

                # Add Shopify 'committed' (unfulfilled-order reservations) so
                # we don't double-decrement vs. Susoft (which already removed
                # the ordered units from on_hand).
                committed = 0
                try:
                    committed_lookup = await shopify_client.get_committed_quantities(
                        [mapping.shopify_inventory_item_id], shopify_location_id
                    )
                    committed = int(
                        committed_lookup.get(str(mapping.shopify_inventory_item_id), 0)
                    )
                except Exception as exc:
                    logger.warning(
                        "Failed to fetch committed quantity; using 0",
                        tenant_id=tenant_id,
                        sku=mapping.sku,
                        error=str(exc),
                    )
                target_on_hand = available_quantity + committed

                # Update inventory in Shopify
                result = await shopify_client.set_inventory_level(
                    inventory_item_id=mapping.shopify_inventory_item_id,
                    location_id=shopify_location_id,
                    available=target_on_hand,
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
                    new=available_quantity,
                    committed=committed,
                    on_hand_set=target_on_hand,
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
    # Distributed lock so concurrent stock syncs (or stock + product sync)
    # don't both pull the full Susoft catalogue in parallel and trigger
    # rate limits. Shared key with product sync since both call the same
    # /product/list/modified endpoint.
    lock_key = f"susoft_catalogue_lock:{tenant_id}"
    lock = redis_client.lock(lock_key, timeout=600)
    if not lock.acquire(blocking=False):
        logger.info(
            "Susoft catalogue fetch already running for tenant; skipping stock sync",
            tenant_id=tenant_id,
        )
        return
    try:
        asyncio.get_event_loop().run_until_complete(
            _sync_stock_to_shopify_async(
                task=self,
                tenant_id=tenant_id,
                product_mappings=product_mappings
            )
        )
    finally:
        try:
            lock.release()
        except Exception:
            pass


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
            # Try Redis cache first - the catalogue fetch hits 50+ Susoft pages
            # and triggers heavy rate-limiting. Cache the stock_lookup per
            # tenant for STOCK_LOOKUP_CACHE_TTL seconds so back-to-back stock
            # syncs reuse the same data. Webhook-driven stock changes still
            # update Shopify in real time via process_susoft_stock_change.
            import json as _json
            STOCK_LOOKUP_CACHE_TTL = 300  # 5 minutes
            cache_key = f"susoft_stock_lookup:{tenant_id}"
            cached = redis_client.get(cache_key)
            stock_lookup: dict
            if cached:
                stock_lookup = _json.loads(cached)
                logger.info(
                    "Reusing cached Susoft stock lookup",
                    tenant_id=tenant_id,
                    stock_lookup_size=len(stock_lookup),
                    mappings=len(mappings),
                )
            else:
                # Fetch all products (including embedded stock) from Susoft
                susoft_products = await susoft_client.get_all_products()

                # Build lookup: susoft product id -> stock quantity
                stock_lookup = {}
                for item in susoft_products:
                    pid = item.get("id") or item.get("productId")
                    if pid is None:
                        continue
                    stock_obj = item.get("stock") or {}
                    if isinstance(stock_obj, dict):
                        qty = stock_obj.get("stock", 0) or 0
                    else:
                        qty = stock_obj or 0
                    stock_lookup[str(pid)] = int(qty) if qty is not None else 0

                try:
                    redis_client.setex(
                        cache_key,
                        STOCK_LOOKUP_CACHE_TTL,
                        _json.dumps(stock_lookup),
                    )
                except Exception as exc:
                    logger.warning(
                        "Failed to cache Susoft stock lookup",
                        tenant_id=tenant_id,
                        error=str(exc),
                    )

                logger.info(
                    "Fetched Susoft products for stock sync",
                    tenant_id=tenant_id,
                    susoft_products=len(susoft_products),
                    stock_lookup_size=len(stock_lookup),
                    mappings=len(mappings),
                    cached_for_seconds=STOCK_LOOKUP_CACHE_TTL,
                )

            # Prepare bulk updates
            updates = []
            skipped_no_location = 0
            skipped_no_inventory_item = 0
            unmatched = 0
            for mapping in mappings:
                key = str(mapping.susoft_product_id)
                if key not in stock_lookup:
                    unmatched += 1
                susoft_qty = stock_lookup.get(key, 0)
                safety_stock = mapping.safety_stock or 0
                available = max(0, susoft_qty - safety_stock)

                shopify_location_id = (
                    mapping.shopify_location_id or
                    tenant.shopify_default_location_id
                )

                if not mapping.shopify_inventory_item_id:
                    skipped_no_inventory_item += 1
                    continue
                # Shopify inventory_item_id must be a numeric string. Skip
                # virtual/placeholder mappings like "shipping" (FRAKT) which
                # are not real Shopify inventory items - sending them causes
                # Shopify to reject the entire bulk update with userErrors.
                if not str(mapping.shopify_inventory_item_id).isdigit():
                    skipped_no_inventory_item += 1
                    logger.debug(
                        "Skipping non-numeric inventory_item_id (virtual product)",
                        tenant_id=tenant_id,
                        susoft_product_id=mapping.susoft_product_id,
                        shopify_inventory_item_id=mapping.shopify_inventory_item_id,
                    )
                    continue
                if not shopify_location_id:
                    skipped_no_location += 1
                    continue

                updates.append({
                    "inventory_item_id": mapping.shopify_inventory_item_id,
                    "location_id": shopify_location_id,
                    "available": available,
                })

            # Add Shopify 'committed' (unfulfilled-order reservations) on top
            # of Susoft on_hand. Susoft decrements stock immediately on order,
            # while Shopify only commits until fulfillment. Without this,
            # Shopify available would be double-decremented.
            # Group by location to batch GraphQL queries.
            if updates:
                by_location: Dict[str, List[str]] = {}
                for u in updates:
                    by_location.setdefault(str(u["location_id"]), []).append(
                        str(u["inventory_item_id"])
                    )
                committed_lookup: Dict[str, int] = {}
                for loc_id, iids in by_location.items():
                    try:
                        loc_committed = await shopify_client.get_committed_quantities(
                            iids, loc_id
                        )
                        for iid, qty in loc_committed.items():
                            committed_lookup[f"{loc_id}:{iid}"] = qty
                    except Exception as exc:
                        logger.warning(
                            "Failed to fetch committed quantities for location; "
                            "proceeding without committed adjustment",
                            tenant_id=tenant_id,
                            location_id=loc_id,
                            error=str(exc),
                        )
                total_committed = 0
                for u in updates:
                    key = f"{u['location_id']}:{u['inventory_item_id']}"
                    committed = committed_lookup.get(key, 0)
                    if committed > 0:
                        u["available"] = u["available"] + committed
                        total_committed += committed
                logger.info(
                    "Adjusted on_hand for Shopify committed quantities",
                    tenant_id=tenant_id,
                    total_committed_added=total_committed,
                    items_with_committed=sum(
                        1 for v in committed_lookup.values() if v > 0
                    ),
                )

            logger.info(
                "Prepared stock updates",
                tenant_id=tenant_id,
                updates=len(updates),
                unmatched_in_susoft=unmatched,
                skipped_no_inventory_item=skipped_no_inventory_item,
                skipped_no_location=skipped_no_location,
            )

            if updates:
                # Use bulk update for efficiency. Catch deterministic Shopify
                # userErrors here so we don't retry+DLQ-pile-up - those errors
                # repeat forever until the underlying mapping issue is fixed.
                try:
                    await shopify_client.bulk_set_inventory_levels(updates)
                except ShopifyAPIError as exc:
                    if getattr(exc, "errors", None):
                        logger.error(
                            "Bulk stock sync rejected by Shopify userErrors; "
                            "skipping until mappings are fixed",
                            tenant_id=tenant_id,
                            user_errors=exc.errors,
                            update_count=len(updates),
                        )
                        # Mark sync attempted so scheduler still throttles,
                        # then return without raising (no retry, no DLQ).
                        tenant.last_stock_sync_at = datetime.now(timezone.utc)
                        await session.flush()
                        return
                    raise

                logger.info(
                    "Bulk stock sync completed",
                    tenant_id=tenant_id,
                    count=len(updates)
                )

            # Record completion time so the scheduler respects sync_interval_seconds.
            tenant.last_stock_sync_at = datetime.now(timezone.utc)
            await session.flush()


# ===================================================================
# Product attribute sync (Susoft -> Shopify): name, price, category, VAT
# ===================================================================

def _extract_susoft_product_fields(p: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize a Susoft product payload into the fields we sync to Shopify.

    Susoft's product schema varies between deployments, so this function
    tries multiple common field names and returns ``None`` for anything
    that cannot be located. Downstream code skips updates for missing
    fields rather than overwriting Shopify values with empty data.
    """
    name = p.get("name") or p.get("productName") or p.get("title")

    # Price: prefer retail price including VAT (matches Shopify storefront
    # price in Norway). Susoft's /product/list/modified uses `retailPrice`
    # (gross, incl VAT). Fall back to other common variants for safety.
    price = (
        p.get("retailPrice")
        if p.get("retailPrice") is not None
        else p.get("priceWithVAT")
        or p.get("priceVAT")
        or p.get("priceInclVat")
        or p.get("priceInclVAT")
        or p.get("salesPrice")
        or p.get("price")
    )

    # VAT rate (e.g. 25 for 25%). Susoft uses `vatPercent`.
    vat_rate = (
        p.get("vatPercent")
        if p.get("vatPercent") is not None
        else p.get("vatRate") if p.get("vatRate") is not None
        else p.get("vat") if p.get("vat") is not None
        else (p.get("vatCode") or {}).get("rate") if isinstance(p.get("vatCode"), dict) else None
    )

    # Category: may be a string or a nested object
    category = None
    cat_obj = p.get("category") or p.get("productGroup") or p.get("group")
    if isinstance(cat_obj, dict):
        category = cat_obj.get("name") or cat_obj.get("title") or cat_obj.get("text")
    elif isinstance(cat_obj, str):
        category = cat_obj
    if not category:
        category = p.get("categoryName") or p.get("groupName")

    return {
        "name": name.strip() if isinstance(name, str) else None,
        "price": price,
        "vat_rate": vat_rate,
        "category": category.strip() if isinstance(category, str) else category,
    }


def _format_price(value: Any) -> Optional[str]:
    """Format a numeric price for Shopify (string with 2 decimals)."""
    if value is None:
        return None
    try:
        return f"{float(value):.2f}"
    except (TypeError, ValueError):
        return None


@celery_app.task(base=BaseTaskWithRetry, bind=True)
def sync_products_to_shopify(self, tenant_id: str):
    """
    Sync product attributes (name, price, category, VAT) from Susoft to Shopify.

    Iterates over all active product mappings for the tenant, compares Susoft
    master data with Shopify, and pushes any deltas via REST. Designed to be
    run on a schedule (e.g. every 30 min) and is safe to run concurrently
    with stock sync.
    """
    # Distributed lock so only one catalogue-fetching task runs per tenant
    # at a time. Shared key with stock sync since both call
    # /product/list/modified.
    lock_key = f"susoft_catalogue_lock:{tenant_id}"
    lock = redis_client.lock(lock_key, timeout=600)  # 10 min safety timeout
    if not lock.acquire(blocking=False):
        logger.info(
            "Product sync already running for tenant; skipping",
            tenant_id=tenant_id,
        )
        return
    try:
        asyncio.get_event_loop().run_until_complete(
            _sync_products_to_shopify_async(task=self, tenant_id=tenant_id)
        )
    finally:
        try:
            lock.release()
        except Exception:
            pass


async def _sync_products_to_shopify_async(task: Task, tenant_id: str):
    """Async implementation of product attribute sync."""
    async with get_session_context() as session:
        tenant_repo = TenantRepository(session)
        mapping_repo = ProductMappingRepository(session)

        tenant = await tenant_repo.get_by_id(tenant_id)
        if not tenant or not tenant.is_active:
            return

        mappings = await mapping_repo.get_active_mappings(tenant_id)
        if not mappings:
            logger.info("No mappings to sync product attributes for", tenant_id=tenant_id)
            return

        susoft_client = create_susoft_client(
            base_url=tenant.susoft_api_url,
            api_key_encrypted=tenant.susoft_api_key_encrypted,
            integration_id=tenant.susoft_integration_id,
        )
        shopify_client = create_shopify_client(
            shop_url=tenant.shopify_shop_url,
            access_token_encrypted=tenant.shopify_access_token_encrypted,
        )

        async with susoft_client, shopify_client:
            susoft_products = await susoft_client.get_all_products()

            # Build lookup: susoft product id -> normalized fields
            product_lookup: Dict[str, Dict[str, Any]] = {}
            for raw in susoft_products:
                pid = raw.get("id") or raw.get("productId")
                if pid is None:
                    continue
                product_lookup[str(pid)] = _extract_susoft_product_fields(raw)

            logger.info(
                "Starting product attribute sync",
                tenant_id=tenant_id,
                susoft_products=len(susoft_products),
                mappings=len(mappings),
            )

            updated_products = 0
            updated_variants = 0
            unmatched = 0
            skipped = 0
            errors = 0

            # Cache product fetches across mappings sharing the same Shopify product
            product_cache: Dict[str, Dict[str, Any]] = {}

            for mapping in mappings:
                key = str(mapping.susoft_product_id)
                susoft_fields = product_lookup.get(key)
                if not susoft_fields:
                    unmatched += 1
                    continue

                if not mapping.shopify_product_id or not mapping.shopify_variant_id:
                    skipped += 1
                    continue

                try:
                    shopify_product_id = str(mapping.shopify_product_id)
                    shopify_variant_id = str(mapping.shopify_variant_id)

                    # Fetch current Shopify product (cached) to compare title / type
                    shop_product = product_cache.get(shopify_product_id)
                    if shop_product is None:
                        shop_product = await shopify_client.get_product(shopify_product_id)
                        product_cache[shopify_product_id] = shop_product

                    product_updates: Dict[str, Any] = {}
                    if susoft_fields["name"] and susoft_fields["name"] != shop_product.get("title"):
                        product_updates["title"] = susoft_fields["name"]
                    if (
                        susoft_fields["category"]
                        and susoft_fields["category"] != shop_product.get("product_type")
                    ):
                        product_updates["product_type"] = susoft_fields["category"]

                    if product_updates:
                        await shopify_client.update_product(shopify_product_id, product_updates)
                        # Refresh cache entry so siblings see new values
                        shop_product.update(product_updates)
                        updated_products += 1

                    # Variant-level: price + taxable + vat metafield
                    variant_updates: Dict[str, Any] = {}
                    new_price = _format_price(susoft_fields["price"])
                    if new_price is not None:
                        # Find current variant price from cached product
                        current_price = None
                        for v in shop_product.get("variants") or []:
                            if str(v.get("id")) == shopify_variant_id:
                                current_price = v.get("price")
                                break
                        if current_price is None or _format_price(current_price) != new_price:
                            variant_updates["price"] = new_price

                    vat_rate = susoft_fields["vat_rate"]
                    if vat_rate is not None:
                        try:
                            vat_float = float(vat_rate)
                        except (TypeError, ValueError):
                            vat_float = None
                        if vat_float is not None:
                            variant_updates["taxable"] = vat_float > 0
                            variant_updates["metafields"] = [{
                                "namespace": "susoft",
                                "key": "vat_rate",
                                "value": f"{vat_float:.2f}",
                                "type": "number_decimal",
                            }]

                    if variant_updates:
                        await shopify_client.update_variant(shopify_variant_id, variant_updates)
                        updated_variants += 1

                except ShopifyAPIError as exc:
                    errors += 1
                    logger.warning(
                        "Failed to sync product attributes",
                        tenant_id=tenant_id,
                        susoft_product_id=key,
                        shopify_product_id=mapping.shopify_product_id,
                        error=str(exc),
                    )

            logger.info(
                "Product attribute sync completed",
                tenant_id=tenant_id,
                updated_products=updated_products,
                updated_variants=updated_variants,
                unmatched=unmatched,
                skipped_no_shopify_ids=skipped,
                errors=errors,
            )

            # Record last run in Redis so the scheduler can throttle.
            try:
                redis_client.set(
                    f"product_sync:last:{tenant_id}",
                    datetime.now(timezone.utc).isoformat(),
                    ex=7 * 24 * 3600,  # keep for 7 days
                )
            except Exception:
                pass


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
