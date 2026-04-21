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
                result = await susoft_client.create_order(
                    order_data=susoft_order,
                    shopify_order_id=order_id,
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
            susoft_order_no = susoft_result.get("orderNo") or susoft_result.get("alternativeId") or ""
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

            await shopify_client.add_order_tags(shopify_order_id, tags_to_add)

            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            note_parts = [f"[Susoft] {ts} — order created in Susoft"]
            if susoft_order_no:
                note_parts.append(f"orderNo={susoft_order_no}")
            if susoft_uuid:
                note_parts.append(f"uuid={susoft_uuid}")
            note_line = " ".join(note_parts) if len(note_parts) == 1 else (
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
    first_name = (customer.get("first_name") or shipping.get("first_name") or billing.get("first_name") or "").strip()
    last_name = (customer.get("last_name") or shipping.get("last_name") or billing.get("last_name") or "").strip()
    if not last_name:
        last_name = first_name or "Shopify Customer"
        first_name = "" if last_name == first_name else first_name

    addr_src = shipping or billing
    susoft_address = {
        "addressLine1": addr_src.get("address1", "") or "",
        "addressLine2": addr_src.get("address2", "") or "",
        "city": addr_src.get("city", "") or "",
        "zipCode": addr_src.get("zip", "") or "",
        "countryCode": addr_src.get("country_code", "NO") or "NO",
        "name": (f"{first_name} {last_name}".strip() or last_name),
        "email": customer.get("email") or order_data.get("email"),
        "mobilePhone": customer.get("phone") or addr_src.get("phone"),
    }
    susoft_customer = {
        "firstName": first_name,
        "lastName": last_name,
        "displayName": f"{first_name} {last_name}".strip() or last_name,
        "address": susoft_address,
        "deliveryAddress": susoft_address,
        "invoiceAddress": susoft_address,
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
    susoft_order = {
        "shopId": susoft_shop_id,
        "orderDateTime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000"),
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
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000")

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
            # Fetch all products (including embedded stock) from Susoft
            susoft_products = await susoft_client.get_all_products()

            # Build lookup: susoft product id -> stock quantity
            stock_lookup: dict = {}
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

            logger.info(
                "Fetched Susoft products for stock sync",
                tenant_id=tenant_id,
                susoft_products=len(susoft_products),
                stock_lookup_size=len(stock_lookup),
                mappings=len(mappings),
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
                if not shopify_location_id:
                    skipped_no_location += 1
                    continue

                updates.append({
                    "inventory_item_id": mapping.shopify_inventory_item_id,
                    "location_id": shopify_location_id,
                    "available": available,
                })

            logger.info(
                "Prepared stock updates",
                tenant_id=tenant_id,
                updates=len(updates),
                unmatched_in_susoft=unmatched,
                skipped_no_inventory_item=skipped_no_inventory_item,
                skipped_no_location=skipped_no_location,
            )

            if updates:
                # Use bulk update for efficiency
                await shopify_client.bulk_set_inventory_levels(updates)

                logger.info(
                    "Bulk stock sync completed",
                    tenant_id=tenant_id,
                    count=len(updates)
                )

            # Record completion time so the scheduler respects sync_interval_seconds.
            tenant.last_stock_sync_at = datetime.now(timezone.utc)
            await session.flush()


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
