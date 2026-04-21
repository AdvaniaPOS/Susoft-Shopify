"""
Shopify Webhook Auto-Registration
==================================
Idempotent reconciliation of Shopify webhook subscriptions for a tenant.

The application exposes webhook receivers under ``/webhooks/shopify/*``
(see ``app/api/webhooks.py``). For each tenant we want Shopify to push to
those endpoints; this module ensures the subscriptions in Shopify match the
desired set.

Usage:
    from app.services.shopify_webhooks import reconcile_tenant_webhooks

    result = await reconcile_tenant_webhooks(tenant, base_url="https://sync.example.com")

The result dict contains lists of created/kept/updated/deleted/errors.
Operations are best-effort: per-topic failures do not abort the rest.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
import structlog

from app.db.models import Tenant
from app.services.shopify_client import ShopifyClient, ShopifyAPIError


logger = structlog.get_logger()


# Mapping: Shopify webhook topic -> path on this service that receives it.
# Keep paths in sync with routes registered in app/api/webhooks.py.
SHOPIFY_WEBHOOK_TOPICS: Dict[str, str] = {
    "orders/create": "/webhooks/shopify/orders/create",
    "orders/updated": "/webhooks/shopify/orders/updated",
    "refunds/create": "/webhooks/shopify/refunds/create",
}


@dataclass
class WebhookReconcileResult:
    """Outcome of a webhook reconciliation pass for one tenant."""
    tenant_id: str
    base_url: str
    created: List[Dict[str, Any]] = field(default_factory=list)
    updated: List[Dict[str, Any]] = field(default_factory=list)
    kept: List[Dict[str, Any]] = field(default_factory=list)
    deleted: List[Dict[str, Any]] = field(default_factory=list)
    errors: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tenant_id": self.tenant_id,
            "base_url": self.base_url,
            "created": self.created,
            "updated": self.updated,
            "kept": self.kept,
            "deleted": self.deleted,
            "errors": self.errors,
            "summary": {
                "created": len(self.created),
                "updated": len(self.updated),
                "kept": len(self.kept),
                "deleted": len(self.deleted),
                "errors": len(self.errors),
            },
        }


def _build_desired(base_url: str) -> Dict[str, str]:
    """Return mapping of topic -> absolute callback URL."""
    base = base_url.rstrip("/")
    return {topic: f"{base}{path}" for topic, path in SHOPIFY_WEBHOOK_TOPICS.items()}


async def reconcile_tenant_webhooks(
    tenant: Tenant,
    base_url: str,
    *,
    delete_stale: bool = True,
    topics: Optional[Dict[str, str]] = None,
) -> WebhookReconcileResult:
    """
    Reconcile Shopify webhook subscriptions for a single tenant.

    Args:
        tenant: Tenant ORM instance with Shopify credentials.
        base_url: Public base URL of this service (no trailing slash required).
        delete_stale: If True, delete existing webhooks that point at this
            service's base_url but use a topic we no longer want, or duplicates.
            Webhooks pointing to other addresses are always left alone.
        topics: Optional override of the desired topic->path map. Defaults to
            ``SHOPIFY_WEBHOOK_TOPICS``.

    Returns:
        WebhookReconcileResult describing actions taken.
    """
    desired_paths = topics or SHOPIFY_WEBHOOK_TOPICS
    desired = {
        topic: f"{base_url.rstrip('/')}{path}"
        for topic, path in desired_paths.items()
    }
    base_prefix = base_url.rstrip("/")

    result = WebhookReconcileResult(tenant_id=str(tenant.id), base_url=base_prefix)

    client = ShopifyClient(
        shop_url=tenant.shopify_shop_url,
        access_token_encrypted=tenant.shopify_access_token_encrypted,
        api_key_encrypted=getattr(tenant, "shopify_api_key_encrypted", None),
        api_secret_encrypted=getattr(tenant, "shopify_api_secret_encrypted", None),
    )

    async with client:
        try:
            existing = await client.list_webhooks()
        except ShopifyAPIError as exc:
            logger.error(
                "Failed to list Shopify webhooks",
                tenant_id=str(tenant.id),
                shop_url=tenant.shopify_shop_url,
                error=str(exc),
            )
            result.errors.append({"phase": "list", "error": str(exc)})
            return result

        # Index existing by topic; keep all entries since duplicates are possible.
        by_topic: Dict[str, List[Dict[str, Any]]] = {}
        for wh in existing:
            by_topic.setdefault(wh.get("topic", ""), []).append(wh)

        # Reconcile each desired topic.
        for topic, target_address in desired.items():
            current = by_topic.get(topic, [])
            matching = [w for w in current if w.get("address") == target_address]
            non_matching_ours = [
                w for w in current
                if w.get("address") != target_address
                and (w.get("address") or "").startswith(base_prefix)
            ]

            try:
                if matching:
                    # Already correct. Keep first, drop accidental duplicates if requested.
                    primary = matching[0]
                    result.kept.append({
                        "topic": topic,
                        "address": target_address,
                        "id": primary.get("id"),
                    })
                    if delete_stale:
                        for dup in matching[1:]:
                            await _safe_delete(client, dup, "duplicate", result)
                        for stale in non_matching_ours:
                            await _safe_delete(client, stale, "stale-address", result)
                elif non_matching_ours and delete_stale:
                    # Same topic, our base URL but wrong path - update by replace.
                    stale = non_matching_ours[0]
                    await _safe_delete(client, stale, "replaced", result)
                    for extra in non_matching_ours[1:]:
                        await _safe_delete(client, extra, "duplicate", result)
                    created = await client.create_webhook(topic=topic, address=target_address)
                    result.updated.append({
                        "topic": topic,
                        "address": target_address,
                        "id": created.get("id"),
                        "previous_id": stale.get("id"),
                    })
                    logger.info(
                        "Replaced Shopify webhook",
                        tenant_id=str(tenant.id),
                        topic=topic,
                        address=target_address,
                    )
                else:
                    created = await client.create_webhook(topic=topic, address=target_address)
                    result.created.append({
                        "topic": topic,
                        "address": target_address,
                        "id": created.get("id"),
                    })
                    logger.info(
                        "Created Shopify webhook",
                        tenant_id=str(tenant.id),
                        topic=topic,
                        address=target_address,
                    )
            except ShopifyAPIError as exc:
                logger.error(
                    "Failed to reconcile Shopify webhook",
                    tenant_id=str(tenant.id),
                    topic=topic,
                    address=target_address,
                    error=str(exc),
                )
                result.errors.append({
                    "topic": topic,
                    "address": target_address,
                    "error": str(exc),
                })

    return result


async def _safe_delete(
    client: ShopifyClient,
    webhook: Dict[str, Any],
    reason: str,
    result: WebhookReconcileResult,
) -> None:
    """Delete a webhook and record the outcome on ``result``."""
    wh_id = webhook.get("id")
    if not wh_id:
        return
    try:
        await client.delete_webhook(str(wh_id))
        result.deleted.append({
            "id": wh_id,
            "topic": webhook.get("topic"),
            "address": webhook.get("address"),
            "reason": reason,
        })
    except ShopifyAPIError as exc:
        result.errors.append({
            "phase": "delete",
            "id": wh_id,
            "topic": webhook.get("topic"),
            "address": webhook.get("address"),
            "reason": reason,
            "error": str(exc),
        })
