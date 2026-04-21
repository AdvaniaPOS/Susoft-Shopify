"""Seed a Tenant row from tenants.json into Postgres.

Reads the first tenant from ../tenants.json, encrypts its credentials with the
project Fernet key, and inserts (or updates) a Tenant row.

Usage:
    python scripts/seed_tenant.py
    python scripts/seed_tenant.py --slug advania-jonb --force   # overwrite
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from sqlalchemy import select  # noqa: E402

from app.core.security import encrypt_credential  # noqa: E402
from app.core.database import get_session_context  # noqa: E402
from app.db.models import Tenant  # noqa: E402


def load_first_tenant() -> dict:
    cfg = json.loads((ROOT / "tenants.json").read_text(encoding="utf-8"))
    if isinstance(cfg, dict) and "tenants" in cfg:
        return cfg["tenants"][0]
    if isinstance(cfg, list):
        return cfg[0]
    return cfg


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--slug", default=None,
                    help="Override tenant slug (default: name from tenants.json)")
    ap.add_argument("--force", action="store_true",
                    help="Update credentials/settings if tenant already exists")
    args = ap.parse_args()

    src = load_first_tenant()
    name = src.get("name", "tenant")
    slug = args.slug or name
    susoft = src["susoft"]
    shopify = src["shopify"]
    cfg = src.get("config", {}) or {}

    print(f"Seeding tenant slug={slug!r}…")

    async with get_session_context() as session:
        existing = (await session.execute(
            select(Tenant).where(Tenant.slug == slug)
        )).scalar_one_or_none()

        if existing and not args.force:
            print(f"  Tenant already exists (id={existing.id}). Use --force to update.")
            return 0

        if existing:
            existing.name = name
            existing.susoft_api_url = susoft["api_url"]
            existing.susoft_api_key_encrypted = encrypt_credential(susoft["password"])
            existing.susoft_integration_id = susoft["shop_id"]
            existing.shopify_shop_url = shopify["shop_domain"]
            existing.shopify_access_token_encrypted = encrypt_credential(shopify["access_token"])
            existing.safety_stock_default = int(cfg.get("safety_stock", 0))
            existing.sync_interval_seconds = int(cfg.get("sync_interval_minutes", 5)) * 60
            await session.flush()
            print(f"  Updated tenant {existing.id}")
            return 0

        tenant = Tenant(
            name=name,
            slug=slug,
            is_active=True,
            susoft_api_url=susoft["api_url"],
            susoft_api_key_encrypted=encrypt_credential(susoft["password"]),
            susoft_integration_id=susoft["shop_id"],
            shopify_shop_url=shopify["shop_domain"],
            shopify_access_token_encrypted=encrypt_credential(shopify["access_token"]),
            safety_stock_default=int(cfg.get("safety_stock", 0)),
            sync_interval_seconds=int(cfg.get("sync_interval_minutes", 5)) * 60,
        )
        session.add(tenant)
        await session.flush()
        await session.refresh(tenant)
        print(f"  Created tenant id={tenant.id}")
        print(f"  shopify_shop_url={tenant.shopify_shop_url}")
        print(f"  close_orders_after_susoft={tenant.close_orders_after_susoft}")
        print(f"  shopify_synced_tag={tenant.shopify_synced_tag}")
        print(f"  shopify_failed_tag={tenant.shopify_failed_tag}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
