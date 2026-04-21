"""
Diagnose stock sync drift between Susoft and Shopify for a tenant.

Compares for every ProductMapping:
  - Live Susoft stock (from /product/list/modified pagination)
  - DB-recorded susoft / shopify stock
  - Live Shopify available (inventory_levels)
  - Safety stock + computed expected available

Run from repo root with the project venv activated:

    python -m scripts.diagnose_stock_sync --tenant-id <uuid>

Add --only-mismatch to hide rows that match.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.database import get_session_context  # noqa: E402
from app.db.repositories import (  # noqa: E402
    ProductMappingRepository,
    TenantRepository,
)
from app.services.shopify_client import create_shopify_client  # noqa: E402
from app.services.susoft_client import create_susoft_client  # noqa: E402


def _build_susoft_stock_lookup(products: List[Dict[str, Any]]) -> Dict[str, int]:
    lookup: Dict[str, int] = {}
    for item in products:
        pid = item.get("id") or item.get("productId")
        if pid is None:
            continue
        stock_obj = item.get("stock") or {}
        if isinstance(stock_obj, dict):
            qty = stock_obj.get("stock", 0) or 0
        else:
            qty = stock_obj or 0
        try:
            lookup[str(pid)] = int(qty)
        except (TypeError, ValueError):
            lookup[str(pid)] = 0
    return lookup


async def diagnose(tenant_id: str, only_mismatch: bool) -> int:
    async with get_session_context() as session:
        tenant_repo = TenantRepository(session)
        mapping_repo = ProductMappingRepository(session)

        tenant = await tenant_repo.get_by_id(tenant_id)
        if not tenant:
            print(f"ERROR: tenant {tenant_id} not found")
            return 2

        mappings = await mapping_repo.get_active_mappings(tenant_id)
        print(f"Tenant: {tenant.name} ({tenant.slug})")
        print(f"  Susoft URL: {tenant.susoft_api_url}")
        print(f"  Susoft shop key: {tenant.susoft_integration_id}")
        print(f"  Shopify shop:  {tenant.shopify_shop_url}")
        print(f"  Default Shopify location: {tenant.shopify_default_location_id}")
        print(f"  Active mappings: {len(mappings)}")
        print()

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
            print("Fetching live Susoft products...")
            susoft_products = await susoft_client.get_all_products()
            print(f"  Got {len(susoft_products)} products")
            stock_lookup = _build_susoft_stock_lookup(susoft_products)

            # Fetch Shopify inventory levels in chunks of 50
            inv_lookup: Dict[str, Dict[str, int]] = {}
            inv_ids = [
                str(m.shopify_inventory_item_id)
                for m in mappings
                if m.shopify_inventory_item_id
            ]
            print(f"Fetching Shopify inventory levels for {len(inv_ids)} items...")
            for i in range(0, len(inv_ids), 50):
                chunk = inv_ids[i : i + 50]
                levels = await shopify_client.get_inventory_levels(chunk)
                for lvl in levels:
                    iid = str(lvl.get("inventory_item_id"))
                    lid = str(lvl.get("location_id"))
                    inv_lookup.setdefault(iid, {})[lid] = int(lvl.get("available") or 0)

            # Print table
            header = (
                f"{'SKU':<20} {'SusoftID':<14} {'liveSus':>8} "
                f"{'dbSus':>6} {'dbShop':>6} {'liveShop':>9} {'safety':>7} {'expectedShop':>12}  status"
            )
            print()
            print(header)
            print("-" * len(header))
            mismatches = 0
            missing_in_susoft = 0
            for m in mappings:
                susoft_key = str(m.susoft_product_id)
                live_sus = stock_lookup.get(susoft_key)
                live_sus_str = "MISSING" if live_sus is None else str(live_sus)
                if live_sus is None:
                    missing_in_susoft += 1
                safety = m.safety_stock or 0
                expected = max(0, (live_sus or 0) - safety) if live_sus is not None else None
                expected_str = "?" if expected is None else str(expected)

                shop_loc = m.shopify_location_id or tenant.shopify_default_location_id
                live_shop = None
                if m.shopify_inventory_item_id and shop_loc:
                    live_shop = inv_lookup.get(str(m.shopify_inventory_item_id), {}).get(str(shop_loc))
                live_shop_str = "?" if live_shop is None else str(live_shop)

                status = "OK"
                if live_sus is None:
                    status = "NO_SUSOFT"
                elif live_shop is None:
                    status = "NO_SHOPIFY_LEVEL"
                elif expected != live_shop:
                    status = f"DRIFT(diff={live_shop - (expected or 0)})"
                    mismatches += 1

                if only_mismatch and status == "OK":
                    continue

                print(
                    f"{(m.sku or '')[:20]:<20} {susoft_key[:14]:<14} {live_sus_str:>8} "
                    f"{m.current_susoft_stock:>6} {m.current_shopify_stock:>6} "
                    f"{live_shop_str:>9} {safety:>7} {expected_str:>12}  {status}"
                )

            print()
            print("Summary")
            print("-------")
            print(f"  Mappings inspected:        {len(mappings)}")
            print(f"  Missing in live Susoft:    {missing_in_susoft}")
            print(f"  Drift (liveShop != expect): {mismatches}")
        return 0


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Compare Susoft vs Shopify stock for a tenant.")
    p.add_argument("--tenant-id", required=True, help="Tenant UUID.")
    p.add_argument(
        "--only-mismatch",
        action="store_true",
        help="Only print rows that drift or have missing data.",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    rc = asyncio.run(diagnose(tenant_id=args.tenant_id, only_mismatch=args.only_mismatch))
    sys.exit(rc)


if __name__ == "__main__":
    main()
