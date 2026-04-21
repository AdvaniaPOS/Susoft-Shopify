"""
Bootstrap ProductMapping rows for a tenant.

Matches Susoft products against Shopify variants by SKU and creates the
``ProductMapping`` rows that ``sync_stock_to_shopify`` (and the order/refund
webhook handlers) depend on.

Usage (from repository root, with venv activated)::

    python -m scripts.bootstrap_mappings \\
        --tenant-id <uuid> \\
        --source json \\
        --products-file products_advania-jonb.json \\
        --match-key barcode

Add ``--apply`` to actually write rows; without it the script runs as a dry
run and only prints what *would* happen. Use ``--set-default-location`` to
auto-populate ``Tenant.shopify_default_location_id`` from the first active
Shopify location if it is missing.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import structlog

# Allow running as `python scripts/bootstrap_mappings.py` from repo root.
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


logger = structlog.get_logger()


def _normalize_sku(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    return s.lower() or None


def _load_susoft_products_from_file(path: Path) -> List[Dict[str, Any]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, dict):
        # Some dumps wrap the list under a key
        for key in ("products", "data", "items"):
            if key in raw and isinstance(raw[key], list):
                return raw[key]
        # Fall through – treat values as the list
        if "result" in raw and isinstance(raw["result"], list):
            return raw["result"]
        raise ValueError(
            f"Could not find a product list in {path}; "
            f"expected a JSON array or an object with key products/data/items/result"
        )
    if isinstance(raw, list):
        return raw
    raise ValueError(f"Unsupported JSON shape in {path}: {type(raw).__name__}")


def _build_shopify_sku_index(
    shopify_products: List[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """Return ``{normalized_sku: {product_id, variant_id, inventory_item_id, title}}``."""
    index: Dict[str, Dict[str, Any]] = {}
    for product in shopify_products:
        product_id = product.get("id")
        title = product.get("title")
        for variant in product.get("variants", []) or []:
            sku = _normalize_sku(variant.get("sku"))
            if not sku:
                continue
            if sku in index:
                # Duplicate SKU – keep first, log later
                continue
            index[sku] = {
                "product_id": str(product_id),
                "variant_id": str(variant.get("id")),
                "inventory_item_id": str(variant.get("inventory_item_id")),
                "title": title,
                "variant_title": variant.get("title"),
            }
    return index


def _extract_susoft_match_key(
    product: Dict[str, Any], match_key: str
) -> Tuple[Optional[str], str]:
    """Return ``(normalized_match_value, susoft_product_id)``."""
    susoft_id = product.get("id") or product.get("productId")
    susoft_id_str = str(susoft_id) if susoft_id is not None else ""
    if match_key == "barcode":
        return _normalize_sku(product.get("barcode") or susoft_id_str), susoft_id_str
    if match_key == "id":
        return _normalize_sku(susoft_id_str), susoft_id_str
    if match_key == "external_ref":
        return _normalize_sku(product.get("externalRefId")), susoft_id_str
    raise ValueError(f"Unknown match-key: {match_key}")


async def _maybe_set_default_location(
    tenant,
    shopify_client,
    session,
    apply: bool,
) -> Optional[str]:
    """If tenant has no default Shopify location, fetch and (optionally) save one."""
    if tenant.shopify_default_location_id:
        return tenant.shopify_default_location_id
    locations = await shopify_client.get_locations()
    if not locations:
        print("  ! Shopify returned no locations; cannot set default location.")
        return None
    # Prefer the first active location
    chosen = next((loc for loc in locations if loc.get("active", True)), locations[0])
    loc_id = str(chosen.get("id"))
    name = chosen.get("name", "?")
    print(f"  > Default Shopify location not set; selected '{name}' (id={loc_id}).")
    if apply:
        tenant.shopify_default_location_id = loc_id
        await session.flush()
        print("    [APPLIED] Tenant.shopify_default_location_id updated.")
    else:
        print("    [DRY-RUN] Would update Tenant.shopify_default_location_id.")
    return loc_id


async def bootstrap(
    tenant_id: str,
    source: str,
    products_file: Optional[Path],
    match_key: str,
    apply: bool,
    safety_stock: int,
    set_default_location: bool,
    limit: Optional[int],
) -> int:
    async with get_session_context() as session:
        tenant_repo = TenantRepository(session)
        mapping_repo = ProductMappingRepository(session)

        tenant = await tenant_repo.get_by_id(tenant_id)
        if not tenant:
            print(f"ERROR: tenant {tenant_id} not found")
            return 2

        print(f"Tenant: {tenant.name} ({tenant.slug})")
        print(f"  Shopify shop:   {tenant.shopify_shop_url}")
        print(f"  Susoft API:     {tenant.susoft_api_url}")
        print(f"  Susoft shop id: {tenant.susoft_integration_id}")
        print(f"  Mode:           {'APPLY' if apply else 'DRY-RUN'}")
        print(f"  Match key:      Susoft.{match_key}  <->  Shopify variant.sku")
        print()

        # --- Load Susoft products ---------------------------------------------------
        if source == "json":
            if not products_file:
                print("ERROR: --products-file is required when --source json")
                return 2
            print(f"Loading Susoft products from file: {products_file}")
            susoft_products = _load_susoft_products_from_file(products_file)
        elif source == "api":
            print("Fetching Susoft products from API ...")
            susoft_client = create_susoft_client(
                base_url=tenant.susoft_api_url,
                api_key_encrypted=tenant.susoft_api_key_encrypted,
                integration_id=tenant.susoft_integration_id,
            )
            async with susoft_client:
                susoft_products = await susoft_client.get_all_products()
        else:
            print(f"ERROR: unknown --source: {source}")
            return 2

        print(f"  Susoft products loaded: {len(susoft_products)}")

        # --- Fetch Shopify products -------------------------------------------------
        shopify_client = create_shopify_client(
            shop_url=tenant.shopify_shop_url,
            access_token_encrypted=tenant.shopify_access_token_encrypted,
        )
        async with shopify_client:
            print("Fetching Shopify products (paginated) ...")
            shopify_products = await shopify_client.get_all_products(
                fields="id,title,variants",
            )
            print(f"  Shopify products loaded: {len(shopify_products)}")

            default_location_id: Optional[str] = tenant.shopify_default_location_id
            if set_default_location:
                default_location_id = await _maybe_set_default_location(
                    tenant=tenant,
                    shopify_client=shopify_client,
                    session=session,
                    apply=apply,
                )

        # --- Build index + match ----------------------------------------------------
        shopify_index = _build_shopify_sku_index(shopify_products)
        print(f"  Shopify variants with SKU: {len(shopify_index)}")
        print()

        created = 0
        skipped_existing = 0
        skipped_no_match = 0
        skipped_no_susoft_key = 0
        examples_no_match: List[str] = []

        iter_products = susoft_products if limit is None else susoft_products[:limit]

        for product in iter_products:
            match_value, susoft_id = _extract_susoft_match_key(product, match_key)
            if not susoft_id:
                continue
            if not match_value:
                skipped_no_susoft_key += 1
                continue

            shopify_match = shopify_index.get(match_value)
            if not shopify_match:
                skipped_no_match += 1
                if len(examples_no_match) < 10:
                    examples_no_match.append(match_value)
                continue

            existing = await mapping_repo.get_by_susoft_id(tenant.id, susoft_id)
            if existing:
                skipped_existing += 1
                continue

            sku_for_row = match_value  # store the matched SKU as-is
            if apply:
                await mapping_repo.create(
                    tenant_id=str(tenant.id),
                    sku=sku_for_row,
                    susoft_product_id=susoft_id,
                    shopify_product_id=shopify_match["product_id"],
                    shopify_variant_id=shopify_match["variant_id"],
                    shopify_inventory_item_id=shopify_match["inventory_item_id"],
                    shopify_location_id=default_location_id,
                    safety_stock=safety_stock,
                )
            created += 1

        print("Result")
        print("------")
        print(f"  Would create / created : {created}")
        print(f"  Already mapped (skip)  : {skipped_existing}")
        print(f"  No Shopify match (skip): {skipped_no_match}")
        print(f"  No Susoft key (skip)   : {skipped_no_susoft_key}")
        if examples_no_match:
            print(f"  Examples of unmatched {match_key} values:")
            for v in examples_no_match:
                print(f"    - {v}")
        if not apply:
            print()
            print("DRY-RUN complete. Re-run with --apply to write to the database.")
        return 0


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Bootstrap ProductMapping rows for a tenant by matching SKUs."
    )
    p.add_argument("--tenant-id", required=True, help="Tenant UUID.")
    p.add_argument(
        "--source",
        choices=["json", "api"],
        default="json",
        help="Where to read Susoft products from (default: json).",
    )
    p.add_argument(
        "--products-file",
        type=Path,
        default=Path("products_advania-jonb.json"),
        help="Path to Susoft products JSON dump (used when --source json).",
    )
    p.add_argument(
        "--match-key",
        choices=["barcode", "id", "external_ref"],
        default="barcode",
        help="Susoft field to match against Shopify variant.sku (default: barcode).",
    )
    p.add_argument(
        "--apply",
        action="store_true",
        help="Actually create rows. Without this flag the script is a dry run.",
    )
    p.add_argument(
        "--safety-stock",
        type=int,
        default=0,
        help="Safety stock to store on each new mapping (default: 0).",
    )
    p.add_argument(
        "--set-default-location",
        action="store_true",
        help="If tenant has no default Shopify location, pick the first active one.",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process only the first N Susoft products (useful for testing).",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    rc = asyncio.run(
        bootstrap(
            tenant_id=args.tenant_id,
            source=args.source,
            products_file=args.products_file,
            match_key=args.match_key,
            apply=args.apply,
            safety_stock=args.safety_stock,
            set_default_location=args.set_default_location,
            limit=args.limit,
        )
    )
    sys.exit(rc)


if __name__ == "__main__":
    main()
