"""End-to-end test of the Susoft -> Shopify post-sync order flow.

Picks an existing OPEN Shopify order, pushes it to Susoft using the canonical
schema from scripts/test_order_creation.py, then exercises the new helpers on
the production ShopifyClient (`add_order_tags`, `append_order_note`,
`close_order`).

Usage (PowerShell):
    & "$pyExe" scripts\test_order_flow.py
    & "$pyExe" scripts\test_order_flow.py --order-id 12052476395884 --no-close
    & "$pyExe" scripts\test_order_flow.py --skip-susoft   # only test Shopify side
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

import httpx

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Seed env so app.core.config.Settings() validates inside this process.
os.environ.setdefault("SECRET_KEY", "test-secret-key-not-for-production-32chars!!")
os.environ.setdefault("ENCRYPTION_KEY", "jAu-o4yTyCuAcLBxnTjb7PR1-oFk8gO8m15e2YPhF1A=")
os.environ.setdefault("ADMIN_PASSWORD", "test-admin-password")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://noop:noop@localhost/noop")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")

from app.core.security import encrypt_credential  # noqa: E402
from app.services.shopify_client import ShopifyClient as ProdShopifyClient, ShopifyAPIError  # noqa: E402


DEFAULT_ORDER_ID = "12052468662636"  # #1002, SKU 10713 (matched in Susoft)


def load_tenant() -> dict:
    cfg = json.loads((ROOT / "tenants.json").read_text(encoding="utf-8"))
    if isinstance(cfg, dict) and "tenants" in cfg:
        return cfg["tenants"][0]
    if isinstance(cfg, list):
        return cfg[0]
    return cfg


async def fetch_shopify_order(shop_url: str, token: str, order_id: str) -> dict:
    url = f"https://{shop_url}/admin/api/2024-01/orders/{order_id}.json"
    async with httpx.AsyncClient(timeout=30.0) as c:
        r = await c.get(url, headers={"X-Shopify-Access-Token": token})
        r.raise_for_status()
        return r.json()["order"]


# ---------------------------------------------------------------------------
# Susoft helpers (canonical schema from scripts/test_order_creation.py)
# ---------------------------------------------------------------------------
class SusoftMini:
    def __init__(self, base_url: str, shop_id: str):
        self.base_url = base_url.rstrip("/")
        self.shop_id = shop_id
        self.token: Optional[str] = None
        self.client = httpx.AsyncClient(timeout=30.0)

    async def authenticate(self, login: str, password: str) -> bool:
        r = await self.client.post(
            f"{self.base_url}/user/auth",
            json={"login": login, "password": password},
            headers={"X-Shop-Url-Key": self.shop_id},
        )
        if r.status_code == 200 and (data := r.json()).get("token"):
            self.token = data["token"]
            return True
        return False

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.token}",
            "X-Shop-Url-Key": self.shop_id,
            "Content-Type": "application/json",
        }

    async def create_order(self, payload: dict) -> tuple[int, dict | str]:
        r = await self.client.post(
            f"{self.base_url}/order",
            json=payload,
            headers=self._headers(),
        )
        try:
            body = r.json()
        except Exception:
            body = r.text
        return r.status_code, body

    async def close(self) -> None:
        await self.client.aclose()


def _split_name(full: str) -> tuple[str, str]:
    parts = (full or "").strip().split(" ", 1)
    return parts[0] or "Shopify", (parts[1] if len(parts) > 1 else "Customer")


def build_susoft_order(
    shopify_order: dict,
    sku_to_product: dict[str, dict],
    susoft_shop_id: str,
    alt_id: str,
) -> tuple[dict, list[str]]:
    cust = shopify_order.get("customer") or {}
    ship = shopify_order.get("shipping_address") or {}
    email = cust.get("email") or shopify_order.get("email") or "noreply@example.com"

    first = cust.get("first_name") or _split_name(ship.get("name", ""))[0]
    last = cust.get("last_name") or _split_name(ship.get("name", ""))[1]

    customer = {
        "firstName": first or "Shopify",
        "lastName": last or "Customer",
        "address": {
            "email": email,
            "mobilePhone": cust.get("phone") or ship.get("phone") or "",
            "addressLine1": ship.get("address1", "") or "n/a",
            "zipCode": ship.get("zip", "") or "0000",
            "city": ship.get("city", "") or "Oslo",
        },
    }

    lines: list[dict] = []
    skipped: list[str] = []
    for it in shopify_order.get("line_items") or []:
        sku = it.get("sku")
        if not sku:
            continue
        prod = sku_to_product.get(sku)
        if not prod:
            skipped.append(sku)
            continue
        qty = int(it.get("quantity", 1) or 1)
        unit_price = float(it.get("price", 0) or 0)
        vat_percent = float(prod.get("vatPercent", 25) or 0)
        total = unit_price * qty
        net_price = unit_price / (1 + vat_percent / 100) if vat_percent else unit_price
        vat_amount = total - (total / (1 + vat_percent / 100)) if vat_percent else 0.0
        lines.append({
            "product": {"id": str(prod["id"])},
            "quantity": qty,
            "unitPrice": unit_price,
            "price": unit_price,
            "netPrice": round(net_price, 4),
            "total": round(total, 4),
            "vatPercent": vat_percent,
            "vatAmount": round(vat_amount, 4),
            "priceRef": 0,
            "text": it.get("name") or prod.get("name") or sku,
        })

    payload: dict = {
        "alternativeId": alt_id,
        "uuid": str(uuid.uuid4()),
        "orderDateTime": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000"),
        "shopId": susoft_shop_id,
        "customer": customer,
        "lines": lines,
    }

    fin = str(shopify_order.get("financial_status", "")).lower()
    if fin in {"paid", "partially_paid"}:
        total_price = float(shopify_order.get("total_price") or 0)
        payload["payments"] = [{
            "paymentType": "TERMINAL",
            "amount": total_price,
            "currencyAmount": total_price,
            "currency": shopify_order.get("currency", "NOK"),
            "rate": 1.0,
            "orderNo": 0,
            "shopId": susoft_shop_id,
            "issuedShopId": susoft_shop_id,
            "transactionId": f"shopify-{shopify_order.get('id')}",
            "note": "Betalt via Shopify checkout",
        }]
    else:
        payload["isForInvoicing"] = True

    return payload, skipped


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--order-id", default=DEFAULT_ORDER_ID)
    ap.add_argument("--no-close", action="store_true")
    ap.add_argument("--skip-susoft", action="store_true",
                    help="Skip Susoft create; just exercise the Shopify helpers")
    ap.add_argument("--alt-suffix", default=datetime.now().strftime("%H%M%S"),
                    help="Suffix appended to alternativeId to avoid collisions")
    args = ap.parse_args()

    tenant = load_tenant()
    shop_url = tenant["shopify"]["shop_domain"]
    shopify_token = tenant["shopify"]["access_token"]
    susoft_url = tenant["susoft"]["api_url"]
    susoft_shop_id = tenant["susoft"]["shop_id"]
    susoft_login = tenant["susoft"]["login"]
    susoft_password = tenant["susoft"]["password"]
    order_id = args.order_id

    print("=== test_order_flow ===")
    print(f"shop      : {shop_url}")
    print(f"order_id  : {order_id}")
    print(f"susoft    : {susoft_url}  shop_id={susoft_shop_id}")
    print()

    print("[1/5] Fetch Shopify order…", end=" ", flush=True)
    order = await fetch_shopify_order(shop_url, shopify_token, order_id)
    print(f"OK  name={order.get('name')}  closed_at={order.get('closed_at')}")
    print(f"      tags={order.get('tags')!r}  fin={order.get('financial_status')}")
    print(f"      line_items SKUs: {[li.get('sku') for li in order.get('line_items', [])]}")

    susoft_uuid = ""
    susoft_order_no: int | str = ""
    alt_id = f"SHOPIFY-{order_id}-{args.alt_suffix}"

    if args.skip_susoft:
        print("[2/5] SKIP Susoft (--skip-susoft); using fake uuid")
        susoft_uuid = f"FAKE-{uuid.uuid4()}"
    else:
        print("[2/5] Susoft auth…", end=" ", flush=True)
        sm = SusoftMini(susoft_url, susoft_shop_id)
        if not await sm.authenticate(susoft_login, susoft_password):
            print("FAILED")
            await sm.close()
            return 2
        print("OK")

        cache_path = ROOT / f"products_advania-{susoft_shop_id}.json"
        sku_to_product: dict[str, dict] = {}
        if cache_path.exists():
            for p in json.loads(cache_path.read_text(encoding="utf-8")):
                bc = p.get("barcode")
                if bc:
                    sku_to_product[str(bc)] = p
        print(f"      loaded {len(sku_to_product)} sku->product mappings from cache")

        payload, skipped = build_susoft_order(order, sku_to_product, susoft_shop_id, alt_id)
        if skipped:
            print(f"      WARN: skipped SKUs (no Susoft mapping): {skipped}")
        if not payload["lines"]:
            print("      ABORT: no usable line items")
            await sm.close()
            return 3
        print(f"[3/5] Susoft create_order  altId={alt_id}  lines={len(payload['lines'])}")
        status, body = await sm.create_order(payload)
        await sm.close()
        if status != 200:
            print(f"      FAILED  status={status}")
            print(f"      body  : {str(body)[:800]}")
            return 4
        susoft_uuid = body.get("uuid") or body.get("id") or alt_id
        susoft_order_no = body.get("orderNo", "?")
        print(f"      OK  orderNo={susoft_order_no}  uuid={susoft_uuid}")

    print("[4/5] Shopify post-actions (production ShopifyClient)…")
    enc_token = encrypt_credential(shopify_token)
    sclient = ProdShopifyClient(shop_url=shop_url, access_token_encrypted=enc_token)
    async with sclient:
        short_ref = str(susoft_order_no) if susoft_order_no else (str(susoft_uuid).split("-")[0] if susoft_uuid else "")
        tags = ["susoft-synced"] + ([f"susoft-id-{short_ref}"] if short_ref else [])
        print(f"      add_order_tags {tags}…", end=" ", flush=True)
        try:
            upd = await sclient.add_order_tags(order_id, tags)
            print(f"OK  tags now: {upd.get('tags')!r}")
        except ShopifyAPIError as e:
            print(f"FAILED status={e.status_code}  body={getattr(e, 'response_body', None)!r}")
            return 5

        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        note_line = f"[Susoft] {ts} — order created in Susoft (orderNo={susoft_order_no} uuid={susoft_uuid})"
        print("      append_order_note…", end=" ", flush=True)
        upd = await sclient.append_order_note(order_id, note_line)
        print("OK")
        rendered = (upd.get("note") or "").replace("\n", "\n        ")
        print(f"      note now:\n        {rendered}")

        if args.no_close:
            print("      SKIP close_order (--no-close)")
        else:
            print("      close_order…", end=" ", flush=True)
            closed = await sclient.close_order(order_id)
            print(f"OK  closed_at={closed.get('closed_at')}")

    print("[5/5] Done.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
