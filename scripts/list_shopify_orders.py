"""List recent Shopify orders for the configured tenant.

Quick diagnostic used to find an order_id we can reuse for end-to-end testing
of the post-Susoft flow (tag + note + close).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
data = json.loads((ROOT / "tenants.json").read_text(encoding="utf-8"))
tenant = data["tenants"][0]
shop = tenant["shopify"]["shop_domain"]
token = tenant["shopify"]["access_token"]

r = httpx.get(
    f"https://{shop}/admin/api/2024-01/orders.json",
    params={
        "status": "any",
        "limit": 20,
        "fields": "id,name,financial_status,fulfillment_status,closed_at,tags,line_items,created_at",
    },
    headers={"X-Shopify-Access-Token": token},
    timeout=30,
)

print("HTTP", r.status_code)
orders = r.json().get("orders", []) if r.status_code == 200 else []
print(f"orders returned: {len(orders)}")
for o in orders:
    li = o.get("line_items") or []
    skus = ",".join((x.get("sku") or "") for x in li)
    print(
        f"  {o.get('name'):>10}  id={o['id']:<14}  "
        f"fin={o.get('financial_status') or '-':<8}  "
        f"ful={o.get('fulfillment_status') or '-':<10}  "
        f"closed={bool(o.get('closed_at'))!s:<5}  "
        f"tags=[{o.get('tags') or ''}]  "
        f"skus=[{skus}]"
    )
sys.exit(0)
