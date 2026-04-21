"""
Admin CLI for Susoft-Shopify Sync.

Commands:
    python cli.py tenant add       - Add new tenant (Susoft + Shopify)
    python cli.py tenant list      - List all tenants
    python cli.py sync products    - Sync products from Susoft to local DB
    python cli.py sync stock       - Sync stock levels
    python cli.py test susoft      - Test Susoft connection
    python cli.py test shopify     - Test Shopify connection
"""
import asyncio
import sys
import json
from datetime import datetime, timedelta
from typing import Optional
import httpx
from rich.console import Console
from rich.table import Table
from rich.prompt import Prompt, Confirm
from rich.panel import Panel
from rich import print as rprint

console = Console()


class SusoftClient:
    """Susoft API client."""
    
    def __init__(self, base_url: str, shop_id: str):
        self.base_url = base_url.rstrip("/")
        self.shop_id = shop_id
        self.token: Optional[str] = None
        self.client = httpx.AsyncClient(timeout=30.0, verify=True)
    
    async def authenticate(self, login: str, password: str) -> bool:
        """Get JWT token from Susoft."""
        response = await self.client.post(
            f"{self.base_url}/user/auth",
            json={"login": login, "password": password},
            headers={"X-Shop-Url-Key": self.shop_id}
        )
        if response.status_code == 200:
            data = response.json()
            if data.get("success") and data.get("token"):
                self.token = data["token"]
                return True
        return False
    
    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.token}",
            "X-Shop-Url-Key": self.shop_id,
            "Content-Type": "application/json"
        }
    
    async def get_shop_info(self) -> dict:
        """Get shop information."""
        response = await self.client.get(
            f"{self.base_url}/shop/info",
            headers=self._headers()
        )
        return response.json() if response.status_code == 200 else {}
    
    async def get_products(self, page: int = 0, page_size: int = 100, since_days: int = 365) -> list:
        """Fetch products modified since X days ago."""
        since = (datetime.now() - timedelta(days=since_days)).strftime("%Y-%m-%dT00:00:00.000")
        response = await self.client.get(
            f"{self.base_url}/product/list/modified",
            params={
                "dateTime": since,
                "page": page,
                "pageSize": page_size,
                "expandConfigurable": "true",
                "withVariants": "true"
            },
            headers=self._headers()
        )
        return response.json() if response.status_code == 200 else []
    
    async def get_all_products(self) -> list:
        """Fetch all products with pagination."""
        all_products = []
        page = 0
        while True:
            products = await self.get_products(page=page, page_size=100)
            if not products:
                break
            all_products.extend(products)
            if len(products) < 100:
                break
            page += 1
        return all_products
    
    async def create_order(
        self,
        order_id: str,
        customer: dict,
        lines: list,
        payment: Optional[dict] = None
    ) -> dict:
        """
        Create order in Susoft.
        
        Args:
            order_id: External reference (e.g., SHOPIFY-12345)
            customer: Customer data
            lines: Order line items
            payment: Payment info (if None, order is for invoicing)
        """
        import uuid
        
        order_data = {
            "alternativeId": order_id,
            "uuid": str(uuid.uuid4()),
            "orderDateTime": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000"),
            "shopId": self.shop_id,
            "customer": customer,
            "lines": lines
        }
        
        if payment:
            order_data["payments"] = [payment]
        else:
            order_data["isForInvoicing"] = True
        
        response = await self.client.post(
            f"{self.base_url}/order",
            json=order_data,
            headers=self._headers()
        )
        
        return {
            "status": response.status_code,
            "data": response.json() if response.status_code == 200 else None,
            "error": response.text if response.status_code != 200 else None
        }
    
    async def close(self):
        await self.client.aclose()


class ShopifyClient:
    """Minimal Shopify API client used by CLI product sync."""

    API_VERSION = "2024-01"

    def __init__(self, shop_domain: str, access_token: str):
        self.shop_domain = shop_domain
        self.base_url = f"https://{shop_domain}/admin/api/{self.API_VERSION}"
        self.client = httpx.AsyncClient(timeout=30.0)
        self._headers = {
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

    async def get_variant_map_by_sku(self) -> dict:
        """Fetch existing Shopify variants and return {sku: variant_id}."""
        response = await self.client.get(
            f"{self.base_url}/products.json",
            params={"limit": 250, "fields": "variants"},
            headers=self._headers,
        )
        if response.status_code != 200:
            return {}

        sku_map = {}
        products = response.json().get("products", [])
        for product in products:
            for variant in product.get("variants", []):
                sku = variant.get("sku")
                variant_id = variant.get("id")
                if sku and variant_id:
                    sku_map[str(sku)] = int(variant_id)
        return sku_map

    async def create_product(self, payload: dict) -> Optional[dict]:
        """Create a Shopify product."""
        response = await self.client.post(
            f"{self.base_url}/products.json",
            json={"product": payload},
            headers=self._headers
        )
        if response.status_code in (200, 201):
            return response.json().get("product")
        return None

    async def update_variant(self, variant_id: int, payload: dict) -> bool:
        """Update an existing variant by ID."""
        response = await self.client.put(
            f"{self.base_url}/variants/{variant_id}.json",
            json={"variant": payload},
            headers=self._headers
        )
        return response.status_code == 200

    @staticmethod
    def _parse_next_link(link_header: Optional[str]) -> Optional[str]:
        if not link_header:
            return None
        for part in link_header.split(","):
            segs = part.strip().split(";")
            if len(segs) < 2:
                continue
            url = segs[0].strip()
            rel = ";".join(segs[1:]).strip()
            if 'rel="next"' in rel and url.startswith("<") and url.endswith(">"):
                return url[1:-1]
        return None

    async def get_full_variant_map_by_sku(self) -> dict:
        """Return ``{sku: {product_id, variant_id, inventory_item_id}}`` for every variant."""
        url: Optional[str] = f"{self.base_url}/products.json"
        params: Optional[dict] = {"limit": 250, "fields": "id,variants"}
        sku_map: dict = {}
        page = 0
        while url and page < 1000:
            response = await self.client.get(
                url, params=params if page == 0 else None, headers=self._headers
            )
            if response.status_code != 200:
                break
            for product in response.json().get("products", []):
                pid = product.get("id")
                for variant in product.get("variants", []) or []:
                    sku = variant.get("sku")
                    if not sku:
                        continue
                    sku_norm = str(sku).strip()
                    if not sku_norm or sku_norm in sku_map:
                        continue
                    sku_map[sku_norm] = {
                        "product_id": str(pid),
                        "variant_id": str(variant.get("id")),
                        "inventory_item_id": str(variant.get("inventory_item_id")),
                    }
            url = self._parse_next_link(
                response.headers.get("Link") or response.headers.get("link")
            )
            page += 1
        return sku_map

    async def list_locations(self) -> list:
        """List Shopify inventory locations."""
        response = await self.client.get(
            f"{self.base_url}/locations.json", headers=self._headers
        )
        if response.status_code != 200:
            return []
        return response.json().get("locations", [])

    async def connect_inventory(
        self, inventory_item_id: str, location_id: str
    ) -> bool:
        """Connect (activate) an inventory item to a location."""
        response = await self.client.post(
            f"{self.base_url}/inventory_levels/connect.json",
            json={
                "inventory_item_id": int(inventory_item_id),
                "location_id": int(location_id),
            },
            headers=self._headers,
        )
        return response.status_code in (200, 201)

    async def enable_tracking(self, inventory_item_id: str) -> bool:
        """Mark an inventory item as tracked so levels can be set."""
        response = await self.client.put(
            f"{self.base_url}/inventory_items/{inventory_item_id}.json",
            json={"inventory_item": {"id": int(inventory_item_id), "tracked": True}},
            headers=self._headers,
        )
        return response.status_code == 200

    async def set_inventory_level(
        self, inventory_item_id: str, location_id: str, available: int
    ) -> tuple:
        """Set absolute inventory level. Returns (ok, error_message_or_None)."""
        response = await self.client.post(
            f"{self.base_url}/inventory_levels/set.json",
            json={
                "inventory_item_id": int(inventory_item_id),
                "location_id": int(location_id),
                "available": int(available),
            },
            headers=self._headers,
        )
        if response.status_code in (200, 201):
            return True, None
        body = ""
        try:
            body = response.text[:300]
        except Exception:
            pass
        return False, f"HTTP {response.status_code}: {body}"

    async def close(self):
        await self.client.aclose()


class TenantManager:
    """Manages tenant configurations (file-based for now)."""
    
    CONFIG_FILE = "tenants.json"
    
    def __init__(self):
        self.tenants = self._load()
    
    def _load(self) -> dict:
        try:
            with open(self.CONFIG_FILE, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            return {"tenants": []}
    
    def _save(self):
        with open(self.CONFIG_FILE, "w") as f:
            json.dump(self.tenants, f, indent=2)
    
    def add_tenant(self, tenant: dict):
        self.tenants["tenants"].append(tenant)
        self._save()
    
    def list_tenants(self) -> list:
        return self.tenants.get("tenants", [])
    
    def get_tenant(self, name: str) -> Optional[dict]:
        for t in self.tenants.get("tenants", []):
            if t.get("name") == name:
                return t
        return None


async def cmd_add_tenant():
    """Interactive: Add new tenant."""
    console.print(Panel("🆕 Add New Tenant", style="bold blue"))
    
    # Get tenant info
    name = Prompt.ask("Tenant name (e.g., 'kunde-xyz')")
    
    console.print("\n[bold yellow]Susoft Configuration[/]")
    susoft_url = Prompt.ask("Susoft API URL", default="https://api.susoft.com:4443")
    susoft_shop_id = Prompt.ask("Susoft Shop ID")
    susoft_login = Prompt.ask("Susoft Login (email)")
    susoft_password = Prompt.ask("Susoft Password", password=True)
    
    # Test Susoft connection
    with console.status("Testing Susoft connection..."):
        client = SusoftClient(susoft_url, susoft_shop_id)
        if await client.authenticate(susoft_login, susoft_password):
            shop_info = await client.get_shop_info()
            console.print(f"  ✅ Connected to: [green]{shop_info.get('shopName', 'Unknown')}[/]")
            console.print(f"     Tenant: {shop_info.get('tenantName', 'Unknown')}")
        else:
            console.print("  ❌ [red]Failed to authenticate with Susoft[/]")
            await client.close()
            return
        await client.close()
    
    console.print("\n[bold yellow]Shopify Configuration[/]")
    shopify_shop = Prompt.ask("Shopify shop domain (e.g., 'mystore.myshopify.com')")
    shopify_token = Prompt.ask("Shopify Admin API access token", password=True)
    
    # Optional: Test Shopify (simplified)
    console.print("  ℹ️  Shopify connection will be tested when syncing")
    
    console.print("\n[bold yellow]Sync Configuration[/]")
    safety_stock = int(Prompt.ask("Default safety stock (buffer)", default="0"))
    
    tenant = {
        "name": name,
        "created_at": datetime.now().isoformat(),
        "active": True,
        "susoft": {
            "api_url": susoft_url,
            "shop_id": susoft_shop_id,
            "login": susoft_login,
            "password": susoft_password  # TODO: Encrypt in production
        },
        "shopify": {
            "shop_domain": shopify_shop,
            "access_token": shopify_token  # TODO: Encrypt in production
        },
        "config": {
            "safety_stock": safety_stock,
            "sync_interval_minutes": 5
        }
    }
    
    if Confirm.ask("\nSave this tenant?"):
        mgr = TenantManager()
        mgr.add_tenant(tenant)
        console.print(f"\n✅ Tenant [green]{name}[/] saved!")


async def cmd_list_tenants():
    """List all tenants."""
    mgr = TenantManager()
    tenants = mgr.list_tenants()
    
    if not tenants:
        console.print("[yellow]No tenants configured yet. Use 'python cli.py tenant add'[/]")
        return
    
    table = Table(title="Configured Tenants")
    table.add_column("Name", style="cyan")
    table.add_column("Susoft Shop", style="green")
    table.add_column("Shopify Shop", style="magenta")
    table.add_column("Status")
    table.add_column("Created")
    
    for t in tenants:
        status = "🟢 Active" if t.get("active") else "🔴 Inactive"
        table.add_row(
            t.get("name", "?"),
            t.get("susoft", {}).get("shop_id", "?"),
            t.get("shopify", {}).get("shop_domain", "?"),
            status,
            t.get("created_at", "?")[:10]
        )
    
    console.print(table)


async def cmd_test_susoft(tenant_name: Optional[str] = None):
    """Test Susoft connection for a tenant."""
    mgr = TenantManager()
    tenants = mgr.list_tenants()
    
    if not tenants:
        console.print("[red]No tenants configured[/]")
        return
    
    if not tenant_name:
        if len(tenants) == 1:
            tenant_name = tenants[0]["name"]
        else:
            tenant_name = Prompt.ask(
                "Select tenant",
                choices=[t["name"] for t in tenants]
            )
    
    tenant = mgr.get_tenant(tenant_name)
    if not tenant:
        console.print(f"[red]Tenant '{tenant_name}' not found[/]")
        return
    
    console.print(Panel(f"🔍 Testing Susoft for: {tenant_name}", style="bold blue"))
    
    susoft_cfg = tenant["susoft"]
    client = SusoftClient(susoft_cfg["api_url"], susoft_cfg["shop_id"])
    
    with console.status("Authenticating..."):
        if not await client.authenticate(susoft_cfg["login"], susoft_cfg["password"]):
            console.print("[red]❌ Authentication failed[/]")
            await client.close()
            return
    console.print("✅ Authentication OK")
    
    with console.status("Fetching shop info..."):
        info = await client.get_shop_info()
    console.print(f"✅ Shop: {info.get('shopName')} | Currency: {info.get('currency')}")
    
    with console.status("Fetching products..."):
        products = await client.get_products(page=0, page_size=50)
    console.print(f"✅ Products: {len(products)} found")
    
    # Show sample products
    if products:
        table = Table(title="Sample Products (first 10)")
        table.add_column("ID", style="cyan")
        table.add_column("Name")
        table.add_column("Barcode", style="dim")
        table.add_column("Price", justify="right")
        table.add_column("Stock", justify="right")
        
        for p in products[:10]:
            stock = p.get("stock", {})
            table.add_row(
                str(p.get("id", "?")),
                str(p.get("name", "?"))[:40],
                str(p.get("barcode", "-")),
                f"{p.get('retailPrice', 0):.2f}",
                str(stock.get("stock", 0)) if stock else "-"
            )
        
        console.print(table)
    
    await client.close()


async def cmd_sync_products(tenant_name: Optional[str] = None):
    """Sync products from Susoft and optionally transfer active products to Shopify."""
    mgr = TenantManager()
    tenants = mgr.list_tenants()
    
    if not tenants:
        console.print("[red]No tenants configured[/]")
        return
    
    if not tenant_name:
        tenant_name = tenants[0]["name"] if len(tenants) == 1 else Prompt.ask(
            "Select tenant",
            choices=[t["name"] for t in tenants]
        )
    
    tenant = mgr.get_tenant(tenant_name)
    if not tenant:
        console.print(f"[red]Tenant '{tenant_name}' not found[/]")
        return
    
    console.print(Panel(f"📦 Syncing Products for: {tenant_name}", style="bold green"))
    
    susoft_cfg = tenant["susoft"]
    client = SusoftClient(susoft_cfg["api_url"], susoft_cfg["shop_id"])
    
    with console.status("Authenticating..."):
        if not await client.authenticate(susoft_cfg["login"], susoft_cfg["password"]):
            console.print("[red]❌ Authentication failed[/]")
            await client.close()
            return
    
    with console.status("Fetching all products..."):
        products = await client.get_all_products()

    console.print(f"\n📊 Total products: [bold]{len(products)}[/]")

    def is_active_product(product: dict) -> bool:
        value = product.get("active")
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"true", "1", "yes", "active"}
        if isinstance(value, (int, float)):
            return value == 1
        return False

    active_products = [p for p in products if is_active_product(p)]
    with_stock = sum(1 for p in active_products if p.get("stock", {}).get("stock", 0) > 0)

    console.print(f"   Active (filtered): {len(active_products)}")
    console.print(f"   Active with stock > 0: {with_stock}")

    # Always save the filtered active products snapshot.
    output_file = f"products_{tenant_name}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(active_products, f, indent=2, ensure_ascii=False)

    console.print(f"\n✅ Active products saved to [cyan]{output_file}[/]")

    if "--shopify" not in sys.argv:
        console.print("ℹ️ Use [bold]python cli.py sync products --shopify[/] to transfer active products to Shopify")
        await client.close()
        return

    dry_run = "--dry-run" in sys.argv
    shopify_cfg = tenant.get("shopify", {})
    shop_domain = shopify_cfg.get("shop_domain")
    access_token = shopify_cfg.get("access_token")

    if not shop_domain or not access_token:
        console.print("[red]❌ Shopify credentials missing for tenant[/]")
        await client.close()
        return

    shopify_client = ShopifyClient(shop_domain=shop_domain, access_token=access_token)

    created = 0
    updated = 0
    skipped = 0
    failed = 0

    try:
        mode_label = "DRY-RUN" if dry_run else "LIVE"
        console.print(Panel(f"🛍️ Shopify transfer mode: [bold]{mode_label}[/]", style="bold magenta"))

        existing_variants_by_sku = await shopify_client.get_variant_map_by_sku()

        for product in active_products:
            sku = (
                product.get("sku")
                or product.get("externalRefId")
                or product.get("barcode")
            )

            if not sku:
                skipped += 1
                continue

            title = product.get("name") or f"Susoft {sku}"
            try:
                price = float(product.get("retailPrice") or 0)
            except (TypeError, ValueError):
                price = 0.0

            product_payload = {
                "title": title,
                "status": "active",
                "published": True,
                "published_scope": "web",
                "variants": [
                    {
                        "sku": str(sku),
                        "price": f"{price:.2f}"
                    }
                ]
            }

            existing_variant_id = existing_variants_by_sku.get(str(sku))

            if existing_variant_id:
                if dry_run:
                    updated += 1
                    continue

                ok = await shopify_client.update_variant(
                    variant_id=existing_variant_id,
                    payload={
                        "id": int(existing_variant_id),
                        "sku": str(sku),
                        "price": f"{price:.2f}"
                    }
                )
                if ok:
                    updated += 1
                else:
                    failed += 1
                    logger_msg = f"Update failed for SKU={sku}, variant_id={existing_variant_id}"
                    console.print(f"[yellow]{logger_msg}[/]")
                continue

            if dry_run:
                created += 1
                continue

            created_product = await shopify_client.create_product(product_payload)
            if created_product:
                created += 1
            else:
                failed += 1
                console.print(f"[yellow]Create failed for SKU={sku}, title={title}[/]")

    finally:
        await shopify_client.close()
    
    await client.close()

    console.print("\n📦 Shopify transfer summary (active products only):")
    console.print(f"   Created: {created}")
    console.print(f"   Updated: {updated}")
    console.print(f"   Skipped (missing SKU): {skipped}")
    console.print(f"   Failed: {failed}")


async def cmd_sync_stock(
    tenant_name: Optional[str] = None,
    apply: bool = False,
    location_id: Optional[str] = None,
    match_key: str = "barcode",
    safety_stock: int = 0,
    limit: Optional[int] = None,
):
    """Push Susoft stock levels into Shopify inventory.

    Matches by SKU: Susoft ``barcode`` (or ``id`` / ``externalRefId``)
    against Shopify ``variant.sku``. By default runs as a dry run; pass
    ``apply=True`` to actually call ``inventory_levels/set.json``.
    """
    mgr = TenantManager()
    tenants = mgr.list_tenants()

    if not tenants:
        console.print("[red]No tenants configured[/]")
        return

    if not tenant_name:
        tenant_name = tenants[0]["name"] if len(tenants) == 1 else Prompt.ask(
            "Select tenant", choices=[t["name"] for t in tenants]
        )

    tenant = mgr.get_tenant(tenant_name)
    if not tenant:
        console.print(f"[red]Tenant '{tenant_name}' not found[/]")
        return

    susoft_cfg = tenant["susoft"]
    shopify_cfg = tenant.get("shopify", {})
    shop_domain = shopify_cfg.get("shop_domain")
    access_token = shopify_cfg.get("access_token")

    if not shop_domain or not access_token:
        console.print("[red]❌ Shopify credentials missing for tenant[/]")
        return

    mode = "APPLY" if apply else "DRY-RUN"
    console.print(Panel(
        f"📦 Stock sync: Susoft → Shopify  ([bold]{mode}[/])\n"
        f"Tenant: {tenant_name}   |   Match key: Susoft.{match_key} ↔ Shopify.variant.sku",
        style="bold green"
    ))

    susoft = SusoftClient(susoft_cfg["api_url"], susoft_cfg["shop_id"])
    shopify = ShopifyClient(shop_domain=shop_domain, access_token=access_token)

    try:
        # ---- Auth + fetch Susoft ----
        with console.status("Authenticating with Susoft..."):
            if not await susoft.authenticate(susoft_cfg["login"], susoft_cfg["password"]):
                console.print("[red]❌ Susoft authentication failed[/]")
                return

        with console.status("Fetching Susoft products..."):
            susoft_products = await susoft.get_all_products()
        console.print(f"  Susoft products: [bold]{len(susoft_products)}[/]")

        # ---- Fetch Shopify variants + locations ----
        with console.status("Fetching Shopify variants (paginated)..."):
            sku_map = await shopify.get_full_variant_map_by_sku()
        console.print(f"  Shopify variants with SKU: [bold]{len(sku_map)}[/]")

        if not location_id:
            with console.status("Fetching Shopify locations..."):
                locations = await shopify.list_locations()
            active_loc = next(
                (loc for loc in locations if loc.get("active", True)),
                locations[0] if locations else None,
            )
            if not active_loc:
                console.print("[red]❌ No Shopify locations found[/]")
                return
            location_id = str(active_loc["id"])
            console.print(
                f"  Using Shopify location: [cyan]{active_loc.get('name', '?')}[/] "
                f"(id={location_id})"
            )
        else:
            console.print(f"  Using Shopify location: [cyan]{location_id}[/] (override)")

        # ---- Match + plan ----
        def extract_key(p: dict) -> Optional[str]:
            if match_key == "barcode":
                v = p.get("barcode") or p.get("id")
            elif match_key == "id":
                v = p.get("id")
            elif match_key == "external_ref":
                v = p.get("externalRefId")
            else:
                v = p.get("barcode")
            return str(v).strip() if v not in (None, "") else None

        iter_products = susoft_products if limit is None else susoft_products[:limit]

        planned: list = []
        no_match: list = []
        no_key = 0

        for p in iter_products:
            key = extract_key(p)
            if not key:
                no_key += 1
                continue
            shopify_match = sku_map.get(key)
            if not shopify_match:
                if len(no_match) < 10:
                    no_match.append(key)
                continue
            stock_obj = p.get("stock") or {}
            qty = 0
            if isinstance(stock_obj, dict):
                qty = stock_obj.get("stock", 0) or 0
            try:
                qty_int = max(0, int(float(qty)) - safety_stock)
            except (TypeError, ValueError):
                qty_int = 0
            planned.append({
                "sku": key,
                "name": p.get("name") or "",
                "qty": qty_int,
                "inventory_item_id": shopify_match["inventory_item_id"],
                "variant_id": shopify_match["variant_id"],
            })

        # ---- Show plan ----
        table = Table(title=f"Planned inventory updates (showing first 25 of {len(planned)})")
        table.add_column("SKU", style="cyan")
        table.add_column("Name", style="white")
        table.add_column("Qty", style="green", justify="right")
        table.add_column("Variant ID", style="dim")
        for row in planned[:25]:
            table.add_row(row["sku"], row["name"][:40], str(row["qty"]), row["variant_id"])
        console.print(table)

        console.print(f"\n  Matched & planned : [bold green]{len(planned)}[/]")
        console.print(f"  No Shopify match  : [bold yellow]{len(susoft_products) - len(planned) - no_key}[/]")
        console.print(f"  Missing key       : [bold yellow]{no_key}[/]")
        if no_match:
            console.print("  Examples of unmatched keys:")
            for v in no_match:
                console.print(f"    • {v}")

        if not apply:
            console.print(
                "\n[bold yellow]DRY-RUN[/] complete. "
                "Re-run with [bold]--apply[/] to push these levels to Shopify."
            )
            return

        # ---- Apply ----
        ok = 0
        fail = 0
        connected = 0
        enabled_tracking = 0
        first_errors: list = []
        with console.status(f"Pushing {len(planned)} inventory levels to Shopify..."):
            for row in planned:
                success, err = await shopify.set_inventory_level(
                    inventory_item_id=row["inventory_item_id"],
                    location_id=location_id,
                    available=row["qty"],
                )
                # Auto-recover from common errors:
                # 1) "Inventory item does not have inventory at the location"
                # 2) "The inventory item is not tracked"
                if not success and err:
                    err_l = err.lower()
                    needs_tracking = (
                        "not tracked" in err_l
                        or "must be tracked" in err_l
                        or "tracking enabled" in err_l
                        or "tracking is not enabled" in err_l
                    )
                    needs_connect = (
                        "does not have inventory at the location" in err_l
                        or "not stocked at" in err_l
                        or "destination_location" in err_l
                    )
                    if needs_tracking:
                        if await shopify.enable_tracking(row["inventory_item_id"]):
                            enabled_tracking += 1
                            success, err = await shopify.set_inventory_level(
                                row["inventory_item_id"], location_id, row["qty"]
                            )
                    if not success and (needs_connect or needs_tracking):
                        if await shopify.connect_inventory(
                            row["inventory_item_id"], location_id
                        ):
                            connected += 1
                            success, err = await shopify.set_inventory_level(
                                row["inventory_item_id"], location_id, row["qty"]
                            )
                if success:
                    ok += 1
                else:
                    fail += 1
                    if len(first_errors) < 5 and err:
                        first_errors.append(f"  • SKU {row['sku']}: {err}")

        console.print(
            f"\n✅ Done. Updated: [green]{ok}[/]   Failed: [red]{fail}[/]"
            f"   (auto-connected: {connected}, tracking enabled: {enabled_tracking})"
        )
        if first_errors:
            console.print("[yellow]First errors:[/]")
            for line in first_errors:
                console.print(line)
    finally:
        await shopify.close()
        await susoft.close()


def main():
    """CLI entry point."""
    if len(sys.argv) < 2:
        console.print(Panel("""
[bold]Susoft-Shopify Sync CLI[/]

Usage:
  python cli.py tenant add       Add a new tenant
  python cli.py tenant list      List all tenants
  python cli.py test susoft      Test Susoft connection
    python cli.py sync products    Export active products from Susoft
    python cli.py sync products --shopify [--dry-run]
                                                                Transfer active products to Shopify
        """, title="Help"))
        return
    
    cmd = sys.argv[1]
    subcmd = sys.argv[2] if len(sys.argv) > 2 else None
    arg = sys.argv[3] if len(sys.argv) > 3 else None
    
    if cmd == "tenant":
        if subcmd == "add":
            asyncio.run(cmd_add_tenant())
        elif subcmd == "list":
            asyncio.run(cmd_list_tenants())
        else:
            console.print("[red]Unknown tenant command. Use: add, list[/]")
    
    elif cmd == "test":
        if subcmd == "susoft":
            asyncio.run(cmd_test_susoft(arg))
        else:
            console.print("[red]Unknown test command. Use: susoft[/]")
    
    elif cmd == "sync":
        if subcmd == "products":
            tenant_arg = None
            if arg and not arg.startswith("--"):
                tenant_arg = arg
            asyncio.run(cmd_sync_products(tenant_arg))
        elif subcmd == "stock":
            tenant_arg = arg if (arg and not arg.startswith("--")) else None
            apply = "--apply" in sys.argv
            match_key = "barcode"
            for flag in ("--match-key=barcode", "--match-key=id", "--match-key=external_ref"):
                if flag in sys.argv:
                    match_key = flag.split("=", 1)[1]
            location_id = None
            for a in sys.argv:
                if a.startswith("--location-id="):
                    location_id = a.split("=", 1)[1]
            limit = None
            for a in sys.argv:
                if a.startswith("--limit="):
                    try:
                        limit = int(a.split("=", 1)[1])
                    except ValueError:
                        pass
            safety_stock = 0
            for a in sys.argv:
                if a.startswith("--safety-stock="):
                    try:
                        safety_stock = int(a.split("=", 1)[1])
                    except ValueError:
                        pass
            asyncio.run(cmd_sync_stock(
                tenant_name=tenant_arg,
                apply=apply,
                location_id=location_id,
                match_key=match_key,
                safety_stock=safety_stock,
                limit=limit,
            ))
        else:
            console.print("[red]Unknown sync command. Use: products, stock[/]")
    
    else:
        console.print(f"[red]Unknown command: {cmd}[/]")


if __name__ == "__main__":
    main()
