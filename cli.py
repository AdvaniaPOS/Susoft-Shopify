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
    """Sync all products from Susoft."""
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
    
    # Categorize products
    with_stock = sum(1 for p in products if p.get("stock", {}).get("stock", 0) > 0)
    active = sum(1 for p in products if p.get("active", False))
    
    console.print(f"   Active: {active}")
    console.print(f"   With stock > 0: {with_stock}")
    
    # Save to local file for now (will be database in production)
    output_file = f"products_{tenant_name}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(products, f, indent=2, ensure_ascii=False)
    
    console.print(f"\n✅ Products saved to [cyan]{output_file}[/]")
    
    await client.close()


def main():
    """CLI entry point."""
    if len(sys.argv) < 2:
        console.print(Panel("""
[bold]Susoft-Shopify Sync CLI[/]

Usage:
  python cli.py tenant add       Add a new tenant
  python cli.py tenant list      List all tenants
  python cli.py test susoft      Test Susoft connection
  python cli.py sync products    Sync products from Susoft
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
            asyncio.run(cmd_sync_products(arg))
        else:
            console.print("[red]Unknown sync command. Use: products[/]")
    
    else:
        console.print(f"[red]Unknown command: {cmd}[/]")


if __name__ == "__main__":
    main()
