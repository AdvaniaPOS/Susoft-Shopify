"""
Test script for creating orders in Susoft.

Demonstrates:
1. Order WITH payment (immediate sale - e.g., paid via Shopify)
2. Order WITHOUT payment (for invoicing - B2B orders)

Usage:
    python scripts/test_order_creation.py
"""
import asyncio
import httpx
import json
import uuid
from datetime import datetime
from typing import Optional
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt, Confirm
from rich.table import Table

console = Console()

# Load tenant config
def load_tenant() -> dict:
    with open("tenants.json", "r") as f:
        data = json.load(f)
        return data["tenants"][0]  # First tenant


class SusoftOrderClient:
    """Client for Susoft order operations."""
    
    def __init__(self, base_url: str, shop_id: str):
        self.base_url = base_url.rstrip("/")
        self.shop_id = shop_id
        self.token: Optional[str] = None
        self.client = httpx.AsyncClient(timeout=30.0, verify=True)
    
    async def authenticate(self, login: str, password: str) -> bool:
        """Get JWT token."""
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
    
    async def get_product_by_id(self, product_id: str) -> Optional[dict]:
        """Get a product by ID."""
        response = await self.client.get(
            f"{self.base_url}/product/id",
            params={"productId": product_id},
            headers=self._headers()
        )
        if response.status_code == 200:
            return response.json()
        return None
    
    async def search_products(self, query: str, limit: int = 10) -> list:
        """Search products by name."""
        response = await self.client.post(
            f"{self.base_url}/product/search",
            params={"page": 0, "pageSize": limit},
            json={
                "filterGroups": [
                    {"filters": [{"field": "name", "operator": "like", "value": f"%{query}%"}]}
                ]
            },
            headers=self._headers()
        )
        return response.json() if response.status_code == 200 else []
    
    async def check_order_exists(self, alternative_id: str) -> Optional[dict]:
        """Check if order already exists by alternativeId."""
        response = await self.client.get(
            f"{self.base_url}/order/altid",
            params={"altId": alternative_id},
            headers=self._headers()
        )
        if response.status_code == 200:
            return response.json()
        return None
    
    async def create_order(
        self,
        shopify_order_id: str,
        customer: dict,
        lines: list,
        payment: Optional[dict] = None
    ) -> dict:
        """
        Create order in Susoft.
        
        Args:
            shopify_order_id: Shopify order ID (used as alternativeId)
            customer: Customer data
            lines: Order line items
            payment: Payment info. If None, order is for invoicing.
        
        Returns:
            dict with status, data, error
        """
        # Create unique alternativeId for idempotency
        alternative_id = f"SHOPIFY-{shopify_order_id}"
        
        # Check if already exists (idempotency)
        existing = await self.check_order_exists(alternative_id)
        if existing:
            return {
                "status": 200,
                "data": existing,
                "already_exists": True,
                "message": f"Order {alternative_id} already exists in Susoft"
            }
        
        order_data = {
            "alternativeId": alternative_id,
            "uuid": str(uuid.uuid4()),
            "orderDateTime": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000"),
            "shopId": self.shop_id,
            "customer": customer,
            "lines": lines
        }
        
        if payment:
            # Paid order - immediate sale
            order_data["payments"] = [payment]
        else:
            # Unpaid order - for invoicing (B2B)
            order_data["isForInvoicing"] = True
        
        console.print("\n[dim]Sending order to Susoft:[/]")
        console.print(json.dumps(order_data, indent=2, default=str)[:1500])
        
        response = await self.client.post(
            f"{self.base_url}/order",
            json=order_data,
            headers=self._headers()
        )
        
        if response.status_code == 200:
            return {
                "status": 200,
                "data": response.json(),
                "already_exists": False
            }
        else:
            return {
                "status": response.status_code,
                "error": response.text,
                "already_exists": False
            }
    
    async def close(self):
        await self.client.aclose()


async def demo_paid_order(client: SusoftOrderClient, products: list):
    """Demo: Create a PAID order (like from Shopify checkout)."""
    console.print(Panel(
        "[bold green]Test 1: Paid Order[/]\n"
        "Simulates a Shopify order where customer paid via card/Vipps.\n"
        "This creates a completed sale in Susoft.",
        title="💳 Paid Order Demo"
    ))
    
    # Use first available product
    if not products:
        console.print("[red]No products available for testing[/]")
        return
    
    product = products[0]
    console.print(f"\nUsing product: [cyan]{product.get('name')}[/] (ID: {product.get('id')})")
    console.print(f"Price: {product.get('retailPrice', 0)} NOK")
    
    # Create test order
    test_order_id = f"TEST-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    quantity = 1
    unit_price = float(product.get('retailPrice', 100))
    total = unit_price * quantity
    vat_percent = float(product.get('vatPercent', 25))
    
    customer = {
        "firstName": "Test",
        "lastName": "Kunde",
        "address": {
            "email": "test@example.com",
            "mobilePhone": "+4712345678",
            "addressLine1": "Testveien 1",
            "zipCode": "0123",
            "city": "Oslo"
        }
    }
    
    lines = [
        {
            "product": {"id": str(product.get('id'))},
            "quantity": quantity,
            "unitPrice": unit_price,
            "price": unit_price,
            "netPrice": unit_price / (1 + vat_percent/100),  # Price excl VAT
            "total": total,
            "vatPercent": vat_percent,
            "vatAmount": total - (total / (1 + vat_percent/100)),
            "priceRef": 0,  # 0 = RETAIL_PRICE
            "text": product.get('name', 'Test product')
        }
    ]
    
    # Payment info - simulating Shopify payment
    payment = {
        "paymentType": "TERMINAL",  # Could be VIPPS, KLARNA, etc.
        "amount": total,
        "currencyAmount": total,
        "currency": "NOK",
        "rate": 1.0,
        "orderNo": 0,  # Will be assigned by Susoft
        "shopId": client.shop_id,
        "issuedShopId": client.shop_id,
        "transactionId": f"shopify-txn-{test_order_id}",
        "note": "Betalt via Shopify checkout"
    }
    
    if Confirm.ask(f"\nSend test order {test_order_id} to Susoft?"):
        result = await client.create_order(
            shopify_order_id=test_order_id,
            customer=customer,
            lines=lines,
            payment=payment
        )
        
        if result.get("already_exists"):
            console.print(f"\n[yellow]⚠️ {result.get('message')}[/]")
        elif result.get("status") == 200:
            order = result.get("data", {})
            console.print(f"\n[green]✅ Order created successfully![/]")
            console.print(f"   Order No: [bold]{order.get('orderNo')}[/]")
            console.print(f"   UUID: {order.get('uuid')}")
            console.print(f"   Alternative ID: {order.get('alternativeId')}")
        else:
            console.print(f"\n[red]❌ Failed to create order[/]")
            console.print(f"   Status: {result.get('status')}")
            console.print(f"   Error: {result.get('error', 'Unknown')[:500]}")
    else:
        console.print("[dim]Skipped[/]")


async def demo_invoice_order(client: SusoftOrderClient, products: list):
    """Demo: Create an UNPAID order for invoicing (B2B)."""
    console.print(Panel(
        "[bold blue]Test 2: Invoice Order (B2B)[/]\n"
        "Simulates a B2B order where payment happens later via invoice.\n"
        "This creates an order ready for invoicing in Susoft.",
        title="📄 Invoice Order Demo"
    ))
    
    if not products:
        console.print("[red]No products available for testing[/]")
        return
    
    # Use a product for B2B order
    product = products[0]
    console.print(f"\nUsing product: [cyan]{product.get('name')}[/] (ID: {product.get('id')})")
    
    test_order_id = f"B2B-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    quantity = 5  # B2B typically has higher quantities
    unit_price = float(product.get('retailPrice', 100))
    vat_percent = float(product.get('vatPercent', 25))
    total = unit_price * quantity
    
    # B2B customer (company)
    customer = {
        "firstName": "Innkjøp",
        "lastName": "Bedrift AS",  # Company name in lastName
        "isCompany": True,
        "orgNo": 123456789,
        "allowCreditSale": True,
        "termOfPayDays": 30,  # Net 30
        "address": {
            "email": "faktura@bedrift.no",
            "addressLine1": "Industrivegen 100",
            "zipCode": "5003",
            "city": "Bergen"
        }
    }
    
    lines = [
        {
            "product": {"id": str(product.get('id'))},
            "quantity": quantity,
            "unitPrice": unit_price,
            "price": unit_price,
            "netPrice": unit_price / (1 + vat_percent/100),
            "total": total,
            "vatPercent": vat_percent,
            "vatAmount": total - (total / (1 + vat_percent/100)),
            "priceRef": 0,
            "text": product.get('name', 'B2B product')
        }
    ]
    
    # NO payment - this means it's for invoicing
    if Confirm.ask(f"\nSend B2B order {test_order_id} to Susoft (for invoicing)?"):
        result = await client.create_order(
            shopify_order_id=test_order_id,
            customer=customer,
            lines=lines,
            payment=None  # No payment = invoice order
        )
        
        if result.get("already_exists"):
            console.print(f"\n[yellow]⚠️ {result.get('message')}[/]")
        elif result.get("status") == 200:
            order = result.get("data", {})
            console.print(f"\n[green]✅ Invoice order created![/]")
            console.print(f"   Order No: [bold]{order.get('orderNo')}[/]")
            console.print(f"   UUID: {order.get('uuid')}")
            console.print(f"   Alternative ID: {order.get('alternativeId')}")
            console.print(f"   [dim]→ This order can now be invoiced in Susoft[/]")
        else:
            console.print(f"\n[red]❌ Failed to create order[/]")
            console.print(f"   Status: {result.get('status')}")
            console.print(f"   Error: {result.get('error', 'Unknown')[:500]}")
    else:
        console.print("[dim]Skipped[/]")


async def main():
    """Run order creation tests."""
    console.print(Panel(
        "[bold]Susoft Order Creation Test[/]\n\n"
        "This script tests creating orders in Susoft:\n"
        "• [green]Paid orders[/] - With payment (completed sales)\n"
        "• [blue]Invoice orders[/] - Without payment (B2B/faktura)",
        title="🛒 Order Test"
    ))
    
    # Load tenant
    try:
        tenant = load_tenant()
        console.print(f"\n📋 Tenant: [cyan]{tenant['name']}[/]")
    except Exception as e:
        console.print(f"[red]Failed to load tenant config: {e}[/]")
        return
    
    susoft_cfg = tenant["susoft"]
    client = SusoftOrderClient(susoft_cfg["api_url"], susoft_cfg["shop_id"])
    
    try:
        # Authenticate
        with console.status("Authenticating with Susoft..."):
            if not await client.authenticate(susoft_cfg["login"], susoft_cfg["password"]):
                console.print("[red]❌ Authentication failed[/]")
                return
        console.print("[green]✅ Authenticated[/]")
        
        # Get some products to use for test orders
        with console.status("Fetching products..."):
            # Load from saved file
            try:
                with open(f"products_{tenant['name']}.json", "r", encoding="utf-8") as f:
                    all_products = json.load(f)
                # Filter to active products with prices
                products = [
                    p for p in all_products 
                    if p.get('active', False) and p.get('retailPrice', 0) > 0
                ][:10]
            except FileNotFoundError:
                # Fallback to API search
                products = await client.search_products("", limit=10)
        
        if products:
            console.print(f"[green]✅ Found {len(products)} products for testing[/]")
            
            # Show available products
            table = Table(title="Available Test Products")
            table.add_column("ID", style="cyan")
            table.add_column("Name")
            table.add_column("Price", justify="right")
            table.add_column("VAT%", justify="right")
            
            for p in products[:5]:
                table.add_row(
                    str(p.get('id', '?')),
                    str(p.get('name', '?'))[:40],
                    f"{p.get('retailPrice', 0):.2f}",
                    f"{p.get('vatPercent', 25)}%"
                )
            console.print(table)
        else:
            console.print("[yellow]⚠️ No products found. Using mock data.[/]")
            products = [{"id": "TEST-001", "name": "Test Product", "retailPrice": 100, "vatPercent": 25}]
        
        console.print("\n" + "="*60)
        
        # Test 1: Paid order
        await demo_paid_order(client, products)
        
        console.print("\n" + "="*60)
        
        # Test 2: Invoice order
        await demo_invoice_order(client, products)
        
        console.print("\n" + "="*60)
        console.print("[bold]Summary:[/]")
        console.print("• Paid orders (med betaling) → Fakturert direkte i Susoft")
        console.print("• Invoice orders (uten betaling) → Klar for fakturering i Susoft")
        console.print("\n[dim]For Shopify-integrasjonen:[/dim]")
        console.print("[dim]  - Betalte ordrer fra Shopify → Send med payment[/dim]")
        console.print("[dim]  - B2B/Faktura-ordrer → Send uten payment[/dim]")
        
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
