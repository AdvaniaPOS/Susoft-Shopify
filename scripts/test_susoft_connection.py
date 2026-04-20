"""
Test script for Susoft API connection.

Tests authentication, fetching products, stock, and order creation.
"""
import asyncio
import httpx
from datetime import datetime, timedelta
from typing import Optional
import json

# Susoft API configuration
SUSOFT_BASE_URL = "https://api.susoft.com:4443"
SUSOFT_LOGIN = "advaniaposapi@gmail.com"
SUSOFT_PASSWORD = "xmGPgLgq"
SUSOFT_SHOP_ID = "jonb"


class SusoftTestClient:
    """Test client for Susoft API."""
    
    def __init__(self):
        self.base_url = SUSOFT_BASE_URL
        self.token: Optional[str] = None
        self.client = httpx.AsyncClient(
            timeout=30.0,
            verify=True  # SSL verification
        )
    
    async def authenticate(self) -> bool:
        """Authenticate with Susoft and get JWT token."""
        print(f"\n🔐 Authenticating with Susoft...")
        print(f"   URL: {self.base_url}/user/auth")
        print(f"   Login: {SUSOFT_LOGIN}")
        print(f"   Shop ID: {SUSOFT_SHOP_ID}")
        
        try:
            response = await self.client.post(
                f"{self.base_url}/user/auth",
                json={
                    "login": SUSOFT_LOGIN,
                    "password": SUSOFT_PASSWORD
                },
                headers={
                    "Content-Type": "application/json",
                    "X-Shop-Url-Key": SUSOFT_SHOP_ID
                }
            )
            
            print(f"   Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                if data.get("success") and data.get("token"):
                    self.token = data["token"]
                    print(f"   ✅ Authentication successful!")
                    print(f"   Token: {self.token[:50]}...")
                    return True
                else:
                    print(f"   ❌ Auth response: {data}")
                    return False
            else:
                print(f"   ❌ Failed: {response.text[:500]}")
                return False
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return False
    
    def _get_headers(self) -> dict:
        """Get headers with auth token."""
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "X-Shop-Url-Key": SUSOFT_SHOP_ID
        }
    
    async def get_shop_info(self) -> dict:
        """Get shop information."""
        print(f"\n🏪 Fetching shop info...")
        
        try:
            response = await self.client.get(
                f"{self.base_url}/shop/info",
                headers=self._get_headers()
            )
            
            print(f"   Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                print(f"   ✅ Shop: {data.get('shopName', 'N/A')}")
                print(f"   Tenant: {data.get('tenantName', 'N/A')}")
                print(f"   Currency: {data.get('currency', 'N/A')}")
                return data
            else:
                print(f"   ❌ Failed: {response.text[:300]}")
                return {}
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return {}
    
    async def get_products(self, page: int = 0, page_size: int = 10) -> list:
        """Fetch products modified in the last year."""
        print(f"\n📦 Fetching products (page {page}, size {page_size})...")
        
        # Get products modified in last year
        since_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%dT00:00:00.000")
        
        try:
            response = await self.client.get(
                f"{self.base_url}/product/list/modified",
                params={
                    "dateTime": since_date,
                    "page": page,
                    "pageSize": page_size,
                    "expandConfigurable": "true",
                    "withVariants": "true"
                },
                headers=self._get_headers()
            )
            
            print(f"   Status: {response.status_code}")
            
            if response.status_code == 200:
                products = response.json()
                print(f"   ✅ Found {len(products)} products")
                
                for i, p in enumerate(products[:5]):  # Show first 5
                    print(f"   [{i+1}] {p.get('id', 'N/A')} - {p.get('name', 'N/A')}")
                    print(f"       SKU/Barcode: {p.get('barcode', 'N/A')}")
                    print(f"       Price: {p.get('retailPrice', 'N/A')}")
                    stock = p.get('stock', {})
                    if stock:
                        print(f"       Stock: {stock.get('stock', 'N/A')} @ {stock.get('shopId', 'N/A')}")
                
                if len(products) > 5:
                    print(f"   ... and {len(products) - 5} more products")
                    
                return products
            else:
                print(f"   ❌ Failed: {response.text[:300]}")
                return []
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return []
    
    async def search_products(self, query: str) -> list:
        """Search for products by name or barcode."""
        print(f"\n🔍 Searching products: '{query}'...")
        
        try:
            response = await self.client.post(
                f"{self.base_url}/product/search",
                params={
                    "page": 0,
                    "pageSize": 10
                },
                json={
                    "filterGroups": [
                        {
                            "filters": [
                                {"field": "name", "operator": "like", "value": f"%{query}%"}
                            ]
                        }
                    ]
                },
                headers=self._get_headers()
            )
            
            print(f"   Status: {response.status_code}")
            
            if response.status_code == 200:
                products = response.json()
                print(f"   ✅ Found {len(products)} matching products")
                for p in products[:5]:
                    print(f"   - {p.get('id', 'N/A')}: {p.get('name', 'N/A')}")
                return products
            else:
                print(f"   ❌ Failed: {response.text[:300]}")
                return []
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return []
    
    async def get_categories(self) -> dict:
        """Get product category tree."""
        print(f"\n📂 Fetching product categories...")
        
        try:
            response = await self.client.get(
                f"{self.base_url}/product/category/tree",
                headers=self._get_headers()
            )
            
            print(f"   Status: {response.status_code}")
            
            if response.status_code == 200:
                tree = response.json()
                print(f"   ✅ Category tree loaded")
                
                def print_tree(node, indent=0):
                    prefix = "   " + "  " * indent
                    print(f"{prefix}- {node.get('name', 'N/A')} (ID: {node.get('id', 'N/A')})")
                    for child in (node.get('children') or {}).values():
                        if indent < 2:  # Limit depth
                            print_tree(child, indent + 1)
                
                print_tree(tree)
                return tree
            else:
                print(f"   ❌ Failed: {response.text[:300]}")
                return {}
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return {}
    
    async def get_webhooks(self) -> list:
        """List existing webhooks."""
        print(f"\n🔔 Fetching webhooks...")
        
        webhook_types = [
            "ON_ORDER_CREATED",
            "ON_PRODUCT_STOCK_CHANGED",
            "ON_PRODUCT_CREATED",
            "ON_PRODUCT_UPDATED"
        ]
        
        all_webhooks = []
        for wh_type in webhook_types:
            try:
                response = await self.client.get(
                    f"{self.base_url}/webhook/settings",
                    params={"webhookType": wh_type},
                    headers=self._get_headers()
                )
                
                if response.status_code == 200:
                    webhooks = response.json()
                    if webhooks:
                        print(f"   {wh_type}: {len(webhooks)} webhook(s)")
                        for wh in webhooks:
                            print(f"     - ID: {wh.get('id')}, URL: {wh.get('url')}, Active: {wh.get('active')}")
                        all_webhooks.extend(webhooks)
                        
            except Exception as e:
                print(f"   Warning: Could not fetch {wh_type}: {e}")
        
        if not all_webhooks:
            print(f"   ℹ️ No webhooks configured")
            
        return all_webhooks
    
    async def test_create_order_dry_run(self) -> None:
        """
        Show what an order would look like (without actually creating it).
        
        Key insight from API docs:
        - Orders WITH payment = general sale order (completed)
        - Orders WITHOUT payment = ready for invoicing (pending payment)
        """
        print(f"\n📝 Order Creation Examples (DRY RUN - not sending)")
        
        # Example 1: Order WITH payment (Shopify paid order)
        order_with_payment = {
            "alternativeId": "SHOPIFY-12345",  # Links to Shopify order
            "uuid": "550e8400-e29b-41d4-a716-446655440000",
            "orderDateTime": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000"),
            "shopId": SUSOFT_SHOP_ID,
            "customer": {
                "firstName": "Test",
                "lastName": "Customer",
                "address": {
                    "email": "test@example.com",
                    "mobilePhone": "+4712345678"
                }
            },
            "lines": [
                {
                    "product": {"id": "PRODUCT-SKU-123"},
                    "quantity": 2,
                    "price": 199.00,
                    "vatPercent": 25.0
                }
            ],
            "payments": [
                {
                    "paymentType": "TERMINAL",  # Or VIPPS, KLARNA, etc.
                    "amount": 398.00,
                    "currencyAmount": 398.00,
                    "currency": "NOK",
                    "rate": 1.0,
                    "orderNo": 0,  # Will be assigned by Susoft
                    "shopId": SUSOFT_SHOP_ID,
                    "issuedShopId": SUSOFT_SHOP_ID
                }
            ]
        }
        
        # Example 2: Order WITHOUT payment (for invoicing)
        order_without_payment = {
            "alternativeId": "SHOPIFY-12346",
            "uuid": "550e8400-e29b-41d4-a716-446655440001",
            "orderDateTime": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000"),
            "shopId": SUSOFT_SHOP_ID,
            "isForInvoicing": True,  # Mark for invoicing
            "customer": {
                "firstName": "Business",
                "lastName": "Customer AS",
                "isCompany": True,
                "orgNo": 123456789,
                "address": {
                    "email": "invoice@company.no"
                }
            },
            "lines": [
                {
                    "product": {"id": "PRODUCT-SKU-456"},
                    "quantity": 5,
                    "price": 500.00,
                    "vatPercent": 25.0
                }
            ]
            # No payments = ready for invoicing
        }
        
        print("\n   📗 Order WITH payment (immediate sale):")
        print(f"   {json.dumps(order_with_payment, indent=4, default=str)[:800]}...")
        
        print("\n   📘 Order WITHOUT payment (for invoicing):")
        print(f"   {json.dumps(order_without_payment, indent=4, default=str)[:800]}...")
        
        print("\n   ℹ️ Payment types available:")
        print("      - TERMINAL (card payment)")
        print("      - VIPPS")
        print("      - KLARNA")
        print("      - CASH")
        print("      - INVOICE (B2B)")
        print("      - GIFT_CARD")
    
    async def close(self):
        """Close the HTTP client."""
        await self.client.aclose()


async def main():
    """Run all Susoft API tests."""
    print("=" * 60)
    print("🚀 SUSOFT API CONNECTION TEST")
    print("=" * 60)
    
    client = SusoftTestClient()
    
    try:
        # 1. Authenticate
        if not await client.authenticate():
            print("\n❌ Authentication failed. Cannot proceed.")
            return
        
        # 2. Get shop info
        await client.get_shop_info()
        
        # 3. Get categories
        await client.get_categories()
        
        # 4. Get products
        await client.get_products(page=0, page_size=20)
        
        # 5. Check existing webhooks
        await client.get_webhooks()
        
        # 6. Show order examples
        await client.test_create_order_dry_run()
        
        print("\n" + "=" * 60)
        print("✅ CONNECTION TEST COMPLETE")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
