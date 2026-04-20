#!/usr/bin/env python3
"""Test Shopify API connection."""

import httpx
import json
import os
from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()

# Shopify credentials from environment or tenants.json
SHOP_URL = os.getenv("SHOPIFY_SHOP_URL", "")
ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN", "")

# Shopify Admin API version
API_VERSION = "2024-01"


async def test_shopify():
    """Test Shopify API connection."""
    
    # Try to load from tenants.json if env vars not set
    global SHOP_URL, ACCESS_TOKEN
    if not SHOP_URL or not ACCESS_TOKEN:
        try:
            tenants_file = Path(__file__).parent.parent / "tenants.json"
            if tenants_file.exists():
                with open(tenants_file) as f:
                    data = json.load(f)
                    if data.get("tenants"):
                        tenant = data["tenants"][0]
                        shopify = tenant.get("shopify", {})
                        SHOP_URL = shopify.get("shop_domain", "")
                        ACCESS_TOKEN = shopify.get("access_token", "")
        except Exception as e:
            console.print(f"[yellow]Kunne ikke lese tenants.json: {e}[/]")
    
    if not SHOP_URL or not ACCESS_TOKEN:
        console.print("[red]❌ Mangler Shopify credentials![/]")
        console.print("Sett SHOPIFY_SHOP_URL og SHOPIFY_ACCESS_TOKEN miljøvariabler,")
        console.print("eller konfigurer shopify i tenants.json")
        return False
    console.print(Panel(
        "[bold]Shopify API Connection Test[/]\n\n"
        f"Shop: {SHOP_URL}\n"
        f"API Version: {API_VERSION}",
        title="🛍️ Shopify Test"
    ))
    
    base_url = f"https://{SHOP_URL}/admin/api/{API_VERSION}"
    headers = {
        "X-Shopify-Access-Token": ACCESS_TOKEN,
        "Content-Type": "application/json"
    }
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        # Test 1: Get shop info
        console.print("\n[bold]1. Henter butikk-info...[/]")
        try:
            response = await client.get(
                f"{base_url}/shop.json",
                headers=headers
            )
            
            if response.status_code == 200:
                shop = response.json().get("shop", {})
                console.print(f"[green]✅ Tilkoblet![/]")
                console.print(f"   Butikk: [bold]{shop.get('name')}[/]")
                console.print(f"   E-post: {shop.get('email')}")
                console.print(f"   Valuta: {shop.get('currency')}")
                console.print(f"   Domene: {shop.get('domain')}")
                console.print(f"   Land: {shop.get('country_name')}")
            elif response.status_code == 401:
                console.print(f"[red]❌ Autentiseringsfeil (401)[/]")
                console.print("[yellow]Tips: Sjekk at access token er riktig.[/]")
                console.print("[dim]Access token skal starte med 'shpat_' for Admin API[/]")
                return False
            elif response.status_code == 404:
                console.print(f"[red]❌ Butikk ikke funnet (404)[/]")
                console.print(f"[yellow]Tips: Sjekk at shop URL er riktig: {SHOP_URL}[/]")
                return False
            else:
                console.print(f"[red]❌ Feil: {response.status_code}[/]")
                console.print(response.text[:500])
                return False
        except Exception as e:
            console.print(f"[red]❌ Tilkoblingsfeil: {e}[/]")
            return False
        
        # Test 2: Get products
        console.print("\n[bold]2. Henter produkter...[/]")
        try:
            response = await client.get(
                f"{base_url}/products.json?limit=5",
                headers=headers
            )
            
            if response.status_code == 200:
                products = response.json().get("products", [])
                console.print(f"[green]✅ Fant {len(products)} produkter (viser maks 5)[/]")
                
                if products:
                    table = Table(title="Shopify Produkter")
                    table.add_column("ID", style="dim")
                    table.add_column("Tittel")
                    table.add_column("Status")
                    table.add_column("Varianter")
                    
                    for p in products[:5]:
                        table.add_row(
                            str(p.get("id")),
                            p.get("title", "")[:40],
                            p.get("status", ""),
                            str(len(p.get("variants", [])))
                        )
                    console.print(table)
        except Exception as e:
            console.print(f"[yellow]⚠️ Kunne ikke hente produkter: {e}[/]")
        
        # Test 3: Get orders
        console.print("\n[bold]3. Henter ordrer...[/]")
        try:
            response = await client.get(
                f"{base_url}/orders.json?limit=5&status=any",
                headers=headers
            )
            
            if response.status_code == 200:
                orders = response.json().get("orders", [])
                console.print(f"[green]✅ Fant {len(orders)} ordrer[/]")
                
                if orders:
                    table = Table(title="Siste Ordrer")
                    table.add_column("Ordre #")
                    table.add_column("Kunde")
                    table.add_column("Total")
                    table.add_column("Status")
                    
                    for o in orders[:5]:
                        customer = o.get("customer", {}) or {}
                        name = f"{customer.get('first_name', '')} {customer.get('last_name', '')}"
                        table.add_row(
                            str(o.get("order_number", o.get("name", ""))),
                            name.strip() or "Gjest",
                            f"{o.get('total_price', '0')} {o.get('currency', '')}",
                            o.get("financial_status", "")
                        )
                    console.print(table)
        except Exception as e:
            console.print(f"[yellow]⚠️ Kunne ikke hente ordrer: {e}[/]")
        
        # Test 4: Check webhooks
        console.print("\n[bold]4. Sjekker webhooks...[/]")
        try:
            response = await client.get(
                f"{base_url}/webhooks.json",
                headers=headers
            )
            
            if response.status_code == 200:
                webhooks = response.json().get("webhooks", [])
                if webhooks:
                    console.print(f"[green]✅ {len(webhooks)} webhooks konfigurert[/]")
                    for wh in webhooks:
                        console.print(f"   • {wh.get('topic')} → {wh.get('address')}")
                else:
                    console.print("[dim]Ingen webhooks konfigurert ennå[/]")
        except Exception as e:
            console.print(f"[yellow]⚠️ Kunne ikke hente webhooks: {e}[/]")
    
    console.print("\n" + "="*60)
    console.print("[bold green]✅ Shopify-tilkobling OK![/]")
    return True


if __name__ == "__main__":
    import asyncio
    asyncio.run(test_shopify())
