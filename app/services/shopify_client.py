"""
Shopify API Client
===================
Async HTTP client for communicating with Shopify Admin API.

Handles:
- GraphQL and REST API calls
- Inventory management
- Order retrieval
- Webhook verification
- Rate limiting (Shopify has strict limits)

Uses Shopify Admin API version 2024-01.
"""

import asyncio
from typing import Optional, Dict, Any, List
from datetime import datetime
import structlog
import hashlib
import hmac
import base64

import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type
)

from app.core.config import settings
from app.core.security import decrypt_credential


logger = structlog.get_logger()

# Shopify API version
SHOPIFY_API_VERSION = "2024-01"


class ShopifyAPIError(Exception):
    """Base exception for Shopify API errors."""
    
    def __init__(
        self, 
        message: str, 
        status_code: Optional[int] = None, 
        response_body: Optional[dict] = None,
        errors: Optional[List[dict]] = None
    ):
        self.message = message
        self.status_code = status_code
        self.response_body = response_body
        self.errors = errors or []
        super().__init__(self.message)


class ShopifyAuthenticationError(ShopifyAPIError):
    """Raised when authentication fails."""
    pass


class ShopifyRateLimitError(ShopifyAPIError):
    """Raised when rate limited."""
    pass


class ShopifyNotFoundError(ShopifyAPIError):
    """Raised when resource is not found."""
    pass


class ShopifyClient:
    """
    Async client for Shopify Admin API.
    
    Supports both REST and GraphQL APIs. Uses GraphQL for inventory
    updates as it's more efficient for bulk operations.
    
    Example:
        client = ShopifyClient(
            shop_url="myshop.myshopify.com",
            access_token_encrypted="..."
        )
        
        async with client:
            inventory = await client.get_inventory_level(
                inventory_item_id="12345",
                location_id="67890"
            )
    """
    
    def __init__(
        self,
        shop_url: str,
        access_token_encrypted: str,
        api_key_encrypted: Optional[str] = None,
        api_secret_encrypted: Optional[str] = None,
        timeout: float = 30.0
    ):
        """
        Initialize Shopify client.
        
        Args:
            shop_url: Shopify shop URL (e.g., myshop.myshopify.com)
            access_token_encrypted: Encrypted access token
            api_key_encrypted: Encrypted API key (for webhook verification)
            api_secret_encrypted: Encrypted API secret (for webhook verification)
            timeout: Request timeout in seconds
        """
        # Clean up shop URL
        self.shop_url = shop_url.replace("https://", "").replace("http://", "").rstrip("/")
        self._access_token_encrypted = access_token_encrypted
        self._api_key_encrypted = api_key_encrypted
        self._api_secret_encrypted = api_secret_encrypted
        self.timeout = timeout
        
        self._client: Optional[httpx.AsyncClient] = None
        self._access_token: Optional[str] = None
        self._api_secret: Optional[str] = None
        
        # Rate limiter
        self._rate_limit = settings.shopify_rate_limit_per_second
        self._last_request_time: float = 0
        
        # API URLs
        self.rest_base_url = f"https://{self.shop_url}/admin/api/{SHOPIFY_API_VERSION}"
        self.graphql_url = f"https://{self.shop_url}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"
    
    async def __aenter__(self) -> "ShopifyClient":
        """Enter async context manager."""
        await self._ensure_client()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit async context manager."""
        await self.close()
    
    async def _ensure_client(self) -> None:
        """Ensure HTTP client is initialized."""
        if self._client is None:
            # Decrypt credentials
            self._access_token = decrypt_credential(self._access_token_encrypted)
            
            if self._api_secret_encrypted:
                self._api_secret = decrypt_credential(self._api_secret_encrypted)
            
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.timeout),
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "X-Shopify-Access-Token": self._access_token
                }
            )
    
    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
    
    async def _rate_limit_wait(self) -> None:
        """Wait if needed to respect rate limits."""
        if self._rate_limit > 0:
            min_interval = 1.0 / self._rate_limit
            elapsed = asyncio.get_event_loop().time() - self._last_request_time
            
            if elapsed < min_interval:
                await asyncio.sleep(min_interval - elapsed)
            
            self._last_request_time = asyncio.get_event_loop().time()
    
    async def _rest_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Make a REST API request to Shopify.
        
        Args:
            method: HTTP method
            endpoint: API endpoint (e.g., /products.json)
            params: Query parameters
            json_data: JSON body data
            
        Returns:
            Response JSON
        """
        await self._ensure_client()
        await self._rate_limit_wait()
        
        url = f"{self.rest_base_url}{endpoint}"
        
        try:
            response = await self._client.request(
                method=method,
                url=url,
                params=params,
                json=json_data
            )
            
            # Handle errors
            if response.status_code == 401:
                raise ShopifyAuthenticationError(
                    "Authentication failed",
                    status_code=401
                )
            
            if response.status_code == 429:
                # Get retry-after header if available
                retry_after = response.headers.get("Retry-After", "2")
                raise ShopifyRateLimitError(
                    f"Rate limited. Retry after {retry_after}s",
                    status_code=429
                )
            
            if response.status_code == 404:
                raise ShopifyNotFoundError(
                    "Resource not found",
                    status_code=404
                )
            
            if response.status_code >= 400:
                error_body = response.json() if response.content else {}
                raise ShopifyAPIError(
                    f"API error: {response.status_code}",
                    status_code=response.status_code,
                    response_body=error_body
                )
            
            if response.content:
                return response.json()
            return {}
            
        except httpx.TimeoutException as e:
            raise ShopifyAPIError(f"Request timeout: {e}")
        except httpx.RequestError as e:
            raise ShopifyAPIError(f"Request error: {e}")
    
    async def _graphql_request(
        self,
        query: str,
        variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Make a GraphQL API request to Shopify.
        
        Args:
            query: GraphQL query string
            variables: Query variables
            
        Returns:
            Response data
        """
        await self._ensure_client()
        await self._rate_limit_wait()
        
        payload = {"query": query}
        if variables:
            payload["variables"] = variables
        
        try:
            response = await self._client.post(
                self.graphql_url,
                json=payload
            )
            
            if response.status_code == 429:
                raise ShopifyRateLimitError(
                    "Rate limited",
                    status_code=429
                )
            
            if response.status_code >= 400:
                raise ShopifyAPIError(
                    f"GraphQL error: {response.status_code}",
                    status_code=response.status_code
                )
            
            result = response.json()
            
            # Check for GraphQL errors
            if "errors" in result:
                raise ShopifyAPIError(
                    "GraphQL query errors",
                    errors=result["errors"]
                )
            
            return result.get("data", {})
            
        except httpx.TimeoutException as e:
            raise ShopifyAPIError(f"Request timeout: {e}")
        except httpx.RequestError as e:
            raise ShopifyAPIError(f"Request error: {e}")
    
    # ===================
    # Webhook Verification
    # ===================
    
    def verify_webhook_signature(
        self,
        payload: bytes,
        signature: str
    ) -> bool:
        """
        Verify Shopify webhook HMAC signature.
        
        Args:
            payload: Raw request body bytes
            signature: X-Shopify-Hmac-SHA256 header value
            
        Returns:
            True if signature is valid
        """
        if not self._api_secret:
            # Try to decrypt if not already done
            if self._api_secret_encrypted:
                self._api_secret = decrypt_credential(self._api_secret_encrypted)
            else:
                raise ValueError("API secret required for webhook verification")
        
        computed_hmac = hmac.new(
            self._api_secret.encode("utf-8"),
            payload,
            hashlib.sha256
        ).digest()
        
        computed_signature = base64.b64encode(computed_hmac).decode("utf-8")
        
        return hmac.compare_digest(computed_signature, signature)
    
    # ===================
    # Shop Operations
    # ===================
    
    async def get_shop_info(self) -> Dict[str, Any]:
        """Get shop information."""
        result = await self._rest_request("GET", "/shop.json")
        return result.get("shop", {})
    
    # ===================
    # Product Operations
    # ===================
    
    async def get_product(self, product_id: str) -> Dict[str, Any]:
        """
        Get product by ID.
        
        Args:
            product_id: Shopify product ID
            
        Returns:
            Product data
        """
        result = await self._rest_request("GET", f"/products/{product_id}.json")
        return result.get("product", {})
    
    async def get_product_variants(self, product_id: str) -> List[Dict[str, Any]]:
        """
        Get all variants for a product.
        
        Args:
            product_id: Shopify product ID
            
        Returns:
            List of variants
        """
        result = await self._rest_request(
            "GET",
            f"/products/{product_id}/variants.json"
        )
        return result.get("variants", [])
    
    async def get_variant(self, variant_id: str) -> Dict[str, Any]:
        """
        Get variant by ID.
        
        Args:
            variant_id: Shopify variant ID
            
        Returns:
            Variant data
        """
        result = await self._rest_request("GET", f"/variants/{variant_id}.json")
        return result.get("variant", {})
    
    # ===================
    # Inventory Operations
    # ===================
    
    async def get_locations(self) -> List[Dict[str, Any]]:
        """Get all inventory locations."""
        result = await self._rest_request("GET", "/locations.json")
        return result.get("locations", [])
    
    async def get_inventory_levels(
        self,
        inventory_item_ids: List[str],
        location_ids: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Get inventory levels for items.
        
        Args:
            inventory_item_ids: List of inventory item IDs
            location_ids: Optional list of location IDs
            
        Returns:
            List of inventory levels
        """
        params = {
            "inventory_item_ids": ",".join(inventory_item_ids)
        }
        
        if location_ids:
            params["location_ids"] = ",".join(location_ids)
        
        result = await self._rest_request(
            "GET",
            "/inventory_levels.json",
            params=params
        )
        return result.get("inventory_levels", [])
    
    @retry(
        retry=retry_if_exception_type(ShopifyRateLimitError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    async def set_inventory_level(
        self,
        inventory_item_id: str,
        location_id: str,
        available: int
    ) -> Dict[str, Any]:
        """
        Set inventory level for an item at a location.
        
        Args:
            inventory_item_id: Shopify inventory item ID
            location_id: Shopify location ID
            available: New available quantity
            
        Returns:
            Updated inventory level
        """
        logger.info(
            "Setting Shopify inventory level",
            inventory_item_id=inventory_item_id,
            location_id=location_id,
            available=available
        )
        
        result = await self._rest_request(
            "POST",
            "/inventory_levels/set.json",
            json_data={
                "inventory_item_id": int(inventory_item_id),
                "location_id": int(location_id),
                "available": available
            }
        )
        return result.get("inventory_level", {})
    
    async def adjust_inventory_level(
        self,
        inventory_item_id: str,
        location_id: str,
        adjustment: int
    ) -> Dict[str, Any]:
        """
        Adjust inventory level by a delta.
        
        Args:
            inventory_item_id: Shopify inventory item ID
            location_id: Shopify location ID
            adjustment: Quantity to add (positive) or remove (negative)
            
        Returns:
            Updated inventory level
        """
        result = await self._rest_request(
            "POST",
            "/inventory_levels/adjust.json",
            json_data={
                "inventory_item_id": int(inventory_item_id),
                "location_id": int(location_id),
                "available_adjustment": adjustment
            }
        )
        return result.get("inventory_level", {})
    
    async def bulk_set_inventory_levels(
        self,
        updates: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Bulk update inventory levels using GraphQL.
        
        More efficient for updating many items at once.
        
        Args:
            updates: List of {inventory_item_id, location_id, available}
            
        Returns:
            List of results
        """
        # GraphQL mutation for bulk inventory update
        mutation = """
        mutation inventorySetOnHandQuantities($input: InventorySetOnHandQuantitiesInput!) {
            inventorySetOnHandQuantities(input: $input) {
                inventoryAdjustmentGroup {
                    id
                }
                userErrors {
                    field
                    message
                }
            }
        }
        """
        
        # Build quantities input
        quantities = []
        for update in updates:
            quantities.append({
                "inventoryItemId": f"gid://shopify/InventoryItem/{update['inventory_item_id']}",
                "locationId": f"gid://shopify/Location/{update['location_id']}",
                "quantity": update["available"]
            })
        
        variables = {
            "input": {
                "reason": "correction",
                "setQuantities": quantities
            }
        }
        
        result = await self._graphql_request(mutation, variables)
        
        # Check for user errors
        if result.get("inventorySetOnHandQuantities", {}).get("userErrors"):
            errors = result["inventorySetOnHandQuantities"]["userErrors"]
            raise ShopifyAPIError(
                "Bulk inventory update errors",
                errors=errors
            )
        
        return updates
    
    # ===================
    # Order Operations
    # ===================
    
    async def get_order(self, order_id: str) -> Dict[str, Any]:
        """
        Get order by ID.
        
        Args:
            order_id: Shopify order ID
            
        Returns:
            Order data
        """
        result = await self._rest_request("GET", f"/orders/{order_id}.json")
        return result.get("order", {})
    
    async def get_orders(
        self,
        status: str = "any",
        financial_status: str = "any",
        fulfillment_status: str = "any",
        created_at_min: Optional[datetime] = None,
        created_at_max: Optional[datetime] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get orders with filters.
        
        Args:
            status: Order status (open, closed, cancelled, any)
            financial_status: Payment status
            fulfillment_status: Fulfillment status
            created_at_min: Minimum creation date
            created_at_max: Maximum creation date
            limit: Max orders to return
            
        Returns:
            List of orders
        """
        params = {
            "status": status,
            "financial_status": financial_status,
            "fulfillment_status": fulfillment_status,
            "limit": limit
        }
        
        if created_at_min:
            params["created_at_min"] = created_at_min.isoformat()
        
        if created_at_max:
            params["created_at_max"] = created_at_max.isoformat()
        
        result = await self._rest_request(
            "GET",
            "/orders.json",
            params=params
        )
        return result.get("orders", [])
    
    # ===================
    # Webhook Operations
    # ===================
    
    async def list_webhooks(self) -> List[Dict[str, Any]]:
        """List all registered webhooks."""
        result = await self._rest_request("GET", "/webhooks.json")
        return result.get("webhooks", [])
    
    async def create_webhook(
        self,
        topic: str,
        address: str,
        format: str = "json"
    ) -> Dict[str, Any]:
        """
        Create a webhook subscription.
        
        Args:
            topic: Webhook topic (e.g., orders/create, inventory_levels/update)
            address: Callback URL
            format: Response format (json or xml)
            
        Returns:
            Created webhook
        """
        result = await self._rest_request(
            "POST",
            "/webhooks.json",
            json_data={
                "webhook": {
                    "topic": topic,
                    "address": address,
                    "format": format
                }
            }
        )
        return result.get("webhook", {})
    
    async def delete_webhook(self, webhook_id: str) -> None:
        """Delete a webhook."""
        await self._rest_request("DELETE", f"/webhooks/{webhook_id}.json")


def create_shopify_client(
    shop_url: str,
    access_token_encrypted: str,
    api_key_encrypted: Optional[str] = None,
    api_secret_encrypted: Optional[str] = None
) -> ShopifyClient:
    """
    Factory function to create a Shopify client.
    
    Args:
        shop_url: Shopify shop URL
        access_token_encrypted: Encrypted access token
        api_key_encrypted: Encrypted API key
        api_secret_encrypted: Encrypted API secret
        
    Returns:
        Configured ShopifyClient instance
    """
    return ShopifyClient(
        shop_url=shop_url,
        access_token_encrypted=access_token_encrypted,
        api_key_encrypted=api_key_encrypted,
        api_secret_encrypted=api_secret_encrypted
    )
