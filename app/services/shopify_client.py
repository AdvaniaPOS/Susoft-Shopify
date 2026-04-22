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


def _next_link_from_header(link_header: Optional[str]) -> Optional[str]:
    """Parse a Shopify ``Link`` response header and return the ``rel="next"`` URL.

    Shopify uses standard RFC 5988 link headers for cursor pagination, e.g.::

        Link: <https://shop.myshopify.com/admin/api/2024-01/products.json?page_info=abc&limit=250>; rel="next"

    Returns ``None`` if there is no next page.
    """
    if not link_header:
        return None
    for part in link_header.split(","):
        segments = part.strip().split(";")
        if len(segments) < 2:
            continue
        url_segment = segments[0].strip()
        rel_segment = ";".join(segments[1:]).strip()
        if 'rel="next"' in rel_segment and url_segment.startswith("<") and url_segment.endswith(">"):
            return url_segment[1:-1]
    return None


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

    async def get_all_products(
        self,
        page_size: int = 250,
        fields: Optional[str] = None,
        max_pages: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        Fetch every product in the shop using cursor pagination.

        Uses the Shopify REST ``Link`` header (since API version 2019-07).
        Each product object includes its variants (with ``sku`` and
        ``inventory_item_id``), which is what we need to build product mappings.

        Args:
            page_size: Items per page (Shopify allows up to 250).
            fields: Optional comma-separated list of fields to request
                (e.g. ``"id,title,variants"``) to reduce payload size.
            max_pages: Safety limit on total pages fetched.

        Returns:
            Flat list of all product dicts.
        """
        await self._ensure_client()

        params: Dict[str, Any] = {"limit": min(max(page_size, 1), 250)}
        if fields:
            params["fields"] = fields

        url: Optional[str] = f"{self.rest_base_url}/products.json"
        all_products: List[Dict[str, Any]] = []
        page_count = 0

        while url and page_count < max_pages:
            await self._rate_limit_wait()
            response = await self._client.get(url, params=params if page_count == 0 else None)

            if response.status_code == 401:
                raise ShopifyAuthenticationError("Authentication failed", status_code=401)
            if response.status_code == 429:
                raise ShopifyRateLimitError("Rate limited", status_code=429)
            if response.status_code >= 400:
                raise ShopifyAPIError(
                    f"API error: {response.status_code}",
                    status_code=response.status_code,
                    response_body=response.json() if response.content else None,
                )

            body = response.json() if response.content else {}
            all_products.extend(body.get("products", []))
            page_count += 1

            url = _next_link_from_header(response.headers.get("Link") or response.headers.get("link"))

        return all_products
    
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

    async def update_product(
        self,
        product_id: str,
        fields: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Update a Shopify product (REST). Only the provided fields are sent.

        Args:
            product_id: Shopify product ID
            fields: Dict of product fields to update, e.g.
                ``{"title": "...", "product_type": "..."}``.

        Returns:
            Updated product data.
        """
        if not fields:
            return {}
        payload = {"product": {"id": int(product_id), **fields}}
        result = await self._rest_request(
            "PUT",
            f"/products/{product_id}.json",
            json_data=payload,
        )
        return result.get("product", {})

    async def create_product(
        self,
        product: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Create a Shopify product (REST). Returns the created product
        including default variant id, inventory_item_id, etc.

        Args:
            product: Dict of product fields. Common keys:
                ``title``, ``product_type``, ``vendor``, ``status``
                ("active"/"draft"), ``tags``, ``variants`` (list of
                variant dicts with ``sku``, ``barcode``, ``price``,
                ``taxable``, ``inventory_management``).

        Returns:
            Created product data with assigned ids.
        """
        payload = {"product": product}
        result = await self._rest_request(
            "POST",
            "/products.json",
            json_data=payload,
        )
        return result.get("product", {})

    async def set_inventory_level(
        self,
        inventory_item_id: str,
        location_id: str,
        available: int,
    ) -> Dict[str, Any]:
        """
        Set the on-hand quantity for an inventory item at a location.
        Used after creating a new product so initial stock is visible.
        """
        payload = {
            "inventory_item_id": int(inventory_item_id),
            "location_id": int(location_id),
            "available": int(available),
        }
        result = await self._rest_request(
            "POST",
            "/inventory_levels/set.json",
            json_data=payload,
        )
        return result.get("inventory_level", {})

    async def update_variant(
        self,
        variant_id: str,
        fields: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Update a Shopify variant (REST). Only the provided fields are sent.

        Args:
            variant_id: Shopify variant ID
            fields: Dict of variant fields, e.g.
                ``{"price": "199.00", "taxable": True, "metafields": [...]}``.

        Returns:
            Updated variant data.
        """
        if not fields:
            return {}
        payload = {"variant": {"id": int(variant_id), **fields}}
        result = await self._rest_request(
            "PUT",
            f"/variants/{variant_id}.json",
            json_data=payload,
        )
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
            # Log the actual error details so we can see what Shopify rejected
            logger.error(
                "Shopify bulk inventory update returned userErrors",
                user_errors=errors,
                update_count=len(updates),
                sample_updates=updates[:3],
            )
            raise ShopifyAPIError(
                f"Bulk inventory update errors: {errors}",
                errors=errors
            )
        
        return updates

    async def get_committed_quantities(
        self,
        inventory_item_ids: List[str],
        location_id: str,
    ) -> Dict[str, int]:
        """
        Fetch the 'committed' quantity per inventory item at a location.

        Committed = items reserved by unfulfilled orders. We need this to
        avoid double-counting when syncing on_hand from Susoft: Susoft
        decrements stock immediately on order, while Shopify only commits
        (not decrements) until fulfillment. So Shopify on_hand must be
        set to (susoft_qty + shopify_committed) to keep available correct.

        Returns: dict {inventory_item_id_str: committed_int}
        """
        if not inventory_item_ids:
            return {}

        result: Dict[str, int] = {}
        loc_gid = f"gid://shopify/Location/{location_id}"

        # Batch in chunks of 50 to stay within GraphQL query cost limits
        for i in range(0, len(inventory_item_ids), 50):
            chunk = inventory_item_ids[i:i + 50]
            ids_gids = [f"gid://shopify/InventoryItem/{iid}" for iid in chunk]

            query = """
            query getCommitted($ids: [ID!]!, $locationId: ID!) {
              nodes(ids: $ids) {
                ... on InventoryItem {
                  id
                  inventoryLevel(locationId: $locationId) {
                    quantities(names: ["committed"]) {
                      name
                      quantity
                    }
                  }
                }
              }
            }
            """

            try:
                data = await self._graphql_request(
                    query, {"ids": ids_gids, "locationId": loc_gid}
                )
            except Exception as exc:
                logger.warning(
                    "Failed to fetch committed quantities; assuming 0",
                    error=str(exc),
                    chunk_size=len(chunk),
                )
                for iid in chunk:
                    result[str(iid)] = 0
                continue

            for node in (data.get("nodes") or []):
                if not node:
                    continue
                gid = node.get("id", "")
                # gid format: gid://shopify/InventoryItem/12345
                iid = gid.rsplit("/", 1)[-1] if gid else ""
                committed = 0
                level = node.get("inventoryLevel") or {}
                for q in (level.get("quantities") or []):
                    if q.get("name") == "committed":
                        committed = int(q.get("quantity") or 0)
                        break
                if iid:
                    result[iid] = committed

            # Fill in zeros for any items that didn't return a node
            for iid in chunk:
                result.setdefault(str(iid), 0)

        return result
    
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

    async def update_order(
        self,
        order_id: str,
        fields: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Update arbitrary fields on a Shopify order (PUT /orders/{id}.json)."""
        body = {"order": {"id": int(order_id), **fields}}
        result = await self._rest_request(
            "PUT", f"/orders/{order_id}.json", json_data=body
        )
        return result.get("order", {})

    async def add_order_tags(
        self,
        order_id: str,
        new_tags: List[str],
    ) -> Dict[str, Any]:
        """Append tags to an order without removing existing ones.

        Shopify stores ``tags`` as a comma-separated string. We fetch current
        tags, merge in ``new_tags`` (case-insensitive de-dupe) and PUT back.
        """
        existing = await self.get_order(order_id)
        existing_tags_raw = (existing.get("tags") or "").strip()
        existing_tags = [t.strip() for t in existing_tags_raw.split(",") if t.strip()]
        seen = {t.lower() for t in existing_tags}
        for t in new_tags:
            if t and t.lower() not in seen:
                existing_tags.append(t)
                seen.add(t.lower())
        return await self.update_order(order_id, {"tags": ", ".join(existing_tags)})

    async def append_order_note(
        self,
        order_id: str,
        note_line: str,
    ) -> Dict[str, Any]:
        """Append a line to the order's note (preserving the existing note)."""
        existing = await self.get_order(order_id)
        current = (existing.get("note") or "").rstrip()
        merged = f"{current}\n{note_line}".strip() if current else note_line
        return await self.update_order(order_id, {"note": merged})

    async def close_order(self, order_id: str) -> Dict[str, Any]:
        """Close an order so it is removed from the open-orders queue.

        See https://shopify.dev/docs/api/admin-rest/2024-01/resources/order#post-orders-order-id-close
        """
        result = await self._rest_request(
            "POST", f"/orders/{order_id}/close.json"
        )
        return result.get("order", {})

    async def reopen_order(self, order_id: str) -> Dict[str, Any]:
        """Re-open a previously closed order."""
        result = await self._rest_request(
            "POST", f"/orders/{order_id}/open.json"
        )
        return result.get("order", {})

    # ===================
    # Fulfillment Operations
    # ===================

    async def list_fulfillment_orders(self, order_id: str) -> List[Dict[str, Any]]:
        """List fulfillment orders for a Shopify order.

        See https://shopify.dev/docs/api/admin-rest/2024-01/resources/fulfillmentorder
        """
        result = await self._rest_request(
            "GET", f"/orders/{order_id}/fulfillment_orders.json"
        )
        return result.get("fulfillment_orders", [])

    async def create_fulfillment(
        self,
        fulfillment_order_id: str,
        tracking_number: Optional[str] = None,
        tracking_company: Optional[str] = None,
        tracking_url: Optional[str] = None,
        notify_customer: bool = True,
    ) -> Dict[str, Any]:
        """Create a fulfillment for a single fulfillment order.

        Uses the 2024-01 fulfillment API which is required for new apps.
        See https://shopify.dev/docs/api/admin-rest/2024-01/resources/fulfillment#post-fulfillments
        """
        line_items_by_fo: Dict[str, Any] = {
            "fulfillment_order_id": int(fulfillment_order_id),
        }
        body: Dict[str, Any] = {
            "fulfillment": {
                "line_items_by_fulfillment_order": [line_items_by_fo],
                "notify_customer": notify_customer,
            }
        }
        tracking_info: Dict[str, Any] = {}
        if tracking_number:
            tracking_info["number"] = tracking_number
        if tracking_company:
            tracking_info["company"] = tracking_company
        if tracking_url:
            tracking_info["url"] = tracking_url
        if tracking_info:
            body["fulfillment"]["tracking_info"] = tracking_info

        result = await self._rest_request(
            "POST", "/fulfillments.json", json_data=body
        )
        return result.get("fulfillment", {})

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
