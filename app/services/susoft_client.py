"""
Susoft API Client
==================
Async HTTP client for communicating with Susoft ERP/POS API.

Handles:
- Authentication via JWT token
- Product and stock operations
- Order creation with idempotency
- Webhook management
- Rate limiting

Reference: Susoft REST API v3.1
"""

import asyncio
import json
from typing import Optional, Dict, Any, List
from datetime import datetime
from uuid import UUID
import structlog

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


class SusoftAPIError(Exception):
    """Base exception for Susoft API errors."""
    
    def __init__(self, message: str, status_code: Optional[int] = None, response_body: Optional[dict] = None):
        self.message = message
        self.status_code = status_code
        self.response_body = response_body
        super().__init__(self.message)


class SusoftAuthenticationError(SusoftAPIError):
    """Raised when authentication fails."""
    pass


class SusoftRateLimitError(SusoftAPIError):
    """Raised when rate limited."""

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        response_body: Optional[dict] = None,
        retry_after: Optional[float] = None,
    ):
        super().__init__(message, status_code=status_code, response_body=response_body)
        self.retry_after = retry_after


class SusoftValidationError(SusoftAPIError):
    """Raised when request validation fails."""
    pass


class SusoftClient:
    """
    Async client for Susoft API.
    
    Implements the key operations needed for Shopify integration:
    - Stock retrieval and updates
    - Order creation with idempotency (uuid/alternativeId)
    - Product lookup
    - Webhook registration
    
    Example:
        client = SusoftClient(
            base_url="https://api.susoft.com:4443",
            shop_url_key="myshop",
            api_key_encrypted="..."
        )
        
        async with client:
            stock = await client.get_product_stock("PROD-001")
    """
    
    def __init__(
        self,
        base_url: str,
        shop_url_key: str,
        api_key_encrypted: str,
        timeout: float = 30.0
    ):
        """
        Initialize Susoft client.
        
        Args:
            base_url: Susoft API base URL (e.g., https://api.susoft.com:4443)
            shop_url_key: Shop URL key for X-Shop-Url-Key header
            api_key_encrypted: Encrypted API key/JWT token
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip("/")
        self.shop_url_key = shop_url_key
        self._api_key_encrypted = api_key_encrypted
        self.timeout = timeout
        
        self._client: Optional[httpx.AsyncClient] = None
        self._token: Optional[str] = None
        
        # Rate limiter (simple token bucket)
        self._rate_limit = settings.susoft_rate_limit_per_second
        self._last_request_time: float = 0
    
    async def __aenter__(self) -> "SusoftClient":
        """Enter async context manager."""
        await self._ensure_client()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit async context manager."""
        await self.close()
    
    async def _ensure_client(self) -> None:
        """Ensure HTTP client is initialized."""
        if self._client is None:
            # Decrypt API key. The stored secret may either be a literal JWT
            # token (legacy) or a JSON blob {"login": "...", "password": "..."}
            # in which case we sign in via /user/auth to obtain a JWT.
            raw_secret = decrypt_credential(self._api_key_encrypted)

            self._login: Optional[str] = None
            self._password: Optional[str] = None
            self._token = None

            try:
                parsed = json.loads(raw_secret)
            except (ValueError, TypeError):
                parsed = None

            if isinstance(parsed, dict) and parsed.get("login") and parsed.get("password"):
                self._login = parsed["login"]
                self._password = parsed["password"]
                logger.info(
                    "Susoft client will use login/password auth",
                    login=self._login,
                    shop_url_key=self.shop_url_key,
                )
            else:
                # Treat as literal token (backward compatible).
                self._token = raw_secret
                logger.warning(
                    "Susoft client using literal-token auth (no login/password found). "
                    "If you see 401, re-seed the tenant so the secret holds JSON {login,password}.",
                    shop_url_key=self.shop_url_key,
                    secret_preview=(raw_secret[:6] + "...") if raw_secret else "<empty>",
                )

            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=httpx.Timeout(self.timeout),
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json"
                }
            )

            if self._login and self._password:
                await self._authenticate()

    async def _authenticate(self) -> None:
        """Sign in to Susoft via POST /user/auth and cache the JWT."""
        if not (self._login and self._password):
            raise SusoftAuthenticationError(
                "Cannot authenticate: no login/password configured"
            )
        assert self._client is not None

        logger.info(
            "Authenticating with Susoft",
            login=self._login,
            shop_url_key=self.shop_url_key,
        )

        response = await self._client.post(
            "/user/auth",
            json={"login": self._login, "password": self._password},
            headers={
                "X-Shop-Url-Key": self.shop_url_key,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )

        if response.status_code != 200:
            body = response.text
            raise SusoftAuthenticationError(
                f"Susoft login failed: HTTP {response.status_code} {body}",
                status_code=response.status_code,
            )

        data = response.json() or {}
        token = data.get("token")
        if not token or not data.get("success", True):
            raise SusoftAuthenticationError(
                f"Susoft login returned no token: {data}",
                status_code=response.status_code,
                response_body=data,
            )
        self._token = token
        logger.info(
            "Susoft authentication succeeded",
            login=self._login,
            shop_url_key=self.shop_url_key,
            token_preview=(token[:12] + "...") if token else None,
        )
    
    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
    
    def _get_headers(self) -> Dict[str, str]:
        """Get request headers with authentication."""
        return {
            "Authorization": f"Bearer {self._token}",
            "X-Shop-Url-Key": self.shop_url_key
        }
    
    async def _rate_limit_wait(self) -> None:
        """Wait if needed to respect rate limits."""
        if self._rate_limit > 0:
            min_interval = 1.0 / self._rate_limit
            elapsed = asyncio.get_event_loop().time() - self._last_request_time
            
            if elapsed < min_interval:
                await asyncio.sleep(min_interval - elapsed)
            
            self._last_request_time = asyncio.get_event_loop().time()
    
    async def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Make an HTTP request to Susoft API.
        
        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            path: API endpoint path
            params: Query parameters
            json_data: JSON body data
            
        Returns:
            Response JSON as dictionary
            
        Raises:
            SusoftAPIError: On API errors
        """
        await self._ensure_client()
        await self._rate_limit_wait()
        
        url = path if path.startswith("/") else f"/{path}"
        
        try:
            response = await self._client.request(
                method=method,
                url=url,
                params=params,
                json=json_data,
                headers=self._get_headers()
            )

            # Handle different status codes
            if response.status_code == 401:
                # If we have login/password, try a single re-auth and retry once.
                if self._login and self._password:
                    logger.info("Susoft returned 401, re-authenticating and retrying once")
                    await self._authenticate()
                    response = await self._client.request(
                        method=method,
                        url=url,
                        params=params,
                        json=json_data,
                        headers=self._get_headers()
                    )
                if response.status_code == 401:
                    raise SusoftAuthenticationError(
                        "Authentication failed",
                        status_code=401
                    )
            
            if response.status_code == 429:
                retry_after_raw = response.headers.get("Retry-After")
                retry_after: Optional[float] = None
                if retry_after_raw:
                    try:
                        retry_after = float(retry_after_raw)
                    except (TypeError, ValueError):
                        retry_after = None
                raise SusoftRateLimitError(
                    "Rate limit exceeded",
                    status_code=429,
                    retry_after=retry_after,
                )
            
            if response.status_code == 400:
                error_body = response.json() if response.content else {}
                raise SusoftValidationError(
                    f"Validation error: {error_body}",
                    status_code=400,
                    response_body=error_body
                )
            
            if response.status_code >= 400:
                try:
                    error_body = response.json() if response.content else {}
                except Exception:
                    error_body = response.text if response.content else ""
                raise SusoftAPIError(
                    f"API error: {response.status_code} body={error_body}",
                    status_code=response.status_code,
                    response_body=error_body
                )
            
            # Return JSON response or empty dict
            if response.content:
                return response.json()
            return {}
            
        except httpx.TimeoutException as e:
            raise SusoftAPIError(f"Request timeout: {e}")
        except httpx.RequestError as e:
            raise SusoftAPIError(f"Request error: {e}")
    
    # ===================
    # Health & Info
    # ===================
    
    async def health_check(self) -> bool:
        """
        Check if Susoft API is reachable.
        
        Returns:
            True if healthy, False otherwise
        """
        try:
            result = await self._request("GET", "/health")
            return result is True or result == {}
        except Exception:
            return False
    
    async def get_shop_info(self) -> Dict[str, Any]:
        """Get shop information."""
        return await self._request("GET", "/shop/info")
    
    # ===================
    # Product Operations
    # ===================
    
    async def get_product_by_id(self, product_id: str) -> Dict[str, Any]:
        """
        Get product by Susoft product ID.
        
        Args:
            product_id: Susoft product ID
            
        Returns:
            Product data dictionary
        """
        return await self._request(
            "GET",
            "/product/id",
            params={"productId": product_id}
        )
    
    async def get_product_by_barcode(self, barcode: str) -> Dict[str, Any]:
        """
        Get product by barcode.
        
        Args:
            barcode: Product barcode
            
        Returns:
            Product data dictionary
        """
        return await self._request(
            "GET",
            "/product/barcode",
            params={"barcode": barcode}
        )
    
    async def get_product_by_sku(self, sku: str) -> Optional[Dict[str, Any]]:
        """
        Search for product by SKU.
        
        Uses the search API with SKU as externalRefId.
        
        Args:
            sku: Product SKU
            
        Returns:
            Product data or None if not found
        """
        # Try by alternative ID first (which we use for SKU)
        try:
            return await self._request(
                "GET",
                "/product/alternativeid",
                params={"alternativeId": sku}
            )
        except SusoftAPIError as e:
            if e.status_code == 404:
                return None
            raise
    
    async def get_products_modified_since(
        self,
        since: datetime,
        page: int = 0,
        page_size: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get products modified after a given datetime.
        
        Args:
            since: Datetime to filter from
            page: Page number (0-indexed)
            page_size: Items per page
            
        Returns:
            List of product dictionaries
        """
        return await self._request(
            "GET",
            "/product/list/modified",
            params={
                "dateTime": since.strftime("%Y-%m-%dT%H:%M:%S.000"),
                "page": page,
                "pageSize": page_size
            }
        )
    
    async def get_all_products(
        self,
        page_size: int = 200,
        max_pages: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        Fetch every product from Susoft by paging through ``/product/list/modified``
        starting from the Unix epoch.

        Args:
            page_size: Items per page.
            max_pages: Safety cap on total pages.

        Returns:
            Flat list of product dicts (each contains ``id``, ``barcode``,
            optional embedded ``stock``, etc.).
        """
        epoch = datetime(1970, 1, 1)
        all_products: List[Dict[str, Any]] = []
        page = 0
        consecutive_failures = 0
        max_consecutive_failures = 5
        # Per-page retry budget so a transient 429/500 doesn't truncate
        # the product list (we used to skip the page outright, which left
        # mappings unmatched and triggered repeated full re-syncs).
        max_page_retries = 5
        while page < max_pages:
            page_attempt = 0
            page_skipped = False  # True if we gave up on this page and advanced
            while True:
                try:
                    batch = await self.get_products_modified_since(
                        since=epoch,
                        page=page,
                        page_size=page_size,
                    )
                    consecutive_failures = 0
                    break  # success, exit retry loop
                except SusoftRateLimitError as e:
                    page_attempt += 1
                    # Honour Retry-After if Susoft sent one, otherwise
                    # exponential backoff capped at 30s.
                    delay = e.retry_after if e.retry_after else min(2 ** page_attempt, 30)
                    logger.warning(
                        "Susoft 429 on product list page; backing off",
                        page=page,
                        attempt=page_attempt,
                        delay=delay,
                    )
                    if page_attempt >= max_page_retries:
                        consecutive_failures += 1
                        if consecutive_failures >= max_consecutive_failures:
                            logger.warning(
                                "Too many consecutive failures; stopping pagination",
                                page=page,
                                collected=len(all_products),
                            )
                            return all_products
                        # Give up on this page after exhausting retries,
                        # advance to next page to avoid infinite loop.
                        page += 1
                        batch = []
                        page_skipped = True
                        break
                    await asyncio.sleep(delay)
                    continue
                except SusoftAPIError as e:
                    if e.status_code in (500, 502, 503, 504):
                        # 500s on /product/list/modified are deterministic
                        # "poison pages" caused by malformed data on Susoft's
                        # side, not transient. Retrying just wastes the rate
                        # limit budget — skip immediately.
                        consecutive_failures += 1
                        logger.warning(
                            "Susoft product list page returned server error; skipping",
                            page=page,
                            status=e.status_code,
                            consecutive_failures=consecutive_failures,
                            collected=len(all_products),
                        )
                        if consecutive_failures >= max_consecutive_failures:
                            logger.warning(
                                "Too many consecutive failures; stopping pagination",
                                page=page,
                                collected=len(all_products),
                            )
                            return all_products
                        page += 1
                        batch = []
                        page_skipped = True
                        break
                    raise
            if page_skipped:
                # Page advanced past a poison page; keep looping.
                continue
            if not batch:
                # Real end-of-list.
                break
            all_products.extend(batch)
            if len(batch) < page_size:
                break
            page += 1
        return all_products

    # ===================
    # Stock Operations
    # ===================
    
    @retry(
        retry=retry_if_exception_type(SusoftRateLimitError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    async def update_stock(
        self,
        product_stock: Dict[str, float]
    ) -> bool:
        """
        Update stock levels for products.
        
        Args:
            product_stock: Dictionary mapping product_id to stock quantity
                          Example: {"PROD-001": 10.0, "PROD-002": 5.0}
            
        Returns:
            True if successful
        """
        logger.info(
            "Updating stock in Susoft",
            product_count=len(product_stock)
        )
        
        result = await self._request(
            "PUT",
            "/product/stock",
            json_data=product_stock
        )
        
        return result is True or result == {}
    
    # ===================
    # Order Operations
    # ===================
    
    @retry(
        retry=retry_if_exception_type(SusoftRateLimitError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    async def create_order(
        self,
        order_data: Dict[str, Any],
        shopify_order_id: str,
        recalculate: bool = True,
        use_pos_endpoint: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        Create a new order in Susoft.

        Uses alternativeId/uuid for idempotency - Susoft will reject duplicate
        orders with the same IDs.

        For paid orders we POST to ``/order/pos`` which behaves like an aPOS
        sale - Susoft deducts stock immediately. ``/order`` creates an order
        that waits for invoicing and does NOT deduct stock until invoiced.

        Args:
            order_data: Order data following Susoft Order schema.
            shopify_order_id: Shopify order ID (used for idempotency).
            recalculate: Whether to recalculate prices/stock movements
                server-side. Default True so Susoft fills missing fields.
            use_pos_endpoint: If True -> /order/pos. If False -> /order.
                If None, auto-select based on presence of payments in
                ``order_data`` (paid order -> POS endpoint).

        Returns:
            Created order data
        """
        # Set idempotency fields
        order_data["alternativeId"] = f"SHOPIFY-{shopify_order_id}"
        # /order/pos rejects user-provided uuid; only set on /order
        has_payments = bool(order_data.get("payments"))

        if use_pos_endpoint is None:
            use_pos_endpoint = has_payments

        if use_pos_endpoint:
            endpoint = "/order/pos"
            params: Dict[str, Any] = {}
            # /order/pos doesn't accept uuid in body
            order_data.pop("uuid", None)
        else:
            endpoint = "/order"
            order_data["uuid"] = f"SHOPIFY-{shopify_order_id}"
            params = {"recalculate": str(recalculate).lower()}

        logger.info(
            "Creating order in Susoft",
            shopify_order_id=shopify_order_id,
            alternative_id=order_data["alternativeId"],
            endpoint=endpoint,
            has_payments=has_payments,
        )
        try:
            import json as _json
            logger.warning(
                "Susoft order payload (debug)",
                shopify_order_id=shopify_order_id,
                payload=_json.dumps(order_data, default=str)[:4000],
            )
        except Exception:
            pass

        try:
            return await self._request(
                "POST",
                endpoint,
                params=params,
                json_data=order_data,
            )
        except SusoftAPIError as exc:
            err_str = str(exc)
            # 409 = order already exists (idempotency). The previous attempt
            # actually succeeded server-side; treat as success and look it up.
            if "409" in err_str or "already exists" in err_str.lower():
                logger.info(
                    "Susoft order already exists; treating as success",
                    shopify_order_id=shopify_order_id,
                    alternative_id=order_data["alternativeId"],
                )
                # 409002 from Susoft is itself proof the order exists. Try
                # to fetch it for the response payload, but NEVER let a
                # lookup failure (500/timeout) re-raise and turn this into
                # a celery retry — that would just loop forever.
                try:
                    existing = await self.get_order_by_alternative_id(
                        order_data["alternativeId"]
                    )
                    if existing:
                        return existing
                except Exception as lookup_exc:  # noqa: BLE001
                    logger.warning(
                        "Susoft lookup after 409 failed; returning stub success",
                        shopify_order_id=shopify_order_id,
                        alternative_id=order_data["alternativeId"],
                        lookup_error=str(lookup_exc),
                    )
                return {
                    "alternativeId": order_data["alternativeId"],
                    "status": "exists",
                    "duplicate": True,
                }

            # 500 may indicate a stale alternativeId lock from a previous
            # crashed attempt. First check if the order actually exists; if
            # so, treat as success. Otherwise retry once with a -R<timestamp>
            # suffix on alternativeId/uuid (mirrors Susoft's own internal
            # retry pattern visible in created orders).
            if "500" in err_str:
                try:
                    existing = await self.get_order_by_alternative_id(
                        order_data["alternativeId"]
                    )
                    if existing:
                        logger.info(
                            "Susoft 500 but order exists; treating as success",
                            shopify_order_id=shopify_order_id,
                            alternative_id=order_data["alternativeId"],
                        )
                        return existing
                except Exception:
                    pass

                import time as _time
                suffix = f"-R{int(_time.time())}"
                new_alt = f"{order_data['alternativeId']}{suffix}"
                logger.warning(
                    "Susoft 500 on create; retrying with suffixed alternativeId",
                    shopify_order_id=shopify_order_id,
                    original_alt=order_data["alternativeId"],
                    new_alt=new_alt,
                )
                order_data["alternativeId"] = new_alt
                if "uuid" in order_data:
                    order_data["uuid"] = new_alt
                try:
                    return await self._request(
                        "POST",
                        endpoint,
                        params=params,
                        json_data=order_data,
                    )
                except SusoftAPIError as retry_exc:
                    retry_err = str(retry_exc)
                    if "409" in retry_err or "already exists" in retry_err.lower():
                        try:
                            existing = await self.get_order_by_alternative_id(new_alt)
                            if existing:
                                return existing
                        except Exception:
                            pass
                        return {
                            "alternativeId": new_alt,
                            "status": "exists",
                            "duplicate": True,
                        }
                    raise

            # /order/pos may return 500/400 for fields it doesn't like, or
            # 404 from an internal post-step even though the order WAS created.
            # Check if the order exists before falling back to avoid duplicate
            # creation attempts.
            if use_pos_endpoint and endpoint == "/order/pos":
                try:
                    existing = await self.get_order_by_alternative_id(
                        order_data["alternativeId"]
                    )
                    if existing:
                        logger.info(
                            "/order/pos returned error but order was created",
                            shopify_order_id=shopify_order_id,
                            alternative_id=order_data["alternativeId"],
                            error=err_str,
                        )
                        return existing
                except Exception:
                    pass

                logger.warning(
                    "/order/pos failed, falling back to /order",
                    shopify_order_id=shopify_order_id,
                    error=err_str,
                )
                order_data["uuid"] = f"SHOPIFY-{shopify_order_id}"
                try:
                    return await self._request(
                        "POST",
                        "/order",
                        params={"recalculate": str(recalculate).lower()},
                        json_data=order_data,
                    )
                except SusoftAPIError as fallback_exc:
                    fb_err = str(fallback_exc)
                    if "409" in fb_err or "already exists" in fb_err.lower():
                        logger.info(
                            "Fallback /order returned 409; treating as success",
                            shopify_order_id=shopify_order_id,
                            alternative_id=order_data["alternativeId"],
                        )
                        try:
                            existing = await self.get_order_by_alternative_id(
                                order_data["alternativeId"]
                            )
                            if existing:
                                return existing
                        except Exception:
                            pass
                        return {
                            "alternativeId": order_data["alternativeId"],
                            "status": "exists",
                            "duplicate": True,
                        }
                    raise
            raise
    
    async def get_order_by_alternative_id(self, alt_id: str) -> Optional[Dict[str, Any]]:
        """
        Get order by alternative ID.
        
        Args:
            alt_id: Alternative ID (set during creation)
            
        Returns:
            Order data or None if not found
        """
        try:
            return await self._request(
                "GET",
                "/order/altid",
                params={"altId": alt_id}
            )
        except SusoftAPIError as e:
            if e.status_code == 404:
                return None
            raise
    
    async def get_order_by_uuid(self, uuid: str) -> Optional[Dict[str, Any]]:
        """
        Get order by UUID.
        
        Args:
            uuid: Order UUID
            
        Returns:
            Order data or None if not found
        """
        try:
            return await self._request(
                "GET",
                "/order/uuid",
                params={"uuid": uuid}
            )
        except SusoftAPIError as e:
            if e.status_code == 404:
                return None
            raise
    
    async def get_orders_in_range(
        self,
        from_date: datetime,
        to_date: datetime,
        mode: str = "FULL"
    ) -> List[Dict[str, Any]]:
        """
        Get orders within a date range.
        
        Args:
            from_date: Start date
            to_date: End date
            mode: Data mode (BASIC, FINANCIAL, FULL, PERSONAL)
            
        Returns:
            List of orders
        """
        return await self._request(
            "GET",
            "/order/list",
            params={
                "fromDate": from_date.strftime("%Y-%m-%dT%H:%M:%S.000"),
                "toDate": to_date.strftime("%Y-%m-%dT%H:%M:%S.000"),
                "mode": mode
            }
        )
    
    # ===================
    # Customer Operations
    # ===================
    
    async def get_customer_by_id(self, customer_id: str) -> Optional[Dict[str, Any]]:
        """Get customer by ID."""
        try:
            return await self._request(
                "GET",
                "/customer/id",
                params={"id": customer_id}
            )
        except SusoftAPIError as e:
            if e.status_code == 404:
                return None
            raise
    
    async def create_customer(self, customer_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new customer."""
        return await self._request(
            "POST",
            "/customer",
            json_data=customer_data
        )
    
    async def search_customers(
        self,
        field: str,
        value: str,
        operator: str = "eq"
    ) -> List[Dict[str, Any]]:
        """
        Search for customers.
        
        Args:
            field: Field to search (id, firstname, lastname, email, mobile)
            value: Value to search for
            operator: Search operator
            
        Returns:
            List of matching customers
        """
        search_criteria = {
            "filterGroups": [
                {
                    "filters": [
                        {
                            "field": field,
                            "operator": operator,
                            "value": value
                        }
                    ]
                }
            ]
        }
        
        return await self._request(
            "POST",
            "/customer/search",
            json_data=search_criteria
        )
    
    # ===================
    # Webhook Operations
    # ===================
    
    async def list_webhooks(self, webhook_type: str) -> List[Dict[str, Any]]:
        """
        List configured webhooks of a type.
        
        Args:
            webhook_type: Type of webhook (e.g., ON_ORDER_CREATED, ON_PRODUCT_STOCK_CHANGED)
            
        Returns:
            List of webhook configurations
        """
        return await self._request(
            "GET",
            "/webhook/settings",
            params={"webhookType": webhook_type}
        )
    
    async def add_webhook(
        self,
        webhook_type: str,
        url: str,
        token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Add a webhook subscription.
        
        Args:
            webhook_type: Type of webhook
            url: Callback URL
            token: Optional security token
            
        Returns:
            Created webhook configuration
        """
        webhook_data = {
            "type": webhook_type,
            "url": url,
            "active": True
        }
        
        if token:
            webhook_data["token"] = token
        
        return await self._request(
            "POST",
            "/webhook/settings",
            json_data=webhook_data
        )
    
    async def remove_webhook(self, webhook_id: int) -> int:
        """
        Remove a webhook subscription.
        
        Args:
            webhook_id: ID of webhook to remove
            
        Returns:
            Number of removed rows
        """
        return await self._request(
            "DELETE",
            "/webhook/settings",
            params={"webhookId": webhook_id}
        )


def create_susoft_client(
    base_url: str,
    api_key_encrypted: str,
    integration_id: Optional[str] = None,
    shop_url_key: Optional[str] = None,
) -> SusoftClient:
    """
    Factory function to create a Susoft client.

    Susoft requires the ``X-Shop-Url-Key`` header for all requests; in this
    project that value is stored as ``Tenant.susoft_integration_id``. Both
    ``integration_id`` and ``shop_url_key`` are accepted (alias) so callers
    can use whichever name fits their context.

    Args:
        base_url: Susoft API base URL
        api_key_encrypted: Encrypted API key
        integration_id: Susoft integration / shop URL key (preferred name)
        shop_url_key: Alias for ``integration_id``

    Returns:
        Configured SusoftClient instance
    """
    key = integration_id or shop_url_key
    if not key:
        raise ValueError(
            "create_susoft_client requires integration_id (or shop_url_key)"
        )
    return SusoftClient(
        base_url=base_url,
        shop_url_key=key,
        api_key_encrypted=api_key_encrypted,
    )
