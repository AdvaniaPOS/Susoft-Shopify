"""
Shared pytest fixtures for tests.
"""

import asyncio
from typing import AsyncGenerator, Generator
import pytest
from unittest.mock import MagicMock, AsyncMock

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker


# Test database URL (SQLite for unit tests)
TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"


@pytest.fixture(scope="session")
def event_loop() -> Generator:
    """Create event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
async def db_session() -> AsyncGenerator[AsyncSession, None]:
    """Create a test database session."""
    from app.db.models import Base
    
    engine = create_async_engine(
        TEST_DATABASE_URL,
        echo=False
    )
    
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    async_session = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False
    )
    
    async with async_session() as session:
        yield session
    
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    
    await engine.dispose()


@pytest.fixture
def mock_susoft_client() -> MagicMock:
    """Create a mock Susoft client."""
    client = AsyncMock()
    client.health_check = AsyncMock(return_value={"status": "healthy"})
    client.get_products = AsyncMock(return_value=[])
    client.get_all_stock = AsyncMock(return_value=[])
    client.create_order = AsyncMock(return_value={"uuid": "test-uuid"})
    return client


@pytest.fixture
def mock_shopify_client() -> MagicMock:
    """Create a mock Shopify client."""
    client = AsyncMock()
    client.get_shop_info = AsyncMock(return_value={"name": "Test Shop"})
    client.set_inventory_level = AsyncMock(return_value={"inventory_level": {}})
    client.verify_webhook_signature = MagicMock(return_value=True)
    return client


@pytest.fixture
def sample_tenant_data() -> dict:
    """Sample tenant data for tests."""
    return {
        "name": "Test Butikk",
        "susoft_api_url": "https://test.susoft.no/api/v3",
        "susoft_api_key": "test-api-key",
        "susoft_integration_id": "int-123",
        "susoft_webhook_secret": "webhook-secret",
        "shopify_shop_url": "test-shop.myshopify.com",
        "shopify_access_token": "shpat_test",
        "shopify_default_location_id": "12345",
        "safety_stock_default": 2
    }


@pytest.fixture
def sample_shopify_order() -> dict:
    """Sample Shopify order webhook payload."""
    return {
        "id": 12345678,
        "name": "#1001",
        "email": "customer@example.com",
        "currency": "NOK",
        "total_price": "999.00",
        "customer": {
            "id": 1234,
            "first_name": "Ola",
            "last_name": "Nordmann",
            "email": "customer@example.com",
            "phone": "+4712345678"
        },
        "shipping_address": {
            "address1": "Testgata 1",
            "address2": "",
            "city": "Oslo",
            "zip": "0150",
            "country_code": "NO"
        },
        "line_items": [
            {
                "id": 111,
                "variant_id": 222,
                "sku": "PROD-001",
                "name": "Test Produkt",
                "quantity": 2,
                "price": "499.50"
            }
        ]
    }


@pytest.fixture
def sample_susoft_stock_event() -> dict:
    """Sample Susoft stock change webhook payload."""
    return {
        "uuid": "stock-event-123",
        "productUuid": "product-uuid-456",
        "sku": "PROD-001",
        "quantity": 50,
        "locationId": "warehouse-1",
        "timestamp": "2024-01-15T10:30:00Z"
    }
