"""
Susoft-Shopify Sync Application
================================
FastAPI application for synchronizing data between Susoft ERP/POS
and Shopify e-commerce platform.

Features:
- Multi-tenant architecture
- Webhook receivers for both platforms
- Admin API for management
- Background task processing via Celery
"""

from contextlib import asynccontextmanager
from datetime import datetime, timezone
import structlog

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.core.config import settings
from app.core.database import engine
from app.core.logging_service import setup_portal_logging
from app.api.webhooks import router as webhooks_router
from app.api.admin import router as admin_router
from app.admin_portal.router import router as portal_router


# Configure structured logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    wrapper_class=structlog.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan handler.
    
    Handles startup and shutdown events.
    """
    # Startup
    setup_portal_logging()  # Initialize portal logging
    
    logger.info(
        "Application starting",
        environment=settings.environment,
        version="1.0.0"
    )
    
    yield
    
    # Shutdown
    logger.info("Application shutting down")
    
    # Close database connections
    await engine.dispose()


# Create FastAPI application
app = FastAPI(
    title="Susoft-Shopify Sync",
    description="""
    Multi-tenant integration service for synchronizing inventory and orders
    between Susoft ERP/POS and Shopify e-commerce platform.
    
    ## Features
    
    * **Order Sync**: Shopify orders → Susoft
    * **Inventory Sync**: Susoft stock → Shopify
    * **Multi-tenant**: Supports multiple customer integrations
    * **Reliable**: Queue-based processing with retry and DLQ
    * **Observable**: Admin dashboard and alerting
    
    ## Architecture
    
    - Susoft is the inventory master
    - SKU-based product mapping
    - 1:1 location mapping
    - Safety stock support
    - Idempotent operations
    """,
    version="1.0.0",
    docs_url="/docs" if settings.environment != "production" else None,
    redoc_url="/redoc" if settings.environment != "production" else None,
    lifespan=lifespan
)


# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Include routers
app.include_router(webhooks_router)
app.include_router(admin_router)
app.include_router(portal_router)


# ===================
# Error Handlers
# ===================


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler for unhandled errors."""
    logger.error(
        "Unhandled exception",
        error=str(exc),
        path=request.url.path,
        method=request.method
    )
    
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "detail": str(exc) if settings.environment == "development" else None
        }
    )


# ===================
# Health Endpoints
# ===================


@app.get("/health", tags=["health"])
async def health_check():
    """
    Basic health check endpoint.
    
    Returns 200 if the application is running.
    """
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "version": "1.0.0"
    }


@app.get("/health/ready", tags=["health"])
async def readiness_check():
    """
    Readiness check for Kubernetes/orchestration.
    
    Verifies database connectivity.
    """
    try:
        from sqlalchemy import text
        from app.core.database import AsyncSessionLocal
        
        async with AsyncSessionLocal() as session:
            await session.execute(text("SELECT 1"))
        
        return {
            "status": "ready",
            "database": "connected"
        }
    except Exception as e:
        logger.error("Readiness check failed", error=str(e))
        return JSONResponse(
            status_code=503,
            content={
                "status": "not_ready",
                "error": str(e)
            }
        )


@app.get("/", tags=["root"])
async def root():
    """Root endpoint with API information."""
    return {
        "name": "Susoft-Shopify Sync",
        "version": "1.0.0",
        "docs": "/docs" if settings.environment != "production" else None,
        "health": "/health"
    }


# ===================
# Application Runner
# ===================


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.environment == "development",
        log_level="info"
    )
