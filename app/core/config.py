"""
Application Configuration
==========================
Centralized configuration using Pydantic Settings.
All sensitive values are loaded from environment variables.
"""

from functools import lru_cache
from typing import List, Optional, Union
from pydantic import Field, field_validator, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.
    
    All sensitive data (passwords, tokens, keys) use SecretStr to prevent
    accidental logging exposure.
    """
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # ===================
    # Application Settings
    # ===================
    app_name: str = Field(default="susoft-shopify-sync", description="Application name")
    app_env: str = Field(default="development", description="Environment: development, staging, production")
    debug: bool = Field(default=False, description="Enable debug mode")
    secret_key: SecretStr = Field(..., description="Secret key for JWT and session signing (min 32 chars)")
    
    # ===================
    # Database Settings
    # ===================
    database_url: str = Field(
        default="postgresql+asyncpg://postgres:password@localhost:5432/susoft_shopify_sync",
        description="PostgreSQL connection string with asyncpg driver"
    )
    database_pool_size: int = Field(default=10, ge=1, le=50, description="Connection pool size")
    database_max_overflow: int = Field(default=20, ge=0, le=100, description="Max overflow connections")
    database_pool_timeout: int = Field(default=30, description="Pool connection timeout in seconds")
    database_pool_recycle: int = Field(default=3600, description="Connection recycle time in seconds")
    
    # ===================
    # Redis Settings
    # ===================
    redis_url: str = Field(default="redis://localhost:6379/0", description="Redis connection URL")
    redis_lock_timeout: int = Field(default=10, ge=1, le=60, description="Distributed lock timeout in seconds")
    redis_lock_blocking_timeout: int = Field(default=5, description="Lock acquisition timeout")
    
    # ===================
    # Encryption Settings
    # ===================
    encryption_key: SecretStr = Field(..., description="Fernet encryption key for API tokens")
    
    # ===================
    # Celery Settings
    # ===================
    celery_broker_url: str = Field(default="redis://localhost:6379/1", description="Celery broker URL")
    celery_result_backend: str = Field(default="redis://localhost:6379/2", description="Celery result backend")
    celery_task_always_eager: bool = Field(default=False, description="Run tasks synchronously (for testing)")
    celery_task_time_limit: int = Field(default=300, description="Task time limit in seconds")
    celery_task_soft_time_limit: int = Field(default=240, description="Soft time limit before SoftTimeLimitExceeded")
    
    # ===================
    # Alert Settings
    # ===================
    alert_queue_timeout_minutes: int = Field(default=5, ge=1, le=60, description="Minutes before alerting on stuck queue")
    alert_max_retries: int = Field(default=3, ge=1, le=10, description="Max retries before DLQ")
    alert_enabled: bool = Field(default=True, description="Enable alert notifications")
    
    # ===================
    # Slack Notifications
    # ===================
    slack_webhook_url: Optional[str] = Field(default=None, description="Slack webhook URL for alerts")
    slack_channel: str = Field(default="#integrasjon-alerts", description="Slack channel for alerts")
    
    # ===================
    # Telegram Notifications
    # ===================
    telegram_bot_token: Optional[str] = Field(default=None, description="Telegram bot token")
    telegram_chat_id: Optional[str] = Field(default=None, description="Telegram chat ID for alerts")
    
    # ===================
    # Admin Portal Settings
    # ===================
    admin_username: str = Field(default="admin", description="Admin portal username")
    admin_password: SecretStr = Field(..., description="Admin portal password")
    admin_session_timeout_minutes: int = Field(default=60, description="Admin session timeout")
    admin_api_key: Optional[SecretStr] = Field(
        default=None,
        description="API key required by /admin endpoints via X-Admin-Api-Key header."
    )

    # ===================
    # HTTP / CORS
    # ===================
    cors_origins: Union[List[str], str] = Field(
        default_factory=list,
        description=(
            "Allowed CORS origins. Accepts a JSON array, comma-separated string, "
            "or '*' for all origins."
        ),
    )

    @field_validator("cors_origins", mode="before")
    @classmethod
    def _split_cors_origins(cls, v):
        if v is None or v == "":
            return []
        if isinstance(v, str):
            v = v.strip()
            if v == "*":
                return ["*"]
            if v.startswith("["):
                import json
                return json.loads(v)
            return [item.strip() for item in v.split(",") if item.strip()]
        return v
    
    # ===================
    # Logging Settings
    # ===================
    log_level: str = Field(default="INFO", description="Log level: DEBUG, INFO, WARNING, ERROR")
    log_format: str = Field(default="json", description="Log format: json or text")
    log_include_request_body: bool = Field(default=False, description="Log request bodies (caution: sensitive data)")
    
    # ===================
    # Rate Limiting
    # ===================
    susoft_rate_limit_per_second: float = Field(default=5.0, ge=0.1, le=100, description="Susoft API rate limit")
    shopify_rate_limit_per_second: float = Field(default=2.0, ge=0.1, le=40, description="Shopify API rate limit")

    # ===================
    # Sync Behavior
    # ===================
    shopify_shipping_sku: Optional[str] = Field(
        default="FRAKT",
        description=(
            "SKU in Susoft used to represent Shopify shipping as an order line. "
            "A product with this SKU/barcode must exist in Susoft (and ideally be "
            "registered in product_mappings) so the shipping amount can be added "
            "to the order. Set to empty string to disable."
        )
    )
    susoft_shipping_product_id: Optional[str] = Field(
        default=None,
        description=(
            "Explicit Susoft product id used for shipping lines on /order/pos. "
            "Required when no product_mapping exists for the shipping SKU. "
            "Find the id in Susoft under the FRAKT product (e.g. '10732')."
        )
    )

    # ===================
    # Webhook Registration
    # ===================
    webhook_base_url: Optional[str] = Field(
        default=None,
        description=(
            "Public base URL where this service receives webhooks "
            "(e.g. https://sync.example.com). Used to auto-register Shopify "
            "webhook subscriptions. Leave unset to disable auto-registration."
        )
    )
    auto_register_webhooks: bool = Field(
        default=True,
        description="Automatically reconcile Shopify webhooks on tenant create / app startup."
    )
    register_webhooks_on_startup: bool = Field(
        default=False,
        description="If true, reconcile webhooks for all active tenants on application startup."
    )

    @field_validator("webhook_base_url")
    @classmethod
    def validate_webhook_base_url(cls, v: Optional[str]) -> Optional[str]:
        if v is None or v == "":
            return None
        v = v.rstrip("/")
        if not (v.startswith("http://") or v.startswith("https://")):
            raise ValueError("webhook_base_url must start with http:// or https://")
        return v
    
    # ===================
    # Health Check
    # ===================
    health_check_interval_seconds: int = Field(default=60, description="Health check interval")
    
    # ===================
    # Validators
    # ===================
    @field_validator("app_env")
    @classmethod
    def validate_app_env(cls, v: str) -> str:
        allowed = {"development", "staging", "production"}
        if v.lower() not in allowed:
            raise ValueError(f"app_env must be one of: {allowed}")
        return v.lower()
    
    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if v.upper() not in allowed:
            raise ValueError(f"log_level must be one of: {allowed}")
        return v.upper()
    
    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, v: str) -> str:
        if not v.startswith("postgresql"):
            raise ValueError("database_url must be a PostgreSQL connection string")
        return v
    
    # ===================
    # Properties
    # ===================
    @property
    def environment(self) -> str:
        """Alias for ``app_env`` so callers can use ``settings.environment``."""
        return self.app_env

    @property
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.app_env == "production"
    
    @property
    def is_development(self) -> bool:
        """Check if running in development environment."""
        return self.app_env == "development"
    
    @property
    def notifications_enabled(self) -> bool:
        """Check if any notification channel is configured."""
        return bool(self.slack_webhook_url or self.telegram_bot_token)


@lru_cache
def get_settings() -> Settings:
    """
    Get cached settings instance.
    
    Uses lru_cache to ensure settings are only loaded once.
    Call get_settings.cache_clear() to reload settings.
    """
    return Settings()


# Convenience alias
settings = get_settings()
