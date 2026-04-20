"""
Susoft-Shopify Sync Integration
================================
A robust, multi-tenant integration platform for synchronizing 
inventory and orders between Shopify and Susoft ERP/POS systems.

Architecture:
- FastAPI for async webhook handling and admin API
- Celery + Redis for reliable task queue processing
- PostgreSQL for persistent storage with full audit trail
- Redis distributed locks for concurrency control

Key Features:
- Multi-tenant support with complete data isolation
- Encrypted API credentials storage
- Automatic retry with exponential backoff
- Dead Letter Queue for failed tasks
- Real-time alerting via Slack/Telegram
- Comprehensive audit logging
"""

__version__ = "1.0.0"
__author__ = "Integration Team"
