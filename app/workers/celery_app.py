"""
Celery Application Configuration
=================================
Configures Celery for async task processing with Redis as broker.

Features:
- Automatic retry with exponential backoff
- Task result tracking
- Beat scheduler for periodic tasks
- Dead letter queue handling
"""

from celery import Celery
from celery.schedules import crontab

from app.core.config import settings


# Create Celery app
celery_app = Celery(
    "susoft_shopify_sync",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
    include=[
        "app.workers.tasks",
        "app.workers.scheduled_tasks"
    ]
)

# Celery configuration
celery_app.conf.update(
    # Task settings
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    
    # Task execution
    task_acks_late=True,  # Acknowledge after task completion (safer)
    task_reject_on_worker_lost=True,  # Requeue if worker dies
    task_time_limit=settings.celery_task_time_limit,
    task_soft_time_limit=settings.celery_task_soft_time_limit,
    
    # Worker settings
    worker_prefetch_multiplier=1,  # Fair dispatch to workers
    worker_concurrency=4,  # Number of concurrent tasks per worker
    
    # Result backend
    result_expires=86400,  # Results expire after 24 hours
    
    # Retry settings
    task_default_retry_delay=5,  # Initial retry delay (seconds)
    task_max_retries=settings.alert_max_retries,
    
    # Rate limiting (per task, per worker)
    task_annotations={
        "app.workers.tasks.process_shopify_order": {
            "rate_limit": "10/m"  # 10 orders per minute per worker
        },
        "app.workers.tasks.process_susoft_stock_change": {
            "rate_limit": "20/m"  # 20 stock updates per minute per worker
        },
        "app.workers.tasks.sync_stock_to_shopify": {
            "rate_limit": "5/m"  # 5 bulk syncs per minute per worker
        }
    },
    
    # Eager mode for testing
    task_always_eager=settings.celery_task_always_eager,
    
    # Beat schedule for periodic tasks
    beat_schedule={
        # Check for stuck tasks every 5 minutes
        "check-stuck-tasks": {
            "task": "app.workers.scheduled_tasks.check_stuck_tasks",
            "schedule": 300.0,  # 5 minutes
        },
        # Send DLQ alerts every 5 minutes
        "send-dlq-alerts": {
            "task": "app.workers.scheduled_tasks.send_dlq_alerts",
            "schedule": 300.0,  # 5 minutes
        },
        # Health check heartbeat every minute
        "tenant-heartbeat-check": {
            "task": "app.workers.scheduled_tasks.check_tenant_heartbeats",
            "schedule": 60.0,  # 1 minute
        },
        # Full stock sync daily at 3 AM
        "daily-stock-reconciliation": {
            "task": "app.workers.scheduled_tasks.daily_stock_reconciliation",
            "schedule": crontab(hour=3, minute=0),
        },
    },
    
    # Queue routing
    task_routes={
        "app.workers.tasks.process_shopify_order": {"queue": "orders"},
        "app.workers.tasks.process_susoft_stock_change": {"queue": "stock"},
        "app.workers.tasks.sync_stock_to_shopify": {"queue": "stock"},
        "app.workers.scheduled_tasks.*": {"queue": "scheduled"},
    },
    
    # Default queue
    task_default_queue="default",
)


def get_celery_app() -> Celery:
    """Get the Celery application instance."""
    return celery_app
