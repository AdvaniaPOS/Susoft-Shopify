"""
Notification System
====================
Sends alerts to administrators via Slack and Telegram.

Features:
- Async notification sending
- Rate limiting to prevent spam
- Rich message formatting
- Fallback handling if one channel fails
"""

import asyncio
from datetime import datetime, timezone
from typing import Optional, List, Any
import structlog

import httpx

from app.core.config import settings


logger = structlog.get_logger()


# Rate limiting
_last_notification_time: float = 0
_notification_count: int = 0
_notification_window: float = 60.0  # 1 minute window
_max_notifications_per_window: int = 10


async def _check_rate_limit() -> bool:
    """Check if we're within rate limit for notifications."""
    global _last_notification_time, _notification_count
    
    current_time = asyncio.get_event_loop().time()
    
    # Reset counter if window has passed
    if current_time - _last_notification_time > _notification_window:
        _notification_count = 0
        _last_notification_time = current_time
    
    if _notification_count >= _max_notifications_per_window:
        logger.warning("Notification rate limit exceeded")
        return False
    
    _notification_count += 1
    return True


async def send_slack_notification(
    message: str,
    channel: Optional[str] = None,
    emoji: str = ":warning:"
) -> bool:
    """
    Send a notification to Slack.
    
    Args:
        message: Message text (supports Slack markdown)
        channel: Optional channel override
        emoji: Icon emoji for the message
        
    Returns:
        True if sent successfully
    """
    webhook_url = settings.slack_webhook_url
    
    if not webhook_url:
        logger.debug("Slack webhook not configured, skipping notification")
        return False
    
    if not await _check_rate_limit():
        return False
    
    payload = {
        "text": message,
        "icon_emoji": emoji,
        "username": "Susoft-Shopify Sync"
    }
    
    if channel:
        payload["channel"] = channel
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                webhook_url,
                json=payload,
                timeout=10.0
            )
            
            if response.status_code == 200:
                logger.info("Slack notification sent")
                return True
            else:
                logger.error(
                    "Slack notification failed",
                    status_code=response.status_code,
                    response=response.text
                )
                return False
                
    except Exception as e:
        logger.error("Failed to send Slack notification", error=str(e))
        return False


async def send_telegram_notification(
    message: str,
    chat_id: Optional[str] = None,
    parse_mode: str = "HTML"
) -> bool:
    """
    Send a notification to Telegram.
    
    Args:
        message: Message text (supports HTML or Markdown)
        chat_id: Optional chat ID override
        parse_mode: Message format (HTML or Markdown)
        
    Returns:
        True if sent successfully
    """
    bot_token = settings.telegram_bot_token
    target_chat_id = chat_id or settings.telegram_chat_id
    
    if not bot_token or not target_chat_id:
        logger.debug("Telegram not configured, skipping notification")
        return False
    
    if not await _check_rate_limit():
        return False
    
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    
    payload = {
        "chat_id": target_chat_id,
        "text": message,
        "parse_mode": parse_mode
    }
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                url,
                json=payload,
                timeout=10.0
            )
            
            if response.status_code == 200:
                logger.info("Telegram notification sent")
                return True
            else:
                logger.error(
                    "Telegram notification failed",
                    status_code=response.status_code,
                    response=response.text
                )
                return False
                
    except Exception as e:
        logger.error("Failed to send Telegram notification", error=str(e))
        return False


# ===================
# Message Formatters
# ===================


def format_dlq_alert(
    tenant_id: str,
    dlq_items: List[Any]
) -> str:
    """
    Format a dead letter queue alert message.
    
    Args:
        tenant_id: Tenant identifier
        dlq_items: List of DLQ items
        
    Returns:
        Formatted alert message
    """
    count = len(dlq_items)
    
    # Group by task type
    by_task = {}
    for item in dlq_items:
        task = item.task_name.split(".")[-1]  # Get just the function name
        if task not in by_task:
            by_task[task] = 0
        by_task[task] += 1
    
    task_summary = ", ".join(f"{task}: {count}" for task, count in by_task.items())
    
    message = f"""🚨 <b>Dead Letter Queue Alert</b>

<b>Tenant:</b> {tenant_id}
<b>Failed Tasks:</b> {count}
<b>Breakdown:</b> {task_summary}

Tasks have failed after maximum retries and need manual review.
Please check the admin dashboard to investigate and retry.

<i>Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}</i>
"""
    
    return message


def format_stuck_task_alert(
    tenant_id: str,
    stuck_tasks: List[Any],
    threshold_minutes: int
) -> str:
    """
    Format a stuck tasks alert message.
    
    Args:
        tenant_id: Tenant identifier
        stuck_tasks: List of stuck tasks
        threshold_minutes: Alert threshold
        
    Returns:
        Formatted alert message
    """
    count = len(stuck_tasks)
    
    message = f"""⚠️ <b>Stuck Tasks Alert</b>

<b>Tenant:</b> {tenant_id}
<b>Stuck Tasks:</b> {count}
<b>Threshold:</b> {threshold_minutes} minutes

Tasks have been in processing state longer than expected.
This may indicate worker issues or failed task completion.

<i>Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}</i>
"""
    
    return message


def format_tenant_offline_alert(
    tenant_name: str,
    tenant_id: str,
    last_heartbeat: Optional[datetime]
) -> str:
    """
    Format a tenant offline alert message.
    
    Args:
        tenant_name: Tenant display name
        tenant_id: Tenant identifier
        last_heartbeat: Last known heartbeat timestamp
        
    Returns:
        Formatted alert message
    """
    last_seen = "Never"
    if last_heartbeat:
        last_seen = last_heartbeat.strftime('%Y-%m-%d %H:%M:%S UTC')
    
    message = f"""📡 <b>Tenant Offline Alert</b>

<b>Tenant:</b> {tenant_name}
<b>ID:</b> {tenant_id}
<b>Last Seen:</b> {last_seen}

This tenant has not synced any data recently.
Please verify the integration is working correctly.

<i>Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}</i>
"""
    
    return message


def format_sync_error_alert(
    tenant_id: str,
    sync_type: str,
    error_message: str,
    context: Optional[dict] = None
) -> str:
    """
    Format a sync error alert message.
    
    Args:
        tenant_id: Tenant identifier
        sync_type: Type of sync (order, stock)
        error_message: Error description
        context: Additional context
        
    Returns:
        Formatted alert message
    """
    context_str = ""
    if context:
        context_str = "\n".join(f"  • {k}: {v}" for k, v in context.items())
        context_str = f"\n<b>Context:</b>\n{context_str}"
    
    message = f"""❌ <b>Sync Error</b>

<b>Tenant:</b> {tenant_id}
<b>Type:</b> {sync_type}
<b>Error:</b> {error_message}
{context_str}

<i>Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}</i>
"""
    
    return message


async def send_alert(
    message: str,
    level: str = "warning"
) -> None:
    """
    Send an alert to all configured channels.
    
    Args:
        message: Alert message
        level: Alert level (info, warning, error)
    """
    # Choose emoji based on level
    emoji_map = {
        "info": ":information_source:",
        "warning": ":warning:",
        "error": ":x:"
    }
    emoji = emoji_map.get(level, ":warning:")
    
    # Send to all channels
    await asyncio.gather(
        send_slack_notification(message, emoji=emoji),
        send_telegram_notification(message),
        return_exceptions=True
    )
