"""
Webhook Event Repository
========================
Repository for webhook event logging.
"""

from datetime import datetime, timezone
from typing import Optional, Dict, Any
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import WebhookEvent, WebhookSource


class WebhookEventRepository:
    """Repository for webhook event operations."""
    
    def __init__(self, session: AsyncSession):
        self.session = session
    
    async def create(
        self,
        tenant_id: str,
        source: WebhookSource,
        event_type: str,
        external_id: str,
        payload: Dict[str, Any],
        headers: Optional[Dict[str, Any]] = None,
        processed: bool = False
    ) -> WebhookEvent:
        """Create a new webhook event record."""
        event = WebhookEvent(
            id=uuid4(),
            tenant_id=tenant_id,
            source=source,
            event_type=event_type,
            external_id=external_id,
            payload=payload,
            headers=headers or {},
            processed=processed,
            received_at=datetime.now(timezone.utc)
        )
        
        self.session.add(event)
        await self.session.commit()
        await self.session.refresh(event)
        
        return event
    
    async def mark_processed(
        self,
        event_id: str,
        error: Optional[str] = None
    ) -> bool:
        """Mark a webhook event as processed."""
        result = await self.session.execute(
            select(WebhookEvent).where(WebhookEvent.id == event_id)
        )
        event = result.scalar_one_or_none()
        
        if not event:
            return False
        
        event.processed = True
        event.processed_at = datetime.now(timezone.utc)
        
        if error:
            event.error = error
        
        await self.session.commit()
        return True
    
    async def get_unprocessed(
        self,
        tenant_id: str,
        limit: int = 100
    ) -> list:
        """Get unprocessed webhook events for a tenant."""
        result = await self.session.execute(
            select(WebhookEvent)
            .where(
                WebhookEvent.tenant_id == tenant_id,
                WebhookEvent.processed == False
            )
            .order_by(WebhookEvent.received_at)
            .limit(limit)
        )
        return list(result.scalars().all())
