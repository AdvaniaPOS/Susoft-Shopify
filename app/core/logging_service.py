"""
Application Logging Service
============================
Structured logging with file and database storage for the admin portal.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any
from collections import deque
import asyncio
import aiofiles
from dataclasses import dataclass, asdict
from enum import Enum
import structlog


class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass
class LogEntry:
    """Represents a single log entry."""
    timestamp: str
    level: str
    message: str
    logger_name: str
    tenant_id: Optional[str] = None
    extra: Optional[Dict[str, Any]] = None
    traceback: Optional[str] = None


class LogBuffer:
    """
    In-memory circular buffer for recent logs.
    Keeps the last N log entries in memory for quick access.
    """
    
    def __init__(self, maxlen: int = 1000):
        self._buffer: deque = deque(maxlen=maxlen)
        self._lock = asyncio.Lock()
    
    async def add(self, entry: LogEntry):
        """Add a log entry to the buffer."""
        async with self._lock:
            self._buffer.append(entry)
    
    async def get_recent(
        self,
        limit: int = 100,
        level: Optional[LogLevel] = None,
        tenant_id: Optional[str] = None,
        search: Optional[str] = None
    ) -> List[LogEntry]:
        """Get recent log entries with optional filtering."""
        async with self._lock:
            entries = list(self._buffer)
        
        # Filter
        if level:
            entries = [e for e in entries if e.level == level.value]
        
        if tenant_id:
            entries = [e for e in entries if e.tenant_id == tenant_id]
        
        if search:
            search_lower = search.lower()
            entries = [
                e for e in entries 
                if search_lower in e.message.lower() or 
                   (e.extra and search_lower in json.dumps(e.extra).lower())
            ]
        
        # Return most recent first
        return list(reversed(entries))[:limit]
    
    async def clear(self):
        """Clear the buffer."""
        async with self._lock:
            self._buffer.clear()


class FileLogWriter:
    """Writes logs to rotating files."""
    
    def __init__(self, log_dir: str = "logs", max_size_mb: int = 10):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        self.max_size = max_size_mb * 1024 * 1024
        self._current_file: Optional[Path] = None
        self._lock = asyncio.Lock()
    
    def _get_current_file(self) -> Path:
        """Get current log file path."""
        date_str = datetime.now().strftime("%Y-%m-%d")
        return self.log_dir / f"app-{date_str}.log"
    
    async def write(self, entry: LogEntry):
        """Write a log entry to file."""
        async with self._lock:
            log_file = self._get_current_file()
            
            # Check if we need to rotate
            if log_file.exists() and log_file.stat().st_size > self.max_size:
                # Rotate by adding timestamp
                timestamp = datetime.now().strftime("%H%M%S")
                new_name = log_file.with_suffix(f".{timestamp}.log")
                log_file.rename(new_name)
            
            # Write entry
            log_line = json.dumps(asdict(entry)) + "\n"
            async with aiofiles.open(log_file, mode='a', encoding='utf-8') as f:
                await f.write(log_line)
    
    async def read_logs(
        self,
        date: Optional[str] = None,
        limit: int = 500
    ) -> List[LogEntry]:
        """Read logs from file."""
        if date:
            log_file = self.log_dir / f"app-{date}.log"
        else:
            log_file = self._get_current_file()
        
        if not log_file.exists():
            return []
        
        entries = []
        async with aiofiles.open(log_file, mode='r', encoding='utf-8') as f:
            async for line in f:
                try:
                    data = json.loads(line.strip())
                    entries.append(LogEntry(**data))
                except (json.JSONDecodeError, TypeError):
                    continue
        
        return list(reversed(entries))[:limit]
    
    def get_available_dates(self) -> List[str]:
        """Get list of dates with available logs."""
        dates = []
        for f in self.log_dir.glob("app-*.log"):
            # Extract date from filename
            name = f.stem
            if name.startswith("app-"):
                date_part = name[4:]
                if len(date_part) == 10:  # YYYY-MM-DD
                    dates.append(date_part)
        return sorted(dates, reverse=True)


# Global logger instances
_log_buffer: Optional[LogBuffer] = None
_file_writer: Optional[FileLogWriter] = None


def get_log_buffer() -> LogBuffer:
    """Get the global log buffer instance."""
    global _log_buffer
    if _log_buffer is None:
        _log_buffer = LogBuffer(maxlen=2000)
    return _log_buffer


def get_file_writer() -> FileLogWriter:
    """Get the global file writer instance."""
    global _file_writer
    if _file_writer is None:
        _file_writer = FileLogWriter()
    return _file_writer


class PortalLogHandler(logging.Handler):
    """
    Custom logging handler that captures logs for the admin portal.
    """
    
    def __init__(self):
        super().__init__()
        self.buffer = get_log_buffer()
        self.file_writer = get_file_writer()
    
    def emit(self, record: logging.LogRecord):
        """Handle a log record."""
        try:
            # Extract extra fields
            extra = {}
            tenant_id = None
            
            # Get extra fields from structlog context
            if hasattr(record, '_extra'):
                extra = record._extra
                tenant_id = extra.pop('tenant_id', None)
            
            # Create log entry
            entry = LogEntry(
                timestamp=datetime.now(timezone.utc).isoformat(),
                level=record.levelname,
                message=record.getMessage(),
                logger_name=record.name,
                tenant_id=tenant_id,
                extra=extra if extra else None,
                traceback=self.format(record) if record.exc_info else None
            )
            
            # Add to buffer (fire and forget)
            asyncio.create_task(self.buffer.add(entry))
            
            # Write to file (fire and forget)
            asyncio.create_task(self.file_writer.write(entry))
            
        except Exception:
            # Don't let logging errors crash the app
            pass


def setup_portal_logging():
    """
    Set up logging to capture logs for the admin portal.
    Call this during application startup.
    """
    # Add our handler to the root logger
    handler = PortalLogHandler()
    handler.setLevel(logging.INFO)
    
    logging.getLogger().addHandler(handler)
    
    # Also capture structlog output
    # This is handled by structlog's ProcessorFormatter

    return get_log_buffer()
