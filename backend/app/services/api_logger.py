"""
API Call Logger for Activity Statistics

Tracks Oxylabs API calls, pages fetched, and citations saved
to enable dashboard activity stats (15min, 1hr, 6hr, 24hr).
"""
import asyncio
import logging
from datetime import datetime
from typing import Optional
from sqlalchemy import text

logger = logging.getLogger(__name__)

# In-memory buffer to batch inserts (avoid DB call per API call)
_log_buffer = []
_buffer_lock = asyncio.Lock()
_BUFFER_SIZE = 50  # Flush after 50 entries
_FLUSH_INTERVAL = 10  # Or every 10 seconds


async def log_api_call(
    call_type: str,
    job_id: Optional[int] = None,
    edition_id: Optional[int] = None,
    count: int = 1,
    success: bool = True,
    extra_info: Optional[str] = None
):
    """
    Log an API call to the buffer for later persistence.

    call_type: 'oxylabs', 'page_fetch', 'citation_save'
    """
    entry = {
        "call_type": call_type,
        "job_id": job_id,
        "edition_id": edition_id,
        "count": count,
        "success": success,
        "extra_info": extra_info,
        "created_at": datetime.utcnow()
    }

    async with _buffer_lock:
        _log_buffer.append(entry)

        # Auto-flush if buffer is full
        if len(_log_buffer) >= _BUFFER_SIZE:
            await _flush_buffer_internal()


async def _flush_buffer_internal():
    """Internal flush - call with lock held"""
    global _log_buffer
    if not _log_buffer:
        return

    entries = _log_buffer.copy()
    _log_buffer = []

    try:
        # Import here to avoid circular imports
        from ..database import async_session

        async with async_session() as db:
            for entry in entries:
                await db.execute(
                    text("""
                        INSERT INTO api_call_logs
                        (call_type, job_id, edition_id, count, success, extra_info, created_at)
                        VALUES (:call_type, :job_id, :edition_id, :count, :success, :extra_info, :created_at)
                    """),
                    entry
                )
            await db.commit()

        logger.debug(f"Flushed {len(entries)} API call logs to database")

    except Exception as e:
        logger.warning(f"Failed to flush API call logs: {e}")
        # Re-add entries to buffer for retry
        async with _buffer_lock:
            _log_buffer = entries + _log_buffer


async def flush_api_logs():
    """Force flush the log buffer to database"""
    async with _buffer_lock:
        await _flush_buffer_internal()


async def get_activity_stats(db) -> dict:
    """
    Get activity statistics for the dashboard.

    Returns counts of Oxylabs calls, pages fetched, and citations saved
    for 15min, 1hr, 6hr, and 24hr time periods.
    """
    from sqlalchemy import func, select
    from ..models import ApiCallLog, Citation

    now = datetime.utcnow()

    # Time periods in minutes
    periods = {
        "15min": 15,
        "1hr": 60,
        "6hr": 360,
        "24hr": 1440
    }

    stats = {}

    for period_name, minutes in periods.items():
        cutoff = datetime.utcnow()
        cutoff = cutoff.replace(
            minute=cutoff.minute - (minutes % 60) if minutes < 60 else 0,
            second=0,
            microsecond=0
        )
        # Proper time delta calculation
        from datetime import timedelta
        cutoff = now - timedelta(minutes=minutes)

        # Count Oxylabs calls
        oxylabs_result = await db.execute(
            select(func.coalesce(func.sum(ApiCallLog.count), 0))
            .where(ApiCallLog.call_type == 'oxylabs')
            .where(ApiCallLog.created_at >= cutoff)
        )
        oxylabs_count = oxylabs_result.scalar() or 0

        # Count page fetches
        pages_result = await db.execute(
            select(func.coalesce(func.sum(ApiCallLog.count), 0))
            .where(ApiCallLog.call_type == 'page_fetch')
            .where(ApiCallLog.created_at >= cutoff)
        )
        pages_count = pages_result.scalar() or 0

        # Count citations saved (from api_call_logs)
        citations_log_result = await db.execute(
            select(func.coalesce(func.sum(ApiCallLog.count), 0))
            .where(ApiCallLog.call_type == 'citation_save')
            .where(ApiCallLog.created_at >= cutoff)
        )
        citations_from_log = citations_log_result.scalar() or 0

        # Also count from citations table directly (more accurate)
        citations_direct_result = await db.execute(
            select(func.count(Citation.id))
            .where(Citation.created_at >= cutoff)
        )
        citations_direct = citations_direct_result.scalar() or 0

        # Use the higher of the two (direct is authoritative, log is backup)
        citations_count = max(citations_from_log, citations_direct)

        stats[period_name] = {
            "oxylabs_calls": int(oxylabs_count),
            "pages_fetched": int(pages_count),
            "citations_saved": int(citations_count)
        }

    return stats


# Background task to periodically flush buffer
_flush_task = None

async def start_flush_task():
    """Start background task to periodically flush logs"""
    global _flush_task

    async def flush_loop():
        while True:
            await asyncio.sleep(_FLUSH_INTERVAL)
            try:
                await flush_api_logs()
            except Exception as e:
                logger.warning(f"Flush task error: {e}")

    _flush_task = asyncio.create_task(flush_loop())
    logger.info("API log flush task started")


async def stop_flush_task():
    """Stop the background flush task"""
    global _flush_task
    if _flush_task:
        _flush_task.cancel()
        try:
            await _flush_task
        except asyncio.CancelledError:
            pass
        _flush_task = None
