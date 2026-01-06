"""
Local citation buffer for resilient saves.

When database saves fail (e.g., TimeoutError to remote DB),
citations are stored locally and retried later.
"""
import json
import logging
import os
import asyncio
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Set
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)

# Buffer directory - use /tmp for ephemeral storage on Render
# Falls back to local .citation_buffer for development
BUFFER_DIR = Path(os.environ.get("CITATION_BUFFER_DIR", "/tmp/citation_buffer"))


@dataclass
class BufferedPage:
    """A page of citations waiting to be saved to DB."""
    job_id: int
    paper_id: int
    edition_id: int
    target_edition_id: int
    page_num: int
    papers: List[Dict]
    created_at: str
    retry_count: int = 0
    last_error: Optional[str] = None


class CitationBuffer:
    """
    Local buffer for citation saves with retry capability.

    Usage:
        buffer = CitationBuffer()

        # Save locally first
        buffer.save_page(job_id, paper_id, edition_id, target_edition_id, page_num, papers)

        # Try DB save
        try:
            await save_to_db(...)
            buffer.mark_saved(job_id, page_num)  # Remove from buffer
        except Exception as e:
            buffer.mark_failed(job_id, page_num, str(e))  # Keep for retry

        # Later, retry failed saves
        pending = buffer.get_pending_pages()
        for page in pending:
            try:
                await save_to_db(page.papers, ...)
                buffer.mark_saved(page.job_id, page.page_num)
            except:
                pass  # Will retry next time
    """

    def __init__(self):
        self._ensure_buffer_dir()

    def _ensure_buffer_dir(self):
        """Create buffer directory if it doesn't exist."""
        try:
            BUFFER_DIR.mkdir(parents=True, exist_ok=True)
            logger.info(f"Citation buffer directory: {BUFFER_DIR}")
        except Exception as e:
            logger.error(f"Failed to create buffer directory: {e}")

    def _page_path(self, job_id: int, page_num: int) -> Path:
        """Path to a specific page's buffer file."""
        return BUFFER_DIR / f"job_{job_id}_page_{page_num}.json"

    def _failed_dir(self) -> Path:
        """Directory for failed saves that need retry."""
        failed_dir = BUFFER_DIR / "failed"
        failed_dir.mkdir(exist_ok=True)
        return failed_dir

    def save_page(
        self,
        job_id: int,
        paper_id: int,
        edition_id: int,
        target_edition_id: int,
        page_num: int,
        papers: List[Dict]
    ) -> bool:
        """
        Save a page of citations to local buffer.
        Returns True if saved successfully.
        """
        try:
            buffered = BufferedPage(
                job_id=job_id,
                paper_id=paper_id,
                edition_id=edition_id,
                target_edition_id=target_edition_id,
                page_num=page_num,
                papers=papers,
                created_at=datetime.utcnow().isoformat(),
            )

            path = self._page_path(job_id, page_num)
            with open(path, 'w') as f:
                json.dump(asdict(buffered), f)

            logger.debug(f"Buffered page {page_num} for job {job_id}: {len(papers)} papers")
            return True

        except Exception as e:
            logger.error(f"Failed to buffer page {page_num} for job {job_id}: {e}")
            return False

    def mark_saved(self, job_id: int, page_num: int) -> bool:
        """
        Mark a page as successfully saved to DB.
        Removes it from the buffer.
        """
        try:
            path = self._page_path(job_id, page_num)
            if path.exists():
                path.unlink()
                logger.debug(f"Removed buffer for job {job_id} page {page_num} (saved to DB)")

            # Also check failed dir
            failed_path = self._failed_dir() / f"job_{job_id}_page_{page_num}.json"
            if failed_path.exists():
                failed_path.unlink()

            return True
        except Exception as e:
            logger.error(f"Failed to remove buffer for job {job_id} page {page_num}: {e}")
            return False

    def mark_failed(self, job_id: int, page_num: int, error: str) -> bool:
        """
        Mark a page as failed to save to DB.
        Moves it to the failed directory for retry.
        """
        try:
            path = self._page_path(job_id, page_num)
            if not path.exists():
                logger.warning(f"No buffer found for job {job_id} page {page_num}")
                return False

            # Load, update, and move to failed dir
            with open(path, 'r') as f:
                data = json.load(f)

            data['retry_count'] = data.get('retry_count', 0) + 1
            data['last_error'] = error
            data['failed_at'] = datetime.utcnow().isoformat()

            failed_path = self._failed_dir() / f"job_{job_id}_page_{page_num}.json"
            with open(failed_path, 'w') as f:
                json.dump(data, f)

            # Remove from main buffer
            path.unlink()

            logger.warning(f"Marked page {page_num} for job {job_id} as failed (retry #{data['retry_count']}): {error[:100]}")
            return True

        except Exception as e:
            logger.error(f"Failed to mark page as failed: {e}")
            return False

    def get_pending_pages(self, max_retries: int = 5) -> List[BufferedPage]:
        """
        Get all pages that failed to save and need retry.
        Filters out pages that have exceeded max_retries.
        """
        pending = []
        try:
            failed_dir = self._failed_dir()
            for path in failed_dir.glob("job_*.json"):
                try:
                    with open(path, 'r') as f:
                        data = json.load(f)

                    if data.get('retry_count', 0) >= max_retries:
                        # Move to permanent failed (don't retry anymore)
                        permanent_dir = BUFFER_DIR / "permanent_failed"
                        permanent_dir.mkdir(exist_ok=True)
                        path.rename(permanent_dir / path.name)
                        logger.warning(f"Page exceeded max retries, moved to permanent_failed: {path.name}")
                        continue

                    pending.append(BufferedPage(**data))

                except Exception as e:
                    logger.error(f"Failed to load buffer file {path}: {e}")

            logger.info(f"Found {len(pending)} pending pages to retry")
            return pending

        except Exception as e:
            logger.error(f"Failed to get pending pages: {e}")
            return []

    def get_buffer_stats(self) -> Dict:
        """Get statistics about the buffer."""
        try:
            # Count files in each directory
            main_count = len(list(BUFFER_DIR.glob("job_*.json")))
            failed_count = len(list(self._failed_dir().glob("job_*.json")))

            permanent_dir = BUFFER_DIR / "permanent_failed"
            permanent_count = len(list(permanent_dir.glob("job_*.json"))) if permanent_dir.exists() else 0

            return {
                "in_progress": main_count,
                "failed_pending_retry": failed_count,
                "permanent_failed": permanent_count,
                "buffer_dir": str(BUFFER_DIR),
            }
        except Exception as e:
            logger.error(f"Failed to get buffer stats: {e}")
            return {"error": str(e)}

    def cleanup_old_buffers(self, max_age_hours: int = 24) -> int:
        """
        Remove buffer files older than max_age_hours.
        Returns count of removed files.
        """
        removed = 0
        try:
            cutoff = datetime.utcnow().timestamp() - (max_age_hours * 3600)

            for path in BUFFER_DIR.rglob("job_*.json"):
                try:
                    if path.stat().st_mtime < cutoff:
                        path.unlink()
                        removed += 1
                except Exception:
                    pass

            logger.info(f"Cleaned up {removed} old buffer files")
            return removed

        except Exception as e:
            logger.error(f"Failed to cleanup old buffers: {e}")
            return removed


# Global buffer instance
_buffer: Optional[CitationBuffer] = None


def get_buffer() -> CitationBuffer:
    """Get the global citation buffer instance."""
    global _buffer
    if _buffer is None:
        _buffer = CitationBuffer()
    return _buffer


async def retry_failed_saves():
    """
    Background task to retry failed citation saves.

    Should be called periodically (e.g., every 5 minutes) or on startup.
    """
    from .job_worker import save_buffered_citations  # Import here to avoid circular

    buffer = get_buffer()
    pending = buffer.get_pending_pages(max_retries=5)

    if not pending:
        logger.debug("No pending pages to retry")
        return 0

    logger.info(f"Retrying {len(pending)} failed citation saves...")

    success_count = 0
    for page in pending:
        try:
            await save_buffered_citations(page)
            buffer.mark_saved(page.job_id, page.page_num)
            success_count += 1
        except Exception as e:
            buffer.mark_failed(page.job_id, page.page_num, str(e))
            logger.warning(f"Retry failed for job {page.job_id} page {page.page_num}: {e}")

        # Small delay between retries to avoid overwhelming DB
        await asyncio.sleep(0.5)

    logger.info(f"Retry complete: {success_count}/{len(pending)} pages saved")
    return success_count
