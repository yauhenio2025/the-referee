"""
Background Job Worker Service

Processes jobs from the queue asynchronously.
Supports concurrent execution of multiple jobs.
Includes watchdog for stuck jobs.
"""
import asyncio
import json
import logging
import traceback
import sys
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from sqlalchemy import select, update, func, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import Job, Paper, Edition, Citation, RawSearchResult, FailedFetch, HarvestTarget, Thinker, ThinkerWork, ThinkerHarvestRun
from ..database import async_session
from .edition_discovery import EditionDiscoveryService
from .scholar_search import get_scholar_service
from .citation_buffer import get_buffer, BufferedPage
from .api_logger import log_api_call
from ..config import get_settings

logger = logging.getLogger(__name__)


# ============== Webhook Callback System ==============

async def send_webhook_callback(job: Job, db: AsyncSession) -> bool:
    """
    Send webhook callback for job completion/failure.

    Uses HMAC-SHA256 signing if callback_secret is provided.
    Returns True if webhook was sent successfully, False otherwise.
    """
    if not job.callback_url:
        return True  # No callback configured, that's fine

    import httpx
    import hmac
    import hashlib

    settings = get_settings()

    # Build payload
    payload = {
        "event": f"job.{job.status}",
        "job_id": job.id,
        "job_type": job.job_type,
        "status": job.status,
        "paper_id": job.paper_id,
        "result": json.loads(job.result) if job.result else None,
        "error": job.error,
        "progress": job.progress,
        "timestamp": datetime.utcnow().isoformat(),
    }

    headers = {"Content-Type": "application/json"}

    # Sign payload if secret is provided
    if job.callback_secret:
        payload_bytes = json.dumps(payload, sort_keys=True).encode()
        signature = hmac.new(
            job.callback_secret.encode(),
            payload_bytes,
            hashlib.sha256
        ).hexdigest()
        headers["X-Webhook-Signature"] = f"sha256={signature}"

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                job.callback_url,
                json=payload,
                headers=headers,
                timeout=settings.webhook_timeout_seconds
            )

            if response.status_code >= 200 and response.status_code < 300:
                job.callback_sent_at = datetime.utcnow()
                job.callback_error = None
                log_now(f"[Webhook] Successfully sent callback for job {job.id} to {job.callback_url}")
                return True
            else:
                job.callback_error = f"HTTP {response.status_code}: {response.text[:500]}"
                log_now(f"[Webhook] Failed for job {job.id}: {job.callback_error}")
                return False

    except Exception as e:
        job.callback_error = f"Exception: {str(e)}"
        log_now(f"[Webhook] Exception for job {job.id}: {e}")
        return False

# Force immediate log output
def log_now(msg: str, level: str = "info"):
    """Log message and immediately flush to stdout"""
    timestamp = datetime.utcnow().strftime("%H:%M:%S")
    print(f"{timestamp} | job_worker | {level.upper()} | {msg}", flush=True)
    sys.stdout.flush()

# Job timeout settings
JOB_TIMEOUT_MINUTES = 30  # Mark job as failed if no progress for this long
HEARTBEAT_INTERVAL = 60  # Seconds between heartbeat updates
ZOMBIE_CHECK_INTERVAL_MINUTES = 5  # How often to check for zombie jobs

# Staleness threshold (for UI indicators)
STALENESS_THRESHOLD_DAYS = 90

# Year-by-year threshold (Google Scholar limits ~1000 results per query)
YEAR_BY_YEAR_THRESHOLD = 1000

# Global worker state
_worker_task: Optional[asyncio.Task] = None
_worker_running = False

# Parallel processing settings
MAX_CONCURRENT_JOBS = 20  # How many jobs can run simultaneously
_job_semaphore: Optional[asyncio.Semaphore] = None
_running_jobs: set = set()  # Track currently running job IDs
_last_zombie_check: Optional[datetime] = None  # Track when we last checked for zombies


async def update_job_progress(
    db: AsyncSession,
    job_id: int,
    progress: float,
    message: str,
    details: Optional[Dict[str, Any]] = None,
):
    """Update job progress in database with heartbeat timestamp and optional detailed progress data"""
    log_now(f"[Job {job_id}] Progress: {progress:.1f}% - {message}")

    values = {
        "progress": progress,
        "progress_message": message,
        "started_at": datetime.utcnow(),  # Use started_at as heartbeat (hacky but works)
    }

    # If details provided, merge into existing params under 'progress_details' key
    if details:
        # Get existing params
        result = await db.execute(select(Job.params).where(Job.id == job_id))
        existing_params_str = result.scalar()
        existing_params = json.loads(existing_params_str) if existing_params_str else {}
        existing_params["progress_details"] = details
        values["params"] = json.dumps(existing_params)

    await db.execute(
        update(Job)
        .where(Job.id == job_id)
        .values(**values)
    )
    await db.commit()


async def save_buffered_citations(page: 'BufferedPage') -> int:
    """
    Save citations from a buffered page to the database.

    Called by the retry mechanism to process failed saves.
    Returns count of citations saved.
    """
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    papers = page.papers
    paper_id = page.paper_id
    target_edition_id = page.target_edition_id

    log_now(f"[RETRY] Processing buffered page {page.page_num} for job {page.job_id}: {len(papers)} papers")

    saved_count = 0

    async with async_session() as db:
        try:
            for paper_data in papers:
                if not isinstance(paper_data, dict):
                    continue

                scholar_id = paper_data.get("scholarId")
                if not scholar_id:
                    continue

                stmt = pg_insert(Citation).values(
                    paper_id=paper_id,
                    edition_id=target_edition_id,
                    scholar_id=scholar_id,
                    title=paper_data.get("title", "Unknown"),
                    authors=paper_data.get("authorsRaw"),
                    year=paper_data.get("year"),
                    venue=paper_data.get("venue"),
                    abstract=paper_data.get("abstract"),
                    link=paper_data.get("link"),
                    citation_count=paper_data.get("citationCount", 0),
                    intersection_count=1,
                    encounter_count=1,
                ).on_conflict_do_update(
                    index_elements=['paper_id', 'scholar_id'],
                    set_={'encounter_count': Citation.encounter_count + 1}
                )
                await db.execute(stmt)
                saved_count += 1

            await db.commit()
            log_now(f"[RETRY] ✓ Saved {saved_count} citations from buffered page {page.page_num}")
            return saved_count

        except Exception as e:
            log_now(f"[RETRY] ✗ Failed to save buffered page {page.page_num}: {e}")
            try:
                await db.rollback()
            except:
                pass
            raise


async def update_edition_harvest_stats(db: AsyncSession, edition_id: int):
    """Update edition harvest tracking after citation extraction"""
    from sqlalchemy import func

    current_year = datetime.now().year

    # Count actual harvested citations for this edition
    result = await db.execute(
        select(func.count(Citation.id)).where(Citation.edition_id == edition_id)
    )
    harvested_count = result.scalar() or 0

    # Update edition
    await db.execute(
        update(Edition)
        .where(Edition.id == edition_id)
        .values(
            last_harvested_at=datetime.utcnow(),
            last_harvest_year=current_year,
            harvested_citation_count=harvested_count
        )
    )
    await db.commit()
    log_now(f"[Harvest] Updated edition {edition_id}: last_harvested_at=now, year={current_year}, count={harvested_count}")


async def update_paper_harvest_stats(db: AsyncSession, paper_id: int):
    """Update paper-level aggregate harvest stats from edition data"""
    from sqlalchemy import func

    # Get aggregates from editions
    result = await db.execute(
        select(
            func.max(Edition.last_harvested_at),
            func.sum(Edition.harvested_citation_count)
        ).where(Edition.paper_id == paper_id)
    )
    row = result.first()

    any_harvested_at = row[0] if row else None
    total_harvested = row[1] or 0 if row else 0

    # Update paper
    await db.execute(
        update(Paper)
        .where(Paper.id == paper_id)
        .values(
            any_edition_harvested_at=any_harvested_at,
            total_harvested_citations=int(total_harvested)
        )
    )
    await db.commit()
    log_now(f"[Harvest] Updated paper {paper_id}: any_harvested_at={any_harvested_at}, total={total_harvested}")

    # Update thinker total_citations if this paper belongs to a thinker
    await update_thinker_citation_stats(db, paper_id)


async def update_thinker_citation_stats(db: AsyncSession, paper_id: int):
    """Update thinker's total_citations if this paper belongs to a thinker work."""
    from sqlalchemy import func

    # Find if this paper is linked to a thinker work
    result = await db.execute(
        select(ThinkerWork.thinker_id)
        .where(ThinkerWork.paper_id == paper_id)
        .where(ThinkerWork.decision == "accepted")
    )
    thinker_id = result.scalar()

    if not thinker_id:
        return  # Not a thinker paper

    # Calculate total citations for this thinker
    citation_result = await db.execute(
        select(func.count(Citation.id.distinct()))
        .select_from(ThinkerWork)
        .join(Paper, ThinkerWork.paper_id == Paper.id)
        .join(Citation, Citation.paper_id == Paper.id)
        .where(ThinkerWork.thinker_id == thinker_id)
        .where(ThinkerWork.decision == "accepted")
    )
    total_citations = citation_result.scalar() or 0

    # Update thinker
    await db.execute(
        update(Thinker)
        .where(Thinker.id == thinker_id)
        .values(total_citations=total_citations)
    )
    await db.commit()
    log_now(f"[Harvest] Updated thinker {thinker_id} total_citations: {total_citations}")


async def create_or_update_harvest_target(
    db: AsyncSession,
    edition_id: int,
    year: Optional[int],
    expected_count: int
) -> HarvestTarget:
    """Create or update a HarvestTarget record for tracking expected vs actual citations.

    Called when we start harvesting a year and know the expected count from Scholar.
    """
    # Check for existing target for this edition+year
    result = await db.execute(
        select(HarvestTarget).where(
            HarvestTarget.edition_id == edition_id,
            HarvestTarget.year == year
        )
    )
    target = result.scalar_one_or_none()

    if target:
        # Update expected count if it's higher (Scholar may report different counts)
        if expected_count > target.expected_count:
            old_expected = target.expected_count
            target.expected_count = expected_count
            target.updated_at = datetime.utcnow()

            # If target was marked complete but expected increased significantly,
            # reset to harvesting so it gets re-scanned
            if target.status == 'complete' and target.pages_attempted == 0 and expected_count > 0:
                target.status = 'harvesting'
                target.completed_at = None
                log_now(f"[HarvestTarget] RESET edition {edition_id} year {year}: expected increased {old_expected} -> {expected_count}, was falsely complete, resetting to harvesting")
            else:
                log_now(f"[HarvestTarget] Updated edition {edition_id} year {year}: expected={expected_count}")

            await db.commit()
    else:
        # Create new target
        target = HarvestTarget(
            edition_id=edition_id,
            year=year,
            expected_count=expected_count,
            actual_count=0,
            status="harvesting",
        )
        db.add(target)
        await db.commit()
        log_now(f"[HarvestTarget] Created for edition {edition_id} year {year}: expected={expected_count}")

    return target


async def update_harvest_target_progress(
    db: AsyncSession,
    edition_id: int,
    year: Optional[int],
    actual_count: int,
    pages_succeeded: int = 0,
    pages_failed: int = 0,
    pages_attempted: int = 0,
    mark_complete: bool = False,
    # Gap tracking parameters (NEW)
    first_gs_count: Optional[int] = None,
    last_gs_count: Optional[int] = None,
    gap_reason: Optional[str] = None,
    gap_details: Optional[dict] = None,
):
    """Update a HarvestTarget with progress data after harvesting.

    Gap tracking parameters:
    - first_gs_count: GS's reported count on page 0 (the "expected" that gets saved)
    - last_gs_count: GS's reported count on the final page (may differ from first!)
    - gap_reason: One of: gs_estimate_changed, rate_limit, parse_error, max_pages_reached,
                  blocked, captcha, empty_page, pagination_ended, unknown
    - gap_details: Additional context as JSONB (e.g., error messages, page numbers)
    """
    result = await db.execute(
        select(HarvestTarget).where(
            HarvestTarget.edition_id == edition_id,
            HarvestTarget.year == year
        )
    )
    target = result.scalar_one_or_none()

    if target:
        target.actual_count = actual_count
        target.pages_succeeded = pages_succeeded
        target.pages_failed = pages_failed
        target.pages_attempted = pages_attempted
        target.updated_at = datetime.utcnow()

        # Update gap tracking fields if provided
        if first_gs_count is not None:
            # Store original expected count (what GS showed on page 0)
            target.original_expected = first_gs_count
        if last_gs_count is not None:
            # Store final GS count (what GS showed on last page - may differ!)
            target.final_gs_count = last_gs_count
        if pages_attempted is not None and pages_attempted > 0:
            target.last_scraped_page = pages_attempted

        # Auto-determine gap reason if not provided
        if gap_reason:
            target.gap_reason = gap_reason
        elif first_gs_count is not None and last_gs_count is not None:
            # Auto-detect if GS estimate changed during pagination
            if first_gs_count != last_gs_count:
                target.gap_reason = "gs_estimate_changed"
                if not gap_details:
                    gap_details = {}
                gap_details["first_gs_count"] = first_gs_count
                gap_details["last_gs_count"] = last_gs_count
                gap_details["estimate_change"] = last_gs_count - first_gs_count
                log_now(f"[HarvestTarget] 📊 GS estimate changed: {first_gs_count} → {last_gs_count} (diff: {last_gs_count - first_gs_count})")
            elif actual_count < last_gs_count:
                # GS estimate didn't change, but we still have a gap - needs investigation
                target.gap_reason = "unknown"
                if not gap_details:
                    gap_details = {}
                gap_details["expected_vs_actual"] = {
                    "expected": last_gs_count,
                    "actual": actual_count,
                    "gap": last_gs_count - actual_count,
                }

        if gap_details:
            target.gap_details = gap_details

        if mark_complete:
            # Only mark as "complete" if we actually attempted pages OR expected was 0
            # This prevents falsely marking complete when harvesting silently failed
            if pages_attempted > 0 and pages_failed == 0:
                target.status = "complete"
            elif pages_attempted > 0 and pages_failed > 0:
                target.status = "incomplete"
            elif target.expected_count == 0:
                # No citations expected and none attempted = legitimately complete
                target.status = "complete"
            else:
                # Expected > 0 but no pages attempted = something went wrong
                target.status = "incomplete"
                log_now(f"[HarvestTarget] WARNING: edition {edition_id} year {year} has expected={target.expected_count} but 0 pages attempted - marking incomplete")
            target.completed_at = datetime.utcnow()

        # AUTO-COMPLETE for GS estimate changes:
        # If gap is due to GS changing its estimate AND we got all the results GS now reports,
        # mark as complete since there's nothing more we can do
        if (target.gap_reason == "gs_estimate_changed" and
            target.final_gs_count is not None and
            actual_count >= target.final_gs_count * 0.95):  # 95% threshold for tolerance
            if target.status == "incomplete":
                target.status = "complete"
                target.completed_at = datetime.utcnow()
                log_now(f"[HarvestTarget] ✓ Auto-completing edition {edition_id} year {year}: GS estimate changed from {target.original_expected} to {target.final_gs_count}, we have {actual_count}")

        await db.commit()
        status_str = f", status={target.status}" if mark_complete else ""
        gap_str = f", gap_reason={target.gap_reason}" if target.gap_reason else ""
        log_now(f"[HarvestTarget] Updated edition {edition_id} year {year}: actual={actual_count}, pages OK={pages_succeeded}, pages FAIL={pages_failed}{status_str}{gap_str}")


async def record_failed_fetch(
    db: AsyncSession,
    edition_id: int,
    url: str,
    page_number: int,
    year: Optional[int],
    error: str
) -> FailedFetch:
    """Record a failed page fetch for later retry.

    Called when all retry attempts fail for a page.
    """
    # Check if we already have this failure recorded
    result = await db.execute(
        select(FailedFetch).where(
            FailedFetch.edition_id == edition_id,
            FailedFetch.page_number == page_number,
            FailedFetch.year == year,
            FailedFetch.status == "pending"
        )
    )
    existing = result.scalar_one_or_none()

    if existing:
        # Update retry count
        existing.retry_count += 1
        existing.last_retry_at = datetime.utcnow()
        existing.last_error = error[:500] if error else None
        await db.commit()
        log_now(f"[FailedFetch] Updated existing: edition {edition_id}, year {year}, page {page_number}, retry #{existing.retry_count}")
        return existing

    # Create new record
    failed = FailedFetch(
        edition_id=edition_id,
        url=url,
        year=year,
        page_number=page_number,
        last_error=error[:500] if error else None,
        status="pending",
    )
    db.add(failed)
    await db.commit()
    log_now(f"[FailedFetch] Recorded: edition {edition_id}, year {year}, page {page_number}, error: {error[:100]}...")
    return failed


# Retry settings
FAILED_FETCH_RETRY_INTERVAL_MINUTES = 60  # Wait at least this long before retrying
MAX_FAILED_FETCH_RETRIES = 5  # Abandon after this many retries
FAILED_FETCH_CHECK_INTERVAL = 300  # Check for pending retries every 5 minutes
_last_failed_fetch_check = None


async def find_pending_failed_fetches(db: AsyncSession, limit: int = 50) -> List[FailedFetch]:
    """Find failed fetches that are ready to be retried.

    Returns failed fetches where:
    - status is 'pending'
    - retry_count < MAX_FAILED_FETCH_RETRIES
    - last_retry_at is null OR > FAILED_FETCH_RETRY_INTERVAL_MINUTES ago
    """
    from sqlalchemy import and_, or_

    cutoff_time = datetime.utcnow() - timedelta(minutes=FAILED_FETCH_RETRY_INTERVAL_MINUTES)

    result = await db.execute(
        select(FailedFetch)
        .where(
            FailedFetch.status == "pending",
            FailedFetch.retry_count < MAX_FAILED_FETCH_RETRIES,
            or_(
                FailedFetch.last_retry_at.is_(None),
                FailedFetch.last_retry_at < cutoff_time
            )
        )
        .order_by(FailedFetch.created_at.asc())
        .limit(limit)
    )
    return list(result.scalars().all())


async def process_retry_failed_fetches_job(job: Job, db: AsyncSession) -> Dict[str, Any]:
    """Process a retry_failed_fetches job - attempt to fetch previously failed pages."""
    log_now("="*70)
    log_now(f"RETRY_FAILED_FETCHES JOB START - Job {job.id}")
    log_now("="*70)

    params = json.loads(job.params) if job.params else {}
    max_retries = params.get("max_retries", 50)

    # Find pending failed fetches
    pending = await find_pending_failed_fetches(db, limit=max_retries)

    if not pending:
        log_now("[RetryFailed] No pending failed fetches found")
        return {"retried": 0, "succeeded": 0, "failed_again": 0}

    log_now(f"[RetryFailed] Found {len(pending)} failed fetches to retry")

    scholar_service = get_scholar_service()
    succeeded = 0
    failed_again = 0
    total_recovered = 0

    for i, failed_fetch in enumerate(pending):
        await update_job_progress(
            db, job.id,
            (i / len(pending)) * 90,
            f"Retrying {i+1}/{len(pending)}: edition {failed_fetch.edition_id}, year {failed_fetch.year}, page {failed_fetch.page_number}"
        )

        # Mark as retrying
        failed_fetch.status = "retrying"
        failed_fetch.last_retry_at = datetime.utcnow()
        failed_fetch.retry_count += 1
        await db.commit()

        try:
            # Get the edition for scholar_id
            edition_result = await db.execute(
                select(Edition).where(Edition.id == failed_fetch.edition_id)
            )
            edition = edition_result.scalar_one_or_none()

            if not edition or not edition.scholar_id:
                log_now(f"[RetryFailed] Edition {failed_fetch.edition_id} not found or no scholar_id")
                failed_fetch.status = "abandoned"
                failed_fetch.last_error = "Edition not found or no scholar_id"
                await db.commit()
                continue

            # Try to fetch the specific page
            log_now(f"[RetryFailed] Attempting to fetch page {failed_fetch.page_number} for edition {edition.id}, year {failed_fetch.year}")

            result = await scholar_service.get_cited_by(
                scholar_id=edition.scholar_id,
                max_results=10,  # Just one page
                year_low=failed_fetch.year,
                year_high=failed_fetch.year,
                start_page=failed_fetch.page_number,
            )

            papers = result.get("papers", []) if isinstance(result, dict) else []

            if papers:
                # SUCCESS - save the citations
                log_now(f"[RetryFailed] ✓ Got {len(papers)} papers from retry")

                # Get existing citations to avoid duplicates
                existing_result = await db.execute(
                    select(Citation.scholar_id).where(Citation.paper_id == edition.paper_id)
                )
                existing_ids = {r[0] for r in existing_result.fetchall() if r[0]}

                new_count = 0
                for paper_data in papers:
                    scholar_id = paper_data.get("scholarId")
                    if not scholar_id or scholar_id in existing_ids:
                        continue

                    # Serialize author_profiles to JSON string
                    author_profiles_json = None
                    if paper_data.get("authorProfiles"):
                        author_profiles_json = json.dumps(paper_data["authorProfiles"])

                    citation = Citation(
                        paper_id=edition.paper_id,
                        edition_id=edition.id,
                        scholar_id=scholar_id,
                        title=paper_data.get("title", "Unknown"),
                        authors=paper_data.get("authorsRaw"),
                        author_profiles=author_profiles_json,
                        year=paper_data.get("year"),
                        venue=paper_data.get("venue"),
                        abstract=paper_data.get("abstract"),
                        link=paper_data.get("link"),
                        citation_count=paper_data.get("citationCount", 0),
                        intersection_count=1,
                    )
                    db.add(citation)
                    existing_ids.add(scholar_id)
                    new_count += 1

                await db.commit()

                # Mark as succeeded
                failed_fetch.status = "succeeded"
                failed_fetch.recovered_citations = new_count
                failed_fetch.resolved_at = datetime.utcnow()
                await db.commit()

                succeeded += 1
                total_recovered += new_count
                log_now(f"[RetryFailed] ✓ Recovered {new_count} citations from page {failed_fetch.page_number}")

            else:
                # Got empty response - might be a real empty page or still failing
                log_now(f"[RetryFailed] Got empty response for page {failed_fetch.page_number}")
                if failed_fetch.retry_count >= MAX_FAILED_FETCH_RETRIES:
                    failed_fetch.status = "abandoned"
                    failed_fetch.last_error = "Max retries reached, still empty"
                else:
                    failed_fetch.status = "pending"  # Try again later
                await db.commit()
                failed_again += 1

        except Exception as e:
            log_now(f"[RetryFailed] ✗ Retry failed: {e}")
            failed_fetch.last_error = str(e)[:500]

            if failed_fetch.retry_count >= MAX_FAILED_FETCH_RETRIES:
                failed_fetch.status = "abandoned"
                log_now(f"[RetryFailed] Abandoning after {failed_fetch.retry_count} retries")
            else:
                failed_fetch.status = "pending"  # Try again later

            await db.commit()
            failed_again += 1

        # Small delay between retries
        await asyncio.sleep(3)

    log_now(f"[RetryFailed] Complete: {succeeded} succeeded, {failed_again} failed, {total_recovered} citations recovered")

    return {
        "retried": len(pending),
        "succeeded": succeeded,
        "failed_again": failed_again,
        "citations_recovered": total_recovered,
    }


async def auto_retry_failed_fetches(db: AsyncSession) -> int:
    """Check for and queue retry jobs for pending failed fetches. Returns jobs queued."""
    global _last_failed_fetch_check

    # Rate limit checks
    now = datetime.utcnow()
    if _last_failed_fetch_check and (now - _last_failed_fetch_check).total_seconds() < FAILED_FETCH_CHECK_INTERVAL:
        return 0
    _last_failed_fetch_check = now

    # Check if there are pending failed fetches
    pending = await find_pending_failed_fetches(db, limit=1)
    if not pending:
        return 0

    # Check if there's already a pending/running retry job
    existing_job = await db.execute(
        select(Job).where(
            Job.job_type == "retry_failed_fetches",
            Job.status.in_(["pending", "running"])
        )
    )
    if existing_job.scalar_one_or_none():
        return 0

    # Queue a retry job
    job = Job(
        job_type="retry_failed_fetches",
        status="pending",
        params=json.dumps({"max_retries": 50}),
        progress=0,
        progress_message="Queued: Retry failed page fetches",
    )
    db.add(job)
    await db.commit()
    log_now("[AutoRetry] Queued retry_failed_fetches job")
    return 1


# Auto-resume settings
AUTO_RESUME_MIN_MISSING = 50  # Only resume if at least this many citations missing (lowered from 100)
AUTO_RESUME_MIN_PERCENT = 0.05  # Or at least 5% missing (lowered from 10%)
AUTO_RESUME_CHECK_INTERVAL = 15  # Seconds between auto-resume checks (lowered from 60)
AUTO_RESUME_MAX_STALL_COUNT = 20  # Stop auto-resume after this many consecutive zero-progress jobs (increased from 5 to 20)
_last_auto_resume_check = None

# Job creation rate monitoring - detect runaway job creation
JOB_CREATION_WINDOW_SECONDS = 60  # Track jobs created in this window
JOB_CREATION_ALERT_THRESHOLD = 50  # Alert if more than this many jobs created in window
_job_creation_times: List[datetime] = []  # Track recent job creation times


def monitor_job_creation_rate(paper_id: int, job_type: str) -> None:
    """Monitor job creation rate and log warnings if abnormally high.

    This helps detect bugs like the auto-resume duplicate issue early.
    """
    global _job_creation_times

    now = datetime.utcnow()
    cutoff = now - timedelta(seconds=JOB_CREATION_WINDOW_SECONDS)

    # Clean up old entries
    _job_creation_times = [t for t in _job_creation_times if t > cutoff]

    # Add this job
    _job_creation_times.append(now)

    # Check if rate is abnormal
    job_count = len(_job_creation_times)
    if job_count > JOB_CREATION_ALERT_THRESHOLD:
        logger.warning(
            f"⚠️ HIGH JOB CREATION RATE: {job_count} jobs created in last "
            f"{JOB_CREATION_WINDOW_SECONDS}s! Latest: {job_type} for paper {paper_id}. "
            f"This may indicate a bug causing duplicate job creation."
        )
        log_now(
            f"⚠️ ALERT: High job creation rate detected - {job_count} jobs in {JOB_CREATION_WINDOW_SECONDS}s",
            "WARNING"
        )


async def find_incomplete_harvests(db: AsyncSession) -> List[Edition]:
    """Find editions with incomplete harvests that should be resumed.

    Returns editions where:
    - selected = True (we want their citations)
    - scholar_id is set (can actually be harvested)
    - citation_count <= skip_threshold (not too large to process)
    - harvested_citation_count < citation_count (incomplete)
    - Gap is significant (at least AUTO_RESUME_MIN_MISSING or AUTO_RESUME_MIN_PERCENT)
    - No pending/running extract_citations job for that paper
    - Paper is not paused (harvest_paused = False)
    - Harvest is not stalled (harvest_stall_count < AUTO_RESUME_MAX_STALL_COUNT)
    - Harvest is not marked complete (harvest_complete = False - gap is GS's fault)
    - Has real work to do (either no harvest_targets, or at least one incomplete target)
    """
    from sqlalchemy import and_, or_, not_, exists
    from sqlalchemy.orm import joinedload

    # Subquery: papers with pending/running extract_citations jobs
    papers_with_jobs = (
        select(Job.paper_id)
        .where(
            Job.job_type == "extract_citations",
            Job.status.in_(["pending", "running"])
        )
    )

    # Subquery: paused papers
    paused_papers = (
        select(Paper.id)
        .where(Paper.harvest_paused == True)
    )

    # Subquery: check if edition has no harvest_targets at all
    has_no_targets = ~exists(
        select(HarvestTarget.id).where(HarvestTarget.edition_id == Edition.id)
    )

    # Subquery: check if edition has at least one incomplete harvest_target
    # Include targets with expected_count=0 (never queried yet) OR expected_count > 0 and not complete
    has_incomplete_target = exists(
        select(HarvestTarget.id).where(
            HarvestTarget.edition_id == Edition.id,
            HarvestTarget.status != 'complete',
            or_(
                HarvestTarget.expected_count > 0,  # Has known work to do
                and_(HarvestTarget.expected_count == 0, HarvestTarget.actual_count == 0)  # Never queried
            )
        )
    )

    # Find incomplete editions
    result = await db.execute(
        select(Edition)
        .where(
            Edition.selected == True,
            Edition.scholar_id.isnot(None),  # Must have scholar_id to harvest
            Edition.citation_count.isnot(None),
            Edition.citation_count > 0,
            Edition.citation_count <= 50000,  # Skip very large editions (matches skip_threshold)
            Edition.harvested_citation_count < Edition.citation_count,
            # Gap must be significant
            or_(
                Edition.citation_count - Edition.harvested_citation_count >= AUTO_RESUME_MIN_MISSING,
                (Edition.citation_count - Edition.harvested_citation_count) * 1.0 / Edition.citation_count >= AUTO_RESUME_MIN_PERCENT
            ),
            # No active job for this paper
            Edition.paper_id.notin_(papers_with_jobs),
            # Paper is not paused
            Edition.paper_id.notin_(paused_papers),
            # Harvest is not stalled (too many zero-progress jobs)
            or_(
                Edition.harvest_stall_count.is_(None),
                Edition.harvest_stall_count < AUTO_RESUME_MAX_STALL_COUNT
            ),
            # Harvest is not already marked complete (gap is GS's fault, not ours)
            or_(
                Edition.harvest_complete.is_(None),
                Edition.harvest_complete == False
            ),
            # Must have real work to do: either no targets yet, or incomplete targets
            # This filters out "false gap" editions where all harvest_targets are complete
            # (gap is due to duplicate citations in Google Scholar that we correctly deduplicate)
            or_(has_no_targets, has_incomplete_target)
        )
        .order_by(
            # Prioritize: larger gaps first
            (Edition.citation_count - Edition.harvested_citation_count).desc()
        )
        .limit(MAX_CONCURRENT_JOBS)  # Don't queue more than we can process
    )

    return list(result.scalars().all())


async def auto_resume_incomplete_harvests(db: AsyncSession) -> int:
    """Find and queue jobs for incomplete harvests. Returns number of jobs queued.

    IMPORTANT: Groups editions by paper_id to avoid creating multiple jobs
    for the same paper (which would waste API credits fetching same citations).
    """
    global _last_auto_resume_check

    # Rate limit checks
    now = datetime.utcnow()
    if _last_auto_resume_check and (now - _last_auto_resume_check).total_seconds() < AUTO_RESUME_CHECK_INTERVAL:
        return 0
    _last_auto_resume_check = now

    incomplete = await find_incomplete_harvests(db)
    log_now(f"[AutoResume] find_incomplete_harvests returned {len(incomplete) if incomplete else 0} editions")
    if not incomplete:
        log_now("[AutoResume] No incomplete harvests found - all caught up!")
        return 0

    # Safety filter: double-check that editions have real work to do
    # (The query now filters this, but we keep this as a sanity check)
    actually_incomplete = []
    log_now(f"[AutoResume] Verifying {len(incomplete)} editions have real work...")
    for edition in incomplete:
        # Check if this edition has any incomplete harvest_targets with expected > 0
        result = await db.execute(
            select(HarvestTarget)
            .where(HarvestTarget.edition_id == edition.id)
            .where(HarvestTarget.status != 'complete')
            .where(HarvestTarget.expected_count > 0)
            .limit(1)  # Just need to know if ANY exist
        )
        has_incomplete = result.scalar_one_or_none() is not None

        # Also check: if edition has NO harvest_targets at all, it needs to be harvested
        if not has_incomplete:
            count_result = await db.execute(
                select(func.count(HarvestTarget.id))
                .where(HarvestTarget.edition_id == edition.id)
            )
            has_any_targets = count_result.scalar() > 0

            if has_any_targets:
                # Has targets but all complete - skip this edition
                log_now(f"[AutoResume] Skipping edition {edition.id}: all harvest_targets complete (gap is inflated metrics)")
                continue
            # else: No targets yet - needs harvesting

        actually_incomplete.append(edition)

    if not actually_incomplete:
        log_now(f"[AutoResume] Found {len(incomplete)} editions with gaps, but all harvest_targets are complete - no real work to do")
        return 0

    if len(actually_incomplete) < len(incomplete):
        log_now(f"[AutoResume] Filtered {len(incomplete)} candidates down to {len(actually_incomplete)} with incomplete harvest_targets")

    # GROUP editions by paper_id to avoid duplicate jobs for same paper
    # This is critical - multiple editions of same paper share citations!
    editions_by_paper: Dict[int, List[Edition]] = {}
    for edition in actually_incomplete:
        if edition.paper_id not in editions_by_paper:
            editions_by_paper[edition.paper_id] = []
        editions_by_paper[edition.paper_id].append(edition)

    log_now(f"[AutoResume] Found {len(actually_incomplete)} incomplete editions across {len(editions_by_paper)} papers")

    jobs_queued = 0
    jobs_skipped_existing = 0
    for paper_id, paper_editions in editions_by_paper.items():
        # Log all editions for this paper
        total_missing = sum(e.citation_count - e.harvested_citation_count for e in paper_editions)
        edition_ids = [e.id for e in paper_editions]
        log_now(f"[AutoResume] Paper {paper_id}: {len(paper_editions)} editions with {total_missing:,} total missing citations")
        for e in paper_editions:
            missing = e.citation_count - e.harvested_citation_count
            log_now(f"[AutoResume]   - Edition {e.id}: {e.harvested_citation_count}/{e.citation_count} harvested ({missing} missing)")

        # Use create_extract_citations_job with duplicate prevention
        # This returns existing job if one exists, preventing duplicates
        before_create = datetime.utcnow()
        job = await create_extract_citations_job(
            db=db,
            paper_id=paper_id,
            edition_ids=edition_ids,
            max_citations_per_edition=1000,
            skip_threshold=50000,
            is_resume=True,
            resume_message=f"Auto-resume: {len(paper_editions)} editions, {total_missing:,} citations remaining",
        )

        # Check if this was an existing job (created before our call) or a new one
        # Job is new if created_at >= before_create (job was created during our call)
        is_new_job = job.created_at and job.created_at >= before_create
        if is_new_job:
            jobs_queued += 1
            log_now(f"[AutoResume] Queued job {job.id} for paper {paper_id} covering {len(paper_editions)} editions")
        else:
            jobs_skipped_existing += 1
            log_now(f"[AutoResume] Paper {paper_id}: existing job {job.id} found (status={job.status}), skipping duplicate")

    if jobs_queued > 0 or jobs_skipped_existing > 0:
        await db.commit()
        log_now(f"[AutoResume] Queued {jobs_queued} new jobs, skipped {jobs_skipped_existing} (existing jobs found)")

    return jobs_queued


async def process_fetch_more_job(job: Job, db: AsyncSession) -> Dict[str, Any]:
    """Process a fetch_more_editions job"""
    params = json.loads(job.params) if job.params else {}
    paper_id = job.paper_id
    language = params.get("language", "english")
    max_results = params.get("max_results", 50)

    log_now(f"[Worker] Processing fetch_more job {job.id} for paper {paper_id}, language={language}")

    # Get paper
    result = await db.execute(select(Paper).where(Paper.id == paper_id))
    paper = result.scalar_one_or_none()
    if not paper:
        raise ValueError(f"Paper {paper_id} not found")

    # Clear NEW status from previous fetch jobs for this paper+language
    await db.execute(
        update(Edition)
        .where(Edition.paper_id == paper_id)
        .where(Edition.language.ilike(f"%{language}%"))
        .values(added_by_job_id=None)
    )
    await db.commit()

    # Get existing editions for duplicate check
    existing_result = await db.execute(
        select(Edition.scholar_id, Edition.title).where(Edition.paper_id == paper_id)
    )
    existing_editions = {(e.scholar_id, e.title.lower()) for e in existing_result.fetchall()}

    # Create discovery service
    service = EditionDiscoveryService(
        language_strategy="custom",
        custom_languages=[language],
    )

    paper_dict = {
        "title": paper.title,
        "authors": paper.authors,
        "year": paper.year,
    }

    # Progress callback to update job status
    async def progress_callback(progress_data: Dict[str, Any]):
        stage = progress_data.get("stage", "")
        if stage == "generating_queries":
            await update_job_progress(db, job.id, 5, "Generating search queries...")
        elif stage == "searching":
            query_num = progress_data.get("query", 0)
            total = progress_data.get("total_queries", 1)
            pct = 10 + (query_num / total) * 50  # Searching is 10-60%
            current_query = progress_data.get('current_query', '')[:40]
            msg = f"Searching [{query_num}/{total}]: {current_query}..."
            await update_job_progress(db, job.id, pct, msg)
        elif stage == "evaluating":
            total = progress_data.get("total_results", 0)
            await update_job_progress(db, job.id, 65, f"Evaluating {total} results with LLM...")

    # Run discovery with progress callback
    discovery_result = await service.fetch_more_in_language(
        paper=paper_dict,
        target_language=language,
        max_results=max_results,
        progress_callback=progress_callback,
    )

    # Save raw results for debugging/auditing (before LLM processing)
    raw_results = discovery_result.get("rawResults", [])
    if raw_results:
        queries_used = discovery_result.get("queriesUsed", [])
        # Handle both dict and string formats for queries
        def get_query_str(q):
            if isinstance(q, dict):
                return q.get("query", str(q))[:100]
            return str(q)[:100]
        query_str = "; ".join([get_query_str(q) for q in queries_used[:3]]) if queries_used else "unknown"

        raw_search_record = RawSearchResult(
            paper_id=paper_id,
            job_id=job.id,
            search_type="fetch_more",
            target_language=language,
            query=query_str,
            raw_results=json.dumps(raw_results, ensure_ascii=False),
            result_count=len(raw_results),
            llm_classification=json.dumps(discovery_result.get("llmClassification", {}), ensure_ascii=False),
        )
        db.add(raw_search_record)
        log_now(f"[Worker] Saved {len(raw_results)} raw results for debugging")

    # Store new editions
    new_editions = []
    genuine = discovery_result.get("genuineEditions", [])
    total = len(genuine)

    if total > 0:
        await update_job_progress(db, job.id, 70, f"Saving {total} editions to database...")

    for i, edition_data in enumerate(genuine):
        scholar_id = edition_data.get("scholarId")
        title = edition_data.get("title", "")

        # Check for duplicates
        if (scholar_id, title.lower()) in existing_editions:
            continue

        edition = Edition(
            paper_id=paper_id,
            scholar_id=scholar_id,
            cluster_id=edition_data.get("clusterId"),
            title=title,
            authors=edition_data.get("authorsRaw"),
            year=edition_data.get("year"),
            venue=edition_data.get("venue"),
            abstract=edition_data.get("abstract"),
            link=edition_data.get("link"),
            citation_count=edition_data.get("citationCount", 0),
            language=edition_data.get("language", language.capitalize()),
            confidence=edition_data.get("confidence", "uncertain"),
            auto_selected=edition_data.get("autoSelected", False),
            selected=edition_data.get("confidence") == "high",
            is_supplementary=True,
            added_by_job_id=job.id,  # Mark as NEW from this job
        )
        db.add(edition)
        new_editions.append({
            "title": title,
            "scholar_id": scholar_id,
            "language": edition_data.get("language"),
        })
        existing_editions.add((scholar_id, title.lower()))

        # Update progress (70-95% for storing)
        if total > 0 and i % 5 == 0:  # Update every 5 editions to reduce DB writes
            pct = 70 + ((i + 1) / total) * 25
            await update_job_progress(db, job.id, pct, f"Saving edition {i+1}/{total}...")

    await db.commit()

    return {
        "paper_id": paper_id,
        "language": language,
        "new_editions_found": len(new_editions),
        "total_results_searched": discovery_result.get("totalSearched", 0),
        "queries_used": discovery_result.get("queriesUsed", []),
        "editions": new_editions,
    }


async def process_extract_citations_job(job: Job, db: AsyncSession) -> Dict[str, Any]:
    """Process a citation extraction job - fetch all papers citing selected editions"""
    log_now("="*70)
    log_now(f"EXTRACT_CITATIONS JOB START - Job {job.id}")
    log_now("="*70)
    log_now(f"Job ID: {job.id}")
    log_now(f"Paper ID: {job.paper_id}")
    log_now(f"Raw params: {job.params}")

    params = json.loads(job.params) if job.params else {}
    paper_id = job.paper_id
    edition_ids = params.get("edition_ids", [])  # Empty = all selected
    max_citations_per_edition = params.get("max_citations_per_edition", 1000)
    skip_threshold = params.get("skip_threshold", 50000)  # Skip editions with > this many citations

    # Refresh mode params
    is_refresh = params.get("is_refresh", False)
    year_low_param = params.get("year_low")  # Year to start fetching from (for incremental refresh)
    batch_id = params.get("batch_id")  # UUID for batch tracking

    log_now(f"[Worker] Parsed params: edition_ids={edition_ids}, max_citations={max_citations_per_edition}, skip_threshold={skip_threshold}")
    if is_refresh:
        log_now(f"[Worker] REFRESH MODE: year_low={year_low_param}, batch_id={batch_id}")

    # Get paper
    result = await db.execute(select(Paper).where(Paper.id == paper_id))
    paper = result.scalar_one_or_none()
    if not paper:
        raise ValueError(f"Paper {paper_id} not found")

    # Get editions to process
    if edition_ids:
        edition_query = select(Edition).where(Edition.id.in_(edition_ids))
    else:
        edition_query = select(Edition).where(
            Edition.paper_id == paper_id,
            Edition.selected == True
        )
    editions_result = await db.execute(edition_query)
    editions = list(editions_result.scalars().all())

    if not editions:
        raise ValueError("No editions selected for extraction")

    # Filter out editions without scholar_id, above threshold, zero citations, or already complete
    # Also handle merged editions: redirect their citations to the canonical edition
    valid_editions = []
    skipped_editions = []
    # Map merged edition IDs to canonical edition IDs for citation redirection
    merged_to_canonical = {}

    for e in editions:
        # If this edition is merged into another, skip it (it will be processed via its canonical edition)
        if e.merged_into_edition_id:
            merged_to_canonical[e.id] = e.merged_into_edition_id
            skipped_editions.append({
                "id": e.id,
                "title": e.title,
                "reason": f"merged_into_edition_{e.merged_into_edition_id}"
            })
            continue

        if not e.scholar_id:
            skipped_editions.append({"id": e.id, "title": e.title, "reason": "no_scholar_id"})
        elif e.citation_count is None or e.citation_count == 0:
            skipped_editions.append({"id": e.id, "title": e.title, "reason": "zero_citations"})
        elif e.citation_count > skip_threshold:
            skipped_editions.append({
                "id": e.id,
                "title": e.title,
                "reason": f"too_many_citations ({e.citation_count} > {skip_threshold})"
            })
        elif (e.harvested_citation_count or 0) >= e.citation_count:
            skipped_editions.append({
                "id": e.id,
                "title": e.title,
                "reason": f"already_complete ({e.harvested_citation_count}/{e.citation_count})"
            })
        else:
            valid_editions.append(e)

    # For canonical editions, also fetch their merged editions for harvesting
    # This allows harvesting from multiple scholar_ids but directing all citations to the canonical edition
    canonical_ids = [e.id for e in valid_editions]
    merged_editions_result = await db.execute(
        select(Edition).where(
            Edition.merged_into_edition_id.in_(canonical_ids),
            Edition.scholar_id.isnot(None)
        )
    )
    merged_editions_for_harvest = list(merged_editions_result.scalars().all())

    # Build a map of canonical edition ID -> list of merged editions to also harvest
    canonical_to_merged = {}
    for me in merged_editions_for_harvest:
        canonical_id = me.merged_into_edition_id
        if canonical_id not in canonical_to_merged:
            canonical_to_merged[canonical_id] = []
        canonical_to_merged[canonical_id].append(me)
        log_now(f"[Worker] Will also harvest merged edition {me.id} (scholar_id={me.scholar_id}) for canonical edition {canonical_id}")

    if not valid_editions:
        raise ValueError(f"No valid editions to process (all {len(skipped_editions)} skipped)")

    log_now(f"[Worker] Processing {len(valid_editions)} editions, skipped {len(skipped_editions)}")

    # Get existing citations to avoid redundant DB calls within this job run
    # IMPORTANT: Only track citations we've already tried to INSERT this session
    # We used to check paper_id here, but that caused cross-edition duplicates to be
    # skipped entirely (never even reaching the ON CONFLICT handler)
    # Now we start with an empty set and add as we go - the ON CONFLICT handles deduplication
    async def get_existing_scholar_ids():
        # Start empty - DB deduplication via ON CONFLICT handles cross-edition duplicates
        # This ensures we always attempt INSERT for citations we haven't seen this session
        # even if they exist in DB from another edition
        return set()

    existing_scholar_ids = await get_existing_scholar_ids()

    # Calculate totals for progress tracking
    # Query actual DB count for display (existing_scholar_ids is empty by design for deduplication)
    total_target_citations = sum(e.citation_count or 0 for e in valid_editions)
    db_count_result = await db.execute(
        select(func.count(Citation.id)).where(Citation.paper_id == paper_id)
    )
    total_previously_harvested = db_count_result.scalar() or 0

    # Initial detailed progress
    await update_job_progress(
        db, job.id, 5,
        f"Processing {len(valid_editions)} editions...",
        details={
            "stage": "initializing",
            "editions_total": len(valid_editions),
            "editions_processed": 0,
            "target_citations_total": total_target_citations,
            "previously_harvested": total_previously_harvested,
            "citations_saved": 0,
            "skipped_editions": len(skipped_editions),
            "editions_info": [
                {
                    "id": e.id,
                    "title": e.title[:60] if e.title else "Unknown",
                    "language": e.language,
                    "citation_count": e.citation_count,
                    "harvested": e.harvested_citation_count or 0,
                }
                for e in valid_editions
            ],
        }
    )

    # Stats tracking
    total_new_citations = 0
    total_updated_citations = 0

    scholar_service = get_scholar_service()
    total_editions = len(valid_editions)

    for i, edition in enumerate(valid_editions):
        edition_start_citations = total_new_citations

        # Track current year for year-by-year mode (accessible in callback)
        current_harvest_year = {"year": None, "mode": "standard"}

        # Target edition ID for citations - normally same as edition.id,
        # but for merged editions it points to their canonical edition
        target_edition_id = edition.id

        # First, update harvest stats to capture any zombie job progress (citations saved but stats not updated)
        await update_edition_harvest_stats(db, edition.id)
        await db.refresh(edition)

        # Calculate resume page from ACTUAL harvested count (updated by update_edition_harvest_stats above)
        # This handles zombie jobs where server died but citations were saved
        calculated_resume_page = 0
        if edition.harvested_citation_count and edition.harvested_citation_count > 0:
            # 10 results per page in Google Scholar
            calculated_resume_page = edition.harvested_citation_count // 10
            log_now(f"[Worker] Edition {edition.id} has {edition.harvested_citation_count} harvested citations -> calculated page {calculated_resume_page}")

        # Also check resume_state from job params (may be stale if created before stats updated)
        params_resume_page = 0
        if params.get("resume_state"):
            resume_state = params["resume_state"]
            if resume_state.get("edition_id") == edition.id:
                params_resume_page = resume_state.get("last_page", 0)
                log_now(f"[Worker] Job params specify resume from page {params_resume_page}")

        # Use the HIGHER of the two to ensure we never go backwards
        resume_page = max(calculated_resume_page, params_resume_page)
        if resume_page > 0:
            log_now(f"[Worker] ✓ Resuming edition {edition.id} from page {resume_page}")

        # Callback to save citations IMMEDIATELY after each page
        # IMPORTANT: Uses fresh DB session to avoid greenlet context issues
        # when callback is invoked from within scholar_search after async context switches
        async def save_page_citations(page_num: int, papers: List[Dict]):
            nonlocal total_new_citations, total_updated_citations, existing_scholar_ids, params, target_edition_id

            log_now(f"[CALLBACK] ═══════════════════════════════════════════════")
            log_now(f"[CALLBACK] save_page_citations called")
            log_now(f"[CALLBACK] page_num: {page_num}")
            log_now(f"[CALLBACK] papers type: {type(papers)}")
            log_now(f"[CALLBACK] papers length: {len(papers) if isinstance(papers, list) else 'NOT A LIST'}")

            if not isinstance(papers, list):
                log_now(f"[CALLBACK] ✗✗✗ PAPERS IS NOT A LIST! Type: {type(papers)}")
                log_now(f"[CALLBACK] Papers value: {papers}")
                raise TypeError(f"Expected list of papers, got {type(papers)}")

            if papers and len(papers) > 0:
                log_now(f"[CALLBACK] First paper: {papers[0]}")
                log_now(f"[CALLBACK] First paper type: {type(papers[0])}")

            new_count = 0
            skipped_no_id = 0

            # STEP 1: Save to local buffer FIRST (resilient to DB timeouts)
            buffer = get_buffer()
            buffer.save_page(
                job_id=job.id,
                paper_id=paper_id,
                edition_id=edition.id,
                target_edition_id=target_edition_id,
                page_num=page_num,
                papers=papers
            )
            log_now(f"[CALLBACK] ✓ Buffered page {page_num + 1} locally ({len(papers)} papers)")

            # STEP 2: Try to save to DB
            # Use a FRESH session to avoid greenlet context issues
            # The parent session's greenlet context can be lost during async operations
            # (rate limits, retries) in scholar_search.py
            # CRITICAL: Wrap the entire session block in try/except to catch CancelledError
            # which can occur during DB connection and corrupts greenlet state
            try:
                async with async_session() as callback_db:
                    for idx, paper_data in enumerate(papers):
                        if not isinstance(paper_data, dict):
                            log_now(f"[CALLBACK] Paper {idx} is not a dict! Type: {type(paper_data)}, Value: {paper_data}")
                            continue

                        scholar_id = paper_data.get("scholarId")
                        if not scholar_id:
                            skipped_no_id += 1
                            continue

                        if scholar_id in existing_scholar_ids:
                            # Already exists in memory - skip
                            total_updated_citations += 1
                        else:
                            # NEW citation - use INSERT ON CONFLICT DO UPDATE to track duplicate encounters
                            # This helps reconcile our count vs GS count (GS tolerates duplicates, we don't)
                            from sqlalchemy import text
                            from sqlalchemy.dialects.postgresql import insert as pg_insert

                            # Use target_edition_id - allows merged editions to redirect citations to canonical
                            from datetime import datetime as dt
                            stmt = pg_insert(Citation).values(
                                paper_id=paper_id,
                                edition_id=target_edition_id,  # May differ from edition.id for merged editions
                                scholar_id=scholar_id,
                                title=paper_data.get("title", "Unknown"),
                                authors=paper_data.get("authorsRaw"),
                                year=paper_data.get("year"),
                                venue=paper_data.get("venue"),
                                abstract=paper_data.get("abstract"),
                                link=paper_data.get("link"),
                                citation_count=paper_data.get("citationCount", 0),
                                intersection_count=1,
                                encounter_count=1,
                                created_at=dt.utcnow(),  # MUST set explicitly for pg_insert (ORM defaults don't apply)
                            ).on_conflict_do_update(
                                index_elements=['paper_id', 'scholar_id'],
                                set_={'encounter_count': Citation.encounter_count + 1}
                            )
                            result = await callback_db.execute(stmt)

                            # rowcount is always 1 for upserts (insert or update)
                            # We need to check xmax to determine if this was an insert
                            # For now, track based on memory set (if not in set, it's new to this run)
                            if scholar_id not in existing_scholar_ids:
                                new_count += 1
                                total_new_citations += 1
                            else:
                                # Duplicate detected - already exists from concurrent job or earlier in this run
                                total_updated_citations += 1

                            existing_scholar_ids.add(scholar_id)

                    # COMMIT IMMEDIATELY after each page
                    await callback_db.commit()

                    # STEP 3: DB save successful - remove from buffer
                    buffer.mark_saved(job.id, page_num)
                    log_now(f"[CALLBACK] ✓ DB save successful, buffer cleared")
                    log_now(f"[CALLBACK] ✓ Page {page_num + 1} complete: {new_count} new, {skipped_no_id} skipped (no ID), total: {total_new_citations}")

                    # Log citation saves for activity stats
                    if new_count > 0:
                        asyncio.create_task(log_api_call(
                            call_type='citation_save',
                            job_id=job.id,
                            edition_id=target_edition_id,
                            count=new_count,
                            success=True
                        ))

                    # Update job progress with current state for resume
                    # Cap progress at 90% (leave 10% for completion), handle year-by-year mode with many pages
                    raw_progress = 10 + ((i + min(page_num / 100, 0.9)) / total_editions) * 80
                    progress_pct = min(raw_progress, 90)
                    year_info = f" ({current_harvest_year['year']})" if current_harvest_year['year'] else ""

                    # Update job progress using fresh session
                    # Store progress details in params["progress_details"] (Job model has no 'details' column)
                    params["progress_details"] = {
                        "edition_index": i + 1,
                        "editions_total": total_editions,
                        "edition_id": edition.id,
                        "edition_title": edition.title[:80] if edition.title else "Unknown",
                        "edition_language": edition.language,
                        "edition_citation_count": edition.citation_count,
                        "current_page": page_num + 1,
                        "current_year": current_harvest_year.get("year"),
                        "harvest_mode": current_harvest_year.get("mode", "standard"),
                        "citations_saved": total_new_citations,
                        "citations_this_edition": total_new_citations - edition_start_citations,
                        "citations_updated": total_updated_citations,
                        "stage": "harvesting",
                        # CRITICAL: Include these for UI display (was missing, causing "Already Had: 0" bug)
                        "previously_harvested": total_previously_harvested,
                        "target_citations_total": total_target_citations,
                    }
                    await callback_db.execute(
                        update(Job)
                        .where(Job.id == job.id)
                        .values(
                            progress=progress_pct,
                            progress_message=f"Edition {i+1}/{total_editions}{year_info}, page {page_num + 1}: {total_new_citations} citations saved",
                            params=json.dumps(params),
                        )
                    )
                    await callback_db.commit()

                    # Save resume state (update params dict and serialize to job)
                    # MUST be inside fresh session block to avoid greenlet issues
                    params["resume_state"] = {
                        "edition_id": edition.id,
                        "last_page": page_num + 1,
                        "total_citations": total_new_citations,
                    }
                    await callback_db.execute(
                        update(Job)
                        .where(Job.id == job.id)
                        .values(params=json.dumps(params))
                    )
                    await callback_db.commit()

            except asyncio.CancelledError:
                # Task was cancelled (timeout, shutdown, etc.) - this corrupts greenlet state
                # Mark for buffer retry and re-raise to propagate cancellation
                log_now(f"[CALLBACK] ⚠ Task cancelled during DB save - page {page_num + 1} buffered for retry")
                buffer.mark_failed(job.id, page_num, "CancelledError during DB save")
                raise

            except Exception as insert_err:
                log_now(f"[CALLBACK] ✗ Error inserting citations: {insert_err}")

                # STEP 3b: DB save failed - mark in buffer for retry
                buffer.mark_failed(job.id, page_num, str(insert_err))
                log_now(f"[CALLBACK] ⚠ Marked for retry in buffer")
                raise  # Re-raise to be handled by caller

        try:
            log_now(f"[EDITION {i+1}/{total_editions}] ═══════════════════════════════════════════════")
            log_now(f"[EDITION {i+1}] Edition ID: {edition.id}")
            log_now(f"[EDITION {i+1}] Scholar ID: {edition.scholar_id}")
            log_now(f"[EDITION {i+1}] Title: {edition.title[:60] if edition.title else 'NO TITLE'}...")
            log_now(f"[EDITION {i+1}] Language: {edition.language}")
            log_now(f"[EDITION {i+1}] Citation count (Scholar): {edition.citation_count}")
            log_now(f"[EDITION {i+1}] max_results: {max_citations_per_edition}")
            log_now(f"[EDITION {i+1}] start_page: {resume_page}")
            log_now(f"[EDITION {i+1}] Previously harvested: {edition.harvested_citation_count}")

            # For editions with >1000 citations, use year-by-year fetching
            current_year = datetime.now().year

            if edition.citation_count and edition.citation_count > YEAR_BY_YEAR_THRESHOLD:
                log_now(f"[EDITION {i+1}] 🗓️ YEAR-BY-YEAR MODE: {edition.citation_count} citations > {YEAR_BY_YEAR_THRESHOLD} threshold")

                # Update progress with edition details
                await update_job_progress(
                    db, job.id, 8 + (i / total_editions) * 10,
                    f"Edition {i+1}/{total_editions}: Year-by-year mode ({edition.citation_count:,} citations)",
                    details={
                        "stage": "year_by_year_init",
                        "edition_index": i + 1,
                        "editions_total": total_editions,
                        "edition_id": edition.id,
                        "edition_title": edition.title[:80] if edition.title else "Unknown",
                        "edition_language": edition.language,
                        "edition_citation_count": edition.citation_count,
                        "edition_harvested": edition.harvested_citation_count or 0,
                        "harvest_mode": "year_by_year",
                        "citations_saved": total_new_citations,
                        "target_citations_total": total_target_citations,
                        "previously_harvested": total_previously_harvested,
                    }
                )

                # For refresh mode, use year_low_param or edition's last harvest year
                # Otherwise, determine min_year dynamically based on edition/paper publication year
                if is_refresh and year_low_param:
                    min_year = year_low_param
                    log_now(f"[EDITION {i+1}] REFRESH: Will fetch from {current_year} backwards to {min_year} (from params)")
                elif is_refresh and edition.last_harvest_year:
                    min_year = edition.last_harvest_year
                    log_now(f"[EDITION {i+1}] REFRESH: Will fetch from {current_year} backwards to {min_year} (from edition last harvest)")
                else:
                    # Determine min_year dynamically:
                    # 1. Use edition's year if it's a plausible original publication year
                    #    (must be > 1900 AND at least 10 years old - recent years are likely reprint metadata)
                    # 2. Fall back to paper's year if available
                    # 3. Default to 1950 for very old or unknown works
                    edition_pub_year = edition.year
                    paper_pub_year = paper.year if hasattr(paper, 'year') else None
                    min_valid_pub_year = current_year - 10  # Years within last 10 years are suspect

                    if edition_pub_year and 1900 < edition_pub_year < min_valid_pub_year:
                        min_year = edition_pub_year
                        log_now(f"[EDITION {i+1}] Using edition publication year: {min_year}")
                    elif paper_pub_year and 1900 < paper_pub_year < min_valid_pub_year:
                        min_year = paper_pub_year
                        log_now(f"[EDITION {i+1}] Using paper publication year: {min_year}")
                    else:
                        min_year = 1950  # Default for older/unknown works (was 1990)
                        log_now(f"[EDITION {i+1}] No valid publication year found (edition={edition_pub_year}, paper={paper_pub_year}), defaulting to {min_year}")

                    log_now(f"[EDITION {i+1}] Will fetch from {current_year} backwards to {min_year}...")

                # Check for existing resume state (year-by-year progress tracking)
                # ALWAYS check harvest_targets first - this is the authoritative source
                # Then merge with resume_state for any in-flight progress
                resume_state_json = edition.harvest_resume_state
                resume_year = None
                resume_page_for_year = 0

                # STEP 1: Always check harvest_targets first - this is the authoritative source
                completed_targets_result = await db.execute(
                    select(HarvestTarget.year)
                    .where(HarvestTarget.edition_id == edition.id)
                    .where(HarvestTarget.status == 'complete')
                )
                completed_years = {row.year for row in completed_targets_result.fetchall()}
                log_now(f"[EDITION {i+1}] 📊 harvest_targets: {len(completed_years)} completed years")

                # STEP 1.5: Check for incomplete targets that are BELOW calculated min_year
                # This handles the case where expected counts were added for older years (e.g., via refresh-expected-counts)
                # but min_year was calculated based on edition.year metadata which may be wrong (e.g., reprint date)
                # Also include targets with expected_count=0 AND actual_count=0 (never queried yet)
                min_incomplete_result = await db.execute(
                    select(func.min(HarvestTarget.year))
                    .where(HarvestTarget.edition_id == edition.id)
                    .where(HarvestTarget.status != 'complete')
                    .where(
                        or_(
                            HarvestTarget.expected_count > 0,  # Has known work to do
                            and_(HarvestTarget.expected_count == 0, HarvestTarget.actual_count == 0)  # Never queried
                        )
                    )
                )
                min_incomplete_year = min_incomplete_result.scalar()

                if min_incomplete_year and min_incomplete_year < min_year:
                    log_now(f"[EDITION {i+1}] ⚠️ Found incomplete harvest_targets down to year {min_incomplete_year} (below calculated min_year {min_year})")
                    log_now(f"[EDITION {i+1}] 📅 EXTENDING min_year from {min_year} to {min_incomplete_year} to cover all incomplete targets")
                    min_year = min_incomplete_year

                # STEP 2: Check resume_state for resume position (year/page to continue from)
                if resume_state_json:
                    try:
                        resume_state = json.loads(resume_state_json)
                        if resume_state.get("mode") == "year_by_year":
                            # Get resume position
                            resume_year = resume_state.get("current_year")
                            resume_page_for_year = resume_state.get("current_page", 0)

                            # BUG FIX: Do NOT merge resume_state.completed_years - use harvest_targets.status as source of truth
                            # The resume_state may have incorrectly marked years as "completed" even when they're incomplete
                            # This was causing years with status='incomplete' in harvest_targets to be skipped
                            resume_completed_count = len(resume_state.get("completed_years", []))
                            if resume_completed_count != len(completed_years):
                                log_now(f"[EDITION {i+1}] ⚠️ resume_state had {resume_completed_count} completed years, but harvest_targets says {len(completed_years)} - USING harvest_targets as truth")

                            log_now(f"[EDITION {i+1}] 🔄 RESUMING: {len(completed_years)} completed years (from harvest_targets), resume from year {resume_year} page {resume_page_for_year}")
                    except json.JSONDecodeError:
                        log_now(f"[EDITION {i+1}] ⚠️ Could not parse resume state")
                else:
                    log_now(f"[EDITION {i+1}] ℹ️ No resume state - starting from current year")

                if completed_years:
                    log_now(f"[EDITION {i+1}] ✓ Will skip {len(completed_years)} completed years: {sorted(completed_years, reverse=True)[:10]}...")

                # Helper to save year-by-year resume state to edition
                # IMPORTANT: Uses fresh DB session to avoid greenlet context issues
                async def save_year_resume_state(year: int, page: int, completed: set):
                    """Save current progress to edition.harvest_resume_state"""
                    state = {
                        "mode": "year_by_year",
                        "current_year": year,
                        "current_page": page,
                        "completed_years": sorted(list(completed), reverse=True),
                    }
                    # Use fresh session to avoid greenlet context issues
                    async with async_session() as state_db:
                        await state_db.execute(
                            update(Edition)
                            .where(Edition.id == edition.id)
                            .values(harvest_resume_state=json.dumps(state))
                        )
                        await state_db.commit()

                # Modify save_page_citations to also save year state
                original_save_page_citations = save_page_citations

                async def save_page_citations_with_year_state(page_num: int, papers: List[Dict]):
                    """Extended callback that also saves year-by-year state"""
                    await original_save_page_citations(page_num, papers)
                    # Save resume state after each page
                    current_yr = current_harvest_year.get("year")
                    if current_yr:
                        await save_year_resume_state(current_yr, page_num + 1, completed_years)

                # Fetch year by year from current year backwards
                current_harvest_year["mode"] = "year_by_year"
                consecutive_empty_years = 0

                # Import partition functions for overflow years
                from .overflow_harvester import harvest_partition, harvest_with_language_stratification

                for year in range(current_year, min_year - 1, -1):
                    # SKIP completed years entirely
                    if year in completed_years:
                        log_now(f"[EDITION {i+1}] 📅 Year {year}: SKIPPING (already completed)")
                        continue

                    # SMART SKIP: Check actual citations in DB vs expected
                    # If we already have >= 90% of expected, mark complete and skip
                    # This prevents wasting Oxylabs calls on years that are "close enough"
                    # (gaps are usually due to GS inconsistency, not missing harvests)
                    target_result = await db.execute(
                        select(HarvestTarget)
                        .where(HarvestTarget.edition_id == edition.id)
                        .where(HarvestTarget.year == year)
                    )
                    existing_target = target_result.scalar_one_or_none()

                    if existing_target and existing_target.expected_count > 0:
                        # Count actual citations in DB for this EDITION+year (not paper!)
                        # Paper can have many editions - must compare edition-specific counts
                        actual_db_count_result = await db.execute(
                            select(func.count(Citation.id))
                            .where(Citation.edition_id == edition.id)
                            .where(Citation.year == year)
                        )
                        actual_db_count = actual_db_count_result.scalar() or 0

                        # If we have >= 90% of expected, mark complete and skip
                        completion_ratio = actual_db_count / existing_target.expected_count
                        if completion_ratio >= 0.90:
                            # Mark as complete with gap reason
                            gap = existing_target.expected_count - actual_db_count
                            await db.execute(
                                update(HarvestTarget)
                                .where(HarvestTarget.id == existing_target.id)
                                .values(
                                    status='complete',
                                    actual_count=actual_db_count,
                                    gap_reason='near_complete',
                                    gap_details=json.dumps({
                                        "completion_ratio": round(completion_ratio, 3),
                                        "gap": gap,
                                        "reason": "Marked complete at 90%+ - remaining gap likely due to GS inconsistency"
                                    })
                                )
                            )
                            await db.commit()
                            completed_years.add(year)
                            log_now(f"[EDITION {i+1}] 📅 Year {year}: SMART SKIP ({actual_db_count}/{existing_target.expected_count} = {completion_ratio:.1%}, gap={gap})")
                            continue

                    year_start_citations = total_new_citations
                    current_harvest_year["year"] = year

                    # Determine start page for this year using ACTUAL DB count (not harvest_target.actual_count)
                    start_page_for_this_year = 0
                    if resume_year == year and resume_page_for_year > 0:
                        # Resume from saved state (preferred - exact page number)
                        start_page_for_this_year = resume_page_for_year
                        log_now(f"[EDITION {i+1}] 📅 Fetching year {year} (RESUMING from saved state page {start_page_for_this_year})...")
                    else:
                        # Count actual citations in DB to calculate resume page
                        # This is more accurate than harvest_target.actual_count which may be stale
                        if existing_target is None:
                            # Fetch target if we didn't already
                            target_result = await db.execute(
                                select(HarvestTarget)
                                .where(HarvestTarget.edition_id == edition.id)
                                .where(HarvestTarget.year == year)
                            )
                            existing_target = target_result.scalar_one_or_none()

                        if existing_target and existing_target.expected_count > 0:
                            # Use actual DB count for resume calculation (already computed in smart skip check above)
                            if actual_db_count > 0:
                                # Calculate resume page from actual DB count (10 citations per page)
                                start_page_for_this_year = actual_db_count // 10
                                log_now(f"[EDITION {i+1}] 📅 Fetching year {year} (RESUMING from page {start_page_for_this_year} based on {actual_db_count} DB citations)...")
                            else:
                                log_now(f"[EDITION {i+1}] 📅 Fetching year {year}...")
                        else:
                            # No existing target or no expected count - fetch actual DB count
                            # Use edition_id (not paper_id) since papers can have many editions
                            actual_db_count_result = await db.execute(
                                select(func.count(Citation.id))
                                .where(Citation.edition_id == edition.id)
                                .where(Citation.year == year)
                            )
                            actual_db_count = actual_db_count_result.scalar() or 0
                            if actual_db_count > 0:
                                start_page_for_this_year = actual_db_count // 10
                                log_now(f"[EDITION {i+1}] 📅 Fetching year {year} (RESUMING from page {start_page_for_this_year} based on {actual_db_count} DB citations)...")
                            else:
                                log_now(f"[EDITION {i+1}] 📅 Fetching year {year}...")

                    # STEP 1: Quick count check - fetch just first page to see total
                    count_result = await scholar_service.get_cited_by(
                        scholar_id=edition.scholar_id,
                        max_results=10,  # Just first page for count
                        year_low=year,
                        year_high=year,
                    )
                    total_this_year = count_result.get('totalResults', 0) if isinstance(count_result, dict) else 0

                    # STEP 1.5: Record the expected count for this year (for completeness tracking)
                    # IMPORTANT: Always create target, even for 0 citations - this marks year as checked
                    await create_or_update_harvest_target(db, edition.id, year, total_this_year)

                    # Calculate year progress for dashboard
                    total_years = current_year - min_year + 1
                    years_completed_count = len(completed_years)
                    years_remaining = total_years - years_completed_count
                    harvest_strategy = "partition" if total_this_year > 1000 else "normal"

                    # Update progress with detailed year info for dashboard
                    year_progress_pct = 10 + (i / total_editions) * 40 + ((current_year - year) / total_years) * 40
                    await update_job_progress(
                        db, job.id, min(year_progress_pct, 95),
                        f"Edition {i+1}/{total_editions}, Year {year}: {total_this_year:,} citations",
                        details={
                            "stage": "harvesting",
                            "edition_index": i + 1,
                            "editions_total": total_editions,
                            "edition_id": edition.id,
                            "edition_title": edition.title[:80] if edition.title else "Unknown",
                            "edition_language": edition.language,
                            "edition_citation_count": edition.citation_count,
                            "edition_harvested": edition.harvested_citation_count or 0,
                            "harvest_mode": "year_by_year",
                            "current_year": year,
                            "year_range_start": current_year,
                            "year_range_end": min_year,
                            "years_total": total_years,
                            "years_completed": years_completed_count,
                            "years_remaining": years_remaining,
                            "year_expected_citations": total_this_year,
                            "year_harvest_strategy": harvest_strategy,
                            "citations_saved": total_new_citations,
                            "target_citations_total": total_target_citations,
                            "previously_harvested": total_previously_harvested,
                        }
                    )

                    # Track pages for this year
                    year_pages_succeeded = 0
                    year_pages_failed = 0
                    year_pages_attempted = 0
                    # Gap tracking for diagnostics
                    year_first_gs_count = None  # GS count from page 0
                    year_last_gs_count = None   # GS count from final page (may differ!)

                    # Create on_page_failed callback for this year
                    async def on_page_failed_for_year(page_num: int, url: str, error: str):
                        nonlocal year_pages_failed, year_pages_attempted
                        year_pages_failed += 1
                        year_pages_attempted += 1
                        await record_failed_fetch(db, edition.id, url, page_num, year, error)

                    # STEP 2: Decide harvest strategy based on count
                    if total_this_year > 1000:
                        # OVERFLOW: Use partition strategy from the start
                        log_now(f"[EDITION {i+1}] 📅 Year {year}: {total_this_year} citations - USING PARTITION STRATEGY")

                        # Track whether this year's harvest succeeded
                        year_harvest_succeeded = False

                        try:
                            # First try stratified language harvesting (non-English first, then English)
                            # This is more effective for highly-cited papers
                            partition_stats = await harvest_with_language_stratification(
                                db=db,
                                scholar_service=scholar_service,
                                edition_id=edition.id,
                                scholar_id=edition.scholar_id,
                                year=year,
                                edition_title=edition.title,
                                paper_id=paper_id,
                                existing_scholar_ids=existing_scholar_ids,
                                on_page_complete=save_page_citations_with_year_state,
                                total_for_year=total_this_year,
                                job_id=job.id,
                            )

                            # NOTE: year_citations from partition_stats is for logging only
                            # The actual total_new_citations was already incremented by save_page_citations callback
                            # during the harvest - DO NOT add again (was causing double counting bug)
                            year_citations = partition_stats.get("total_new", 0)
                            year_harvest_succeeded = partition_stats.get("success", False)

                            if year_harvest_succeeded:
                                # Don't add year_citations - callback already incremented total_new_citations
                                log_now(f"[EDITION {i+1}] 📅 Year {year}: ✓ Stratified harvest complete - {year_citations} new citations")
                            else:
                                # Partition failed (couldn't reduce below 990)
                                error_msg = partition_stats.get("error", "Unknown partition failure")
                                log_now(f"[EDITION {i+1}] 📅 Year {year}: ⚠️ Stratified harvest FAILED - {error_msg}", "warning")
                                log_now(f"[EDITION {i+1}] 📅 Year {year}: Will NOT mark as complete - needs manual review or different strategy")
                                # Don't add year_citations - callback already incremented total_new_citations

                        except Exception as partition_err:
                            log_now(f"[EDITION {i+1}] ⚠️ Stratified harvest exception: {partition_err}", "warning")
                            year_harvest_succeeded = False
                            # Fallback to normal harvest (will only get ~1000)
                            result = await scholar_service.get_cited_by(
                                scholar_id=edition.scholar_id,
                                max_results=1000,
                                year_low=year,
                                year_high=year,
                                on_page_complete=save_page_citations_with_year_state,
                                on_page_failed=on_page_failed_for_year,
                            )
                            year_citations = total_new_citations - year_start_citations
                            # Track pages and gap data from result
                            if isinstance(result, dict):
                                year_pages_succeeded = result.get("pages_succeeded", 0)
                                year_pages_attempted = result.get("pages_fetched", 0)
                                # Extract gap tracking data
                                year_first_gs_count = result.get("first_gs_count")
                                year_last_gs_count = result.get("last_gs_count")
                                if result.get("gs_count_changed"):
                                    log_now(f"[EDITION {i+1}] 📅 Year {year}: ⚠️ GS COUNT CHANGED during fallback: {year_first_gs_count} → {year_last_gs_count}")
                            # Fallback harvest counts as partial success if we got pages
                            if year_pages_attempted > 0:
                                year_harvest_succeeded = True
                                log_now(f"[EDITION {i+1}] 📅 Year {year}: Fallback harvest got {year_citations} citations (partial coverage)")

                    else:
                        # Normal case: <= 1000 citations, standard harvest
                        # NOTE: Don't set year_harvest_succeeded here - determine after harvest based on actual vs expected
                        result = await scholar_service.get_cited_by(
                            scholar_id=edition.scholar_id,
                            max_results=1000,  # Max per year
                            year_low=year,
                            year_high=year,
                            on_page_complete=save_page_citations_with_year_state,
                            on_page_failed=on_page_failed_for_year,
                            start_page=start_page_for_this_year,
                        )

                        year_citations = total_new_citations - year_start_citations
                        # Track pages and gap data from result
                        if isinstance(result, dict):
                            year_pages_succeeded = result.get("pages_succeeded", 0)
                            year_pages_failed = result.get("pages_failed", 0)
                            year_pages_attempted = result.get("pages_fetched", 0)
                            # Extract gap tracking data
                            year_first_gs_count = result.get("first_gs_count")
                            year_last_gs_count = result.get("last_gs_count")
                            if result.get("gs_count_changed"):
                                log_now(f"[EDITION {i+1}] 📅 Year {year}: ⚠️ GS COUNT CHANGED during scraping: {year_first_gs_count} → {year_last_gs_count}")
                        log_now(f"[EDITION {i+1}] 📅 Year {year}: {year_citations} new citations (Scholar reports {total_this_year} total, pages OK={year_pages_succeeded}, FAIL={year_pages_failed})")

                    # Query ACTUAL count of citations in DB for this edition+year
                    # This is the authoritative count, not year_citations (which is just new this job)
                    actual_count_result = await db.execute(
                        select(func.count(Citation.id))
                        .where(Citation.edition_id == edition.id)
                        .where(Citation.year == year)
                    )
                    actual_citations_in_db = actual_count_result.scalar() or 0

                    # Determine if year harvest succeeded by comparing actual vs expected
                    # Use 95% threshold to allow for GS counting discrepancies
                    # Also succeed if expected is 0 (nothing to harvest)
                    completion_threshold = 0.95
                    if total_this_year == 0:
                        year_harvest_succeeded = True  # Nothing expected, nothing to do
                    elif actual_citations_in_db >= total_this_year * completion_threshold:
                        year_harvest_succeeded = True  # Got enough citations
                        log_now(f"[EDITION {i+1}] 📅 Year {year}: ✓ COMPLETE ({actual_citations_in_db}/{total_this_year} = {actual_citations_in_db/total_this_year*100:.1f}%)")
                    else:
                        year_harvest_succeeded = False  # Still missing citations
                        log_now(f"[EDITION {i+1}] 📅 Year {year}: ⚠️ INCOMPLETE ({actual_citations_in_db}/{total_this_year} = {actual_citations_in_db/total_this_year*100:.1f}%)")

                    # Only mark year as completed if harvest succeeded
                    # For overflow years, year_harvest_succeeded is set based on partition/stratified result
                    if year_harvest_succeeded:
                        completed_years.add(year)
                        await save_year_resume_state(year, 0, completed_years)  # page=0 since year is done

                        # Update HarvestTarget with ACTUAL count from DB (not just new this job)
                        # Include gap tracking data for diagnostics
                        await update_harvest_target_progress(
                            db=db,
                            edition_id=edition.id,
                            year=year,
                            actual_count=actual_citations_in_db,
                            pages_succeeded=year_pages_succeeded,
                            pages_failed=year_pages_failed,
                            pages_attempted=year_pages_attempted,
                            mark_complete=True,
                            first_gs_count=year_first_gs_count,
                            last_gs_count=year_last_gs_count,
                        )
                    else:
                        # Harvest incomplete - still need more citations
                        # Update HarvestTarget with actual count but mark as INCOMPLETE so it gets retried
                        log_now(f"[EDITION {i+1}] 📅 Year {year}: Will retry in future jobs (need {total_this_year - actual_citations_in_db} more)")
                        await update_harvest_target_progress(
                            db=db,
                            edition_id=edition.id,
                            year=year,
                            actual_count=actual_citations_in_db,
                            pages_succeeded=year_pages_succeeded,
                            pages_failed=year_pages_failed,
                            pages_attempted=year_pages_attempted,
                            mark_complete=False,  # Don't mark complete - needs retry
                            first_gs_count=year_first_gs_count,
                            last_gs_count=year_last_gs_count,
                        )

                    # Track consecutive empty years for early termination
                    # Use high threshold (10) since old papers may have scattered citations across decades
                    if year_citations == 0 and total_this_year == 0:
                        consecutive_empty_years += 1
                        if consecutive_empty_years >= 10 and year < current_year - 20:
                            log_now(f"[EDITION {i+1}] 📅 {consecutive_empty_years} consecutive empty years (going back to {year}), stopping year-by-year fetch")
                            break
                    else:
                        consecutive_empty_years = 0

                    # Small delay between year queries
                    await asyncio.sleep(2)

                # NOTE: Don't clear resume state - the completed_years in harvest_targets is the authoritative source
                # This allows future jobs to correctly skip already-scanned years
                log_now(f"[EDITION {i+1}] ✓ Year-by-year harvest complete for this job run")

            else:
                # Standard fetch for editions with <=1000 citations
                # For refresh mode, use year_low to only get newer citations
                effective_year_low = None
                if is_refresh:
                    if year_low_param:
                        effective_year_low = year_low_param
                        log_now(f"[EDITION {i+1}] REFRESH: Using year_low={effective_year_low} from params")
                    elif edition.last_harvest_year:
                        effective_year_low = edition.last_harvest_year
                        log_now(f"[EDITION {i+1}] REFRESH: Using year_low={effective_year_low} from edition last harvest")

                log_now(f"[EDITION {i+1}] Calling scholar_service.get_cited_by(year_low={effective_year_low})...")

                # Track standard harvest failures
                std_pages_failed = 0

                async def on_page_failed_standard(page_num: int, url: str, error: str):
                    nonlocal std_pages_failed
                    std_pages_failed += 1
                    # year=None for standard (non-year-partitioned) harvests
                    await record_failed_fetch(db, edition.id, url, page_num, None, error)

                # Record expected count for standard harvest (year=None means "all years")
                if edition.citation_count and edition.citation_count > 0:
                    await create_or_update_harvest_target(db, edition.id, None, edition.citation_count)

                # Update progress for standard mode dashboard
                std_progress_pct = 10 + (i / total_editions) * 80
                await update_job_progress(
                    db, job.id, min(std_progress_pct, 95),
                    f"Edition {i+1}/{total_editions}: Harvesting {edition.citation_count or '?':,} citations",
                    details={
                        "stage": "harvesting",
                        "edition_index": i + 1,
                        "editions_total": total_editions,
                        "edition_id": edition.id,
                        "edition_title": edition.title[:80] if edition.title else "Unknown",
                        "edition_language": edition.language,
                        "edition_citation_count": edition.citation_count,
                        "edition_harvested": edition.harvested_citation_count or 0,
                        "harvest_mode": "standard",
                        "is_refresh": is_refresh,
                        "year_low": effective_year_low,
                        "citations_saved": total_new_citations,
                        "target_citations_total": total_target_citations,
                        "previously_harvested": total_previously_harvested,
                    }
                )

                result = await scholar_service.get_cited_by(
                    scholar_id=edition.scholar_id,
                    max_results=max_citations_per_edition,
                    year_low=effective_year_low,  # Pass year_low for refresh filtering
                    on_page_complete=save_page_citations,
                    on_page_failed=on_page_failed_standard,
                    start_page=resume_page,
                )

                log_now(f"[EDITION {i+1}] get_cited_by returned:")
                log_now(f"[EDITION {i+1}]   result type: {type(result)}")
                log_now(f"[EDITION {i+1}]   result keys: {result.keys() if isinstance(result, dict) else 'NOT A DICT'}")
                log_now(f"[EDITION {i+1}]   papers count: {len(result.get('papers', [])) if isinstance(result, dict) else 'N/A'}")
                log_now(f"[EDITION {i+1}]   totalResults: {result.get('totalResults', 'N/A') if isinstance(result, dict) else 'N/A'}")
                # Log gap tracking info
                if isinstance(result, dict) and result.get("gs_count_changed"):
                    log_now(f"[EDITION {i+1}]   ⚠️ GS COUNT CHANGED: {result.get('first_gs_count')} → {result.get('last_gs_count')}")

                # Update HarvestTarget with ACTUAL total count from database (not just new this job)
                if isinstance(result, dict) and edition.citation_count and edition.citation_count > 0:
                    # Query actual total citations in DB for this edition (all years)
                    std_actual_result = await db.execute(
                        select(func.count(Citation.id))
                        .where(Citation.edition_id == edition.id)
                    )
                    std_actual_count = std_actual_result.scalar() or 0

                    # Include gap tracking data for diagnostics
                    await update_harvest_target_progress(
                        db=db,
                        edition_id=edition.id,
                        year=None,  # Standard harvest = all years
                        actual_count=std_actual_count,  # Total in DB, not just new this job
                        pages_succeeded=result.get("pages_succeeded", 0),
                        pages_failed=result.get("pages_failed", 0),
                        pages_attempted=result.get("pages_fetched", 0),
                        mark_complete=True,
                        first_gs_count=result.get("first_gs_count"),
                        last_gs_count=result.get("last_gs_count"),
                    )

            edition_citations = total_new_citations - edition_start_citations
            log_now(f"[EDITION {i+1}] ✓ Complete: {edition_citations} new citations saved")

            # Update edition harvest stats (always, not just refresh mode)
            await update_edition_harvest_stats(db, edition.id)

            # ====== MERGED EDITIONS HARVESTING ======
            # After processing a canonical edition, also harvest from its merged editions
            # Citations go to the canonical edition (target_edition_id stays as edition.id)
            merged_editions = canonical_to_merged.get(edition.id, [])
            if merged_editions:
                log_now(f"[EDITION {i+1}] 🔗 Processing {len(merged_editions)} merged edition(s)...")
                for merged_ed in merged_editions:
                    try:
                        log_now(f"[MERGED] Harvesting from merged edition {merged_ed.id} (scholar_id={merged_ed.scholar_id})")
                        # Track citations saved before processing this merged edition
                        merged_start_citations = total_new_citations

                        # Use standard harvest for merged editions (smaller, just picking up extras)
                        merged_result = await scholar_service.get_cited_by(
                            scholar_id=merged_ed.scholar_id,
                            max_results=max_citations_per_edition,
                            on_page_complete=save_page_citations,  # Uses target_edition_id which is still the canonical edition
                        )
                        merged_citations = merged_result.get("papers", []) if isinstance(merged_result, dict) else []

                        # Track how many NEW citations came from this merged edition
                        merged_contribution = total_new_citations - merged_start_citations
                        log_now(f"[MERGED] ✓ Merged edition {merged_ed.id} complete: {merged_contribution} new citations (fetched {len(merged_citations)} results)")

                        # Update merged edition's harvest stats (tracks when harvested AND contribution)
                        merged_ed.last_harvested_at = datetime.utcnow()
                        merged_ed.redirected_harvest_count = (merged_ed.redirected_harvest_count or 0) + merged_contribution
                        await db.commit()

                        # Small delay between merged editions
                        await asyncio.sleep(2)
                    except Exception as merged_err:
                        log_now(f"[MERGED] ⚠️ Error harvesting merged edition {merged_ed.id}: {merged_err}", "warning")
                        # Continue with other merged editions

            # Rate limit between editions
            if i < total_editions - 1:
                log_now(f"[EDITION {i+1}] Sleeping 3 seconds before next edition...")
                await asyncio.sleep(3)

        except Exception as e:
            log_now(f"[EDITION {i+1}] ✗✗✗ EXCEPTION ✗✗✗")
            log_now(f"[EDITION {i+1}] Error type: {type(e).__name__}")
            log_now(f"[EDITION {i+1}] Error message: {e}")
            log_now(f"[EDITION {i+1}] Traceback: {traceback.format_exc()}")
            log_now(f"[EDITION {i+1}] Continuing with next edition. {total_new_citations} citations saved so far.")

            # CRITICAL: Rollback any aborted transaction before attempting recovery operations
            try:
                await db.rollback()
                log_now(f"[EDITION {i+1}] Rolled back transaction to recover session state")
            except Exception as rollback_err:
                log_now(f"[EDITION {i+1}] Rollback failed: {rollback_err}")

            # Still update harvest stats for partial progress - citations already saved to DB
            try:
                await update_edition_harvest_stats(db, edition.id)
                log_now(f"[EDITION {i+1}] Updated harvest stats for partial progress")
            except Exception as stats_err:
                log_now(f"[EDITION {i+1}] Failed to update harvest stats: {stats_err}")
            # Continue with other editions - we've already saved what we got

    log_now(f"╔{'═'*70}╗")
    log_now(f"║  EXTRACT_CITATIONS JOB COMPLETE")
    log_now(f"╠{'═'*70}╣")
    log_now(f"║  Total new citations: {total_new_citations}")
    log_now(f"║  Total duplicates skipped: {total_updated_citations}")
    log_now(f"║  Editions processed: {len(valid_editions)}")
    log_now(f"╚{'═'*70}╝")

    # Update paper-level aggregate harvest stats
    await update_paper_harvest_stats(db, paper_id)

    # Track harvest stall for auto-resume detection
    # IMPORTANT: Only mark as stalled if we TRIED to harvest but couldn't make progress
    # Do NOT stall if harvest is actually complete (all targets done)
    for edition in valid_editions:
        if total_new_citations == 0 and total_updated_citations == 0:
            # No citations found at all - check if harvest is actually complete
            # Query harvest_targets to see if there are incomplete years
            incomplete_targets = await db.execute(
                select(HarvestTarget)
                .where(HarvestTarget.edition_id == edition.id)
                .where(HarvestTarget.status != 'complete')
                .where(HarvestTarget.expected_count > 0)
            )
            incomplete_list = list(incomplete_targets.scalars().all())

            if incomplete_list:
                # Before marking as stalled, check if edition is MOSTLY complete
                # If >= 95% complete overall, auto-complete remaining targets instead of stalling
                all_targets_result = await db.execute(
                    select(HarvestTarget)
                    .where(HarvestTarget.edition_id == edition.id)
                    .where(HarvestTarget.expected_count > 0)
                )
                all_targets = list(all_targets_result.scalars().all())

                if all_targets:
                    total_expected = sum(t.expected_count or 0 for t in all_targets)
                    total_actual = sum(t.actual_count or 0 for t in all_targets)
                    overall_completion = total_actual / total_expected if total_expected > 0 else 0
                    total_gap = total_expected - total_actual

                    # AUTO-COMPLETE: If >= 95% complete OR gap is tiny (< 100 citations), mark remaining as complete
                    # This prevents stalling on small unfetchable gaps due to GS inconsistency
                    if overall_completion >= 0.95 or total_gap < 100:
                        log_now(f"[AUTO-COMPLETE] Edition {edition.id}: {overall_completion*100:.1f}% complete ({total_actual}/{total_expected}), gap={total_gap}")
                        log_now(f"[AUTO-COMPLETE] Auto-completing {len(incomplete_list)} remaining incomplete targets")

                        for target in incomplete_list:
                            target.status = 'complete'
                            target.completed_at = datetime.utcnow()

                        # Reset stall count - harvest is effectively done
                        if edition.harvest_stall_count and edition.harvest_stall_count > 0:
                            log_now(f"[AUTO-COMPLETE] Resetting stall count (was {edition.harvest_stall_count})")
                        edition.harvest_stall_count = 0

                        await db.commit()
                        continue  # Skip to next edition, don't increment stall count

                # Edition is NOT mostly complete - this is a real stall
                current_stall = edition.harvest_stall_count or 0
                edition.harvest_stall_count = current_stall + 1
                incomplete_years = [t.year for t in incomplete_list if t.year]

                # Track stall details for diagnostics
                if incomplete_list:
                    # Get the first incomplete target's details
                    first_incomplete = incomplete_list[0]
                    edition.last_stall_year = first_incomplete.year
                    edition.last_stall_offset = first_incomplete.pages_attempted or 0
                    edition.last_stall_reason = "zero_new"
                    edition.last_stall_at = datetime.utcnow()

                if edition.harvest_stall_count >= AUTO_RESUME_MAX_STALL_COUNT:
                    log_now(f"[STALL] Edition {edition.id} has stalled after {edition.harvest_stall_count} consecutive zero-progress jobs")
                    log_now(f"[STALL] Incomplete years: {incomplete_years[:10]}")
                    log_now(f"[STALL] Last stall point: year={edition.last_stall_year}, offset={edition.last_stall_offset}")
                    # Log completion stats for debugging
                    if all_targets:
                        log_now(f"[STALL] Completion: {total_actual}/{total_expected} ({overall_completion*100:.1f}%), gap={total_gap}")
                else:
                    log_now(f"[STALL] Edition {edition.id} made no progress (stall count: {edition.harvest_stall_count})")
            else:
                # All targets are complete - NOT a stall, just nothing new to find
                # Reset stall count since harvest is actually done
                if edition.harvest_stall_count and edition.harvest_stall_count > 0:
                    log_now(f"[HARVEST] Edition {edition.id}: All targets complete, resetting stall count (was {edition.harvest_stall_count})")
                edition.harvest_stall_count = 0
        elif total_new_citations > 0:
            # Made progress with new citations - reset stall count
            edition.harvest_stall_count = 0
        # Note: if total_new_citations == 0 but total_updated_citations > 0,
        # we successfully processed pages but found only duplicates - NOT a stall
        elif total_updated_citations > 0 and edition.harvest_stall_count and edition.harvest_stall_count > 0:
            log_now(f"[HARVEST] Edition {edition.id}: Found {total_updated_citations} duplicates, resetting stall count (was {edition.harvest_stall_count})")
            edition.harvest_stall_count = 0
    await db.commit()

    # Clear resume state on successful completion
    if params.get("resume_state"):
        params.pop("resume_state", None)
        job.params = json.dumps(params)
        await db.commit()

    return {
        "paper_id": paper_id,
        "editions_processed": len(valid_editions),
        "editions_skipped": len(skipped_editions),
        "skipped_details": skipped_editions[:10],
        "new_citations_added": total_new_citations,
        "duplicates_skipped": total_updated_citations,
        "is_refresh": is_refresh,
    }


async def process_resolve_job(job: Job, db: AsyncSession):
    """Process a paper resolution job"""
    from .paper_resolution import PaperResolutionService

    paper_id = job.paper_id
    log_now(f"[Resolve] Starting resolution for paper {paper_id}")

    service = PaperResolutionService(db)

    # Update progress
    job.progress = 10
    job.progress_message = "Searching Google Scholar..."
    await db.commit()

    try:
        result = await service.resolve_paper(paper_id, job_id=job.id)

        if result.get("success"):
            if result.get("needs_reconciliation"):
                job.progress_message = f"Multiple candidates found - user selection required"
                return {
                    "paper_id": paper_id,
                    "needs_reconciliation": True,
                    "candidates_count": len(result.get("candidates", [])),
                }
            else:
                job.progress_message = f"Resolved: {result.get('citation_count', 0)} citations"
                return {
                    "paper_id": paper_id,
                    "scholar_id": result.get("scholar_id"),
                    "citation_count": result.get("citation_count", 0),
                }
        else:
            raise ValueError(result.get("error", "Resolution failed"))

    except Exception as e:
        log_now(f"[Resolve] Error: {e}")
        raise


async def process_partition_harvest_test(job: Job, db: AsyncSession) -> Dict[str, Any]:
    """Process a partition harvest test job for a single year"""
    from .overflow_harvester import harvest_partition

    params = json.loads(job.params) if job.params else {}
    edition_id = params.get("edition_id")
    year = params.get("year")
    total_count = params.get("total_count", 0)

    log_now(f"[PartitionTest] Starting partition harvest for edition {edition_id}, year {year} ({total_count} citations)")

    # Get the edition
    result = await db.execute(select(Edition).where(Edition.id == edition_id))
    edition = result.scalar_one_or_none()

    if not edition:
        raise ValueError(f"Edition {edition_id} not found")

    if not edition.scholar_id:
        raise ValueError(f"Edition {edition_id} has no scholar_id")

    # Update progress
    job.progress = 5
    job.progress_message = f"Starting partition harvest for year {year}..."
    await db.commit()

    # Get scholar service (not async)
    scholar_service = get_scholar_service()

    # Get existing citation scholar IDs to avoid duplicate key violations
    # NOTE: Unlike the main harvester (which uses ON CONFLICT to increment encounter_count),
    # this job uses ORM insert so we MUST skip existing citations to avoid constraint errors.
    # This means cross-edition duplicates won't increment encounter_count, but that's acceptable
    # for this diagnostic job type.
    existing_result = await db.execute(
        select(Citation.scholar_id).where(Citation.paper_id == edition.paper_id)
    )
    existing_scholar_ids = {r[0] for r in existing_result.fetchall() if r[0]}
    log_now(f"[PartitionTest] Found {len(existing_scholar_ids)} existing citations to skip")

    # Track new citations for this job
    new_citations_count = {"total": 0}

    # Define save callback for citations (matches harvest_partition's on_page_complete signature)
    async def on_page_complete(page_num: int, papers: List[Dict]):
        """Save citations from each page"""
        new_count = 0
        for paper in papers:
            scholar_id = paper.get("scholarId") or paper.get("id")
            if not scholar_id or scholar_id in existing_scholar_ids:
                continue

            # Serialize author_profiles to JSON string
            author_profiles_json = None
            if paper.get("authorProfiles"):
                author_profiles_json = json.dumps(paper["authorProfiles"])

            # Create citation
            citation = Citation(
                edition_id=edition_id,
                paper_id=edition.paper_id,
                scholar_id=scholar_id,
                title=paper.get("title", ""),
                authors=paper.get("authorsRaw") or ", ".join(paper.get("authors", [])),
                author_profiles=author_profiles_json,
                year=paper.get("year"),
                venue=paper.get("venue", ""),
                abstract=paper.get("abstract", ""),
                citation_count=paper.get("citationCount", 0),
                link=paper.get("link", ""),
            )
            db.add(citation)
            existing_scholar_ids.add(scholar_id)
            new_count += 1

        await db.commit()
        new_citations_count["total"] += new_count

        # Update job progress
        job.progress = min(90, 10 + page_num * 2)
        job.progress_message = f"Page {page_num + 1}: {new_citations_count['total']} new citations"
        await db.commit()

        log_now(f"[PartitionTest] Page {page_num + 1}: saved {new_count} new citations (total: {new_citations_count['total']})")

    try:
        # Run the partition harvest
        partition_result = await harvest_partition(
            scholar_service=scholar_service,
            db=db,
            edition_id=edition_id,
            scholar_id=edition.scholar_id,
            year=year,
            edition_title=edition.title,
            paper_id=edition.paper_id,
            existing_scholar_ids=existing_scholar_ids,
            on_page_complete=on_page_complete,
            total_for_year=total_count,
        )

        # Update edition harvest stats
        await update_edition_harvest_stats(db, edition_id)

        log_now(f"[PartitionTest] Completed: {partition_result}")

        return {
            "edition_id": edition_id,
            "year": year,
            "success": True,
            "total_citations_harvested": partition_result.get("total_new", 0),
            "exclusion_harvested": partition_result.get("exclusion_harvested", 0),
            "inclusion_harvested": partition_result.get("inclusion_harvested", 0),
            "excluded_terms": partition_result.get("excluded_terms", []),
        }

    except Exception as e:
        log_now(f"[PartitionTest] Error: {e}")
        raise


async def process_verify_and_repair_job(job: Job, db: AsyncSession) -> Dict[str, Any]:
    """Process a verify_and_repair job - detect gaps and fill missing citations.

    This job:
    1. For each year in the range, verifies the last page exists
    2. Compares Scholar's count to our harvested count
    3. Identifies missing pages
    4. Fetches missing pages and saves citations
    """
    log_now("="*70)
    log_now(f"VERIFY_AND_REPAIR JOB START - Job {job.id}")
    log_now("="*70)

    params = json.loads(job.params) if job.params else {}
    paper_id = params.get("paper_id")
    edition_ids = params.get("edition_ids", [])
    year_start = params.get("year_start", 2025)
    year_end = params.get("year_end", 2011)
    fix_gaps = params.get("fix_gaps", True)

    if not paper_id:
        raise ValueError("paper_id is required")

    # Get the editions to verify
    if edition_ids:
        editions_result = await db.execute(
            select(Edition).where(Edition.id.in_(edition_ids), Edition.scholar_id.isnot(None))
        )
    else:
        # Get all selected editions for this paper
        editions_result = await db.execute(
            select(Edition).where(
                Edition.paper_id == paper_id,
                Edition.selected == True,
                Edition.excluded == False,
                Edition.scholar_id.isnot(None)
            )
        )
    editions = list(editions_result.scalars().all())

    if not editions:
        raise ValueError(f"No editions with Scholar IDs found for paper {paper_id}")

    scholar_service = get_scholar_service()

    # Track results across all editions
    total_years_checked = 0
    total_years_with_gaps = 0
    total_missing = 0
    total_recovered = 0
    all_gap_details = []
    edition_results = []

    # Get existing citations to avoid duplicate key violations
    # NOTE: Unlike the main harvester (which uses ON CONFLICT to increment encounter_count),
    # this job uses ORM insert so we MUST skip existing citations to avoid constraint errors.
    # This means cross-edition duplicates won't increment encounter_count, but that's acceptable
    # for this diagnostic job type.
    existing_result = await db.execute(
        select(Citation.scholar_id).where(Citation.paper_id == paper_id)
    )
    existing_scholar_ids = {r[0] for r in existing_result.fetchall() if r[0]}

    log_now(f"[VerifyRepair] Processing {len(editions)} editions for years {year_start}-{year_end}")
    log_now(f"[VerifyRepair] fix_gaps={fix_gaps}, existing citations: {len(existing_scholar_ids)}")

    # Process each edition
    for edition_idx, edition in enumerate(editions):
        log_now(f"[VerifyRepair] ─── Edition {edition_idx + 1}/{len(editions)}: {edition.title} (id={edition.id}) ───")

        edition_years_checked = 0
        edition_years_with_gaps = 0
        edition_missing = 0
        edition_recovered = 0
        edition_gap_details = []

        for year in range(year_start, year_end - 1, -1):
            edition_years_checked += 1
            total_years_checked += 1

            # Calculate progress: (edition progress + year progress within edition) / total
            edition_progress = edition_idx / len(editions)
            year_progress_in_edition = (year_start - year) / (year_start - year_end + 1)
            overall_progress = (edition_progress + year_progress_in_edition / len(editions)) * 90

            await update_job_progress(
                db, job.id,
                overall_progress,
                f"Edition {edition_idx + 1}/{len(editions)}: Verifying year {year}..."
            )

            # Step 1: Get count from first page
            count_result = await scholar_service.get_cited_by(
                scholar_id=edition.scholar_id,
                max_results=10,
                year_low=year,
                year_high=year,
            )
            scholar_count = count_result.get('totalResults', 0) if isinstance(count_result, dict) else 0

            if scholar_count == 0:
                log_now(f"[VerifyRepair] Year {year}: No citations reported by Scholar")
                continue

            # Step 2: Verify last page exists
            verify_result = await scholar_service.verify_last_page(
                scholar_id=edition.scholar_id,
                expected_count=scholar_count,
                year_low=year,
                year_high=year,
            )

            verified_count = verify_result.get("verified_count") or scholar_count
            log_now(f"[VerifyRepair] Year {year}: Scholar reports {scholar_count}, verified last page shows {verified_count}")

            # Step 3: Count our harvested citations for this year (for this edition)
            our_count_result = await db.execute(
                select(func.count(Citation.id))
                .where(Citation.edition_id == edition.id)
                .where(Citation.year == year)
            )
            our_count = our_count_result.scalar() or 0

            # Step 4: Calculate gap
            gap = verified_count - our_count
            if gap > 0:
                edition_years_with_gaps += 1
                total_years_with_gaps += 1
                edition_missing += gap
                total_missing += gap
                log_now(f"[VerifyRepair] Year {year}: GAP DETECTED - Scholar has {verified_count}, we have {our_count}, missing {gap}")

                if fix_gaps:
                    # Step 5: Calculate which pages we're missing
                    # The safest approach: fetch pages from (our_count // 10 * 10) to end
                    start_from = (our_count // 10) * 10  # Round down to page boundary
                    end_at = verified_count

                    pages_to_fetch = []
                    for start in range(start_from, end_at, 10):
                        pages_to_fetch.append(start)

                    log_now(f"[VerifyRepair] Year {year}: Will fetch pages starting at: {pages_to_fetch}")

                    year_recovered = 0
                    for page_start in pages_to_fetch:
                        page_result = await scholar_service.fetch_specific_page(
                            scholar_id=edition.scholar_id,
                            page_start=page_start,
                            year_low=year,
                            year_high=year,
                        )

                        if page_result.get("success"):
                            papers = page_result.get("papers", [])
                            new_count = 0

                            for paper_data in papers:
                                cit_scholar_id = paper_data.get("scholarId")
                                if not cit_scholar_id or cit_scholar_id in existing_scholar_ids:
                                    continue

                                # Serialize author_profiles to JSON string
                                author_profiles_json = None
                                if paper_data.get("authorProfiles"):
                                    author_profiles_json = json.dumps(paper_data["authorProfiles"])

                                citation = Citation(
                                    paper_id=paper_id,
                                    edition_id=edition.id,
                                    scholar_id=cit_scholar_id,
                                    title=paper_data.get("title", "Unknown"),
                                    authors=paper_data.get("authorsRaw"),
                                    author_profiles=author_profiles_json,
                                    year=paper_data.get("year"),
                                    venue=paper_data.get("venue"),
                                    abstract=paper_data.get("abstract"),
                                    link=paper_data.get("link"),
                                    citation_count=paper_data.get("citationCount", 0),
                                    intersection_count=1,
                                )
                                db.add(citation)
                                existing_scholar_ids.add(cit_scholar_id)
                                new_count += 1

                            await db.commit()
                            year_recovered += new_count
                            log_now(f"[VerifyRepair] Year {year}, page start={page_start}: recovered {new_count} new citations")

                        await asyncio.sleep(2)  # Rate limit

                    edition_recovered += year_recovered
                    total_recovered += year_recovered
                    edition_gap_details.append({
                        "year": year,
                        "scholar_count": verified_count,
                        "our_count_before": our_count,
                        "gap": gap,
                        "recovered": year_recovered,
                        "pages_fetched": len(pages_to_fetch),
                    })

                    # Update HarvestTarget if it exists
                    target_result = await db.execute(
                        select(HarvestTarget).where(
                            HarvestTarget.edition_id == edition.id,
                            HarvestTarget.year == year
                        )
                    )
                    target = target_result.scalar_one_or_none()
                    if target:
                        target.expected_count = verified_count
                        target.actual_count = our_count + year_recovered
                        target.status = "complete" if (our_count + year_recovered) >= verified_count * 0.95 else "incomplete"
                        await db.commit()
                else:
                    # Just record the gap without fixing
                    edition_gap_details.append({
                        "year": year,
                        "scholar_count": verified_count,
                        "our_count": our_count,
                        "gap": gap,
                        "fix_gaps": False,
                    })

            else:
                log_now(f"[VerifyRepair] Year {year}: OK - Scholar has {verified_count}, we have {our_count}")

            await asyncio.sleep(3)  # Rate limit between years

        # Update edition harvest stats after processing
        await update_edition_harvest_stats(db, edition.id)
        all_gap_details.extend(edition_gap_details)
        edition_results.append({
            "edition_id": edition.id,
            "edition_title": edition.title,
            "years_checked": edition_years_checked,
            "years_with_gaps": edition_years_with_gaps,
            "missing": edition_missing,
            "recovered": edition_recovered,
            "gap_details": edition_gap_details,
        })

    # Update paper harvest stats at the end
    await update_paper_harvest_stats(db, paper_id)

    log_now(f"[VerifyRepair] ═══════════════════════════════════════════════")
    log_now(f"[VerifyRepair] COMPLETE: {len(editions)} editions, {total_years_checked} year-checks, {total_years_with_gaps} gaps found")
    log_now(f"[VerifyRepair] Total missing: {total_missing}, Total recovered: {total_recovered}")
    log_now(f"[VerifyRepair] ═══════════════════════════════════════════════")

    return {
        "paper_id": paper_id,
        "editions_processed": len(editions),
        "years_checked": total_years_checked,
        "years_with_gaps": total_years_with_gaps,
        "total_missing": total_missing,
        "total_recovered": total_recovered,
        "edition_results": edition_results,
        "gap_details": all_gap_details,
    }


# ============== Thinker Bibliography Jobs ==============

async def process_thinker_discover_works(job: Job, db: AsyncSession) -> Dict[str, Any]:
    """
    Discover works by a thinker using author searches.

    For each name variant:
    1. Execute author search on Google Scholar
    2. Paginate through ALL results
    3. Filter each page via LLM to identify works BY the thinker
    4. Save accepted works to ThinkerWork table
    5. Deduplicate across variants (by scholar_id)
    """
    from .thinker_service import get_thinker_service

    # Parse job params
    params = json.loads(job.params) if job.params else {}
    thinker_id = params.get("thinker_id")
    max_pages_per_variant = params.get("max_pages", 50)  # Safety limit

    if not thinker_id:
        raise ValueError("thinker_id required in job params")

    thinker = await db.get(Thinker, thinker_id)
    if not thinker:
        raise ValueError(f"Thinker {thinker_id} not found")

    log_now(f"[ThinkerDiscover] Starting work discovery for: {thinker.canonical_name}")

    service = get_thinker_service(db)
    scholar = get_scholar_service()

    # Parse name variants
    variants = []
    if thinker.name_variants:
        try:
            variants = json.loads(thinker.name_variants)
        except json.JSONDecodeError:
            variants = [thinker.name_variants]

    if not variants:
        # Generate variants if not available
        log_now(f"[ThinkerDiscover] No variants found, generating...")
        variant_result = await service.generate_name_variants(thinker)
        if variant_result.get("success"):
            variants = [v.get("query", "") for v in variant_result.get("variants", [])]
        else:
            # Fallback to initial + surname format (how GS actually indexes)
            name_parts = thinker.canonical_name.split()
            if len(name_parts) >= 2:
                first_initial = name_parts[0][0]
                last_name = name_parts[-1]
                variants = [
                    f'author:"{first_initial} {last_name}"',
                    f'author:"{first_initial}* {last_name}"',
                ]
            else:
                variants = [f'author:"{thinker.canonical_name}"']

    log_now(f"[ThinkerDiscover] Processing {len(variants)} name variants")

    # Update thinker status
    thinker.status = "harvesting"
    thinker.harvest_started_at = datetime.utcnow()

    # Track results
    total_results = 0
    total_accepted = 0
    total_rejected = 0
    total_uncertain = 0
    seen_scholar_ids = set()
    variant_results = []

    # Get existing works' scholar_ids to avoid duplicates
    existing_result = await db.execute(
        select(ThinkerWork.scholar_id)
        .where(ThinkerWork.thinker_id == thinker_id)
        .where(ThinkerWork.scholar_id.isnot(None))
    )
    seen_scholar_ids = set(r[0] for r in existing_result.all())
    log_now(f"[ThinkerDiscover] Found {len(seen_scholar_ids)} existing works to skip")

    for var_idx, variant in enumerate(variants):
        if not variant:
            continue

        log_now(f"[ThinkerDiscover] Variant {var_idx+1}/{len(variants)}: {variant}")

        # Create harvest run record
        harvest_run = ThinkerHarvestRun(
            thinker_id=thinker_id,
            query_used=variant,
            variant_type="search_query",
            status="running",
            started_at=datetime.utcnow(),
        )
        db.add(harvest_run)
        await db.flush()

        var_accepted = 0
        var_rejected = 0
        var_uncertain = 0
        pages_fetched = 0
        page_count = [0]  # Use list for mutable reference in callback

        # Page callback - process and save papers incrementally as each page is fetched
        async def on_page_complete(page_num: int, papers: list):
            nonlocal var_accepted, var_rejected, var_uncertain
            page_count[0] = page_num + 1

            # Skip papers we've already seen
            # Note: _parse_scholar_page returns camelCase keys: scholarId, authorsRaw, etc.
            new_papers = []
            for p in papers:
                sid = p.get("scholarId")  # camelCase from parser
                if sid and sid not in seen_scholar_ids:
                    new_papers.append(p)
                    seen_scholar_ids.add(sid)

            if not new_papers:
                log_now(f"[ThinkerDiscover]   Page {page_num + 1}: all {len(papers)} papers already seen, skipping")
                return

            log_now(f"[ThinkerDiscover]   Page {page_num + 1}: filtering {len(new_papers)} new papers via LLM...")

            # Filter via LLM
            filter_result = await service.filter_page_results(thinker, new_papers)

            if filter_result.get("success"):
                decisions = filter_result.get("decisions", [])
                page_accepted = 0

                for i, decision in enumerate(decisions):
                    if i >= len(new_papers):
                        break

                    paper_data = new_papers[i]
                    verdict = decision.get("decision", "uncertain")
                    confidence = decision.get("confidence", 0.5)
                    reason = decision.get("reason", "")

                    if verdict == "accept":
                        var_accepted += 1
                        page_accepted += 1
                    elif verdict == "reject":
                        var_rejected += 1
                    else:
                        var_uncertain += 1

                    # Save work record immediately
                    # Note: parser returns camelCase keys: scholarId, authorsRaw, citationCount
                    work = ThinkerWork(
                        thinker_id=thinker_id,
                        scholar_id=paper_data.get("scholarId"),
                        title=paper_data.get("title", "Unknown"),
                        authors_raw=paper_data.get("authorsRaw"),
                        year=paper_data.get("year"),
                        citation_count=paper_data.get("citationCount", 0),
                        decision="accepted" if verdict == "accept" else ("rejected" if verdict == "reject" else "uncertain"),
                        confidence=confidence,
                        reason=reason,
                        created_at=datetime.utcnow(),
                    )
                    db.add(work)

                # Commit after each page - saves progress incrementally
                try:
                    await db.commit()
                except Exception as commit_err:
                    log_now(f"[ThinkerDiscover]   Page {page_num + 1}: commit error, rolling back: {commit_err}")
                    await db.rollback()
                    # Re-add the works that weren't committed
                    raise
                log_now(f"[ThinkerDiscover]   Page {page_num + 1}: saved {page_accepted} accepted, {var_accepted} total so far")

            # Update job progress after each page
            job.progress = int(((var_idx + (page_num / max_pages_per_variant)) / len(variants)) * 100)
            job.progress_message = f"Variant {var_idx+1}/{len(variants)}, page {page_num + 1}: {var_accepted} accepted"
            try:
                await db.commit()
            except Exception:
                await db.rollback()

        try:
            # Combine author variant with full name to reduce false positives
            # e.g., author:"C Durand" "Cédric Durand" - filters out other "C Durand" academics
            combined_query = f'{variant} "{thinker.canonical_name}"'
            log_now(f"[ThinkerDiscover]   Combined query: {combined_query}")

            # Execute author search with pagination - papers saved incrementally via callback
            search_result = await scholar.search_by_author(
                author_query=combined_query,
                max_results=max_pages_per_variant * 10,  # 10 results per page
                start_page=0,
                on_page_complete=on_page_complete,  # Process each page as it's fetched
            )

            pages_fetched = search_result.get("pages_fetched", 0)
            total_results += search_result.get("totalResults", 0)

            log_now(f"[ThinkerDiscover]   Variant complete: {pages_fetched} pages, {var_accepted} accepted")

        except Exception as e:
            log_now(f"[ThinkerDiscover] Error processing variant: {e}")
            harvest_run.status = "failed"
        else:
            harvest_run.status = "completed"

        # Update harvest run
        harvest_run.pages_fetched = pages_fetched
        harvest_run.results_processed = var_accepted + var_rejected + var_uncertain
        harvest_run.results_accepted = var_accepted
        harvest_run.results_rejected = var_rejected
        harvest_run.results_uncertain = var_uncertain
        harvest_run.completed_at = datetime.utcnow()

        total_accepted += var_accepted
        total_rejected += var_rejected
        total_uncertain += var_uncertain

        variant_results.append({
            "variant": variant,
            "accepted": var_accepted,
            "rejected": var_rejected,
            "uncertain": var_uncertain,
        })

        # Safe commit with rollback on error
        try:
            await db.commit()
        except Exception as e:
            log_now(f"[ThinkerDiscover] Commit error after variant, rolling back: {e}")
            await db.rollback()
            # Refresh objects from database
            await db.refresh(harvest_run)
            harvest_run.pages_fetched = pages_fetched
            harvest_run.results_processed = var_accepted + var_rejected + var_uncertain
            harvest_run.results_accepted = var_accepted
            harvest_run.results_rejected = var_rejected
            harvest_run.results_uncertain = var_uncertain
            harvest_run.completed_at = datetime.utcnow()
            harvest_run.status = "completed"
            await db.commit()

    # Update thinker stats and status
    thinker.works_discovered = total_accepted + total_uncertain  # Include uncertain for review
    thinker.status = "complete"
    thinker.harvest_completed_at = datetime.utcnow()
    try:
        await db.commit()
    except Exception as e:
        log_now(f"[ThinkerDiscover] Commit error updating thinker stats, rolling back: {e}")
        await db.rollback()
        await db.refresh(thinker)
        thinker.works_discovered = total_accepted + total_uncertain
        thinker.status = "complete"
        thinker.harvest_completed_at = datetime.utcnow()
        await db.commit()

    log_now(f"[ThinkerDiscover] ═══════════════════════════════════════════════")
    log_now(f"[ThinkerDiscover] COMPLETE: {len(variants)} variants, {total_results} total results")
    log_now(f"[ThinkerDiscover] Accepted: {total_accepted}, Rejected: {total_rejected}, Uncertain: {total_uncertain}")
    log_now(f"[ThinkerDiscover] ═══════════════════════════════════════════════")

    return {
        "thinker_id": thinker_id,
        "thinker_name": thinker.canonical_name,
        "variants_processed": len(variants),
        "total_results": total_results,
        "accepted": total_accepted,
        "rejected": total_rejected,
        "uncertain": total_uncertain,
        "variant_results": variant_results,
    }


async def process_thinker_harvest_citations(job: Job, db: AsyncSession) -> Dict[str, Any]:
    """
    Harvest citations for a thinker's accepted works.

    For each accepted ThinkerWork:
    1. Check if Paper already exists (by scholar_id or title match)
    2. If not, create Paper + Edition
    3. Queue extract_citations job
    4. Track progress at thinker level
    """
    # Parse job params
    params = json.loads(job.params) if job.params else {}
    thinker_id = params.get("thinker_id")
    max_works = params.get("max_works", 100)  # Safety limit per job

    if not thinker_id:
        raise ValueError("thinker_id required in job params")

    thinker = await db.get(Thinker, thinker_id)
    if not thinker:
        raise ValueError(f"Thinker {thinker_id} not found")

    log_now(f"[ThinkerHarvest] Starting citation harvest for: {thinker.canonical_name}")

    # Get accepted works that haven't been harvested
    result = await db.execute(
        select(ThinkerWork)
        .where(ThinkerWork.thinker_id == thinker_id)
        .where(ThinkerWork.decision == "accepted")
        .where(ThinkerWork.citations_harvested == False)
        .order_by(ThinkerWork.citation_count.desc())  # Prioritize high-citation works
        .limit(max_works)
    )
    works = list(result.scalars().all())

    if not works:
        log_now(f"[ThinkerHarvest] No works to harvest for {thinker.canonical_name}")
        return {
            "thinker_id": thinker_id,
            "thinker_name": thinker.canonical_name,
            "works_processed": 0,
            "jobs_queued": 0,
            "message": "No accepted works pending harvest",
        }

    log_now(f"[ThinkerHarvest] Processing {len(works)} works")

    # Generate batch ID for tracking job completion
    import uuid
    from ..config import get_settings
    settings = get_settings()
    batch_id = str(uuid.uuid4())

    # Build callback URL for job completion notifications
    callback_url = f"{settings.internal_base_url}/api/internal/thinker-harvest-callback/{thinker_id}"
    callback_secret = settings.internal_webhook_secret

    log_now(f"[ThinkerHarvest] Batch ID: {batch_id}, callback: {callback_url}")

    jobs_queued = 0
    papers_created = 0
    papers_linked = 0

    # Import Scholar service for cluster ID lookups
    from .scholar_search import get_scholar_service
    scholar_service = get_scholar_service()

    for idx, work in enumerate(works):
        try:
            # Check if scholar_id is in profile format (user:article_id) vs cluster ID (numeric)
            # Profile format IDs don't work for citation lookup - need to find real cluster ID
            cluster_id = work.scholar_id
            if work.scholar_id and ':' in work.scholar_id:
                log_now(f"[ThinkerHarvest] Work '{work.title[:40]}' has profile-format ID, searching for cluster ID...")
                # Search for the paper to get the real cluster ID
                search_results = await scholar_service.search(
                    query=f'"{work.title}"',
                    max_results=5,
                )
                papers = search_results.get('papers', []) if search_results else []
                if papers:
                    # Find best match by title similarity
                    for result in papers:
                        result_title = result.get('title', '').lower().strip()
                        work_title = work.title.lower().strip()
                        # Check for exact or close match
                        if result_title == work_title or work_title in result_title or result_title in work_title:
                            cluster_id = result.get('scholar_id')
                            log_now(f"[ThinkerHarvest] Found cluster ID: {cluster_id} for '{work.title[:40]}'")
                            break
                    else:
                        # No exact match, use first result if reasonable
                        cluster_id = papers[0].get('scholar_id')
                        log_now(f"[ThinkerHarvest] Using first result cluster ID: {cluster_id} for '{work.title[:40]}'")
                else:
                    log_now(f"[ThinkerHarvest] WARNING: No cluster ID found for '{work.title[:40]}', harvest may fail")
                    # Keep original profile ID - will fail but better than no ID

            # Always create a new paper for each thinker work
            # Thinker works are discovered via author name search - they belong to this thinker
            # No fuzzy matching - that leads to linking "Human Action" by Gasparski to Mises' book
            paper = Paper(
                title=work.title,
                authors=work.authors_raw,
                year=work.year,
                citation_count=work.citation_count or 0,
                status="resolved",
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
            db.add(paper)
            await db.flush()
            papers_created += 1

            # Create edition for the paper
            # IMPORTANT: Must set scholar_id for harvesting - harvester checks e.scholar_id
            # Use looked-up cluster_id for citation extraction to work
            edition = Edition(
                paper_id=paper.id,
                title=work.title,
                scholar_id=cluster_id,  # Use numeric cluster ID for citation lookup
                cluster_id=cluster_id,
                citation_count=work.citation_count or 0,
                confidence="high",
                auto_selected=True,
                selected=True,
                created_at=datetime.utcnow(),
                redirected_harvest_count=0,
                harvest_reset_count=0,
            )
            db.add(edition)
            await db.flush()

            # Link work to paper
            work.paper_id = paper.id

            # Queue citation extraction job (with duplicate prevention)
            # Include callback URL for harvest completion tracking
            before_create = datetime.utcnow()
            extract_job = await create_extract_citations_job(
                db=db,
                paper_id=paper.id,
                resume_message=f"Thinker harvest: {work.title[:50]}...",
                callback_url=callback_url,
                callback_secret=callback_secret,
            )

            # Check if new job was created or existing returned
            is_new = extract_job.created_at and extract_job.created_at >= before_create
            work.citations_harvested = True
            work.harvest_job_id = extract_job.id
            if is_new:
                jobs_queued += 1
            else:
                log_now(f"[ThinkerHarvest] Existing job {extract_job.id} found for paper {paper.id}")

            # Update progress
            job.progress = int(((idx + 1) / len(works)) * 100)
            job.progress_message = f"Processing work {idx+1}/{len(works)}"
            await db.commit()

        except Exception as e:
            log_now(f"[ThinkerHarvest] Error processing work {work.id}: {e}")
            continue

    # Update thinker stats and batch tracking
    thinker.works_harvested = (thinker.works_harvested or 0) + jobs_queued
    thinker.harvest_batch_id = batch_id
    thinker.harvest_batch_jobs_total = jobs_queued
    thinker.harvest_batch_jobs_completed = 0
    thinker.harvest_batch_jobs_failed = 0
    thinker.profiles_prefetch_status = "pending" if jobs_queued > 0 else None
    await db.commit()

    log_now(f"[ThinkerHarvest] ═══════════════════════════════════════════════")
    log_now(f"[ThinkerHarvest] COMPLETE: {len(works)} works processed")
    log_now(f"[ThinkerHarvest] Papers created: {papers_created}, Linked: {papers_linked}")
    log_now(f"[ThinkerHarvest] Jobs queued: {jobs_queued} (batch: {batch_id})")
    log_now(f"[ThinkerHarvest] Profile pre-fetch will trigger when all jobs complete")
    log_now(f"[ThinkerHarvest] ═══════════════════════════════════════════════")

    return {
        "thinker_id": thinker_id,
        "thinker_name": thinker.canonical_name,
        "works_processed": len(works),
        "papers_created": papers_created,
        "papers_linked": papers_linked,
        "jobs_queued": jobs_queued,
        "batch_id": batch_id,
    }


async def process_single_job(job_id: int):
    """Process a single job by ID with concurrency control"""
    global _job_semaphore, _running_jobs

    # Acquire semaphore to limit concurrent jobs
    if _job_semaphore is None:
        _job_semaphore = asyncio.Semaphore(MAX_CONCURRENT_JOBS)

    async with _job_semaphore:
        # Track this job as running
        _running_jobs.add(job_id)
        log_now(f"[Worker] Job {job_id} acquired slot ({len(_running_jobs)}/{MAX_CONCURRENT_JOBS} running)")

        try:
            async with async_session() as db:
                try:
                    # Get job
                    result = await db.execute(select(Job).where(Job.id == job_id))
                    job = result.scalar_one_or_none()
                    if not job:
                        log_now(f"[Worker] Job {job_id} not found")
                        return

                    if job.status != "pending":
                        log_now(f"[Worker] Job {job_id} status is {job.status}, skipping")
                        return

                    # Mark as running
                    job.status = "running"
                    job.started_at = datetime.utcnow()
                    job.progress = 0
                    job.progress_message = "Starting..."
                    await db.commit()

                    log_now(f"[Worker] Starting job {job_id} ({job.job_type})")

                    # Process based on job type
                    if job.job_type == "fetch_more_editions":
                        result = await process_fetch_more_job(job, db)
                    elif job.job_type == "extract_citations":
                        result = await process_extract_citations_job(job, db)
                    elif job.job_type == "resolve":
                        result = await process_resolve_job(job, db)
                    elif job.job_type == "partition_harvest_test":
                        result = await process_partition_harvest_test(job, db)
                    elif job.job_type == "retry_failed_fetches":
                        result = await process_retry_failed_fetches_job(job, db)
                    elif job.job_type == "verify_and_repair":
                        result = await process_verify_and_repair_job(job, db)
                    elif job.job_type == "thinker_discover_works":
                        result = await process_thinker_discover_works(job, db)
                    elif job.job_type == "thinker_harvest_citations":
                        result = await process_thinker_harvest_citations(job, db)
                    else:
                        raise ValueError(f"Unknown job type: {job.job_type}")

                    # Mark as completed
                    job.status = "completed"
                    job.progress = 100
                    job.progress_message = "Completed"
                    job.result = json.dumps(result)
                    job.completed_at = datetime.utcnow()
                    await db.commit()

                    log_now(f"[Worker] Completed job {job_id}")

                    # Send webhook callback if configured
                    if job.callback_url:
                        await send_webhook_callback(job, db)
                        await db.commit()

                except Exception as e:
                    log_now(f"[Worker] Job {job_id} failed: {e}")
                    log_now(f"[Worker] Traceback: {traceback.format_exc()}")
                    # Mark as failed
                    try:
                        job.status = "failed"
                        job.error = str(e)
                        job.completed_at = datetime.utcnow()
                        await db.commit()

                        # Send webhook callback for failure if configured
                        if job.callback_url:
                            await send_webhook_callback(job, db)
                            await db.commit()
                    except:
                        pass
        finally:
            # Always remove from running set
            _running_jobs.discard(job_id)
            log_now(f"[Worker] Job {job_id} released slot ({len(_running_jobs)}/{MAX_CONCURRENT_JOBS} running)")


async def check_and_reset_zombie_jobs() -> int:
    """
    Periodically check for zombie jobs - jobs marked as 'running' in the database
    but not actually being processed by this worker instance.

    This catches jobs that got stuck due to:
    - Worker crashes mid-job
    - Unhandled exceptions that didn't properly release the job
    - Database connection issues

    Returns the number of zombie jobs reset.
    """
    global _last_zombie_check, _running_jobs

    now = datetime.utcnow()

    # Only check every ZOMBIE_CHECK_INTERVAL_MINUTES
    if _last_zombie_check and (now - _last_zombie_check).total_seconds() < ZOMBIE_CHECK_INTERVAL_MINUTES * 60:
        return 0

    _last_zombie_check = now
    zombie_count = 0

    try:
        async with async_session() as db:
            # Find jobs that are "running" in DB but:
            # 1. Not in our in-memory _running_jobs set, OR
            # 2. Have been running for longer than JOB_TIMEOUT_MINUTES
            timeout_threshold = now - timedelta(minutes=JOB_TIMEOUT_MINUTES)

            result = await db.execute(
                select(Job).where(
                    Job.status == "running",
                    Job.started_at < timeout_threshold
                )
            )
            stale_jobs = result.scalars().all()

            if stale_jobs:
                # Filter to only jobs not in our running set (true zombies)
                zombie_jobs = [j for j in stale_jobs if j.id not in _running_jobs]

                if zombie_jobs:
                    zombie_ids = [j.id for j in zombie_jobs]
                    zombie_count = len(zombie_jobs)

                    log_now(f"[ZOMBIE CHECK] Found {zombie_count} zombie jobs: {zombie_ids}")

                    # Reset them to pending
                    await db.execute(
                        update(Job)
                        .where(Job.id.in_(zombie_ids))
                        .values(status="pending", started_at=None)
                    )
                    await db.commit()

                    log_now(f"[ZOMBIE CHECK] Reset {zombie_count} zombie jobs to 'pending'")

    except Exception as e:
        log_now(f"[ZOMBIE CHECK] Error checking for zombies: {e}", "error")

    return zombie_count


async def worker_loop():
    """Main worker loop - processes pending jobs with parallel execution"""
    global _worker_running, _job_semaphore, _running_jobs, _last_zombie_check
    _worker_running = True
    _job_semaphore = asyncio.Semaphore(MAX_CONCURRENT_JOBS)
    _last_zombie_check = None  # Reset on startup so first check runs immediately after startup detection
    _running_jobs = set()

    log_now(f"[Worker] Starting parallel job worker (max {MAX_CONCURRENT_JOBS} concurrent jobs)")

    # ZOMBIE JOB DETECTION: Reset any "running" jobs to "pending"
    # Since this worker just started, any "running" jobs are zombies from a previous worker
    try:
        async with async_session() as db:
            result = await db.execute(
                select(Job).where(Job.status == "running")
            )
            zombie_jobs = result.scalars().all()

            if zombie_jobs:
                zombie_count = len(zombie_jobs)
                zombie_ids = [j.id for j in zombie_jobs]
                log_now(f"[Worker] ZOMBIE DETECTION: Found {zombie_count} jobs stuck in 'running' state")
                log_now(f"[Worker] ZOMBIE DETECTION: Job IDs: {zombie_ids}")

                # Reset them to pending so they get picked up again
                await db.execute(
                    update(Job)
                    .where(Job.status == "running")
                    .values(status="pending", started_at=None)
                )
                await db.commit()
                log_now(f"[Worker] ZOMBIE DETECTION: Reset {zombie_count} zombie jobs to 'pending'")
            else:
                log_now("[Worker] ZOMBIE DETECTION: No zombie jobs found - clean startup")
    except Exception as e:
        log_now(f"[Worker] ZOMBIE DETECTION ERROR: {e}")

    # ORPHAN DETECTION: Find editions with partial harvests but no resume state
    # This catches cases where jobs crashed without saving progress
    try:
        async with async_session() as db:
            # Find year-by-year editions (>1000 citations) with partial harvests but no resume state
            orphan_result = await db.execute(
                select(Edition)
                .where(
                    Edition.selected == True,
                    Edition.scholar_id.isnot(None),
                    Edition.citation_count > YEAR_BY_YEAR_THRESHOLD,  # Should use year-by-year
                    Edition.harvested_citation_count > 100,  # Has some progress
                    Edition.harvested_citation_count < Edition.citation_count,  # Not complete
                    or_(
                        Edition.harvest_resume_state.is_(None),  # No resume state
                        Edition.harvest_resume_state == ""
                    )
                )
            )
            orphan_editions = list(orphan_result.scalars().all())

            if orphan_editions:
                log_now(f"[Worker] ORPHAN DETECTION: Found {len(orphan_editions)} editions with partial harvest but no resume state")

                for edition in orphan_editions:
                    log_now(f"[Worker] ORPHAN: Edition {edition.id} - {edition.harvested_citation_count}/{edition.citation_count} harvested, rebuilding state...")

                    # BUG FIX: Use harvest_targets.status as source of truth instead of citation count heuristic
                    # The old code used "count >= 50" which incorrectly marked years as complete
                    completed_targets_result = await db.execute(
                        select(HarvestTarget.year)
                        .where(HarvestTarget.edition_id == edition.id)
                        .where(HarvestTarget.status == 'complete')
                    )
                    completed_years = [row.year for row in completed_targets_result.fetchall()]

                    if completed_years:
                        reconstructed_state = {
                            "mode": "year_by_year",
                            "current_year": None,
                            "current_page": 0,
                            "completed_years": sorted(completed_years, reverse=True),
                            "reconstructed": True,
                            "reconstructed_at": datetime.utcnow().isoformat(),
                        }
                        await db.execute(
                            update(Edition)
                            .where(Edition.id == edition.id)
                            .values(harvest_resume_state=json.dumps(reconstructed_state))
                        )
                        log_now(f"[Worker] ORPHAN: Edition {edition.id} - Reconstructed {len(completed_years)} completed years")
                    else:
                        log_now(f"[Worker] ORPHAN: Edition {edition.id} - No year data found, will scan from scratch")

                await db.commit()
                log_now(f"[Worker] ORPHAN DETECTION: Rebuilt resume states for {len(orphan_editions)} editions")
            else:
                log_now("[Worker] ORPHAN DETECTION: No orphaned editions found")
    except Exception as e:
        log_now(f"[Worker] ORPHAN DETECTION ERROR: {e}")

    while _worker_running:
        try:
            # Calculate how many slots are available
            available_slots = MAX_CONCURRENT_JOBS - len(_running_jobs)

            if available_slots > 0:
                async with async_session() as db:
                    # Get multiple pending jobs (up to available slots)
                    result = await db.execute(
                        select(Job)
                        .where(Job.status == "pending")
                        .order_by(Job.priority.desc(), Job.created_at.asc())
                        .limit(available_slots)
                    )
                    pending_jobs = result.scalars().all()

                    if pending_jobs:
                        log_now(f"[Worker] Found {len(pending_jobs)} pending jobs, {available_slots} slots available")

                        # Start all pending jobs in parallel
                        for job in pending_jobs:
                            if job.id not in _running_jobs:
                                asyncio.create_task(process_single_job(job.id))
                                await asyncio.sleep(0.5)  # Small stagger to avoid race conditions

                        await asyncio.sleep(2)  # Brief pause before checking for more

                    # ALWAYS check for incomplete harvests if we have spare capacity
                    # This runs even when there are some pending jobs, ensuring we don't miss orphaned work
                    remaining_slots = MAX_CONCURRENT_JOBS - len(_running_jobs) - len(pending_jobs)
                    if remaining_slots > 0:
                        try:
                            resumed = await auto_resume_incomplete_harvests(db)
                            if resumed > 0:
                                log_now(f"[Worker] Auto-resumed {resumed} incomplete harvests with {remaining_slots} spare slots")
                                continue  # Immediately process the new jobs
                        except Exception as e:
                            log_now(f"[Worker] Auto-resume check failed: {e}")

                        # Check for failed fetches that need retry
                        try:
                            retried = await auto_retry_failed_fetches(db)
                            if retried > 0:
                                continue  # Immediately process the new job
                        except Exception as e:
                            log_now(f"[Worker] Auto-retry check failed: {e}")

                        # Retry buffered citation saves that failed (DB timeouts, etc.)
                        try:
                            from .citation_buffer import retry_failed_saves
                            retried_citations = await retry_failed_saves()
                            if retried_citations > 0:
                                log_now(f"[Worker] Retried {retried_citations} buffered citation pages")
                        except Exception as e:
                            log_now(f"[Worker] Citation buffer retry failed: {e}")

                    # If no pending jobs at all, wait before checking again
                    if not pending_jobs:
                        await asyncio.sleep(5)
            else:
                # All slots full, wait for one to free up
                await asyncio.sleep(3)

            # Sync _running_jobs with database - remove jobs that were cancelled externally
            if _running_jobs:
                async with async_session() as sync_db:
                    actually_running = await sync_db.execute(
                        select(Job.id).where(
                            Job.id.in_(list(_running_jobs)),
                            Job.status == "running"
                        )
                    )
                    actually_running_ids = set(row[0] for row in actually_running.fetchall())
                    stale_ids = _running_jobs - actually_running_ids
                    if stale_ids:
                        log_now(f"[Worker] Cleaning {len(stale_ids)} stale job IDs from _running_jobs: {stale_ids}")
                        _running_jobs.difference_update(stale_ids)

            # Periodically check for zombie jobs (runs every ZOMBIE_CHECK_INTERVAL_MINUTES)
            await check_and_reset_zombie_jobs()

        except Exception as e:
            log_now(f"[Worker] Loop error: {e}")
            log_now(f"[Worker] Traceback: {traceback.format_exc()}")
            await asyncio.sleep(10)

    log_now("[Worker] Worker loop stopped")


def start_worker():
    """Start the background worker (called at app startup)"""
    global _worker_task
    if _worker_task is None or _worker_task.done():
        _worker_task = asyncio.create_task(worker_loop())
        log_now("[Worker] Background worker started")


def stop_worker():
    """Stop the background worker"""
    global _worker_running, _worker_task
    _worker_running = False
    if _worker_task:
        _worker_task.cancel()
        _worker_task = None
    log_now("[Worker] Background worker stopped")


async def create_fetch_more_job(
    db: AsyncSession,
    paper_id: int,
    language: str,
    max_results: int = 50,
) -> Job:
    """Create a fetch_more_editions job"""
    job = Job(
        paper_id=paper_id,
        job_type="fetch_more_editions",
        status="pending",
        params=json.dumps({
            "language": language,
            "max_results": max_results,
        }),
        progress=0,
        progress_message=f"Queued: Fetch {language} editions",
    )
    db.add(job)
    await db.flush()
    await db.refresh(job)
    return job


async def create_extract_citations_job(
    db: AsyncSession,
    paper_id: int,
    edition_ids: list = None,
    max_citations_per_edition: int = 1000,
    skip_threshold: int = 50000,
    # Refresh mode params
    is_refresh: bool = False,
    year_low: int = None,
    batch_id: str = None,
    force_create: bool = False,
    # Resume mode params
    is_resume: bool = False,
    resume_message: str = None,
    # Callback params for completion notification
    callback_url: str = None,
    callback_secret: str = None,
) -> Job:
    """Create an extract_citations job

    Args:
        paper_id: Paper to extract citations for
        edition_ids: Specific editions (empty = all selected)
        max_citations_per_edition: Max citations to fetch per edition
        skip_threshold: Skip editions with more citations than this
        is_refresh: If True, this is a refresh job (updates harvest timestamps)
        year_low: Only fetch citations from this year onwards (for incremental refresh)
        batch_id: UUID to track collection/global refresh batches
        force_create: If True, skip duplicate check (for special cases)
        is_resume: If True, this is an auto-resume job
        resume_message: Custom message for resume jobs
        callback_url: URL to POST to when job completes (for thinker harvest tracking)
        callback_secret: HMAC secret for signing callback requests
    """
    # DUPLICATE PREVENTION: Check for existing pending/running job for same paper
    if not force_create:
        from sqlalchemy import select
        existing_result = await db.execute(
            select(Job).where(
                Job.paper_id == paper_id,
                Job.job_type == "extract_citations",
                Job.status.in_(["pending", "running"])
            )
        )
        existing_job = existing_result.scalar_one_or_none()
        if existing_job:
            logger.info(f"Duplicate prevention: returning existing job {existing_job.id} for paper {paper_id} (status: {existing_job.status})")
            return existing_job

    params = {
        "edition_ids": edition_ids or [],
        "max_citations_per_edition": max_citations_per_edition,
        "skip_threshold": skip_threshold,
    }

    # Add refresh params if this is a refresh job
    if is_refresh:
        params["is_refresh"] = True
        if year_low:
            params["year_low"] = year_low
        if batch_id:
            params["batch_id"] = batch_id

    # Add resume params if this is an auto-resume job
    if is_resume:
        params["is_resume"] = True

    # Set appropriate message
    if resume_message:
        message = resume_message
    elif is_refresh:
        message = "Queued: Refresh citations"
    elif is_resume:
        message = "Queued: Auto-resume harvest"
    else:
        message = "Queued: Extract citations"

    job = Job(
        paper_id=paper_id,
        job_type="extract_citations",
        status="pending",
        params=json.dumps(params),
        progress=0,
        progress_message=message,
        callback_url=callback_url,
        callback_secret=callback_secret,
    )
    db.add(job)
    await db.flush()
    await db.refresh(job)

    # Monitor job creation rate to detect runaway bugs
    monitor_job_creation_rate(paper_id, "extract_citations")

    return job
