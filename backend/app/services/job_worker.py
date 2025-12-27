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
from sqlalchemy import select, update, func
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import Job, Paper, Edition, Citation, RawSearchResult, FailedFetch, HarvestTarget
from ..database import async_session
from .edition_discovery import EditionDiscoveryService
from .scholar_search import get_scholar_service

logger = logging.getLogger(__name__)

# Force immediate log output
def log_now(msg: str, level: str = "info"):
    """Log message and immediately flush to stdout"""
    timestamp = datetime.utcnow().strftime("%H:%M:%S")
    print(f"{timestamp} | job_worker | {level.upper()} | {msg}", flush=True)
    sys.stdout.flush()

# Job timeout settings
JOB_TIMEOUT_MINUTES = 30  # Mark job as failed if no progress for this long
HEARTBEAT_INTERVAL = 60  # Seconds between heartbeat updates

# Staleness threshold (for UI indicators)
STALENESS_THRESHOLD_DAYS = 90

# Global worker state
_worker_task: Optional[asyncio.Task] = None
_worker_running = False

# Parallel processing settings
MAX_CONCURRENT_JOBS = 5  # How many jobs can run simultaneously
_job_semaphore: Optional[asyncio.Semaphore] = None
_running_jobs: set = set()  # Track currently running job IDs


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
            target.expected_count = expected_count
            target.updated_at = datetime.utcnow()
            await db.commit()
            log_now(f"[HarvestTarget] Updated edition {edition_id} year {year}: expected={expected_count}")
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
    mark_complete: bool = False
):
    """Update a HarvestTarget with progress data after harvesting."""
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

        if mark_complete:
            target.status = "complete" if pages_failed == 0 else "incomplete"
            target.completed_at = datetime.utcnow()

        await db.commit()
        status_str = f", status={target.status}" if mark_complete else ""
        log_now(f"[HarvestTarget] Updated edition {edition_id} year {year}: actual={actual_count}, pages OK={pages_succeeded}, pages FAIL={pages_failed}{status_str}")


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

                    citation = Citation(
                        paper_id=edition.paper_id,
                        edition_id=edition.id,
                        scholar_id=scholar_id,
                        title=paper_data.get("title", "Unknown"),
                        authors=paper_data.get("authorsRaw"),
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
AUTO_RESUME_MAX_STALL_COUNT = 5  # Stop auto-resume after this many consecutive zero-progress jobs (increased from 3)
_last_auto_resume_check = None


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
            )
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
    if not incomplete:
        return 0

    # GROUP editions by paper_id to avoid duplicate jobs for same paper
    # This is critical - multiple editions of same paper share citations!
    editions_by_paper: Dict[int, List[Edition]] = {}
    for edition in incomplete:
        if edition.paper_id not in editions_by_paper:
            editions_by_paper[edition.paper_id] = []
        editions_by_paper[edition.paper_id].append(edition)

    log_now(f"[AutoResume] Found {len(incomplete)} incomplete editions across {len(editions_by_paper)} papers")

    jobs_queued = 0
    for paper_id, paper_editions in editions_by_paper.items():
        # Log all editions for this paper
        total_missing = sum(e.citation_count - e.harvested_citation_count for e in paper_editions)
        edition_ids = [e.id for e in paper_editions]
        log_now(f"[AutoResume] Paper {paper_id}: {len(paper_editions)} editions with {total_missing:,} total missing citations")
        for e in paper_editions:
            missing = e.citation_count - e.harvested_citation_count
            log_now(f"[AutoResume]   - Edition {e.id}: {e.harvested_citation_count}/{e.citation_count} harvested ({missing} missing)")

        # Create ONE job for ALL editions of this paper
        job = Job(
            paper_id=paper_id,
            job_type="extract_citations",
            status="pending",
            params=json.dumps({
                "edition_ids": edition_ids,  # ALL editions for this paper
                "max_citations_per_edition": 1000,
                "skip_threshold": 50000,
                "is_resume": True,
            }),
            progress=0,
            progress_message=f"Auto-resume: {len(paper_editions)} editions, {total_missing:,} citations remaining",
        )
        db.add(job)
        jobs_queued += 1
        log_now(f"[AutoResume] Queued 1 job for paper {paper_id} covering {len(paper_editions)} editions")

    if jobs_queued > 0:
        await db.commit()
        log_now(f"[AutoResume] Queued {jobs_queued} auto-resume jobs (1 per paper)")

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
    valid_editions = []
    skipped_editions = []
    for e in editions:
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

    if not valid_editions:
        raise ValueError(f"No valid editions to process (all {len(skipped_editions)} skipped)")

    log_now(f"[Worker] Processing {len(valid_editions)} editions, skipped {len(skipped_editions)}")

    # Get existing citations to avoid duplicates (refreshed after each save)
    async def get_existing_scholar_ids():
        result = await db.execute(
            select(Citation.scholar_id).where(Citation.paper_id == paper_id)
        )
        return {r[0] for r in result.fetchall() if r[0]}

    existing_scholar_ids = await get_existing_scholar_ids()

    # Calculate totals for progress tracking (AFTER existing_scholar_ids is fetched)
    total_target_citations = sum(e.citation_count or 0 for e in valid_editions)
    total_previously_harvested = len(existing_scholar_ids)

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
        async def save_page_citations(page_num: int, papers: List[Dict]):
            nonlocal total_new_citations, total_updated_citations, existing_scholar_ids, params

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
                    # NEW citation - use INSERT ON CONFLICT DO NOTHING to prevent duplicates
                    # even if concurrent jobs have the same citation
                    from sqlalchemy import text
                    from sqlalchemy.dialects.postgresql import insert as pg_insert

                    stmt = pg_insert(Citation).values(
                        paper_id=paper_id,
                        edition_id=edition.id,
                        scholar_id=scholar_id,
                        title=paper_data.get("title", "Unknown"),
                        authors=paper_data.get("authorsRaw"),
                        year=paper_data.get("year"),
                        venue=paper_data.get("venue"),
                        abstract=paper_data.get("abstract"),
                        link=paper_data.get("link"),
                        citation_count=paper_data.get("citationCount", 0),
                        intersection_count=1,
                    ).on_conflict_do_nothing(
                        index_elements=['paper_id', 'scholar_id']
                    )
                    result = await db.execute(stmt)

                    # Check if row was actually inserted (rowcount = 1) or skipped due to conflict
                    if result.rowcount > 0:
                        new_count += 1
                        total_new_citations += 1
                    else:
                        # Duplicate detected by database - already exists from concurrent job
                        total_updated_citations += 1

                    existing_scholar_ids.add(scholar_id)

            # COMMIT IMMEDIATELY after each page
            await db.commit()
            log_now(f"[CALLBACK] ✓ Page {page_num + 1} complete: {new_count} new, {skipped_no_id} skipped (no ID), total: {total_new_citations}")

            # Update job progress with current state for resume
            # Cap progress at 90% (leave 10% for completion), handle year-by-year mode with many pages
            raw_progress = 10 + ((i + min(page_num / 100, 0.9)) / total_editions) * 80
            progress_pct = min(raw_progress, 90)
            year_info = f" ({current_harvest_year['year']})" if current_harvest_year['year'] else ""
            await update_job_progress(
                db, job.id, progress_pct,
                f"Edition {i+1}/{total_editions}{year_info}, page {page_num + 1}: {total_new_citations} citations saved",
                details={
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
                }
            )

            # Save resume state (update params dict and serialize to job)
            params["resume_state"] = {
                "edition_id": edition.id,
                "last_page": page_num + 1,
                "total_citations": total_new_citations,
            }
            job.params = json.dumps(params)
            await db.commit()

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
            # Google Scholar limits ~1000 results per query
            YEAR_BY_YEAR_THRESHOLD = 1000
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
                # IMPORTANT: Only trust completed_years if we have saved state from year-by-year mode
                # If previous harvest was non-year-by-year, existing citations are scattered across years
                # and we need to re-scan all years to get complete coverage
                resume_state_json = edition.harvest_resume_state
                completed_years = set()  # Start empty - only trust saved state
                resume_year = None
                resume_page_for_year = 0

                if resume_state_json:
                    try:
                        resume_state = json.loads(resume_state_json)
                        if resume_state.get("mode") == "year_by_year":
                            # Only trust completed_years from saved year-by-year state
                            completed_years = set(resume_state.get("completed_years", []))
                            resume_year = resume_state.get("current_year")
                            resume_page_for_year = resume_state.get("current_page", 0)
                            log_now(f"[EDITION {i+1}] 🔄 RESUMING from saved state: completed years={sorted(completed_years, reverse=True)[:5]}..., resume from year {resume_year} page {resume_page_for_year}")
                    except json.JSONDecodeError:
                        log_now(f"[EDITION {i+1}] ⚠️ Could not parse resume state, starting fresh")
                else:
                    # No saved state - RECONSTRUCT completed_years from existing citations in DB
                    # This prevents re-scanning years that are already fully harvested
                    if edition.harvested_citation_count and edition.harvested_citation_count > 100:
                        log_now(f"[EDITION {i+1}] 🔧 No saved state but {edition.harvested_citation_count:,} citations exist - reconstructing completed years from DB...")

                        # Query citations grouped by year to find which years have data
                        year_counts_result = await db.execute(
                            select(Citation.year, func.count(Citation.id).label('count'))
                            .where(Citation.edition_id == edition.id)
                            .where(Citation.year.isnot(None))
                            .group_by(Citation.year)
                            .order_by(Citation.year.desc())
                        )
                        year_counts = {row.year: row.count for row in year_counts_result.fetchall()}

                        if year_counts:
                            # Years with significant citations (>50) are likely complete
                            # Mark them as completed so we skip them
                            for year, count in year_counts.items():
                                if count >= 50:  # Threshold: year with 50+ citations is probably complete
                                    completed_years.add(year)

                            log_now(f"[EDITION {i+1}] 🔧 Reconstructed {len(completed_years)} completed years from DB: {sorted(completed_years, reverse=True)[:10]}...")
                            log_now(f"[EDITION {i+1}] 🔧 Year counts sample: {dict(list(year_counts.items())[:5])}")

                            # Save this reconstructed state so future resumes are faster
                            reconstructed_state = {
                                "mode": "year_by_year",
                                "current_year": None,  # Start fresh from current year
                                "current_page": 0,
                                "completed_years": sorted(list(completed_years), reverse=True),
                                "reconstructed": True,  # Mark as reconstructed
                            }
                            await db.execute(
                                update(Edition)
                                .where(Edition.id == edition.id)
                                .values(harvest_resume_state=json.dumps(reconstructed_state))
                            )
                            await db.commit()
                            log_now(f"[EDITION {i+1}] ✓ Saved reconstructed resume state")
                        else:
                            log_now(f"[EDITION {i+1}] ℹ️ No year data found in citations - will scan all years")
                    else:
                        log_now(f"[EDITION {i+1}] ℹ️ No saved year-by-year state - will scan all years")

                # Helper to save year-by-year resume state to edition
                async def save_year_resume_state(year: int, page: int, completed: set):
                    """Save current progress to edition.harvest_resume_state"""
                    state = {
                        "mode": "year_by_year",
                        "current_year": year,
                        "current_page": page,
                        "completed_years": sorted(list(completed), reverse=True),
                    }
                    await db.execute(
                        update(Edition)
                        .where(Edition.id == edition.id)
                        .values(harvest_resume_state=json.dumps(state))
                    )
                    await db.commit()

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

                # Import harvest_partition for overflow years
                from .overflow_harvester import harvest_partition

                for year in range(current_year, min_year - 1, -1):
                    # SKIP completed years entirely
                    if year in completed_years:
                        log_now(f"[EDITION {i+1}] 📅 Year {year}: SKIPPING (already completed)")
                        continue

                    year_start_citations = total_new_citations
                    current_harvest_year["year"] = year

                    # Determine start page for this year
                    start_page_for_this_year = 0
                    if resume_year == year and resume_page_for_year > 0:
                        start_page_for_this_year = resume_page_for_year
                        log_now(f"[EDITION {i+1}] 📅 Fetching year {year} (RESUMING from page {start_page_for_this_year})...")
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
                    if total_this_year > 0:
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

                        try:
                            partition_stats = await harvest_partition(
                                scholar_service=scholar_service,
                                db=db,
                                edition_id=edition.id,
                                scholar_id=edition.scholar_id,
                                year=year,
                                edition_title=edition.title,
                                paper_id=paper_id,
                                existing_scholar_ids=existing_scholar_ids,
                                on_page_complete=save_page_citations_with_year_state,
                                total_for_year=total_this_year,
                            )

                            year_citations = partition_stats.get("total_new", 0)
                            total_new_citations += year_citations
                            log_now(f"[EDITION {i+1}] 📅 Year {year}: ✓ Partition harvest complete - {year_citations} new citations")

                        except Exception as partition_err:
                            log_now(f"[EDITION {i+1}] ⚠️ Partition harvest failed: {partition_err}", "warning")
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
                            # Track pages from result
                            if isinstance(result, dict):
                                year_pages_succeeded = result.get("pages_succeeded", 0)
                                year_pages_attempted = result.get("pages_fetched", 0)

                    else:
                        # Normal case: <= 1000 citations, standard harvest
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
                        # Track pages from result
                        if isinstance(result, dict):
                            year_pages_succeeded = result.get("pages_succeeded", 0)
                            year_pages_failed = result.get("pages_failed", 0)
                            year_pages_attempted = result.get("pages_fetched", 0)
                        log_now(f"[EDITION {i+1}] 📅 Year {year}: {year_citations} new citations (Scholar reports {total_this_year} total, pages OK={year_pages_succeeded}, FAIL={year_pages_failed})")

                    # Mark this year as completed
                    completed_years.add(year)
                    await save_year_resume_state(year, 0, completed_years)  # page=0 since year is done

                    # Update HarvestTarget with actual results for this year
                    if total_this_year > 0:
                        await update_harvest_target_progress(
                            db=db,
                            edition_id=edition.id,
                            year=year,
                            actual_count=year_citations,
                            pages_succeeded=year_pages_succeeded,
                            pages_failed=year_pages_failed,
                            pages_attempted=year_pages_attempted,
                            mark_complete=True
                        )

                    # Track consecutive empty years for early termination
                    if year_citations == 0 and total_this_year == 0:
                        consecutive_empty_years += 1
                        if consecutive_empty_years >= 3 and year < current_year - 5:
                            log_now(f"[EDITION {i+1}] 📅 {consecutive_empty_years} consecutive empty years, stopping year-by-year fetch")
                            break
                    else:
                        consecutive_empty_years = 0

                    # Small delay between year queries
                    await asyncio.sleep(2)

                # Clear resume state on successful completion of all years
                await db.execute(
                    update(Edition)
                    .where(Edition.id == edition.id)
                    .values(harvest_resume_state=None)
                )
                await db.commit()
                log_now(f"[EDITION {i+1}] ✓ Year-by-year harvest complete, cleared resume state")

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

                # Update HarvestTarget with actual results
                if isinstance(result, dict) and edition.citation_count and edition.citation_count > 0:
                    std_citations = total_new_citations - edition_start_citations
                    await update_harvest_target_progress(
                        db=db,
                        edition_id=edition.id,
                        year=None,  # Standard harvest = all years
                        actual_count=std_citations,
                        pages_succeeded=result.get("pages_succeeded", 0),
                        pages_failed=result.get("pages_failed", 0),
                        pages_attempted=result.get("pages_fetched", 0),
                        mark_complete=True
                    )

            edition_citations = total_new_citations - edition_start_citations
            log_now(f"[EDITION {i+1}] ✓ Complete: {edition_citations} new citations saved")

            # Update edition harvest stats (always, not just refresh mode)
            await update_edition_harvest_stats(db, edition.id)

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
    # If no new citations were found, this might be a stalled harvest (can't progress further)
    for edition in valid_editions:
        if total_new_citations == 0:
            # No progress - increment stall count
            current_stall = edition.harvest_stall_count or 0
            edition.harvest_stall_count = current_stall + 1
            if edition.harvest_stall_count >= AUTO_RESUME_MAX_STALL_COUNT:
                log_now(f"[STALL] Edition {edition.id} has stalled after {edition.harvest_stall_count} consecutive zero-progress jobs")
        else:
            # Made progress - reset stall count
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

    # Get existing citation scholar IDs to avoid duplicates
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

            # Create citation
            citation = Citation(
                edition_id=edition_id,
                paper_id=edition.paper_id,
                scholar_id=scholar_id,
                title=paper.get("title", ""),
                authors=paper.get("authorsRaw") or ", ".join(paper.get("authors", [])),
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

    # Get existing citations to avoid duplicates
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

                                citation = Citation(
                                    paper_id=paper_id,
                                    edition_id=edition.id,
                                    scholar_id=cit_scholar_id,
                                    title=paper_data.get("title", "Unknown"),
                                    authors=paper_data.get("authorsRaw"),
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

                except Exception as e:
                    log_now(f"[Worker] Job {job_id} failed: {e}")
                    log_now(f"[Worker] Traceback: {traceback.format_exc()}")
                    # Mark as failed
                    try:
                        job.status = "failed"
                        job.error = str(e)
                        job.completed_at = datetime.utcnow()
                        await db.commit()
                    except:
                        pass
        finally:
            # Always remove from running set
            _running_jobs.discard(job_id)
            log_now(f"[Worker] Job {job_id} released slot ({len(_running_jobs)}/{MAX_CONCURRENT_JOBS} running)")


async def worker_loop():
    """Main worker loop - processes pending jobs with parallel execution"""
    global _worker_running, _job_semaphore, _running_jobs
    _worker_running = True
    _job_semaphore = asyncio.Semaphore(MAX_CONCURRENT_JOBS)
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

                    # Reconstruct completed years from existing citations
                    year_counts_result = await db.execute(
                        select(Citation.year, func.count(Citation.id).label('count'))
                        .where(Citation.edition_id == edition.id)
                        .where(Citation.year.isnot(None))
                        .group_by(Citation.year)
                    )
                    year_counts = {row.year: row.count for row in year_counts_result.fetchall()}

                    # Years with significant citations (>50) are likely complete
                    completed_years = [year for year, count in year_counts.items() if count >= 50]

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

                    # If no pending jobs at all, wait before checking again
                    if not pending_jobs:
                        await asyncio.sleep(5)
            else:
                # All slots full, wait for one to free up
                await asyncio.sleep(3)

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
    """
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

    message = "Queued: Refresh citations" if is_refresh else "Queued: Extract citations"

    job = Job(
        paper_id=paper_id,
        job_type="extract_citations",
        status="pending",
        params=json.dumps(params),
        progress=0,
        progress_message=message,
    )
    db.add(job)
    await db.flush()
    await db.refresh(job)
    return job
