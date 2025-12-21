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
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import Job, Paper, Edition, Citation, RawSearchResult
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


# Auto-resume settings
AUTO_RESUME_MIN_MISSING = 100  # Only resume if at least this many citations missing
AUTO_RESUME_MIN_PERCENT = 0.10  # Or at least 10% missing
AUTO_RESUME_CHECK_INTERVAL = 60  # Seconds between auto-resume checks
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
    """
    from sqlalchemy import and_, or_, not_, exists

    # Subquery: papers with pending/running extract_citations jobs
    papers_with_jobs = (
        select(Job.paper_id)
        .where(
            Job.job_type == "extract_citations",
            Job.status.in_(["pending", "running"])
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
            Edition.paper_id.notin_(papers_with_jobs)
        )
        .order_by(
            # Prioritize: larger gaps first
            (Edition.citation_count - Edition.harvested_citation_count).desc()
        )
        .limit(MAX_CONCURRENT_JOBS)  # Don't queue more than we can process
    )

    return list(result.scalars().all())


async def auto_resume_incomplete_harvests(db: AsyncSession) -> int:
    """Find and queue jobs for incomplete harvests. Returns number of jobs queued."""
    global _last_auto_resume_check

    # Rate limit checks
    now = datetime.utcnow()
    if _last_auto_resume_check and (now - _last_auto_resume_check).total_seconds() < AUTO_RESUME_CHECK_INTERVAL:
        return 0
    _last_auto_resume_check = now

    incomplete = await find_incomplete_harvests(db)
    if not incomplete:
        return 0

    jobs_queued = 0
    for edition in incomplete:
        missing = edition.citation_count - edition.harvested_citation_count
        # Calculate resume page: Google Scholar shows 10 results per page
        # If we have 643 citations, we've fetched ~64 pages, resume from page 64
        resume_page = edition.harvested_citation_count // 10
        log_now(f"[AutoResume] Edition {edition.id} (paper {edition.paper_id}): {edition.harvested_citation_count}/{edition.citation_count} harvested, missing {missing}, resume from page {resume_page}")

        # Create resume job with proper resume_state to skip already-fetched pages
        job = Job(
            paper_id=edition.paper_id,
            job_type="extract_citations",
            status="pending",
            params=json.dumps({
                "edition_ids": [edition.id],
                "max_citations_per_edition": 1000,
                "skip_threshold": 50000,
                "is_resume": True,
                "resume_state": {
                    "edition_id": edition.id,
                    "last_page": resume_page,
                    "total_citations": edition.harvested_citation_count,
                },
            }),
            progress=0,
            progress_message=f"Auto-resume from page {resume_page}: {missing:,} citations remaining",
        )
        db.add(job)
        jobs_queued += 1
        log_now(f"[AutoResume] Queued resume job for edition {edition.id} starting at page {resume_page}")

    if jobs_queued > 0:
        await db.commit()
        log_now(f"[AutoResume] Queued {jobs_queued} auto-resume jobs")

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

    # Filter out editions without scholar_id or above threshold
    valid_editions = []
    skipped_editions = []
    for e in editions:
        if not e.scholar_id:
            skipped_editions.append({"id": e.id, "title": e.title, "reason": "no_scholar_id"})
        elif e.citation_count > skip_threshold:
            skipped_editions.append({
                "id": e.id,
                "title": e.title,
                "reason": f"too_many_citations ({e.citation_count} > {skip_threshold})"
            })
        else:
            valid_editions.append(e)

    if not valid_editions:
        raise ValueError(f"No valid editions to process (all {len(skipped_editions)} skipped)")

    log_now(f"[Worker] Processing {len(valid_editions)} editions, skipped {len(skipped_editions)}")
    await update_job_progress(db, job.id, 5, f"Processing {len(valid_editions)} editions...")

    # Get existing citations to avoid duplicates (refreshed after each save)
    async def get_existing_scholar_ids():
        result = await db.execute(
            select(Citation.scholar_id).where(Citation.paper_id == paper_id)
        )
        return {r[0] for r in result.fetchall() if r[0]}

    existing_scholar_ids = await get_existing_scholar_ids()

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
                    # Already exists - could update intersection count here if needed
                    total_updated_citations += 1
                else:
                    # NEW citation - save immediately
                    citation = Citation(
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
                    )
                    db.add(citation)
                    existing_scholar_ids.add(scholar_id)
                    new_count += 1
                    total_new_citations += 1

            # COMMIT IMMEDIATELY after each page
            await db.commit()
            log_now(f"[CALLBACK] ✓ Page {page_num + 1} complete: {new_count} new, {skipped_no_id} skipped (no ID), total: {total_new_citations}")

            # Update job progress with current state for resume
            progress_pct = 10 + ((i + (page_num / 20)) / total_editions) * 70
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

                # For refresh mode, use year_low_param or edition's last harvest year
                # Otherwise, go back to 1990
                if is_refresh and year_low_param:
                    min_year = year_low_param
                    log_now(f"[EDITION {i+1}] REFRESH: Will fetch from {current_year} backwards to {min_year} (from params)")
                elif is_refresh and edition.last_harvest_year:
                    min_year = edition.last_harvest_year
                    log_now(f"[EDITION {i+1}] REFRESH: Will fetch from {current_year} backwards to {min_year} (from edition last harvest)")
                else:
                    min_year = 1990  # Full harvest - go back to 1990
                    log_now(f"[EDITION {i+1}] Will fetch from {current_year} backwards to {min_year}...")

                # Fetch year by year from current year backwards
                current_harvest_year["mode"] = "year_by_year"
                for year in range(current_year, min_year - 1, -1):
                    year_start_citations = total_new_citations
                    current_harvest_year["year"] = year

                    log_now(f"[EDITION {i+1}] 📅 Fetching year {year}...")

                    result = await scholar_service.get_cited_by(
                        scholar_id=edition.scholar_id,
                        max_results=1000,  # Max per year
                        year_low=year,
                        year_high=year,
                        on_page_complete=save_page_citations,
                        start_page=0,  # Always start from 0 for each year
                    )

                    year_citations = total_new_citations - year_start_citations
                    total_this_year = result.get('totalResults', 0) if isinstance(result, dict) else 0
                    log_now(f"[EDITION {i+1}] 📅 Year {year}: {year_citations} new citations (Scholar reports {total_this_year} total for year)")

                    # If no results for this year and previous few years, we're likely done
                    if year_citations == 0 and total_this_year == 0:
                        # Check if we've had 3 consecutive empty years
                        if year < current_year - 2:  # Give some buffer for recent years
                            log_now(f"[EDITION {i+1}] 📅 No citations found for {year}, stopping year-by-year fetch")
                            break

                    # Small delay between year queries
                    await asyncio.sleep(2)

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

                result = await scholar_service.get_cited_by(
                    scholar_id=edition.scholar_id,
                    max_results=max_citations_per_edition,
                    year_low=effective_year_low,  # Pass year_low for refresh filtering
                    on_page_complete=save_page_citations,
                    start_page=resume_page,
                )

                log_now(f"[EDITION {i+1}] get_cited_by returned:")
                log_now(f"[EDITION {i+1}]   result type: {type(result)}")
                log_now(f"[EDITION {i+1}]   result keys: {result.keys() if isinstance(result, dict) else 'NOT A DICT'}")
                log_now(f"[EDITION {i+1}]   papers count: {len(result.get('papers', [])) if isinstance(result, dict) else 'N/A'}")
                log_now(f"[EDITION {i+1}]   totalResults: {result.get('totalResults', 'N/A') if isinstance(result, dict) else 'N/A'}")

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
                    else:
                        # No pending jobs - check for incomplete harvests to auto-resume
                        try:
                            resumed = await auto_resume_incomplete_harvests(db)
                            if resumed > 0:
                                continue  # Immediately process the new jobs
                        except Exception as e:
                            log_now(f"[Worker] Auto-resume check failed: {e}")

                        # Still no jobs, wait before checking again
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
