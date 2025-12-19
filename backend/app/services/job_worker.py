"""
Background Job Worker Service

Processes jobs from the queue asynchronously.
Supports concurrent execution of multiple jobs.
"""
import asyncio
import json
import logging
import traceback
from datetime import datetime
from typing import Optional, Dict, Any, List
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import Job, Paper, Edition, Citation, RawSearchResult
from ..database import async_session
from .edition_discovery import EditionDiscoveryService
from .scholar_search import get_scholar_service

logger = logging.getLogger(__name__)

# Global worker state
_worker_task: Optional[asyncio.Task] = None
_worker_running = False


async def update_job_progress(
    db: AsyncSession,
    job_id: int,
    progress: float,
    message: str,
):
    """Update job progress in database"""
    await db.execute(
        update(Job)
        .where(Job.id == job_id)
        .values(progress=progress, progress_message=message)
    )
    await db.commit()


async def process_fetch_more_job(job: Job, db: AsyncSession) -> Dict[str, Any]:
    """Process a fetch_more_editions job"""
    params = json.loads(job.params) if job.params else {}
    paper_id = job.paper_id
    language = params.get("language", "english")
    max_results = params.get("max_results", 50)

    logger.info(f"[Worker] Processing fetch_more job {job.id} for paper {paper_id}, language={language}")

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
        logger.info(f"[Worker] Saved {len(raw_results)} raw results for debugging")

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
    logger.info(f"╔{'═'*70}╗")
    logger.info(f"║  EXTRACT_CITATIONS JOB ENTRY POINT")
    logger.info(f"╠{'═'*70}╣")
    logger.info(f"║  Job ID: {job.id}")
    logger.info(f"║  Paper ID: {job.paper_id}")
    logger.info(f"║  Raw params: {job.params}")
    logger.info(f"╚{'═'*70}╝")

    params = json.loads(job.params) if job.params else {}
    paper_id = job.paper_id
    edition_ids = params.get("edition_ids", [])  # Empty = all selected
    max_citations_per_edition = params.get("max_citations_per_edition", 500)
    skip_threshold = params.get("skip_threshold", 5000)  # Skip editions with > this many citations

    logger.info(f"[Worker] Parsed params: edition_ids={edition_ids}, max_citations={max_citations_per_edition}, skip_threshold={skip_threshold}")

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

    logger.info(f"[Worker] Processing {len(valid_editions)} editions, skipped {len(skipped_editions)}")
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

        # Check for resume state from previous run
        resume_page = 0
        if params.get("resume_state"):
            resume_state = params["resume_state"]
            if resume_state.get("edition_id") == edition.id:
                resume_page = resume_state.get("last_page", 0)
                logger.info(f"[Worker] Resuming edition {edition.id} from page {resume_page}")

        # Callback to save citations IMMEDIATELY after each page
        async def save_page_citations(page_num: int, papers: List[Dict]):
            nonlocal total_new_citations, total_updated_citations, existing_scholar_ids, params

            logger.info(f"[CALLBACK] ═══════════════════════════════════════════════")
            logger.info(f"[CALLBACK] save_page_citations called")
            logger.info(f"[CALLBACK] page_num: {page_num}")
            logger.info(f"[CALLBACK] papers type: {type(papers)}")
            logger.info(f"[CALLBACK] papers length: {len(papers) if isinstance(papers, list) else 'NOT A LIST'}")

            if not isinstance(papers, list):
                logger.error(f"[CALLBACK] ✗✗✗ PAPERS IS NOT A LIST! Type: {type(papers)}")
                logger.error(f"[CALLBACK] Papers value: {papers}")
                raise TypeError(f"Expected list of papers, got {type(papers)}")

            if papers and len(papers) > 0:
                logger.info(f"[CALLBACK] First paper: {papers[0]}")
                logger.info(f"[CALLBACK] First paper type: {type(papers[0])}")

            new_count = 0
            skipped_no_id = 0
            for idx, paper_data in enumerate(papers):
                if not isinstance(paper_data, dict):
                    logger.error(f"[CALLBACK] Paper {idx} is not a dict! Type: {type(paper_data)}, Value: {paper_data}")
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
            logger.info(f"[CALLBACK] ✓ Page {page_num + 1} complete: {new_count} new, {skipped_no_id} skipped (no ID), total: {total_new_citations}")

            # Update job progress with current state for resume
            progress_pct = 10 + ((i + (page_num / 20)) / total_editions) * 70
            await update_job_progress(
                db, job.id, progress_pct,
                f"Edition {i+1}/{total_editions}, page {page_num + 1}: {total_new_citations} citations saved"
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
            logger.info(f"[EDITION {i+1}/{total_editions}] ═══════════════════════════════════════════════")
            logger.info(f"[EDITION {i+1}] Edition ID: {edition.id}")
            logger.info(f"[EDITION {i+1}] Scholar ID: {edition.scholar_id}")
            logger.info(f"[EDITION {i+1}] Title: {edition.title[:60] if edition.title else 'NO TITLE'}...")
            logger.info(f"[EDITION {i+1}] Language: {edition.language}")
            logger.info(f"[EDITION {i+1}] Citation count (Scholar): {edition.citation_count}")
            logger.info(f"[EDITION {i+1}] max_results: {max_citations_per_edition}")
            logger.info(f"[EDITION {i+1}] start_page: {resume_page}")
            logger.info(f"[EDITION {i+1}] Calling scholar_service.get_cited_by()...")

            result = await scholar_service.get_cited_by(
                scholar_id=edition.scholar_id,
                max_results=max_citations_per_edition,
                on_page_complete=save_page_citations,
                start_page=resume_page,
            )

            logger.info(f"[EDITION {i+1}] get_cited_by returned:")
            logger.info(f"[EDITION {i+1}]   result type: {type(result)}")
            logger.info(f"[EDITION {i+1}]   result keys: {result.keys() if isinstance(result, dict) else 'NOT A DICT'}")
            logger.info(f"[EDITION {i+1}]   papers count: {len(result.get('papers', [])) if isinstance(result, dict) else 'N/A'}")
            logger.info(f"[EDITION {i+1}]   totalResults: {result.get('totalResults', 'N/A') if isinstance(result, dict) else 'N/A'}")

            edition_citations = total_new_citations - edition_start_citations
            logger.info(f"[EDITION {i+1}] ✓ Complete: {edition_citations} new citations saved")

            # Rate limit between editions
            if i < total_editions - 1:
                logger.info(f"[EDITION {i+1}] Sleeping 3 seconds before next edition...")
                await asyncio.sleep(3)

        except Exception as e:
            logger.error(f"[EDITION {i+1}] ✗✗✗ EXCEPTION ✗✗✗")
            logger.error(f"[EDITION {i+1}] Error type: {type(e).__name__}")
            logger.error(f"[EDITION {i+1}] Error message: {e}")
            logger.error(f"[EDITION {i+1}] Traceback: {traceback.format_exc()}")
            logger.info(f"[EDITION {i+1}] Continuing with next edition. {total_new_citations} citations saved so far.")
            # Continue with other editions - we've already saved what we got

    logger.info(f"╔{'═'*70}╗")
    logger.info(f"║  EXTRACT_CITATIONS JOB COMPLETE")
    logger.info(f"╠{'═'*70}╣")
    logger.info(f"║  Total new citations: {total_new_citations}")
    logger.info(f"║  Total duplicates skipped: {total_updated_citations}")
    logger.info(f"║  Editions processed: {len(valid_editions)}")
    logger.info(f"╚{'═'*70}╝")

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
    }


async def process_single_job(job_id: int):
    """Process a single job by ID"""
    async with async_session() as db:
        try:
            # Get job
            result = await db.execute(select(Job).where(Job.id == job_id))
            job = result.scalar_one_or_none()
            if not job:
                logger.error(f"[Worker] Job {job_id} not found")
                return

            if job.status != "pending":
                logger.info(f"[Worker] Job {job_id} status is {job.status}, skipping")
                return

            # Mark as running
            job.status = "running"
            job.started_at = datetime.utcnow()
            job.progress = 0
            job.progress_message = "Starting..."
            await db.commit()

            logger.info(f"[Worker] Starting job {job_id} ({job.job_type})")

            # Process based on job type
            if job.job_type == "fetch_more_editions":
                result = await process_fetch_more_job(job, db)
            elif job.job_type == "extract_citations":
                result = await process_extract_citations_job(job, db)
            else:
                raise ValueError(f"Unknown job type: {job.job_type}")

            # Mark as completed
            job.status = "completed"
            job.progress = 100
            job.progress_message = "Completed"
            job.result = json.dumps(result)
            job.completed_at = datetime.utcnow()
            await db.commit()

            logger.info(f"[Worker] Completed job {job_id}")

        except Exception as e:
            logger.error(f"[Worker] Job {job_id} failed: {e}")
            # Mark as failed
            try:
                job.status = "failed"
                job.error = str(e)
                job.completed_at = datetime.utcnow()
                await db.commit()
            except:
                pass


async def worker_loop():
    """Main worker loop - processes pending jobs"""
    global _worker_running
    _worker_running = True
    logger.info("[Worker] Starting job worker loop")

    while _worker_running:
        try:
            async with async_session() as db:
                # Get next pending job (oldest first, respecting priority)
                result = await db.execute(
                    select(Job)
                    .where(Job.status == "pending")
                    .order_by(Job.priority.desc(), Job.created_at.asc())
                    .limit(1)
                )
                job = result.scalar_one_or_none()

                if job:
                    # Process in separate task (allows concurrent processing)
                    asyncio.create_task(process_single_job(job.id))
                    await asyncio.sleep(1)  # Small delay before checking for more
                else:
                    # No pending jobs, wait before checking again
                    await asyncio.sleep(5)

        except Exception as e:
            logger.error(f"[Worker] Loop error: {e}")
            await asyncio.sleep(10)

    logger.info("[Worker] Worker loop stopped")


def start_worker():
    """Start the background worker (called at app startup)"""
    global _worker_task
    if _worker_task is None or _worker_task.done():
        _worker_task = asyncio.create_task(worker_loop())
        logger.info("[Worker] Background worker started")


def stop_worker():
    """Stop the background worker"""
    global _worker_running, _worker_task
    _worker_running = False
    if _worker_task:
        _worker_task.cancel()
        _worker_task = None
    logger.info("[Worker] Background worker stopped")


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
    max_citations_per_edition: int = 500,
    skip_threshold: int = 5000,
) -> Job:
    """Create an extract_citations job"""
    job = Job(
        paper_id=paper_id,
        job_type="extract_citations",
        status="pending",
        params=json.dumps({
            "edition_ids": edition_ids or [],
            "max_citations_per_edition": max_citations_per_edition,
            "skip_threshold": skip_threshold,
        }),
        progress=0,
        progress_message="Queued: Extract citations",
    )
    db.add(job)
    await db.flush()
    await db.refresh(job)
    return job
