"""
Paper Resolution Service

Handles the complete paper resolution workflow:
1. Resolve paper against Google Scholar (get Scholar ID, citation count)
2. Store resolved metadata back to database
3. Optionally trigger edition discovery
"""
import asyncio
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from ..models import Paper, Edition, Job
from .scholar_search import get_scholar_service, ScholarSearchService
from .edition_discovery import EditionDiscoveryService

logger = logging.getLogger(__name__)


class PaperResolutionService:
    """Service for resolving papers against Google Scholar"""

    def __init__(self, db: AsyncSession):
        self.db = db
        self.scholar = get_scholar_service()

    async def resolve_paper(
        self,
        paper_id: int,
        job_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Resolve a paper against Google Scholar

        Args:
            paper_id: Database ID of the paper
            job_id: Optional job ID for progress tracking

        Returns:
            Dict with resolution results
        """
        # Get paper from database
        result = await self.db.execute(select(Paper).where(Paper.id == paper_id))
        paper = result.scalar_one_or_none()

        if not paper:
            raise ValueError(f"Paper {paper_id} not found")

        logger.info(f"[RESOLUTION] Starting resolution for: \"{paper.title[:60]}...\"")

        # Update job status if tracking
        if job_id:
            await self._update_job(job_id, status="running", progress=0.1, message="Searching Google Scholar...")

        try:
            # Search Google Scholar with LLM verification
            search_result = await self.scholar.search_and_verify_match(
                title=paper.title,
                author=paper.authors,
                year=paper.year,
                publisher=paper.venue,
            )

            if search_result.get("error") or not search_result.get("paper"):
                logger.warning(f"[RESOLUTION] No match found for: {paper.title}")
                paper.status = "error"
                await self.db.flush()

                if job_id:
                    await self._update_job(job_id, status="failed", error="No matching paper found on Google Scholar")

                return {
                    "success": False,
                    "error": "No matching paper found on Google Scholar",
                    "paper_id": paper_id,
                }

            matched_paper = search_result["paper"]
            verification = search_result.get("verification", {})
            all_results = search_result.get("allResults", [])

            # Check if we need user reconciliation
            # Low confidence or multiple similar results means we should ask user
            confidence = verification.get("confidence", 0.8)
            needs_reconciliation = confidence < 0.7 and len(all_results) > 1

            if needs_reconciliation:
                logger.info(f"[RESOLUTION] Low confidence ({confidence}), needs reconciliation with {len(all_results)} candidates")

                # Store candidates in paper
                import json
                candidates_data = []
                for r in all_results[:5]:  # Max 5 candidates
                    candidates_data.append({
                        "scholarId": r.get("scholarId"),
                        "clusterId": r.get("clusterId"),
                        "title": r.get("title"),
                        "authors": r.get("authors"),
                        "authorsRaw": r.get("authorsRaw"),
                        "year": r.get("year"),
                        "venue": r.get("venue"),
                        "abstract": r.get("abstract"),
                        "link": r.get("link"),
                        "citationCount": r.get("citationCount", 0),
                    })

                paper.candidates = json.dumps(candidates_data)
                paper.status = "needs_reconciliation"
                await self.db.flush()

                if job_id:
                    await self._update_job(job_id, status="completed", progress=1.0,
                                          message="Multiple candidates found - user selection required")

                return {
                    "success": True,
                    "needs_reconciliation": True,
                    "paper_id": paper_id,
                    "candidates": candidates_data,
                    "verification": verification,
                }

            # Update paper with Scholar metadata - use canonical title from Scholar
            paper.scholar_id = matched_paper.get("scholarId")
            paper.cluster_id = matched_paper.get("clusterId")
            paper.citation_count = matched_paper.get("citationCount", 0)
            paper.link = matched_paper.get("link")
            paper.abstract = matched_paper.get("abstract")
            paper.status = "resolved"
            paper.resolved_at = datetime.utcnow()

            # Update title to canonical Scholar title (proper capitalization, full title)
            if matched_paper.get("title"):
                paper.title = matched_paper["title"]

            # Update authors from Scholar (properly formatted)
            if matched_paper.get("authors"):
                authors = matched_paper["authors"]
                if isinstance(authors, list):
                    paper.authors = ", ".join(authors)
                else:
                    paper.authors = authors
            elif matched_paper.get("authorsRaw"):
                paper.authors = matched_paper["authorsRaw"]

            # Update year and venue from Scholar
            if matched_paper.get("year"):
                paper.year = matched_paper["year"]
            if matched_paper.get("venue"):
                paper.venue = matched_paper["venue"]

            await self.db.flush()

            if job_id:
                await self._update_job(job_id, status="completed", progress=1.0, message="Resolution complete")

            logger.info(f"[RESOLUTION] ✓ Resolved: Scholar ID={paper.scholar_id}, Citations={paper.citation_count}")

            return {
                "success": True,
                "paper_id": paper_id,
                "scholar_id": paper.scholar_id,
                "cluster_id": paper.cluster_id,
                "citation_count": paper.citation_count,
                "verification": verification,
            }

        except Exception as e:
            logger.error(f"[RESOLUTION] Error resolving paper {paper_id}: {e}")
            paper.status = "error"
            await self.db.flush()

            if job_id:
                await self._update_job(job_id, status="failed", error=str(e))

            return {
                "success": False,
                "error": str(e),
                "paper_id": paper_id,
            }

    async def discover_editions(
        self,
        paper_id: int,
        job_id: Optional[int] = None,
        language_strategy: str = "major_languages",
        custom_languages: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Discover all editions of a paper

        Args:
            paper_id: Database ID of the paper
            job_id: Optional job ID for progress tracking
            language_strategy: Which languages to search
            custom_languages: Additional custom languages

        Returns:
            Dict with discovery results
        """
        # Get paper from database
        result = await self.db.execute(select(Paper).where(Paper.id == paper_id))
        paper = result.scalar_one_or_none()

        if not paper:
            raise ValueError(f"Paper {paper_id} not found")

        logger.info(f"[EDITION DISCOVERY] Starting for: \"{paper.title[:60]}...\"")

        if job_id:
            await self._update_job(job_id, status="running", progress=0.1, message="Generating search queries...")

        try:
            discovery = EditionDiscoveryService(
                language_strategy=language_strategy,
                custom_languages=custom_languages,
            )

            async def progress_callback(progress: Dict):
                if job_id:
                    stage = progress.get("stage", "")
                    if stage == "searching":
                        pct = 0.1 + (progress.get("query", 0) / progress.get("total_queries", 1)) * 0.6
                        await self._update_job(
                            job_id,
                            progress=pct,
                            message=f"Query {progress.get('query')}/{progress.get('total_queries')}: {progress.get('current_query', '')[:40]}..."
                        )
                    elif stage == "evaluating":
                        await self._update_job(
                            job_id,
                            progress=0.8,
                            message=f"Evaluating {progress.get('total_results')} results..."
                        )

            result = await discovery.discover_editions(
                paper={
                    "title": paper.title,
                    "author": paper.authors,
                    "year": paper.year,
                },
                progress_callback=progress_callback,
            )

            # Store discovered editions in database
            editions_stored = 0
            for edition_data in result.get("genuineEditions", []):
                # Check if edition already exists
                existing = await self.db.execute(
                    select(Edition).where(
                        Edition.paper_id == paper_id,
                        Edition.scholar_id == edition_data.get("scholarId")
                    )
                )
                if existing.scalar_one_or_none():
                    continue

                edition = Edition(
                    paper_id=paper_id,
                    scholar_id=edition_data.get("scholarId"),
                    cluster_id=edition_data.get("clusterId"),
                    title=edition_data.get("title"),
                    authors=edition_data.get("authorsRaw"),
                    year=edition_data.get("year"),
                    venue=edition_data.get("venue"),
                    abstract=edition_data.get("abstract"),
                    link=edition_data.get("link"),
                    citation_count=edition_data.get("citationCount", 0),
                    language=edition_data.get("language"),
                    confidence=edition_data.get("confidence", "uncertain"),
                    auto_selected=edition_data.get("autoSelected", False),
                    selected=edition_data.get("autoSelected", False),  # Pre-select high confidence
                    found_by_query=", ".join(edition_data.get("foundBy", [])[:3]),
                )
                self.db.add(edition)
                editions_stored += 1

            await self.db.flush()

            if job_id:
                await self._update_job(
                    job_id,
                    status="completed",
                    progress=1.0,
                    message=f"Found {editions_stored} editions"
                )

            logger.info(f"[EDITION DISCOVERY] ✓ Complete: {editions_stored} editions stored")

            return {
                "success": True,
                "paper_id": paper_id,
                "editions_found": len(result.get("genuineEditions", [])),
                "editions_stored": editions_stored,
                "high_confidence": len(result.get("highConfidence", [])),
                "uncertain": len(result.get("uncertain", [])),
                "rejected": len(result.get("rejected", [])),
                "summary": result.get("summary", {}),
            }

        except Exception as e:
            logger.error(f"[EDITION DISCOVERY] Error for paper {paper_id}: {e}")

            if job_id:
                await self._update_job(job_id, status="failed", error=str(e))

            return {
                "success": False,
                "error": str(e),
                "paper_id": paper_id,
            }

    async def confirm_candidate(
        self,
        paper_id: int,
        candidate_index: int,
    ) -> Dict[str, Any]:
        """
        Confirm a candidate selection during reconciliation

        Args:
            paper_id: Database ID of the paper
            candidate_index: Index of the selected candidate (0-based)

        Returns:
            Dict with resolution results
        """
        import json

        result = await self.db.execute(select(Paper).where(Paper.id == paper_id))
        paper = result.scalar_one_or_none()

        if not paper:
            raise ValueError(f"Paper {paper_id} not found")

        if paper.status != "needs_reconciliation":
            raise ValueError(f"Paper {paper_id} is not awaiting reconciliation (status: {paper.status})")

        if not paper.candidates:
            raise ValueError(f"Paper {paper_id} has no candidates")

        candidates = json.loads(paper.candidates)

        if candidate_index < 0 or candidate_index >= len(candidates):
            raise ValueError(f"Invalid candidate index {candidate_index} (0-{len(candidates)-1} available)")

        selected = candidates[candidate_index]

        logger.info(f"[RESOLUTION] User selected candidate {candidate_index}: {selected.get('title', '')[:60]}...")

        # Update paper with selected candidate
        paper.scholar_id = selected.get("scholarId")
        paper.cluster_id = selected.get("clusterId")
        paper.citation_count = selected.get("citationCount", 0)
        paper.link = selected.get("link")
        paper.abstract = selected.get("abstract")
        paper.status = "resolved"
        paper.resolved_at = datetime.utcnow()
        paper.candidates = None  # Clear candidates

        # Update title to canonical Scholar title
        if selected.get("title"):
            paper.title = selected["title"]

        # Update authors from Scholar
        if selected.get("authors"):
            authors = selected["authors"]
            if isinstance(authors, list):
                paper.authors = ", ".join(authors)
            else:
                paper.authors = authors
        elif selected.get("authorsRaw"):
            paper.authors = selected["authorsRaw"]

        # Update year and venue
        if selected.get("year"):
            paper.year = selected["year"]
        if selected.get("venue"):
            paper.venue = selected["venue"]

        await self.db.flush()

        logger.info(f"[RESOLUTION] ✓ Confirmed: Scholar ID={paper.scholar_id}, Citations={paper.citation_count}")

        return {
            "success": True,
            "paper_id": paper_id,
            "scholar_id": paper.scholar_id,
            "cluster_id": paper.cluster_id,
            "citation_count": paper.citation_count,
        }

    async def _update_job(
        self,
        job_id: int,
        status: Optional[str] = None,
        progress: Optional[float] = None,
        message: Optional[str] = None,
        error: Optional[str] = None,
    ):
        """Update job status in database"""
        result = await self.db.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one_or_none()

        if not job:
            return

        if status:
            job.status = status
            if status == "running" and not job.started_at:
                job.started_at = datetime.utcnow()
            elif status in ("completed", "failed"):
                job.completed_at = datetime.utcnow()

        if progress is not None:
            job.progress = progress

        if message:
            job.progress_message = message

        if error:
            job.error = error

        await self.db.flush()


async def process_pending_jobs(db: AsyncSession, max_jobs: int = 10):
    """
    Process pending jobs in the queue

    This should be called periodically (e.g., by a background task or cron)
    """
    # Get pending jobs ordered by priority
    result = await db.execute(
        select(Job)
        .where(Job.status == "pending")
        .order_by(Job.priority.desc(), Job.created_at.asc())
        .limit(max_jobs)
    )
    pending_jobs = result.scalars().all()

    if not pending_jobs:
        return {"processed": 0}

    logger.info(f"[JOB WORKER] Processing {len(pending_jobs)} pending jobs")

    service = PaperResolutionService(db)
    results = []

    for job in pending_jobs:
        try:
            if job.job_type == "resolve":
                result = await service.resolve_paper(job.paper_id, job.id)
            elif job.job_type == "discover_editions":
                result = await service.discover_editions(job.paper_id, job.id)
            else:
                logger.warning(f"Unknown job type: {job.job_type}")
                job.status = "failed"
                job.error = f"Unknown job type: {job.job_type}"
                await db.flush()
                continue

            results.append(result)

        except Exception as e:
            logger.error(f"[JOB WORKER] Error processing job {job.id}: {e}")
            job.status = "failed"
            job.error = str(e)
            await db.flush()

    await db.commit()

    return {"processed": len(results), "results": results}
