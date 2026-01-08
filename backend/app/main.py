"""
The Referee - Citation Analysis API

A robust API for discovering editions and extracting citations from academic papers.

Fixed greenlet context issues in harvest callbacks (2025-12-30).
"""
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, update, text, or_
from sqlalchemy.orm import selectinload
from typing import List, Optional
from pydantic import BaseModel
import json

from .config import get_settings
from .database import init_db, get_db
from .models import Paper, Edition, Citation, Job, RawSearchResult, Collection, Dossier, PaperAdditionalDossier, FailedFetch, HarvestTarget, Thinker, ThinkerWork, ThinkerHarvestRun, ThinkerLLMCall
from .schemas import (
    PaperCreate, PaperResponse, PaperDetail, PaperSubmitBatch, PapersPaginatedResponse,
    EditionResponse, EditionDiscoveryRequest, EditionDiscoveryResponse, EditionSelectRequest,
    EditionUpdateConfidenceRequest, EditionFetchMoreRequest, EditionFetchMoreResponse,
    EditionExcludeRequest, EditionAddAsSeedRequest, EditionAddAsSeedResponse,
    EditionMergeRequest, EditionMergeResponse,
    ManualEditionAddRequest, ManualEditionAddResponse,
    CitationResponse, CitationExtractionRequest, CitationExtractionResponse, CrossCitationResult, CitationMarkReviewedRequest,
    JobResponse, JobDetail, FetchMoreJobRequest, FetchMoreJobResponse,
    LanguageRecommendationRequest, LanguageRecommendationResponse, AvailableLanguagesResponse,
    CollectionCreate, CollectionUpdate, CollectionResponse, CollectionDetail,
    DossierCreate, DossierUpdate, DossierResponse, DossierDetail,
    CanonicalEditionSummary,
    # Refresh/Auto-Updater schemas
    RefreshRequest, RefreshJobResponse, RefreshStatusResponse, StalenessReportResponse,
    # Harvest Completeness schemas
    HarvestTargetResponse, FailedFetchResponse, HarvestCompletenessResponse, FailedFetchesSummary,
    # AI Gap Analysis schemas
    GapDetail, GapFix, AIGapAnalysisResponse,
    # Quick-add schemas
    QuickAddRequest, QuickAddResponse,
    # External API schemas
    BatchCrossRequest, BatchCrossResult, CrossCitationItem,
    ExternalAnalyzeRequest, ExternalAnalyzeResponse,
    # Batch Operations schemas
    BatchCollectionAssignment, BatchForeignEditionRequest, BatchForeignEditionResponse,
    # Dashboard schemas
    HarvestDashboardResponse, JobHistoryResponse, JobHistoryItem,
    SystemHealthStats, ActiveHarvestInfo, RecentlyCompletedPaper, DashboardAlert, JobHistorySummary,
    # Thinker Bibliographies schemas
    ThinkerCreate, ThinkerUpdate, ThinkerResponse, ThinkerDetail, ThinkerConfirmRequest,
    ThinkerWorkResponse, ThinkerHarvestRunResponse, ThinkerLLMCallResponse,
    DisambiguationResponse, NameVariantsResponse, ThinkerQuickAddRequest, ThinkerQuickAddResponse,
    StartWorkDiscoveryRequest, StartWorkDiscoveryResponse,
    DetectTranslationsRequest, DetectTranslationsResponse,
    RetrospectiveMatchRequest, RetrospectiveMatchResponse,
    HarvestCitationsRequest, HarvestCitationsResponse,
    # Citation to Seed schemas
    CitationMakeSeedRequest, CitationMakeSeedResponse,
    # Author Search schemas
    AuthorPaperResult, AuthorSearchResponse,
)

# Configure logging with immediate flush for Render
import sys

# Remove any existing handlers
root_logger = logging.getLogger()
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)

# Create handler that flushes immediately
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter(
    '%(asctime)s | %(name)s | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
))

# Force immediate flush after each log
class FlushHandler(logging.StreamHandler):
    def emit(self, record):
        super().emit(record)
        self.flush()

flush_handler = FlushHandler(sys.stdout)
flush_handler.setLevel(logging.INFO)
flush_handler.setFormatter(logging.Formatter(
    '%(asctime)s | %(name)s | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
))

root_logger.setLevel(logging.INFO)
root_logger.addHandler(flush_handler)

# Also log to stderr for Render's log capture
stderr_handler = FlushHandler(sys.stderr)
stderr_handler.setLevel(logging.WARNING)
stderr_handler.setFormatter(logging.Formatter(
    '%(asctime)s | %(name)s | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
))
root_logger.addHandler(stderr_handler)

logger = logging.getLogger(__name__)
logger.info("="*60)
logger.info("THE REFEREE API STARTING UP")
logger.info("="*60)

settings = get_settings()


# ============== API Key Authentication ==============
# Used for external API endpoints (/api/external/*)

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(api_key: str = Security(api_key_header)) -> str:
    """
    Verify API key for external endpoints.
    Returns the API key if valid, raises HTTPException if invalid.
    """
    if not settings.api_auth_enabled:
        return "auth_disabled"  # Auth disabled, allow all

    if not api_key:
        raise HTTPException(
            status_code=401,
            detail="Missing API key. Provide X-API-Key header.",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    valid_keys = settings.get_api_keys_list()
    if api_key not in valid_keys:
        raise HTTPException(
            status_code=401,
            detail="Invalid API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    return api_key


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize database on startup, start background worker"""
    await init_db()

    # Start background job worker
    from .services.job_worker import start_worker
    start_worker()

    # Start API logger flush task
    from .services.api_logger import start_flush_task, stop_flush_task
    await start_flush_task()

    # Start health monitor (LLM-powered autonomous diagnostics)
    from .services.health_monitor import start_health_monitor, stop_health_monitor
    await start_health_monitor()

    yield

    # Stop health monitor
    await stop_health_monitor()

    # Stop worker on shutdown
    from .services.job_worker import stop_worker
    stop_worker()

    # Stop API logger flush task
    await stop_flush_task()


app = FastAPI(
    title="The Referee",
    description="Citation Analysis API - Discover editions, extract citations, find cross-citations",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS - allow all origins for API access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,  # Must be False when allow_origins=["*"]
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============== Health Check ==============

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "the-referee"}


@app.get("/health/db")
async def db_health_check(db: AsyncSession = Depends(get_db)):
    """Check database connectivity with detailed timing"""
    import time
    start = time.time()
    try:
        # Simple count query
        result = await db.execute(text("SELECT 1"))
        result.scalar()
        elapsed = time.time() - start
        return {
            "status": "healthy",
            "database": "connected",
            "query_time_ms": round(elapsed * 1000, 2)
        }
    except Exception as e:
        elapsed = time.time() - start
        return {
            "status": "unhealthy",
            "database": "error",
            "error": str(e),
            "error_type": type(e).__name__,
            "elapsed_ms": round(elapsed * 1000, 2)
        }


@app.get("/health/db/collections")
async def db_collections_check(db: AsyncSession = Depends(get_db)):
    """Test collections table query"""
    import time
    start = time.time()
    try:
        result = await db.execute(text("SELECT COUNT(*) FROM collections"))
        count = result.scalar()
        return {"table": "collections", "count": count, "time_ms": round((time.time() - start) * 1000, 2)}
    except Exception as e:
        return {"table": "collections", "error": str(e), "time_ms": round((time.time() - start) * 1000, 2)}


@app.get("/health/db/papers")
async def db_papers_check(db: AsyncSession = Depends(get_db)):
    """Test papers table query"""
    import time
    start = time.time()
    try:
        result = await db.execute(text("SELECT COUNT(*) FROM papers"))
        count = result.scalar()
        return {"table": "papers", "count": count, "time_ms": round((time.time() - start) * 1000, 2)}
    except Exception as e:
        return {"table": "papers", "error": str(e), "time_ms": round((time.time() - start) * 1000, 2)}


@app.get("/health/db/editions")
async def db_editions_check(db: AsyncSession = Depends(get_db)):
    """Test editions table query"""
    import time
    start = time.time()
    try:
        result = await db.execute(text("SELECT COUNT(*) FROM editions"))
        count = result.scalar()
        return {"table": "editions", "count": count, "time_ms": round((time.time() - start) * 1000, 2)}
    except Exception as e:
        return {"table": "editions", "error": str(e), "time_ms": round((time.time() - start) * 1000, 2)}


@app.get("/health/db/citations")
async def db_citations_check(db: AsyncSession = Depends(get_db)):
    """Test citations table query"""
    import time
    start = time.time()
    try:
        result = await db.execute(text("SELECT COUNT(*) FROM citations"))
        count = result.scalar()
        return {"table": "citations", "count": count, "time_ms": round((time.time() - start) * 1000, 2)}
    except Exception as e:
        return {"table": "citations", "error": str(e), "time_ms": round((time.time() - start) * 1000, 2)}


@app.get("/health/db/jobs")
async def db_jobs_check(db: AsyncSession = Depends(get_db)):
    """Test jobs table query"""
    import time
    start = time.time()
    try:
        result = await db.execute(text("SELECT COUNT(*) FROM jobs"))
        count = result.scalar()
        # Also get running jobs
        running = await db.execute(text("SELECT COUNT(*) FROM jobs WHERE status = 'running'"))
        running_count = running.scalar()
        return {
            "table": "jobs",
            "count": count,
            "running": running_count,
            "time_ms": round((time.time() - start) * 1000, 2)
        }
    except Exception as e:
        return {"table": "jobs", "error": str(e), "time_ms": round((time.time() - start) * 1000, 2)}


@app.get("/health/db/locks")
async def db_locks_check(db: AsyncSession = Depends(get_db)):
    """Check for PostgreSQL locks"""
    import time
    start = time.time()
    try:
        # Query to find blocking queries
        result = await db.execute(text("""
            SELECT
                blocked_locks.pid AS blocked_pid,
                blocking_locks.pid AS blocking_pid,
                blocked_activity.query AS blocked_query,
                blocking_activity.query AS blocking_query
            FROM pg_locks blocked_locks
            JOIN pg_stat_activity blocked_activity ON blocked_locks.pid = blocked_activity.pid
            JOIN pg_locks blocking_locks ON blocking_locks.locktype = blocked_locks.locktype
                AND blocking_locks.relation = blocked_locks.relation
                AND blocking_locks.granted
            JOIN pg_stat_activity blocking_activity ON blocking_locks.pid = blocking_activity.pid
            WHERE NOT blocked_locks.granted
            LIMIT 10
        """))
        locks = [dict(row._mapping) for row in result]
        return {"locks": locks, "time_ms": round((time.time() - start) * 1000, 2)}
    except Exception as e:
        return {"error": str(e), "time_ms": round((time.time() - start) * 1000, 2)}


@app.get("/health/db/activity")
async def db_activity_check(db: AsyncSession = Depends(get_db)):
    """Check active PostgreSQL queries"""
    import time
    start = time.time()
    try:
        result = await db.execute(text("""
            SELECT pid, state, query, query_start,
                   EXTRACT(EPOCH FROM (NOW() - query_start)) as duration_seconds
            FROM pg_stat_activity
            WHERE state != 'idle'
            AND query NOT LIKE '%pg_stat_activity%'
            ORDER BY query_start
            LIMIT 10
        """))
        activities = [dict(row._mapping) for row in result]
        return {"activities": activities, "time_ms": round((time.time() - start) * 1000, 2)}
    except Exception as e:
        return {"error": str(e), "time_ms": round((time.time() - start) * 1000, 2)}


@app.post("/health/db/kill/{pid}")
async def kill_db_process(pid: int, db: AsyncSession = Depends(get_db)):
    """Kill a specific PostgreSQL backend process"""
    import time
    start = time.time()
    try:
        # First check if this PID exists and get info
        info_result = await db.execute(text(f"""
            SELECT pid, state, query, query_start,
                   EXTRACT(EPOCH FROM (NOW() - query_start)) as duration_seconds
            FROM pg_stat_activity
            WHERE pid = {pid}
        """))
        info = info_result.fetchone()
        if not info:
            return {"error": f"PID {pid} not found", "time_ms": round((time.time() - start) * 1000, 2)}

        # Kill it
        result = await db.execute(text(f"SELECT pg_terminate_backend({pid})"))
        killed = result.scalar()
        return {
            "killed": killed,
            "pid": pid,
            "was_state": info.state,
            "was_running_seconds": float(info.duration_seconds) if info.duration_seconds else None,
            "time_ms": round((time.time() - start) * 1000, 2)
        }
    except Exception as e:
        return {"error": str(e), "time_ms": round((time.time() - start) * 1000, 2)}


@app.get("/")
async def root():
    return {
        "name": "The Referee",
        "version": "1.0.0",
        "description": "Citation Analysis API",
        "docs": "/docs",
    }


# ============== Helper Functions ==============

def build_edition_response_with_staleness(edition: Edition) -> EditionResponse:
    """Build an EditionResponse with staleness fields computed"""
    # Compute staleness fields
    is_stale = False
    days_since_harvest = None
    if edition.last_harvested_at is None:
        is_stale = True  # Never harvested = stale
    else:
        days_since_harvest = (datetime.utcnow() - edition.last_harvested_at).days
        is_stale = days_since_harvest > 90

    # Build dict from edition
    ed_dict = {k: v for k, v in edition.__dict__.items() if not k.startswith('_')}

    return EditionResponse(
        **ed_dict,
        is_stale=is_stale,
        days_since_harvest=days_since_harvest,
    )


def build_paper_response_with_preloaded_editions(paper: Paper, editions: list) -> PaperResponse:
    """Build a PaperResponse with pre-loaded editions (avoids N+1 queries)"""
    # Calculate stats
    edition_count = len(editions)
    total_edition_citations = sum(e.citation_count or 0 for e in editions)

    # Get canonical edition (highest citations - list is already sorted)
    canonical_edition = None
    if editions:
        top_edition = editions[0]
        canonical_edition = CanonicalEditionSummary(
            id=top_edition.id,
            title=top_edition.title,
            citation_count=top_edition.citation_count or 0,
            language=top_edition.language,
        )

    # Compute staleness fields
    is_stale = False
    days_since_harvest = None
    if paper.any_edition_harvested_at is None:
        is_stale = True
    else:
        days_since_harvest = (datetime.utcnow() - paper.any_edition_harvested_at).days
        is_stale = days_since_harvest > 90

    # Build response
    paper_dict = {k: v for k, v in paper.__dict__.items() if not k.startswith('_')}
    return PaperResponse(
        **paper_dict,
        edition_count=edition_count,
        total_edition_citations=total_edition_citations,
        canonical_edition=canonical_edition,
        is_stale=is_stale,
        days_since_harvest=days_since_harvest,
    )


async def build_paper_response_with_editions(paper: Paper, db: AsyncSession) -> PaperResponse:
    """Build a PaperResponse with edition statistics (canonical edition, total citations)"""
    from datetime import timedelta

    # Get editions for this paper
    editions_result = await db.execute(
        select(Edition).where(Edition.paper_id == paper.id).order_by(Edition.citation_count.desc())
    )
    editions = editions_result.scalars().all()

    # Calculate stats
    edition_count = len(editions)
    total_edition_citations = sum(e.citation_count or 0 for e in editions)

    # Get canonical edition (highest citations)
    canonical_edition = None
    if editions:
        top_edition = editions[0]  # Already sorted by citation_count desc
        canonical_edition = CanonicalEditionSummary(
            id=top_edition.id,
            title=top_edition.title,
            citation_count=top_edition.citation_count or 0,
            language=top_edition.language,
        )

    # Compute staleness fields
    is_stale = False
    days_since_harvest = None
    if paper.any_edition_harvested_at is None:
        is_stale = True  # Never harvested = stale
    else:
        days_since_harvest = (datetime.utcnow() - paper.any_edition_harvested_at).days
        is_stale = days_since_harvest > 90

    # Build response
    paper_dict = {k: v for k, v in paper.__dict__.items() if not k.startswith('_')}
    return PaperResponse(
        **paper_dict,
        edition_count=edition_count,
        total_edition_citations=total_edition_citations,
        canonical_edition=canonical_edition,
        is_stale=is_stale,
        days_since_harvest=days_since_harvest,
    )


# ============== Collection Endpoints ==============

@app.post("/api/collections", response_model=CollectionResponse)
async def create_collection(
    collection: CollectionCreate,
    db: AsyncSession = Depends(get_db)
):
    """Create a new collection"""
    db_collection = Collection(
        name=collection.name,
        description=collection.description,
        color=collection.color,
    )
    db.add(db_collection)
    await db.flush()
    await db.refresh(db_collection)
    return CollectionResponse(
        id=db_collection.id,
        name=db_collection.name,
        description=db_collection.description,
        color=db_collection.color,
        created_at=db_collection.created_at,
        updated_at=db_collection.updated_at,
        paper_count=0,
    )


@app.get("/api/collections", response_model=List[CollectionResponse])
async def list_collections(db: AsyncSession = Depends(get_db)):
    """List all collections with paper counts"""
    import time
    logger.info("list_collections: Starting query...")

    # Get all collections first (simpler query)
    start = time.time()
    try:
        result = await db.execute(
            select(Collection).order_by(Collection.name)
        )
        collections = result.scalars().all()
        logger.info(f"list_collections: Got {len(collections)} collections in {time.time() - start:.2f}s")
    except Exception as e:
        logger.error(f"list_collections: Collection query failed after {time.time() - start:.2f}s: {e}")
        raise

    # Get paper counts in a separate query
    start = time.time()
    paper_counts = {}
    try:
        counts_result = await db.execute(
            select(Paper.collection_id, func.count(Paper.id).label('count'))
            .where(Paper.deleted_at.is_(None))
            .group_by(Paper.collection_id)
        )
        paper_counts = {row.collection_id: row.count for row in counts_result}
        logger.info(f"list_collections: Got paper counts in {time.time() - start:.2f}s")
    except Exception as e:
        logger.warning(f"list_collections: Paper count query failed after {time.time() - start:.2f}s: {e}")
        # Continue with empty counts rather than failing

    return [
        CollectionResponse(
            id=c.id,
            name=c.name,
            description=c.description,
            color=c.color,
            created_at=c.created_at,
            updated_at=c.updated_at,
            paper_count=paper_counts.get(c.id, 0),
        )
        for c in collections
    ]


@app.get("/api/collections/{collection_id}", response_model=CollectionDetail)
async def get_collection(collection_id: int, db: AsyncSession = Depends(get_db)):
    """Get collection details with papers (including edition stats)"""
    result = await db.execute(select(Collection).where(Collection.id == collection_id))
    collection = result.scalar_one_or_none()
    if not collection:
        raise HTTPException(status_code=404, detail="Collection not found")

    # Get papers in collection (excluding soft-deleted)
    papers_result = await db.execute(
        select(Paper)
        .where(Paper.collection_id == collection_id)
        .where(Paper.deleted_at.is_(None))
        .order_by(Paper.created_at.desc())
    )
    papers = papers_result.scalars().all()
    paper_ids = [p.id for p in papers]

    # Pre-load all editions for all papers in ONE query (avoids N+1)
    editions_by_paper = {}
    if paper_ids:
        editions_result = await db.execute(
            select(Edition)
            .where(Edition.paper_id.in_(paper_ids))
            .order_by(Edition.paper_id, Edition.citation_count.desc())
        )
        for edition in editions_result.scalars().all():
            if edition.paper_id not in editions_by_paper:
                editions_by_paper[edition.paper_id] = []
            editions_by_paper[edition.paper_id].append(edition)

    # Build paper responses using pre-loaded editions
    paper_responses = []
    for paper in papers:
        editions = editions_by_paper.get(paper.id, [])
        paper_response = build_paper_response_with_preloaded_editions(paper, editions)
        paper_responses.append(paper_response)

    return CollectionDetail(
        id=collection.id,
        name=collection.name,
        description=collection.description,
        color=collection.color,
        created_at=collection.created_at,
        updated_at=collection.updated_at,
        paper_count=len(papers),
        papers=paper_responses,
    )


@app.put("/api/collections/{collection_id}", response_model=CollectionResponse)
async def update_collection(
    collection_id: int,
    update: CollectionUpdate,
    db: AsyncSession = Depends(get_db)
):
    """Update a collection"""
    result = await db.execute(select(Collection).where(Collection.id == collection_id))
    collection = result.scalar_one_or_none()
    if not collection:
        raise HTTPException(status_code=404, detail="Collection not found")

    if update.name is not None:
        collection.name = update.name
    if update.description is not None:
        collection.description = update.description
    if update.color is not None:
        collection.color = update.color

    # Get paper count
    count_result = await db.execute(
        select(func.count(Paper.id)).where(Paper.collection_id == collection_id)
    )
    paper_count = count_result.scalar() or 0

    return CollectionResponse(
        id=collection.id,
        name=collection.name,
        description=collection.description,
        color=collection.color,
        created_at=collection.created_at,
        updated_at=collection.updated_at,
        paper_count=paper_count,
    )


@app.delete("/api/collections/{collection_id}")
async def delete_collection(collection_id: int, db: AsyncSession = Depends(get_db)):
    """Delete a collection (papers are unassigned, not deleted)"""
    result = await db.execute(select(Collection).where(Collection.id == collection_id))
    collection = result.scalar_one_or_none()
    if not collection:
        raise HTTPException(status_code=404, detail="Collection not found")

    # Unassign papers from collection
    await db.execute(
        select(Paper).where(Paper.collection_id == collection_id)
    )

    await db.delete(collection)
    return {"deleted": True, "collection_id": collection_id}


class PaperCollectionAssignment(BaseModel):
    paper_ids: List[int]
    collection_id: Optional[int] = None  # None to unassign


@app.post("/api/collections/assign")
async def assign_papers_to_collection(
    assignment: PaperCollectionAssignment,
    db: AsyncSession = Depends(get_db)
):
    """Assign papers to a collection (or unassign if collection_id is None)"""
    # Verify collection exists if provided
    if assignment.collection_id:
        result = await db.execute(select(Collection).where(Collection.id == assignment.collection_id))
        if not result.scalar_one_or_none():
            raise HTTPException(status_code=404, detail="Collection not found")

    # Update papers
    result = await db.execute(
        select(Paper).where(Paper.id.in_(assignment.paper_ids))
    )
    papers = result.scalars().all()

    for paper in papers:
        paper.collection_id = assignment.collection_id

    return {
        "updated": len(papers),
        "collection_id": assignment.collection_id,
        "paper_ids": [p.id for p in papers],
    }


# ============== Dossier Endpoints ==============

@app.post("/api/dossiers", response_model=DossierResponse)
async def create_dossier(
    dossier: DossierCreate,
    db: AsyncSession = Depends(get_db)
):
    """Create a new dossier within a collection"""
    # Verify collection exists
    result = await db.execute(select(Collection).where(Collection.id == dossier.collection_id))
    collection = result.scalar_one_or_none()
    if not collection:
        raise HTTPException(status_code=404, detail="Collection not found")

    db_dossier = Dossier(
        name=dossier.name,
        description=dossier.description,
        color=dossier.color,
        collection_id=dossier.collection_id,
    )
    db.add(db_dossier)
    await db.flush()
    await db.refresh(db_dossier)
    return DossierResponse(
        id=db_dossier.id,
        name=db_dossier.name,
        description=db_dossier.description,
        color=db_dossier.color,
        collection_id=db_dossier.collection_id,
        created_at=db_dossier.created_at,
        updated_at=db_dossier.updated_at,
        paper_count=0,
    )


@app.get("/api/dossiers", response_model=List[DossierResponse])
async def list_dossiers(
    collection_id: Optional[int] = None,
    db: AsyncSession = Depends(get_db)
):
    """List all dossiers, optionally filtered by collection"""
    # Get paper counts grouped by dossier in one query (avoids N+1)
    counts_query = select(Paper.dossier_id, func.count(Paper.id).label('count')).group_by(Paper.dossier_id)
    counts_result = await db.execute(counts_query)
    paper_counts = {row.dossier_id: row.count for row in counts_result}

    # Get dossiers
    query = select(Dossier).order_by(Dossier.name)
    if collection_id is not None:
        query = query.where(Dossier.collection_id == collection_id)
    result = await db.execute(query)
    dossiers = result.scalars().all()

    return [
        DossierResponse(
            id=d.id,
            name=d.name,
            description=d.description,
            color=d.color,
            collection_id=d.collection_id,
            created_at=d.created_at,
            updated_at=d.updated_at,
            paper_count=paper_counts.get(d.id, 0),
        )
        for d in dossiers
    ]


@app.get("/api/dossiers/{dossier_id}", response_model=DossierDetail)
async def get_dossier(dossier_id: int, db: AsyncSession = Depends(get_db)):
    """Get dossier details with papers"""
    result = await db.execute(select(Dossier).where(Dossier.id == dossier_id))
    dossier = result.scalar_one_or_none()
    if not dossier:
        raise HTTPException(status_code=404, detail="Dossier not found")

    # Get collection name
    collection_result = await db.execute(select(Collection).where(Collection.id == dossier.collection_id))
    collection = collection_result.scalar_one_or_none()

    # Get papers in dossier
    papers_result = await db.execute(
        select(Paper).where(Paper.dossier_id == dossier_id).order_by(Paper.created_at.desc())
    )
    papers = papers_result.scalars().all()

    # Pre-load all editions for all papers in ONE query (avoids N+1)
    paper_ids = [p.id for p in papers]
    editions_by_paper = {}
    if paper_ids:
        editions_result = await db.execute(
            select(Edition)
            .where(Edition.paper_id.in_(paper_ids))
            .order_by(Edition.paper_id, Edition.citation_count.desc())
        )
        for edition in editions_result.scalars().all():
            if edition.paper_id not in editions_by_paper:
                editions_by_paper[edition.paper_id] = []
            editions_by_paper[edition.paper_id].append(edition)

    # Build paper responses using pre-loaded editions (no N+1!)
    paper_responses = [
        build_paper_response_with_preloaded_editions(paper, editions_by_paper.get(paper.id, []))
        for paper in papers
    ]

    return DossierDetail(
        id=dossier.id,
        name=dossier.name,
        description=dossier.description,
        color=dossier.color,
        collection_id=dossier.collection_id,
        created_at=dossier.created_at,
        updated_at=dossier.updated_at,
        paper_count=len(papers),
        papers=paper_responses,
        collection_name=collection.name if collection else None,
    )


@app.put("/api/dossiers/{dossier_id}", response_model=DossierResponse)
async def update_dossier(
    dossier_id: int,
    update: DossierUpdate,
    db: AsyncSession = Depends(get_db)
):
    """Update a dossier"""
    result = await db.execute(select(Dossier).where(Dossier.id == dossier_id))
    dossier = result.scalar_one_or_none()
    if not dossier:
        raise HTTPException(status_code=404, detail="Dossier not found")

    # Verify new collection if changing
    if update.collection_id is not None and update.collection_id != dossier.collection_id:
        coll_result = await db.execute(select(Collection).where(Collection.id == update.collection_id))
        if not coll_result.scalar_one_or_none():
            raise HTTPException(status_code=404, detail="Target collection not found")
        dossier.collection_id = update.collection_id

    if update.name is not None:
        dossier.name = update.name
    if update.description is not None:
        dossier.description = update.description
    if update.color is not None:
        dossier.color = update.color

    # Get paper count
    count_result = await db.execute(
        select(func.count(Paper.id)).where(Paper.dossier_id == dossier_id)
    )
    paper_count = count_result.scalar() or 0

    return DossierResponse(
        id=dossier.id,
        name=dossier.name,
        description=dossier.description,
        color=dossier.color,
        collection_id=dossier.collection_id,
        created_at=dossier.created_at,
        updated_at=dossier.updated_at,
        paper_count=paper_count,
    )


@app.delete("/api/dossiers/{dossier_id}")
async def delete_dossier(dossier_id: int, db: AsyncSession = Depends(get_db)):
    """Delete a dossier (papers are unassigned, not deleted)"""
    result = await db.execute(select(Dossier).where(Dossier.id == dossier_id))
    dossier = result.scalar_one_or_none()
    if not dossier:
        raise HTTPException(status_code=404, detail="Dossier not found")

    # Unassign papers from dossier (set dossier_id to NULL)
    papers_result = await db.execute(
        select(Paper).where(Paper.dossier_id == dossier_id)
    )
    papers = papers_result.scalars().all()
    for paper in papers:
        paper.dossier_id = None

    await db.delete(dossier)
    return {"deleted": True, "dossier_id": dossier_id, "papers_unassigned": len(papers)}


class PaperDossierAssignment(BaseModel):
    paper_ids: List[int]
    dossier_id: Optional[int] = None  # None to unassign


@app.post("/api/dossiers/assign")
async def assign_papers_to_dossier(
    assignment: PaperDossierAssignment,
    db: AsyncSession = Depends(get_db)
):
    """Assign papers to a dossier (or unassign if dossier_id is None)"""
    # Verify dossier exists if provided
    if assignment.dossier_id:
        result = await db.execute(select(Dossier).where(Dossier.id == assignment.dossier_id))
        dossier = result.scalar_one_or_none()
        if not dossier:
            raise HTTPException(status_code=404, detail="Dossier not found")

    # Update papers
    result = await db.execute(
        select(Paper).where(Paper.id.in_(assignment.paper_ids))
    )
    papers = result.scalars().all()

    for paper in papers:
        paper.dossier_id = assignment.dossier_id
        # Also set collection_id based on dossier's collection for backward compatibility
        if assignment.dossier_id:
            dossier_result = await db.execute(select(Dossier).where(Dossier.id == assignment.dossier_id))
            dossier = dossier_result.scalar_one_or_none()
            if dossier:
                paper.collection_id = dossier.collection_id

    return {
        "updated": len(papers),
        "dossier_id": assignment.dossier_id,
        "paper_ids": [p.id for p in papers],
    }


class MultiDossierAssignment(BaseModel):
    """Request to add a paper to multiple dossiers"""
    dossier_ids: List[int]  # List of dossier IDs (first one becomes primary)


@app.post("/api/papers/{paper_id}/add-to-dossiers")
async def add_paper_to_multiple_dossiers(
    paper_id: int,
    request: MultiDossierAssignment,
    db: AsyncSession = Depends(get_db)
):
    """
    Add a paper to multiple dossiers.

    - First dossier becomes the primary (stored in Paper.dossier_id)
    - Additional dossiers are stored in paper_additional_dossiers junction table
    """
    from sqlalchemy import delete

    dossier_ids = request.dossier_ids

    # Verify paper exists
    result = await db.execute(select(Paper).where(Paper.id == paper_id))
    paper = result.scalar_one_or_none()
    if not paper:
        raise HTTPException(status_code=404, detail="Paper not found")

    if not dossier_ids:
        return {"paper_id": paper_id, "primary_dossier_id": None, "additional_dossier_ids": []}

    # Verify all dossiers exist
    result = await db.execute(select(Dossier).where(Dossier.id.in_(dossier_ids)))
    dossiers = {d.id: d for d in result.scalars().all()}

    valid_dossier_ids = [did for did in dossier_ids if did in dossiers]
    if not valid_dossier_ids:
        raise HTTPException(status_code=404, detail="No valid dossiers found")

    # First dossier is primary
    primary_dossier_id = valid_dossier_ids[0]
    additional_dossier_ids = valid_dossier_ids[1:]

    # Update paper's primary dossier
    paper.dossier_id = primary_dossier_id
    paper.collection_id = dossiers[primary_dossier_id].collection_id

    # Delete existing additional dossier associations
    await db.execute(
        delete(PaperAdditionalDossier).where(PaperAdditionalDossier.paper_id == paper_id)
    )

    # Add new additional dossiers
    for dossier_id in additional_dossier_ids:
        # Don't duplicate the primary dossier
        if dossier_id != primary_dossier_id:
            additional = PaperAdditionalDossier(
                paper_id=paper_id,
                dossier_id=dossier_id
            )
            db.add(additional)

    await db.commit()

    return {
        "paper_id": paper_id,
        "primary_dossier_id": primary_dossier_id,
        "additional_dossier_ids": additional_dossier_ids,
        "total_dossiers": 1 + len(additional_dossier_ids),
    }


# ============== Paper Endpoints ==============

@app.post("/api/papers", response_model=PaperResponse)
async def create_paper(
    paper: PaperCreate,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db)
):
    """Submit a single paper for analysis"""
    db_paper = Paper(
        title=paper.title,
        authors=paper.authors,
        year=paper.year,
        venue=paper.venue,
        collection_id=paper.collection_id,
        dossier_id=paper.dossier_id,
        status="pending",
    )
    db.add(db_paper)
    await db.flush()
    await db.refresh(db_paper)

    # Create resolution job
    job = Job(
        paper_id=db_paper.id,
        job_type="resolve",
        status="pending",
    )
    db.add(job)

    # TODO: Queue background task for resolution
    # background_tasks.add_task(resolve_paper, db_paper.id)

    return db_paper


@app.post("/api/papers/batch", response_model=List[PaperResponse])
async def create_papers_batch(
    request: PaperSubmitBatch,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db)
):
    """Submit multiple papers for analysis"""
    created_papers = []

    for paper_data in request.papers:
        # Use paper's collection_id if set, otherwise use batch default
        collection_id = paper_data.collection_id or request.collection_id
        db_paper = Paper(
            title=paper_data.title,
            authors=paper_data.authors,
            year=paper_data.year,
            venue=paper_data.venue,
            collection_id=collection_id,
            status="pending",
        )
        db.add(db_paper)
        await db.flush()
        await db.refresh(db_paper)
        created_papers.append(db_paper)

        # Create resolution job
        job = Job(
            paper_id=db_paper.id,
            job_type="resolve",
            status="pending",
        )
        db.add(job)

    return created_papers


@app.post("/api/papers/quick-add", response_model=QuickAddResponse)
async def quick_add_paper(
    request: QuickAddRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Quick-add a paper using a Google Scholar ID or URL.

    Accepts:
    - Raw scholar ID: "2586223056195525242"
    - Cites URL: "https://scholar.google.com/scholar?cites=2586223056195525242..."
    - Cluster URL: "https://scholar.google.com/scholar?cluster=2586223056195525242..."

    Creates both a Paper and an Edition with the scholar_id, ready for harvesting.
    """
    import re
    from .services.scholar_search import ScholarSearchService

    input_text = request.input.strip()

    # Extract scholar_id from input
    scholar_id = None

    # Try to extract from URL patterns
    cites_match = re.search(r'cites=(\d+)', input_text)
    cluster_match = re.search(r'cluster=(\d+)', input_text)

    if cites_match:
        scholar_id = cites_match.group(1)
    elif cluster_match:
        scholar_id = cluster_match.group(1)
    elif input_text.isdigit():
        # Raw ID
        scholar_id = input_text
    else:
        raise HTTPException(
            status_code=400,
            detail="Invalid input. Provide a Google Scholar ID (digits) or a Scholar URL containing cites= or cluster="
        )

    # Check if this scholar_id already exists
    existing = await db.execute(
        select(Edition).where(Edition.scholar_id == scholar_id)
    )
    existing_edition = existing.scalar_one_or_none()
    if existing_edition:
        raise HTTPException(
            status_code=409,
            detail=f"Paper with this Scholar ID already exists (paper_id={existing_edition.paper_id}, edition_id={existing_edition.id})"
        )

    # Look up the paper metadata from Google Scholar
    scholar_service = ScholarSearchService()
    paper_data = await scholar_service.get_paper_by_scholar_id(scholar_id)

    if not paper_data:
        raise HTTPException(
            status_code=404,
            detail=f"Could not find paper with Scholar ID {scholar_id}"
        )

    # Format authors
    authors = paper_data.get('authorsRaw') or ''
    if isinstance(paper_data.get('authors'), list):
        authors = ', '.join(paper_data['authors'])

    # Create the Paper
    db_paper = Paper(
        title=paper_data.get('title', 'Unknown Title'),
        authors=authors,
        year=paper_data.get('year'),
        venue=paper_data.get('venue'),
        collection_id=request.collection_id,
        dossier_id=request.dossier_id,
        status="resolved",  # Skip resolution since we have the scholar_id
    )
    db.add(db_paper)
    await db.flush()
    await db.refresh(db_paper)

    # Create the Edition
    db_edition = Edition(
        paper_id=db_paper.id,
        scholar_id=scholar_id,
        cluster_id=paper_data.get('clusterId'),
        title=paper_data.get('title', 'Unknown Title'),
        authors=authors,
        year=paper_data.get('year'),
        venue=paper_data.get('venue'),
        abstract=paper_data.get('abstract'),
        link=paper_data.get('link'),
        citation_count=paper_data.get('citationCount', 0),
        confidence="high",  # User explicitly provided this ID
        auto_selected=True,
        selected=True,  # Ready for harvesting
    )
    db.add(db_edition)
    await db.flush()
    await db.refresh(db_edition)

    # Optionally start harvesting
    harvest_job_id = None
    if request.start_harvest:
        from .services.job_worker import create_extract_citations_job
        job = await create_extract_citations_job(db, db_paper.id)
        if job:
            harvest_job_id = job.id

    await db.commit()

    return QuickAddResponse(
        paper_id=db_paper.id,
        edition_id=db_edition.id,
        title=db_paper.title,
        authors=db_paper.authors,
        year=db_paper.year,
        citation_count=db_edition.citation_count,
        scholar_id=scholar_id,
        harvest_job_id=harvest_job_id,
        message=f"Paper '{db_paper.title[:50]}...' added successfully" + (
            f". Harvest job {harvest_job_id} queued." if harvest_job_id else ""
        ),
    )


def paper_to_response(paper: Paper, harvest_stats: dict = None) -> dict:
    """Convert Paper model to response dict, handling JSON fields"""
    data = {k: v for k, v in paper.__dict__.items() if not k.startswith('_')}
    # Parse candidates JSON if present
    if data.get('candidates') and isinstance(data['candidates'], str):
        try:
            parsed = json.loads(data['candidates'])
            if isinstance(parsed, list):
                data['candidates'] = parsed
            else:
                data['candidates'] = None
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse candidates JSON for paper {paper.id}: {e}")
            data['candidates'] = None

    # Add harvest progress stats if provided
    if harvest_stats:
        data['harvest_expected'] = harvest_stats.get('expected', 0)
        data['harvest_actual'] = harvest_stats.get('actual', 0)
        if data['harvest_expected'] > 0:
            data['harvest_percent'] = round((data['harvest_actual'] / data['harvest_expected']) * 100, 1)
        else:
            data['harvest_percent'] = 0.0
    else:
        data['harvest_expected'] = 0
        data['harvest_actual'] = data.get('total_harvested_citations', 0)
        data['harvest_percent'] = 0.0

    return data


@app.get("/api/papers", response_model=PapersPaginatedResponse)
async def list_papers(
    page: int = 1,
    per_page: int = 25,
    status: str = None,
    collection_id: Optional[int] = None,
    include_deleted: bool = False,
    foreign_edition_needed: Optional[bool] = None,
    db: AsyncSession = Depends(get_db)
):
    """List papers with pagination, optionally filtered by status or collection"""
    # Build base query for count
    count_query = select(func.count(Paper.id))
    if status:
        count_query = count_query.where(Paper.status == status)
    if collection_id is not None:
        count_query = count_query.where(Paper.collection_id == collection_id)
    if not include_deleted:
        count_query = count_query.where(Paper.deleted_at.is_(None))
    if foreign_edition_needed is not None:
        count_query = count_query.where(Paper.foreign_edition_needed == foreign_edition_needed)

    # Get total count
    total_result = await db.execute(count_query)
    total = total_result.scalar() or 0

    # Calculate pagination
    total_pages = (total + per_page - 1) // per_page if per_page > 0 else 1
    skip = (page - 1) * per_page

    # Build query for data
    query = select(Paper).offset(skip).limit(per_page).order_by(Paper.created_at.desc())
    if status:
        query = query.where(Paper.status == status)
    if collection_id is not None:
        query = query.where(Paper.collection_id == collection_id)
    if not include_deleted:
        query = query.where(Paper.deleted_at.is_(None))
    if foreign_edition_needed is not None:
        query = query.where(Paper.foreign_edition_needed == foreign_edition_needed)

    result = await db.execute(query)
    papers = result.scalars().all()

    # Get harvest stats for all papers in batch
    paper_ids = [p.id for p in papers]
    harvest_stats_map = {}
    if paper_ids:
        # Get sum of citation_count from selected editions (expected)
        expected_result = await db.execute(
            select(Edition.paper_id, func.sum(Edition.citation_count).label('expected'))
            .where(Edition.paper_id.in_(paper_ids))
            .where(Edition.selected == True)
            .group_by(Edition.paper_id)
        )
        for row in expected_result:
            harvest_stats_map[row.paper_id] = {'expected': row.expected or 0, 'actual': 0}

        # Get sum of harvested citations per paper (actual)
        actual_result = await db.execute(
            select(Citation.paper_id, func.count(Citation.id).label('actual'))
            .where(Citation.paper_id.in_(paper_ids))
            .group_by(Citation.paper_id)
        )
        for row in actual_result:
            if row.paper_id in harvest_stats_map:
                harvest_stats_map[row.paper_id]['actual'] = row.actual or 0
            else:
                harvest_stats_map[row.paper_id] = {'expected': 0, 'actual': row.actual or 0}

    paper_responses = [
        PaperResponse(**paper_to_response(p, harvest_stats_map.get(p.id, {})))
        for p in papers
    ]

    return PapersPaginatedResponse(
        papers=paper_responses,
        total=total,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
        has_next=page < total_pages,
        has_prev=page > 1
    )


@app.get("/api/papers/{paper_id}", response_model=PaperDetail)
async def get_paper(paper_id: int, db: AsyncSession = Depends(get_db)):
    """Get paper details with editions"""
    result = await db.execute(select(Paper).where(Paper.id == paper_id))
    paper = result.scalar_one_or_none()
    if not paper:
        raise HTTPException(status_code=404, detail="Paper not found")

    # Get editions
    editions_result = await db.execute(
        select(Edition).where(Edition.paper_id == paper_id).order_by(Edition.citation_count.desc())
    )
    editions = editions_result.scalars().all()

    # Get citation count
    citation_count = await db.execute(
        select(func.count(Citation.id)).where(Citation.paper_id == paper_id)
    )

    paper_data = paper_to_response(paper)
    return PaperDetail(
        **paper_data,
        editions=[build_edition_response_with_staleness(e) for e in editions],
        citations_count=citation_count.scalar() or 0,
    )


@app.delete("/api/papers/{paper_id}")
async def delete_paper(
    paper_id: int,
    permanent: bool = False,
    db: AsyncSession = Depends(get_db)
):
    """Soft delete a paper (or permanently delete if permanent=true)"""
    result = await db.execute(select(Paper).where(Paper.id == paper_id))
    paper = result.scalar_one_or_none()
    if not paper:
        raise HTTPException(status_code=404, detail="Paper not found")

    if permanent:
        await db.delete(paper)
        return {"deleted": True, "paper_id": paper_id, "permanent": True}
    else:
        # Soft delete - just set deleted_at timestamp
        paper.deleted_at = datetime.utcnow()
        return {
            "deleted": True,
            "paper_id": paper_id,
            "permanent": False,
            "title": paper.title,
            "can_restore": True,
        }


@app.post("/api/papers/{paper_id}/restore")
async def restore_paper(paper_id: int, db: AsyncSession = Depends(get_db)):
    """Restore a soft-deleted paper"""
    result = await db.execute(select(Paper).where(Paper.id == paper_id))
    paper = result.scalar_one_or_none()
    if not paper:
        raise HTTPException(status_code=404, detail="Paper not found")

    if paper.deleted_at is None:
        raise HTTPException(status_code=400, detail="Paper is not deleted")

    paper.deleted_at = None
    return {"restored": True, "paper_id": paper_id, "title": paper.title}


# ============== Metadata Update Endpoints ==============

class PaperMetadataUpdate(BaseModel):
    """Update paper bibliographic metadata"""
    title: Optional[str] = None
    authors: Optional[str] = None
    year: Optional[int] = None
    venue: Optional[str] = None
    link: Optional[str] = None
    abstract: Optional[str] = None


class EditionMetadataUpdate(BaseModel):
    """Update edition bibliographic metadata"""
    title: Optional[str] = None
    authors: Optional[str] = None
    year: Optional[int] = None
    venue: Optional[str] = None
    link: Optional[str] = None
    abstract: Optional[str] = None
    language: Optional[str] = None


@app.patch("/api/papers/{paper_id}/metadata")
async def update_paper_metadata(
    paper_id: int,
    update: PaperMetadataUpdate,
    db: AsyncSession = Depends(get_db)
):
    """Update bibliographic metadata for a paper"""
    result = await db.execute(
        select(Paper).where(Paper.id == paper_id, Paper.deleted_at.is_(None))
    )
    paper = result.scalar_one_or_none()
    if not paper:
        raise HTTPException(status_code=404, detail="Paper not found")

    # Update only provided fields
    if update.title is not None:
        paper.title = update.title
    if update.authors is not None:
        paper.authors = update.authors
    if update.year is not None:
        paper.year = update.year
    if update.venue is not None:
        paper.venue = update.venue
    if update.link is not None:
        paper.link = update.link
    if update.abstract is not None:
        paper.abstract = update.abstract

    await db.commit()
    await db.refresh(paper)

    return {
        "id": paper.id,
        "title": paper.title,
        "authors": paper.authors,
        "year": paper.year,
        "venue": paper.venue,
        "link": paper.link,
        "abstract": paper.abstract,
        "updated": True
    }


@app.patch("/api/editions/{edition_id}/metadata")
async def update_edition_metadata(
    edition_id: int,
    update: EditionMetadataUpdate,
    db: AsyncSession = Depends(get_db)
):
    """Update bibliographic metadata for an edition"""
    result = await db.execute(
        select(Edition).where(Edition.id == edition_id)
    )
    edition = result.scalar_one_or_none()
    if not edition:
        raise HTTPException(status_code=404, detail="Edition not found")

    # Update only provided fields
    if update.title is not None:
        edition.title = update.title
    if update.authors is not None:
        edition.authors = update.authors
    if update.year is not None:
        edition.year = update.year
    if update.venue is not None:
        edition.venue = update.venue
    if update.link is not None:
        edition.link = update.link
    if update.abstract is not None:
        edition.abstract = update.abstract
    if update.language is not None:
        edition.language = update.language

    await db.commit()
    await db.refresh(edition)

    return {
        "id": edition.id,
        "paper_id": edition.paper_id,
        "title": edition.title,
        "authors": edition.authors,
        "year": edition.year,
        "venue": edition.venue,
        "link": edition.link,
        "abstract": edition.abstract,
        "language": edition.language,
        "updated": True
    }


# ============== Edition Discovery Endpoints ==============

@app.post("/api/editions/discover", response_model=EditionDiscoveryResponse)
async def discover_editions(
    request: EditionDiscoveryRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db)
):
    """Discover all editions of a paper using LLM-driven search"""
    from .services.paper_resolution import PaperResolutionService

    result = await db.execute(select(Paper).where(Paper.id == request.paper_id))
    paper = result.scalar_one_or_none()
    if not paper:
        raise HTTPException(status_code=404, detail="Paper not found")

    # Create discovery job
    job = Job(
        paper_id=paper.id,
        job_type="discover_editions",
        status="pending",
    )
    db.add(job)
    await db.flush()
    await db.refresh(job)

    # Run discovery synchronously for now (can be moved to background task later)
    service = PaperResolutionService(db)

    try:
        discovery_result = await service.discover_editions(
            paper_id=paper.id,
            job_id=job.id,
            language_strategy=request.language_strategy,
            custom_languages=request.custom_languages,
        )

        # Get stored editions
        editions_result = await db.execute(
            select(Edition).where(Edition.paper_id == request.paper_id).order_by(Edition.citation_count.desc())
        )
        editions = editions_result.scalars().all()

        await db.commit()

        return EditionDiscoveryResponse(
            paper_id=paper.id,
            total_found=discovery_result.get("editions_found", 0),
            high_confidence=discovery_result.get("high_confidence", 0),
            uncertain=discovery_result.get("uncertain", 0),
            rejected=discovery_result.get("rejected", 0),
            editions=[build_edition_response_with_staleness(e) for e in editions],
            queries_used=discovery_result.get("summary", {}).get("queriesGenerated", []),
        )

    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Edition discovery failed: {str(e)}")


@app.get("/api/papers/{paper_id}/editions", response_model=List[EditionResponse])
async def get_paper_editions(paper_id: int, db: AsyncSession = Depends(get_db)):
    """Get all editions of a paper, including harvested citation counts"""
    # Get editions
    result = await db.execute(
        select(Edition).where(Edition.paper_id == paper_id).order_by(Edition.citation_count.desc())
    )
    editions = result.scalars().all()

    # Get harvested citation counts per edition
    citation_counts = await db.execute(
        select(Citation.edition_id, func.count(Citation.id).label('count'))
        .where(Citation.edition_id.in_([e.id for e in editions]))
        .group_by(Citation.edition_id)
    )
    harvested_map = {row.edition_id: row.count for row in citation_counts}

    # Build response with harvested counts, staleness, and incompleteness
    responses = []
    for ed in editions:
        ed_dict = {k: v for k, v in ed.__dict__.items() if not k.startswith('_')}
        ed_dict['harvested_citations'] = harvested_map.get(ed.id, 0)
        # Compute staleness
        if ed.last_harvested_at is None:
            ed_dict['is_stale'] = True
            ed_dict['days_since_harvest'] = None
        else:
            ed_dict['days_since_harvest'] = (datetime.utcnow() - ed.last_harvested_at).days
            ed_dict['is_stale'] = ed_dict['days_since_harvest'] > 90
        # Compute incompleteness (significant gap between total and harvested)
        total = ed.citation_count or 0
        harvested = ed.harvested_citation_count or 0
        missing = max(0, total - harvested)
        ed_dict['missing_citations'] = missing
        # Incomplete if missing at least 100 citations OR at least 10% of total
        ed_dict['is_incomplete'] = missing >= 100 or (total > 0 and missing / total >= 0.10)
        responses.append(EditionResponse(**ed_dict))

    return responses


@app.delete("/api/papers/{paper_id}/editions")
async def clear_paper_editions(paper_id: int, db: AsyncSession = Depends(get_db)):
    """Clear all editions of a paper to allow fresh discovery"""
    result = await db.execute(
        select(Edition).where(Edition.paper_id == paper_id)
    )
    editions = result.scalars().all()
    count = len(editions)

    for edition in editions:
        await db.delete(edition)

    return {"deleted": count, "paper_id": paper_id}


@app.post("/api/editions/select")
async def select_editions(request: EditionSelectRequest, db: AsyncSession = Depends(get_db)):
    """Select/deselect editions for citation extraction"""
    result = await db.execute(
        select(Edition).where(Edition.id.in_(request.edition_ids))
    )
    editions = result.scalars().all()

    for edition in editions:
        edition.selected = request.selected

    return {"updated": len(editions), "selected": request.selected}


@app.post("/api/editions/confidence")
async def update_edition_confidence(request: EditionUpdateConfidenceRequest, db: AsyncSession = Depends(get_db)):
    """Update confidence level for editions (high/uncertain/rejected)"""
    if request.confidence not in ["high", "uncertain", "rejected"]:
        raise HTTPException(status_code=400, detail="Confidence must be 'high', 'uncertain', or 'rejected'")

    result = await db.execute(
        select(Edition).where(Edition.id.in_(request.edition_ids))
    )
    editions = result.scalars().all()

    for edition in editions:
        edition.confidence = request.confidence
        # If rejecting, also deselect
        if request.confidence == "rejected":
            edition.selected = False

    return {"updated": len(editions), "confidence": request.confidence}


@app.post("/api/editions/clear-new")
async def clear_new_badges(paper_id: int, language: Optional[str] = None, db: AsyncSession = Depends(get_db)):
    """Clear NEW badges for editions (set added_by_job_id to null)"""
    from sqlalchemy import update

    query = update(Edition).where(Edition.paper_id == paper_id)
    if language:
        query = query.where(Edition.language.ilike(f"%{language}%"))
    query = query.values(added_by_job_id=None)

    result = await db.execute(query)
    await db.commit()

    return {"cleared": result.rowcount, "paper_id": paper_id, "language": language}


@app.post("/api/editions/exclude")
async def exclude_editions(request: EditionExcludeRequest, db: AsyncSession = Depends(get_db)):
    """Exclude/unexclude editions from view"""
    result = await db.execute(
        select(Edition).where(Edition.id.in_(request.edition_ids))
    )
    editions = result.scalars().all()

    for edition in editions:
        edition.excluded = request.excluded

    await db.commit()
    return {"updated": len(editions), "excluded": request.excluded}


@app.post("/api/editions/merge", response_model=EditionMergeResponse)
async def merge_editions(request: EditionMergeRequest, db: AsyncSession = Depends(get_db)):
    """Merge one edition into another (canonical) edition.

    Use case: Same work appears under different URLs/scholar_ids (e.g., JSTOR + marcuse.org).
    - Source edition's citations are moved to target edition
    - Source keeps its scholar_id for future harvesting
    - Source is marked as merged_into_edition_id = target
    - Optionally copy target's metadata to source
    """
    # Get both editions
    result = await db.execute(
        select(Edition).where(Edition.id.in_([request.source_edition_id, request.target_edition_id]))
    )
    editions = {e.id: e for e in result.scalars().all()}

    source = editions.get(request.source_edition_id)
    target = editions.get(request.target_edition_id)

    if not source:
        raise HTTPException(status_code=404, detail=f"Source edition {request.source_edition_id} not found")
    if not target:
        raise HTTPException(status_code=404, detail=f"Target edition {request.target_edition_id} not found")

    # Ensure they belong to the same paper
    if source.paper_id != target.paper_id:
        raise HTTPException(status_code=400, detail="Cannot merge editions from different papers")

    # Check for circular merge
    if target.merged_into_edition_id:
        raise HTTPException(status_code=400, detail="Target edition is already merged into another edition")

    # Move citations from source to target
    citations_result = await db.execute(
        select(Citation).where(Citation.edition_id == source.id)
    )
    source_citations = citations_result.scalars().all()
    citations_moved = 0

    for citation in source_citations:
        citation.edition_id = target.id
        citations_moved += 1

    # Mark source as merged
    source.merged_into_edition_id = target.id

    # Optionally copy metadata from target to source
    if request.copy_metadata:
        source.title = target.title
        source.authors = target.authors
        source.year = target.year
        source.venue = target.venue
        source.abstract = target.abstract
        # Keep source's own link and scholar_id

    # Update target's harvested citation count
    target.harvested_citation_count = (target.harvested_citation_count or 0) + citations_moved

    await db.commit()

    return EditionMergeResponse(
        success=True,
        message=f"Merged edition {source.id} into {target.id}. {citations_moved} citations moved.",
        citations_moved=citations_moved,
        source_edition_id=source.id,
        target_edition_id=target.id
    )


@app.post("/api/editions/{edition_id}/add-as-seed", response_model=EditionAddAsSeedResponse)
async def add_edition_as_seed(
    edition_id: int,
    request: EditionAddAsSeedRequest = None,
    db: AsyncSession = Depends(get_db)
):
    """Convert an edition into a new independent seed paper.

    Creates a new Paper from the edition's data (title, authors, year, venue).
    Optionally excludes the edition from the current paper.
    Supports dossier selection:
    - dossier_id: specific dossier to add to
    - create_new_dossier: create a new dossier first
    - Falls back to parent paper's dossier
    """
    if request is None:
        request = EditionAddAsSeedRequest()

    # Get the edition
    result = await db.execute(select(Edition).where(Edition.id == edition_id))
    edition = result.scalar_one_or_none()
    if not edition:
        raise HTTPException(status_code=404, detail="Edition not found")

    # Get the parent paper to get default dossier/collection
    parent_result = await db.execute(select(Paper).where(Paper.id == edition.paper_id))
    parent_paper = parent_result.scalar_one_or_none()

    # Determine target dossier
    target_dossier_id = None
    target_dossier_name = None
    target_collection_id = parent_paper.collection_id if parent_paper else None

    if request.create_new_dossier and request.new_dossier_name:
        # Create a new dossier
        collection_id = request.collection_id or (parent_paper.collection_id if parent_paper else None)
        if not collection_id:
            raise HTTPException(status_code=400, detail="Collection ID required when creating new dossier")

        # Verify collection exists
        coll_result = await db.execute(select(Collection).where(Collection.id == collection_id))
        collection = coll_result.scalar_one_or_none()
        if not collection:
            raise HTTPException(status_code=404, detail="Collection not found")

        new_dossier = Dossier(
            name=request.new_dossier_name,
            collection_id=collection_id,
        )
        db.add(new_dossier)
        await db.flush()
        await db.refresh(new_dossier)
        target_dossier_id = new_dossier.id
        target_dossier_name = new_dossier.name
        target_collection_id = collection_id

    elif request.dossier_id:
        # Use specified dossier
        dossier_result = await db.execute(select(Dossier).where(Dossier.id == request.dossier_id))
        dossier = dossier_result.scalar_one_or_none()
        if not dossier:
            raise HTTPException(status_code=404, detail="Dossier not found")
        target_dossier_id = dossier.id
        target_dossier_name = dossier.name
        target_collection_id = dossier.collection_id

    else:
        # Default to parent paper's dossier
        if parent_paper and parent_paper.dossier_id:
            dossier_result = await db.execute(select(Dossier).where(Dossier.id == parent_paper.dossier_id))
            dossier = dossier_result.scalar_one_or_none()
            if dossier:
                target_dossier_id = dossier.id
                target_dossier_name = dossier.name

    # Create new paper from edition data
    new_paper = Paper(
        title=edition.title,
        authors=edition.authors,
        year=edition.year,
        venue=edition.venue,
        abstract=edition.abstract,
        link=edition.link,
        collection_id=target_collection_id,
        dossier_id=target_dossier_id,
        status="pending",  # Will need to be resolved
    )
    db.add(new_paper)
    await db.flush()
    await db.refresh(new_paper)

    # Create resolution job for the new paper
    job = Job(
        paper_id=new_paper.id,
        job_type="resolve",
        status="pending",
    )
    db.add(job)

    # Exclude the edition from current paper (per user decision: always exclude)
    if request.exclude_from_current:
        edition.excluded = True

    await db.commit()

    return EditionAddAsSeedResponse(
        new_paper_id=new_paper.id,
        title=new_paper.title,
        message=f"Created new seed paper from edition: {new_paper.title[:50]}...",
        dossier_id=target_dossier_id,
        dossier_name=target_dossier_name,
    )


@app.post("/api/papers/{paper_id}/finalize-editions")
async def finalize_editions(paper_id: int, db: AsyncSession = Depends(get_db)):
    """Finalize edition selection for a paper.

    - Sets editions_finalized = True on the paper
    - Bulk excludes all unselected editions
    - Final view will show only selected editions
    """
    # Get paper
    result = await db.execute(select(Paper).where(Paper.id == paper_id))
    paper = result.scalar_one_or_none()
    if not paper:
        raise HTTPException(status_code=404, detail="Paper not found")

    # Mark paper as finalized
    paper.editions_finalized = True

    # Exclude all unselected editions
    from sqlalchemy import update
    exclude_result = await db.execute(
        update(Edition)
        .where(Edition.paper_id == paper_id)
        .where(Edition.selected == False)
        .values(excluded=True)
    )

    await db.commit()

    return {
        "finalized": True,
        "paper_id": paper_id,
        "editions_excluded": exclude_result.rowcount,
    }


@app.post("/api/papers/{paper_id}/reopen-editions")
async def reopen_editions(paper_id: int, db: AsyncSession = Depends(get_db)):
    """Reopen edition selection for a paper.

    - Sets editions_finalized = False
    - Does NOT un-exclude editions (user must do that manually)
    """
    result = await db.execute(select(Paper).where(Paper.id == paper_id))
    paper = result.scalar_one_or_none()
    if not paper:
        raise HTTPException(status_code=404, detail="Paper not found")

    paper.editions_finalized = False
    await db.commit()

    return {"reopened": True, "paper_id": paper_id}


@app.post("/api/papers/{paper_id}/pause-harvest")
async def pause_harvest(paper_id: int, db: AsyncSession = Depends(get_db)):
    """Pause auto-resume harvesting for a paper.

    - Sets harvest_paused = True
    - Does NOT cancel running jobs (user can do that separately)
    - Auto-resume will skip this paper until unpaused
    """
    result = await db.execute(select(Paper).where(Paper.id == paper_id))
    paper = result.scalar_one_or_none()
    if not paper:
        raise HTTPException(status_code=404, detail="Paper not found")

    paper.harvest_paused = True
    await db.commit()

    return {"paused": True, "paper_id": paper_id, "title": paper.title}


@app.post("/api/papers/{paper_id}/unpause-harvest")
async def unpause_harvest(paper_id: int, db: AsyncSession = Depends(get_db)):
    """Unpause auto-resume harvesting for a paper.

    - Sets harvest_paused = False
    - Auto-resume will resume queueing jobs for this paper
    """
    result = await db.execute(select(Paper).where(Paper.id == paper_id))
    paper = result.scalar_one_or_none()
    if not paper:
        raise HTTPException(status_code=404, detail="Paper not found")

    paper.harvest_paused = False
    await db.commit()

    return {"paused": False, "paper_id": paper_id, "title": paper.title}


@app.post("/api/editions/{edition_id}/mark-complete")
async def mark_edition_complete(edition_id: int, db: AsyncSession = Depends(get_db)):
    """Mark an edition's harvest as complete.

    Use this when:
    - All years have been scraped (status=complete on all HarvestTargets)
    - The remaining gap is due to GS data inaccuracy, not incomplete scraping
    - You want to stop auto-resume for this edition

    This sets harvest_complete=True and harvest_complete_reason='manual'.
    Also resets harvest_stall_count to 0.
    """
    result = await db.execute(select(Edition).where(Edition.id == edition_id))
    edition = result.scalar_one_or_none()
    if not edition:
        raise HTTPException(status_code=404, detail="Edition not found")

    edition.harvest_complete = True
    edition.harvest_complete_reason = "manual"
    edition.harvest_stall_count = 0
    await db.commit()

    # Get paper title for response
    paper_result = await db.execute(select(Paper).where(Paper.id == edition.paper_id))
    paper = paper_result.scalar_one_or_none()

    return {
        "edition_id": edition_id,
        "paper_id": edition.paper_id,
        "paper_title": paper.title if paper else None,
        "harvest_complete": True,
        "harvest_complete_reason": "manual",
    }


@app.post("/api/editions/{edition_id}/mark-incomplete")
async def mark_edition_incomplete(edition_id: int, db: AsyncSession = Depends(get_db)):
    """Mark an edition's harvest as incomplete (undo mark-complete).

    Use this to re-enable auto-resume for an edition that was previously
    marked as complete.
    """
    result = await db.execute(select(Edition).where(Edition.id == edition_id))
    edition = result.scalar_one_or_none()
    if not edition:
        raise HTTPException(status_code=404, detail="Edition not found")

    edition.harvest_complete = False
    edition.harvest_complete_reason = None
    await db.commit()

    return {
        "edition_id": edition_id,
        "harvest_complete": False,
    }


class MarkCompleteRequest(BaseModel):
    edition_ids: List[int]


@app.post("/api/dashboard/mark-complete-batch")
async def mark_editions_complete_batch(request: MarkCompleteRequest, db: AsyncSession = Depends(get_db)):
    """Mark multiple editions as complete at once.

    Useful for batch-completing stalled papers that have been diagnosed
    as 'gs_fault' (all years complete but gap remains due to GS data inaccuracy).
    """
    if not request.edition_ids:
        return {"marked_complete": 0, "editions": []}

    marked = []
    for edition_id in request.edition_ids:
        result = await db.execute(select(Edition).where(Edition.id == edition_id))
        edition = result.scalar_one_or_none()
        if edition:
            edition.harvest_complete = True
            edition.harvest_complete_reason = "manual"
            edition.harvest_stall_count = 0
            marked.append(edition_id)

    await db.commit()

    return {
        "marked_complete": len(marked),
        "editions": marked,
    }


@app.post("/api/editions/{edition_id}/ai-diagnose")
async def ai_diagnose_edition(
    edition_id: int,
    db: AsyncSession = Depends(get_db)
):
    """
    Use AI (Claude Opus 4.5) to analyze why a harvest is stalled.

    Collects all relevant data (harvest targets, job history, failed fetches, etc.)
    and sends to Claude with extended thinking for comprehensive analysis.

    Returns:
    - Root cause diagnosis (RESUME_BUG, RATE_LIMITING, OVERFLOW_YEAR, etc.)
    - Whether gap is recoverable
    - Specific recommended action with exact parameters
    """
    logger.info(f"AI Diagnosis endpoint called for edition {edition_id}")
    try:
        from .services.ai_diagnosis import get_diagnosis_service

        logger.info("Importing diagnosis service...")
        service = get_diagnosis_service()
        logger.info("Service obtained, calling diagnose_edition...")
        result = await service.diagnose_edition(db, edition_id)
        logger.info(f"Diagnosis complete, success={result.get('success')}")

        return result
    except Exception as e:
        logger.error(f"AI Diagnosis endpoint error: {e}", exc_info=True)
        return {"success": False, "error": str(e), "edition_id": edition_id}


@app.post("/api/editions/fetch-more", response_model=EditionFetchMoreResponse)
async def fetch_more_editions(
    request: EditionFetchMoreRequest,
    db: AsyncSession = Depends(get_db)
):
    """Fetch more editions in a specific language (supplementary search) - SYNCHRONOUS version"""
    from .services.edition_discovery import EditionDiscoveryService

    # Get paper
    result = await db.execute(select(Paper).where(Paper.id == request.paper_id))
    paper = result.scalar_one_or_none()
    if not paper:
        raise HTTPException(status_code=404, detail="Paper not found")

    # Get existing editions to check for duplicates
    existing_result = await db.execute(
        select(Edition.scholar_id, Edition.title).where(Edition.paper_id == request.paper_id)
    )
    existing_editions = {(e.scholar_id, e.title.lower()) for e in existing_result.fetchall()}

    # Run targeted search
    service = EditionDiscoveryService(
        language_strategy="custom",
        custom_languages=[request.language],
    )

    paper_dict = {
        "title": paper.title,
        "authors": paper.authors,
        "year": paper.year,
    }

    try:
        discovery_result = await service.fetch_more_in_language(
            paper=paper_dict,
            target_language=request.language,
            max_results=request.max_results,
        )

        # Store new editions (skip duplicates)
        new_editions = []
        for edition_data in discovery_result.get("genuineEditions", []):
            scholar_id = edition_data.get("scholarId")
            title = edition_data.get("title", "")

            # Check for duplicates
            if (scholar_id, title.lower()) in existing_editions:
                continue

            edition = Edition(
                paper_id=request.paper_id,
                scholar_id=scholar_id,
                cluster_id=edition_data.get("clusterId"),
                title=title,
                authors=edition_data.get("authorsRaw"),
                year=edition_data.get("year"),
                venue=edition_data.get("venue"),
                abstract=edition_data.get("abstract"),
                link=edition_data.get("link"),
                citation_count=edition_data.get("citationCount", 0),
                language=edition_data.get("language", request.language.capitalize()),
                confidence=edition_data.get("confidence", "uncertain"),
                auto_selected=edition_data.get("autoSelected", False),
                selected=edition_data.get("confidence") == "high",
                is_supplementary=True,  # Mark as supplementary addition
            )
            db.add(edition)
            new_editions.append(edition)
            existing_editions.add((scholar_id, title.lower()))

        await db.commit()

        return EditionFetchMoreResponse(
            paper_id=request.paper_id,
            language=request.language,
            new_editions_found=len(new_editions),
            total_results_searched=discovery_result.get("totalSearched", 0),
            queries_used=discovery_result.get("queriesUsed", []),
        )

    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Fetch more failed: {str(e)}")


@app.post("/api/editions/fetch-more-async", response_model=FetchMoreJobResponse)
async def fetch_more_editions_async(
    request: FetchMoreJobRequest,
    db: AsyncSession = Depends(get_db)
):
    """Queue a fetch-more job to run in background - returns immediately with job ID"""
    from .services.job_worker import create_fetch_more_job

    # Verify paper exists
    result = await db.execute(select(Paper).where(Paper.id == request.paper_id))
    paper = result.scalar_one_or_none()
    if not paper:
        raise HTTPException(status_code=404, detail="Paper not found")

    # Check for existing pending/running job for same paper+language
    existing = await db.execute(
        select(Job).where(
            Job.paper_id == request.paper_id,
            Job.job_type == "fetch_more_editions",
            Job.status.in_(["pending", "running"])
        )
    )
    existing_job = existing.scalar_one_or_none()
    if existing_job:
        # Parse params to check language
        params = json.loads(existing_job.params) if existing_job.params else {}
        if params.get("language") == request.language:
            return FetchMoreJobResponse(
                job_id=existing_job.id,
                paper_id=request.paper_id,
                language=request.language,
                status=existing_job.status,
                message=f"Job already {existing_job.status} for {request.language}",
            )

    # Create new job
    job = await create_fetch_more_job(
        db=db,
        paper_id=request.paper_id,
        language=request.language,
        max_results=request.max_results,
    )
    await db.commit()

    return FetchMoreJobResponse(
        job_id=job.id,
        paper_id=request.paper_id,
        language=request.language,
        status="pending",
        message=f"Queued fetch for {request.language} editions",
    )


@app.post("/api/editions/add-manual", response_model=ManualEditionAddResponse)
async def add_manual_edition(
    request: ManualEditionAddRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Manually add an edition using LLM-assisted resolution.

    Input can be:
    - Google Scholar URL (e.g., https://scholar.google.com/citations?...&cites=...)
    - Translated title (e.g., "Smarte Neue Welt" for German edition)
    - Pasted text from Google Scholar search result
    - Raw bibliographic entry

    The LLM will parse the input and search Google Scholar to find the exact edition.
    """
    import anthropic
    import re

    # Get the parent paper for context
    result = await db.execute(select(Paper).where(Paper.id == request.paper_id))
    paper = result.scalar_one_or_none()
    if not paper:
        raise HTTPException(status_code=404, detail="Paper not found")

    # Get existing editions to avoid duplicates
    existing_result = await db.execute(
        select(Edition.scholar_id, Edition.title).where(Edition.paper_id == request.paper_id)
    )
    existing_editions = [(r[0], r[1]) for r in existing_result.fetchall()]
    existing_scholar_ids = {e[0] for e in existing_editions if e[0]}
    existing_titles = {e[1].lower() for e in existing_editions}

    input_text = request.input_text.strip()

    # Check if it's a Google Scholar URL with cites= or cluster=
    scholar_url_match = re.search(r'scholar\.google\.[^/]+/scholar\?.*cites=(\d+)', input_text)
    cluster_id_match = re.search(r'cluster=(\d+)', input_text)

    resolution_details = {"input_type": "unknown", "llm_used": False}
    search_query = None
    expected_language = request.language_hint

    # If we have a cites= or cluster= ID, fetch the paper directly
    if scholar_url_match or cluster_id_match:
        scholar_id = cluster_id_match.group(1) if cluster_id_match else scholar_url_match.group(1)
        resolution_details["input_type"] = "scholar_id_direct"
        resolution_details["scholar_id"] = scholar_id

        # Check if this edition already exists
        if scholar_id in existing_scholar_ids:
            return ManualEditionAddResponse(
                success=False,
                message=f"An edition with Scholar ID {scholar_id} already exists",
                resolution_details=resolution_details,
            )

        # Fetch the paper directly by scholar ID
        from .services.scholar_search import get_scholar_service
        scholar_service = get_scholar_service()

        try:
            paper_data = await scholar_service.get_paper_by_scholar_id(scholar_id)
            if paper_data:
                # Create the edition directly
                new_edition = Edition(
                    paper_id=request.paper_id,
                    scholar_id=scholar_id,
                    title=paper_data.get("title", "Unknown"),
                    authors=paper_data.get("authorsRaw") or ", ".join(paper_data.get("authors", [])),
                    year=paper_data.get("year"),
                    venue=paper_data.get("venue"),
                    abstract=paper_data.get("abstract"),
                    link=paper_data.get("link"),
                    citation_count=paper_data.get("citationCount") or paper_data.get("citations") or 0,
                    language=expected_language,
                    confidence="high",
                    auto_selected=False,
                    selected=True,
                    is_supplementary=True,
                )
                db.add(new_edition)
                await db.commit()
                await db.refresh(new_edition)

                resolution_details["matched_title"] = paper_data.get("title")

                return ManualEditionAddResponse(
                    success=True,
                    edition=build_edition_response_with_staleness(new_edition),
                    message=f"Added edition: {new_edition.title}",
                    resolution_details=resolution_details,
                )
            else:
                return ManualEditionAddResponse(
                    success=False,
                    message=f"Could not find paper with Scholar ID {scholar_id}",
                    resolution_details=resolution_details,
                )
        except Exception as e:
            logging.error(f"Direct scholar ID lookup failed: {e}")
            return ManualEditionAddResponse(
                success=False,
                message=f"Failed to fetch paper: {str(e)}",
                resolution_details=resolution_details,
            )

    if not search_query:
        # Use Claude to parse the input and generate a search query
        resolution_details["llm_used"] = True

        try:
            client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

            prompt = f"""You are helping to find a specific edition of an academic work on Google Scholar.

PARENT WORK:
- Title: {paper.title}
- Authors: {paper.authors or 'Unknown'}
- Year: {paper.year or 'Unknown'}

USER INPUT (might be a translated title, pasted Scholar entry, or partial info):
{input_text}

{f"Language hint: {expected_language}" if expected_language else ""}

Your task:
1. Determine what type of input this is (translated title, bibliographic entry, partial info)
2. Generate the BEST Google Scholar search query to find this specific edition
3. If it's a translation, identify the language

Respond in JSON format:
{{
    "input_type": "translated_title" | "bibliographic_entry" | "partial_info" | "google_scholar_paste",
    "detected_language": "german" | "french" | "spanish" | etc. (or null if unknown),
    "search_query": "the exact query to use on Google Scholar",
    "expected_title": "the title we expect to find (if known)",
    "confidence": "high" | "medium" | "low",
    "reasoning": "brief explanation of your interpretation"
}}"""

            response = client.messages.create(
                model="claude-sonnet-4-5-20250929",
                max_tokens=500,
                messages=[{"role": "user", "content": prompt}]
            )

            response_text = response.content[0].text
            # Parse JSON from response
            json_match = re.search(r'\{[^{}]*\}', response_text, re.DOTALL)
            if json_match:
                llm_result = json.loads(json_match.group())
                resolution_details["llm_interpretation"] = llm_result
                search_query = llm_result.get("search_query", input_text)
                expected_language = llm_result.get("detected_language") or expected_language
                resolution_details["input_type"] = llm_result.get("input_type", "unknown")
            else:
                search_query = input_text

        except Exception as e:
            logging.error(f"LLM parsing failed: {e}")
            # Fall back to using input as search query
            search_query = input_text

    if not search_query:
        return ManualEditionAddResponse(
            success=False,
            message="Could not determine search query from input",
            resolution_details=resolution_details,
        )

    # Search Google Scholar for the edition
    from .services.scholar_service import get_scholar_service
    scholar_service = get_scholar_service()

    try:
        search_result = await scholar_service.search(
            query=search_query,
            max_results=10,
        )

        papers_found = search_result.get("papers", [])
        resolution_details["search_query_used"] = search_query
        resolution_details["results_found"] = len(papers_found)

        if not papers_found:
            return ManualEditionAddResponse(
                success=False,
                message=f"No results found for query: {search_query}",
                resolution_details=resolution_details,
            )

        # Find the best match - prioritize by author match and citation count
        best_match = None
        for p in papers_found:
            scholar_id = p.get("scholarId") or p.get("id")
            title = p.get("title", "")

            # Skip if already exists
            if scholar_id and scholar_id in existing_scholar_ids:
                continue
            if title.lower() in existing_titles:
                continue

            # First non-duplicate is best match (results are ranked by Scholar)
            if best_match is None:
                best_match = p
                break

        if not best_match:
            return ManualEditionAddResponse(
                success=False,
                message="All matching results already exist as editions",
                resolution_details=resolution_details,
            )

        # Create the edition
        new_edition = Edition(
            paper_id=request.paper_id,
            scholar_id=best_match.get("scholarId") or best_match.get("id"),
            title=best_match.get("title", "Unknown"),
            authors=best_match.get("authorsRaw") or ", ".join(best_match.get("authors", [])),
            year=best_match.get("year"),
            venue=best_match.get("venue"),
            abstract=best_match.get("abstract"),
            link=best_match.get("link"),
            citation_count=best_match.get("citationCount") or best_match.get("citations") or 0,
            language=expected_language,
            confidence="high",  # Manual additions are high confidence
            auto_selected=False,
            selected=True,  # Auto-select manual additions
            is_supplementary=True,  # Mark as supplementary (manually added)
        )
        db.add(new_edition)
        await db.commit()
        await db.refresh(new_edition)

        resolution_details["matched_title"] = best_match.get("title")
        resolution_details["matched_scholar_id"] = best_match.get("scholarId") or best_match.get("id")

        return ManualEditionAddResponse(
            success=True,
            edition=build_edition_response_with_staleness(new_edition),
            message=f"Added edition: {new_edition.title}",
            resolution_details=resolution_details,
        )

    except Exception as e:
        logging.error(f"Scholar search failed: {e}")
        return ManualEditionAddResponse(
            success=False,
            message=f"Search failed: {str(e)}",
            resolution_details=resolution_details,
        )


# ============== Citation Extraction Endpoints ==============

@app.post("/api/citations/extract", response_model=CitationExtractionResponse)
async def extract_citations(
    request: CitationExtractionRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db)
):
    """Extract citations for a paper (from selected editions)"""
    from .services.job_worker import create_extract_citations_job

    result = await db.execute(select(Paper).where(Paper.id == request.paper_id))
    paper = result.scalar_one_or_none()
    if not paper:
        raise HTTPException(status_code=404, detail="Paper not found")

    # Get editions to process
    if request.edition_ids:
        editions_query = select(Edition).where(Edition.id.in_(request.edition_ids))
    else:
        editions_query = select(Edition).where(
            Edition.paper_id == request.paper_id,
            Edition.selected == True
        )
    editions_result = await db.execute(editions_query)
    editions = list(editions_result.scalars().all())

    if not editions:
        raise HTTPException(status_code=400, detail="No editions selected for extraction")

    # Check for existing pending/running job for the SAME editions
    # Allow parallel jobs for different editions of the same paper
    requested_edition_ids = set(request.edition_ids) if request.edition_ids else set(e.id for e in editions)

    existing_jobs = await db.execute(
        select(Job).where(
            Job.paper_id == request.paper_id,
            Job.job_type == "extract_citations",
            Job.status.in_(["pending", "running"])
        )
    )
    for existing_job in existing_jobs.scalars().all():
        # Check if this job is for the same editions
        job_params = json.loads(existing_job.params) if existing_job.params else {}
        job_edition_ids = set(job_params.get("edition_ids", []))

        # If job has no specific editions or there's overlap, return existing job
        if not job_edition_ids or job_edition_ids & requested_edition_ids:
            return CitationExtractionResponse(
                job_id=existing_job.id,
                paper_id=paper.id,
                editions_to_process=len(editions),
                estimated_time_minutes=0,
            )

    # Create extraction job with proper params
    job = await create_extract_citations_job(
        db=db,
        paper_id=paper.id,
        edition_ids=request.edition_ids or [],
        max_citations_per_edition=min(request.max_citations_threshold, 1000),
        skip_threshold=request.max_citations_threshold,
    )
    await db.commit()

    # Estimate time
    total_citations = sum(e.citation_count for e in editions if e.citation_count <= request.max_citations_threshold)
    estimated_minutes = max(1, total_citations // 100)

    return CitationExtractionResponse(
        job_id=job.id,
        paper_id=paper.id,
        editions_to_process=len(editions),
        estimated_time_minutes=estimated_minutes,
    )


@app.get("/api/papers/{paper_id}/citations", response_model=List[CitationResponse])
async def get_paper_citations(
    paper_id: int,
    skip: int = 0,
    limit: Optional[int] = None,  # No limit by default - return ALL citations
    language: Optional[str] = None,
    edition_id: Optional[int] = None,
    db: AsyncSession = Depends(get_db)
):
    """Get citations for a paper with optional language/edition filter"""
    # Build query with edition join for language and title
    query = (
        select(
            Citation,
            Edition.language.label('edition_language'),
            Edition.title.label('edition_title')
        )
        .outerjoin(Edition, Citation.edition_id == Edition.id)
        .where(Citation.paper_id == paper_id)
    )

    # Apply filters
    if language:
        query = query.where(Edition.language.ilike(f"%{language}%"))
    if edition_id:
        query = query.where(Citation.edition_id == edition_id)

    query = query.order_by(Citation.citation_count.desc()).offset(skip)
    if limit:
        query = query.limit(limit)

    result = await db.execute(query)
    rows = result.all()

    # Build response with edition info
    citations = []
    for citation, edition_lang, edition_title in rows:
        citation_dict = {k: v for k, v in citation.__dict__.items() if not k.startswith('_')}
        citation_dict['edition_language'] = edition_lang
        citation_dict['edition_title'] = edition_title
        citations.append(CitationResponse(**citation_dict))

    return citations


# TODO: Enable after adding 'reviewed' column to production DB
# @app.post("/api/citations/mark-reviewed")
# async def mark_citations_reviewed(
#     request: CitationMarkReviewedRequest,
#     db: AsyncSession = Depends(get_db)
# ):
#     """Bulk mark citations as reviewed/unseen"""
#     if not request.citation_ids:
#         return {"updated": 0}
#
#     stmt = (
#         update(Citation)
#         .where(Citation.id.in_(request.citation_ids))
#         .values(reviewed=request.reviewed)
#     )
#     result = await db.execute(stmt)
#     await db.commit()
#
#     return {"updated": result.rowcount, "reviewed": request.reviewed}


@app.get("/api/papers/{paper_id}/cross-citations", response_model=CrossCitationResult)
async def get_cross_citations(
    paper_id: int,
    min_intersection: int = 2,
    db: AsyncSession = Depends(get_db)
):
    """Get cross-citation analysis results"""
    result = await db.execute(
        select(Citation)
        .where(Citation.paper_id == paper_id, Citation.intersection_count >= min_intersection)
        .order_by(Citation.intersection_count.desc())
    )
    citations = result.scalars().all()

    # Group by intersection count
    by_count = {}
    for c in citations:
        count = c.intersection_count
        by_count[count] = by_count.get(count, 0) + 1

    # Total unique
    total_result = await db.execute(
        select(func.count(Citation.id)).where(Citation.paper_id == paper_id)
    )

    return CrossCitationResult(
        paper_id=paper_id,
        total_unique_citations=total_result.scalar() or 0,
        intersections=[CitationResponse(**{k: v for k, v in c.__dict__.items() if not k.startswith('_')}) for c in citations],
        by_intersection_count=by_count,
    )


# ============== Job Endpoints ==============

@app.get("/api/jobs", response_model=List[JobResponse])
async def list_jobs(
    status: str = None,
    job_type: str = None,
    paper_id: int = None,
    limit: int = 50,
    db: AsyncSession = Depends(get_db)
):
    """List jobs with parsed params.

    IMPORTANT: Always returns ALL active (running/pending) jobs first,
    regardless of when they were created. This ensures the UI can display
    all jobs being processed. Recent completed/failed jobs fill the remaining limit.
    """
    from sqlalchemy import or_

    # If filtering by specific status, use the original simple query
    if status:
        query = select(Job).where(Job.status == status).order_by(Job.created_at.desc()).limit(limit)
        if job_type:
            query = query.where(Job.job_type == job_type)
        if paper_id:
            query = query.where(Job.paper_id == paper_id)
        result = await db.execute(query)
        jobs = list(result.scalars().all())
    else:
        # First: Get ALL active jobs (running/pending) - no limit!
        active_query = select(Job).where(
            Job.status.in_(["running", "pending"])
        ).order_by(Job.created_at.desc())
        if job_type:
            active_query = active_query.where(Job.job_type == job_type)
        if paper_id:
            active_query = active_query.where(Job.paper_id == paper_id)
        active_result = await db.execute(active_query)
        active_jobs = list(active_result.scalars().all())

        # Second: Get recent non-active jobs to fill remaining limit
        remaining_limit = max(0, limit - len(active_jobs))
        if remaining_limit > 0:
            inactive_query = select(Job).where(
                ~Job.status.in_(["running", "pending"])
            ).order_by(Job.created_at.desc()).limit(remaining_limit)
            if job_type:
                inactive_query = inactive_query.where(Job.job_type == job_type)
            if paper_id:
                inactive_query = inactive_query.where(Job.paper_id == paper_id)
            inactive_result = await db.execute(inactive_query)
            inactive_jobs = list(inactive_result.scalars().all())
        else:
            inactive_jobs = []

        # Combine: active jobs first, then recent inactive
        jobs = active_jobs + inactive_jobs

    # Parse params for each job
    response = []
    for job in jobs:
        job_dict = {k: v for k, v in job.__dict__.items() if not k.startswith('_')}
        # Parse params JSON
        if job.params:
            try:
                job_dict['params'] = json.loads(job.params)
            except:
                job_dict['params'] = None
        response.append(JobResponse(**job_dict))
    return response


@app.get("/api/jobs/{job_id}", response_model=JobDetail)
async def get_job(job_id: int, db: AsyncSession = Depends(get_db)):
    """Get job details"""
    result = await db.execute(select(Job).where(Job.id == job_id))
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # Build response dict, excluding internal fields
    job_dict = {k: v for k, v in job.__dict__.items() if not k.startswith('_') and k not in ('result', 'params')}
    # Parse JSON fields
    job_dict['result'] = json.loads(job.result) if job.result else None
    job_dict['params'] = json.loads(job.params) if job.params else None

    return JobDetail(**job_dict)


@app.post("/api/jobs/{job_id}/cancel")
async def cancel_job(job_id: int, db: AsyncSession = Depends(get_db)):
    """Cancel a pending or running job"""
    result = await db.execute(select(Job).where(Job.id == job_id))
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status not in ["pending", "running"]:
        raise HTTPException(status_code=400, detail=f"Cannot cancel job with status: {job.status}")

    job.status = "cancelled"
    job.error = "Cancelled by user"
    job.completed_at = datetime.utcnow()
    await db.commit()
    logger.info(f"Job {job_id} cancelled by user")
    return {"cancelled": True, "job_id": job_id}


@app.post("/api/jobs/{job_id}/fail")
async def force_fail_job(job_id: int, reason: str = "Manually marked as failed", db: AsyncSession = Depends(get_db)):
    """Force a stuck job to failed status"""
    result = await db.execute(select(Job).where(Job.id == job_id))
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    old_status = job.status
    job.status = "failed"
    job.error = reason
    job.completed_at = datetime.utcnow()
    await db.commit()
    logger.info(f"Job {job_id} force-failed: {reason} (was: {old_status})")
    return {"failed": True, "job_id": job_id, "previous_status": old_status, "reason": reason}


# ============== Debug Endpoints ==============

@app.get("/api/debug/raw-results/{paper_id}")
async def get_raw_search_results(
    paper_id: int,
    limit: int = 10,
    db: AsyncSession = Depends(get_db)
):
    """Get raw search results for a paper (for debugging LLM classification)"""
    query = (
        select(RawSearchResult)
        .where(RawSearchResult.paper_id == paper_id)
        .order_by(RawSearchResult.created_at.desc())
        .limit(limit)
    )
    result = await db.execute(query)
    records = result.scalars().all()

    return [
        {
            "id": r.id,
            "job_id": r.job_id,
            "search_type": r.search_type,
            "target_language": r.target_language,
            "query": r.query,
            "result_count": r.result_count,
            "raw_results": json.loads(r.raw_results) if r.raw_results else [],
            "llm_classification": json.loads(r.llm_classification) if r.llm_classification else {},
            "created_at": r.created_at.isoformat(),
        }
        for r in records
    ]


# ============== Language Endpoints ==============

AVAILABLE_LANGUAGES = [
    {"code": "english", "name": "English", "icon": "🇬🇧"},
    {"code": "german", "name": "German", "icon": "🇩🇪"},
    {"code": "french", "name": "French", "icon": "🇫🇷"},
    {"code": "spanish", "name": "Spanish", "icon": "🇪🇸"},
    {"code": "portuguese", "name": "Portuguese", "icon": "🇧🇷"},
    {"code": "italian", "name": "Italian", "icon": "🇮🇹"},
    {"code": "russian", "name": "Russian", "icon": "🇷🇺"},
    {"code": "chinese", "name": "Chinese", "icon": "🇨🇳"},
    {"code": "japanese", "name": "Japanese", "icon": "🇯🇵"},
    {"code": "korean", "name": "Korean", "icon": "🇰🇷"},
    {"code": "arabic", "name": "Arabic", "icon": "🇸🇦"},
    {"code": "dutch", "name": "Dutch", "icon": "🇳🇱"},
    {"code": "polish", "name": "Polish", "icon": "🇵🇱"},
    {"code": "turkish", "name": "Turkish", "icon": "🇹🇷"},
    {"code": "persian", "name": "Persian/Farsi", "icon": "🇮🇷"},
    {"code": "hindi", "name": "Hindi", "icon": "🇮🇳"},
    {"code": "hebrew", "name": "Hebrew", "icon": "🇮🇱"},
    {"code": "greek", "name": "Greek", "icon": "🇬🇷"},
    {"code": "swedish", "name": "Swedish", "icon": "🇸🇪"},
    {"code": "danish", "name": "Danish", "icon": "🇩🇰"},
    {"code": "norwegian", "name": "Norwegian", "icon": "🇳🇴"},
    {"code": "finnish", "name": "Finnish", "icon": "🇫🇮"},
    {"code": "czech", "name": "Czech", "icon": "🇨🇿"},
    {"code": "hungarian", "name": "Hungarian", "icon": "🇭🇺"},
    {"code": "romanian", "name": "Romanian", "icon": "🇷🇴"},
    {"code": "ukrainian", "name": "Ukrainian", "icon": "🇺🇦"},
    {"code": "vietnamese", "name": "Vietnamese", "icon": "🇻🇳"},
    {"code": "thai", "name": "Thai", "icon": "🇹🇭"},
    {"code": "indonesian", "name": "Indonesian", "icon": "🇮🇩"},
]


@app.get("/api/languages", response_model=AvailableLanguagesResponse)
async def get_available_languages():
    """Get list of available languages for edition discovery"""
    return AvailableLanguagesResponse(languages=AVAILABLE_LANGUAGES)


@app.post("/api/languages/recommend", response_model=LanguageRecommendationResponse)
async def recommend_languages(request: LanguageRecommendationRequest):
    """Get LLM recommendation for languages to search"""
    from .services.edition_discovery import EditionDiscoveryService

    result = await EditionDiscoveryService.recommend_languages({
        "title": request.title,
        "author": request.author,
        "year": request.year,
    })

    return LanguageRecommendationResponse(
        recommended=result.get("recommended", ["english", "german", "french", "spanish"]),
        reasoning=result.get("reasoning", "Default recommendation"),
        author_language=result.get("authorLanguage"),
        primary_markets=result.get("primaryMarkets", ["english"]),
    )


# ============== Paper Resolution Endpoints ==============

@app.post("/api/papers/{paper_id}/resolve")
async def resolve_paper(paper_id: int, db: AsyncSession = Depends(get_db)):
    """Manually trigger paper resolution against Google Scholar"""
    from .services.paper_resolution import PaperResolutionService

    result = await db.execute(select(Paper).where(Paper.id == paper_id))
    paper = result.scalar_one_or_none()
    if not paper:
        raise HTTPException(status_code=404, detail="Paper not found")

    if paper.status == "resolved":
        return {
            "success": True,
            "message": "Paper already resolved",
            "paper_id": paper_id,
            "scholar_id": paper.scholar_id,
            "citation_count": paper.citation_count,
        }

    # Create resolution job
    job = Job(
        paper_id=paper.id,
        job_type="resolve",
        status="pending",
    )
    db.add(job)
    await db.flush()
    await db.refresh(job)

    service = PaperResolutionService(db)

    try:
        resolution_result = await service.resolve_paper(paper_id=paper.id, job_id=job.id)
        await db.commit()
        return resolution_result

    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Resolution failed: {str(e)}")


class BatchResolveRequest(BaseModel):
    paper_ids: List[int] = []  # Empty = all pending papers


class BatchResolveResponse(BaseModel):
    jobs_created: int
    paper_ids: List[int]
    message: str


@app.post("/api/papers/batch-resolve", response_model=BatchResolveResponse)
async def batch_resolve_papers(request: BatchResolveRequest = None, db: AsyncSession = Depends(get_db)):
    """Queue resolution jobs for multiple papers in parallel

    If paper_ids is empty, resolves all papers with status='pending'
    """
    if request is None:
        request = BatchResolveRequest()

    # Get papers to resolve
    if request.paper_ids:
        # Specific papers
        result = await db.execute(
            select(Paper).where(
                Paper.id.in_(request.paper_ids),
                Paper.status.in_(["pending", "error"])  # Only pending or error status
            )
        )
    else:
        # All pending papers
        result = await db.execute(
            select(Paper).where(Paper.status == "pending")
        )

    papers = list(result.scalars().all())

    if not papers:
        return BatchResolveResponse(
            jobs_created=0,
            paper_ids=[],
            message="No papers to resolve"
        )

    # Create resolution jobs for each paper
    paper_ids = []
    for paper in papers:
        # Check if there's already a pending/running resolve job for this paper
        existing_job = await db.execute(
            select(Job).where(
                Job.paper_id == paper.id,
                Job.job_type == "resolve",
                Job.status.in_(["pending", "running"])
            )
        )
        if existing_job.scalar_one_or_none():
            continue  # Skip, already has a job

        job = Job(
            paper_id=paper.id,
            job_type="resolve",
            status="pending",
            progress_message="Queued for resolution",
        )
        db.add(job)
        paper_ids.append(paper.id)

    await db.commit()

    return BatchResolveResponse(
        jobs_created=len(paper_ids),
        paper_ids=paper_ids,
        message=f"Queued {len(paper_ids)} papers for resolution"
    )


# ============== Batch Operations Endpoints ==============

@app.post("/api/papers/batch-assign-collection")
async def batch_assign_to_collection(
    request: BatchCollectionAssignment,
    db: AsyncSession = Depends(get_db)
):
    """Assign multiple papers to a collection/dossier at once"""
    from sqlalchemy import delete

    paper_ids = request.paper_ids
    if not paper_ids:
        raise HTTPException(status_code=400, detail="No paper IDs provided")

    # Verify papers exist
    result = await db.execute(select(Paper).where(Paper.id.in_(paper_ids)))
    papers = list(result.scalars().all())
    if not papers:
        raise HTTPException(status_code=404, detail="No papers found")

    # Determine collection_id - create new dossier if requested
    dossier_id = request.dossier_id
    collection_id = request.collection_id

    if request.create_new_dossier and request.new_dossier_name:
        if not collection_id:
            raise HTTPException(status_code=400, detail="collection_id required when creating new dossier")
        # Create new dossier
        new_dossier = Dossier(
            collection_id=collection_id,
            name=request.new_dossier_name,
        )
        db.add(new_dossier)
        await db.flush()
        dossier_id = new_dossier.id

    # Update papers
    updated_count = 0
    for paper in papers:
        if collection_id:
            paper.collection_id = collection_id
        if dossier_id:
            paper.dossier_id = dossier_id
        updated_count += 1

    await db.commit()

    return {
        "updated": updated_count,
        "paper_ids": [p.id for p in papers],
        "collection_id": collection_id,
        "dossier_id": dossier_id,
        "message": f"Assigned {updated_count} papers to collection/dossier"
    }


@app.post("/api/papers/batch-foreign-edition", response_model=BatchForeignEditionResponse)
async def batch_mark_foreign_edition_needed(
    request: BatchForeignEditionRequest,
    db: AsyncSession = Depends(get_db)
):
    """Mark multiple papers as needing foreign edition lookup"""
    paper_ids = request.paper_ids
    if not paper_ids:
        raise HTTPException(status_code=400, detail="No paper IDs provided")

    # Update papers
    result = await db.execute(
        update(Paper)
        .where(Paper.id.in_(paper_ids))
        .values(foreign_edition_needed=request.foreign_edition_needed)
    )

    await db.commit()

    return BatchForeignEditionResponse(
        updated=result.rowcount,
        paper_ids=paper_ids
    )


@app.post("/api/papers/{paper_id}/foreign-edition-needed")
async def toggle_foreign_edition_needed(
    paper_id: int,
    needed: bool = True,
    db: AsyncSession = Depends(get_db)
):
    """Toggle the foreign_edition_needed flag for a single paper"""
    result = await db.execute(select(Paper).where(Paper.id == paper_id))
    paper = result.scalar_one_or_none()
    if not paper:
        raise HTTPException(status_code=404, detail="Paper not found")

    paper.foreign_edition_needed = needed
    await db.commit()

    return {
        "paper_id": paper_id,
        "foreign_edition_needed": needed,
        "title": paper.title
    }


@app.get("/api/papers/foreign-edition-needed", response_model=PapersPaginatedResponse)
async def list_papers_needing_foreign_edition(
    page: int = 1,
    per_page: int = 25,
    db: AsyncSession = Depends(get_db)
):
    """List all papers marked as needing foreign edition lookup"""
    # Get total count
    count_query = select(func.count(Paper.id)).where(
        Paper.foreign_edition_needed == True,
        Paper.deleted_at.is_(None)
    )
    total_result = await db.execute(count_query)
    total = total_result.scalar() or 0

    # Calculate pagination
    total_pages = (total + per_page - 1) // per_page if per_page > 0 else 1
    skip = (page - 1) * per_page

    # Get papers
    query = (
        select(Paper)
        .where(Paper.foreign_edition_needed == True, Paper.deleted_at.is_(None))
        .offset(skip)
        .limit(per_page)
        .order_by(Paper.created_at.desc())
    )
    result = await db.execute(query)
    papers = result.scalars().all()

    paper_responses = [PaperResponse(**paper_to_response(p)) for p in papers]

    return PapersPaginatedResponse(
        papers=paper_responses,
        total=total,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
        has_next=page < total_pages,
        has_prev=page > 1
    )


class LinkAsEditionRequest(BaseModel):
    """Request to link one paper as an edition of another"""
    source_paper_id: int  # Paper to convert to an edition
    target_paper_id: int  # Paper to link it to (as an edition of)
    delete_source: bool = True  # Whether to delete source paper after linking


class LinkAsEditionResponse(BaseModel):
    """Response from linking papers"""
    edition_id: int
    target_paper_id: int
    source_deleted: bool
    message: str


@app.post("/api/papers/link-as-edition", response_model=LinkAsEditionResponse)
async def link_paper_as_edition(
    request: LinkAsEditionRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Convert one paper into an edition of another.

    Useful for linking foreign translations to their canonical editions.
    Creates an Edition record from the source paper's data and links it to the target paper.
    """
    # Get source paper
    source_result = await db.execute(
        select(Paper).where(Paper.id == request.source_paper_id, Paper.deleted_at.is_(None))
    )
    source_paper = source_result.scalar_one_or_none()
    if not source_paper:
        raise HTTPException(status_code=404, detail=f"Source paper {request.source_paper_id} not found")

    # Get target paper
    target_result = await db.execute(
        select(Paper).where(Paper.id == request.target_paper_id, Paper.deleted_at.is_(None))
    )
    target_paper = target_result.scalar_one_or_none()
    if not target_paper:
        raise HTTPException(status_code=404, detail=f"Target paper {request.target_paper_id} not found")

    if source_paper.id == target_paper.id:
        raise HTTPException(status_code=400, detail="Cannot link a paper to itself")

    # Create Edition from source paper data
    edition = Edition(
        paper_id=target_paper.id,
        scholar_id=source_paper.scholar_id,
        cluster_id=None,
        title=source_paper.title,
        authors=source_paper.authors if isinstance(source_paper.authors, str) else (
            ", ".join(source_paper.authors) if source_paper.authors else None
        ),
        year=source_paper.year,
        venue=source_paper.venue,
        abstract=source_paper.abstract,
        link=source_paper.link,
        citation_count=source_paper.citation_count or 0,
        language=None,  # Could try to detect from title
        confidence="high",  # User-confirmed
        auto_selected=False,
        selected=True,  # Mark as selected for harvesting
        excluded=False,
    )
    db.add(edition)
    await db.flush()

    edition_id = edition.id
    source_deleted = False
    citations_moved = 0
    citations_deleted_duplicates = 0

    # Optionally delete source paper
    if request.delete_source:
        # IMPORTANT: Move citations from source paper to target paper BEFORE deleting
        # Otherwise they become orphaned (linked to a deleted paper)

        # First, find which citations would be duplicates (same scholar_id already on target)
        duplicate_check = await db.execute(
            text("""
                SELECT c_source.id
                FROM citations c_source
                WHERE c_source.paper_id = :source_paper_id
                AND c_source.scholar_id IN (
                    SELECT scholar_id FROM citations WHERE paper_id = :target_paper_id
                )
            """),
            {"source_paper_id": source_paper.id, "target_paper_id": target_paper.id}
        )
        duplicate_ids = [row[0] for row in duplicate_check.fetchall()]

        # Delete duplicates to avoid unique constraint violation
        if duplicate_ids:
            await db.execute(
                text("DELETE FROM citations WHERE id = ANY(:ids)"),
                {"ids": duplicate_ids}
            )
            citations_deleted_duplicates = len(duplicate_ids)

        # Move remaining citations from source to target paper with new edition_id
        move_result = await db.execute(
            text("""
                UPDATE citations
                SET paper_id = :target_paper_id, edition_id = :edition_id
                WHERE paper_id = :source_paper_id
            """),
            {
                "target_paper_id": target_paper.id,
                "edition_id": edition_id,
                "source_paper_id": source_paper.id
            }
        )
        citations_moved = move_result.rowcount

        # Update the new edition's harvested_citation_count and citation_count
        if citations_moved > 0:
            edition.harvested_citation_count = citations_moved
            # If citation_count is 0, use migrated citations as baseline estimate
            if edition.citation_count == 0:
                edition.citation_count = citations_moved + citations_deleted_duplicates

        # Now safe to soft-delete the source paper
        source_paper.deleted_at = datetime.utcnow()
        source_deleted = True

    await db.commit()

    message = f"Linked '{source_paper.title[:50]}...' as edition of '{target_paper.title[:50]}...'"
    if citations_moved > 0:
        message += f" ({citations_moved} citations migrated"
        if citations_deleted_duplicates > 0:
            message += f", {citations_deleted_duplicates} duplicates removed"
        message += ")"

    return LinkAsEditionResponse(
        edition_id=edition_id,
        target_paper_id=target_paper.id,
        source_deleted=source_deleted,
        message=message
    )


class CandidateConfirmRequest(BaseModel):
    candidate_index: int


@app.post("/api/papers/{paper_id}/confirm-candidate")
async def confirm_candidate(paper_id: int, request: CandidateConfirmRequest, db: AsyncSession = Depends(get_db)):
    """Confirm a candidate selection during reconciliation"""
    from .services.paper_resolution import PaperResolutionService

    service = PaperResolutionService(db)

    try:
        result = await service.confirm_candidate(paper_id=paper_id, candidate_index=request.candidate_index)
        await db.commit()
        return result

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Confirmation failed: {str(e)}")


class QuickHarvestResponse(BaseModel):
    """Response from quick harvest endpoint"""
    job_id: int
    paper_id: int
    edition_id: int
    edition_created: bool  # True if new edition was created
    estimated_citations: int
    message: str


@app.post("/api/papers/{paper_id}/quick-harvest", response_model=QuickHarvestResponse)
async def quick_harvest_paper(
    paper_id: int,
    db: AsyncSession = Depends(get_db)
):
    """
    Quick harvest citations directly from a resolved paper, skipping edition discovery.

    This creates an edition from the paper's Scholar data (if one doesn't exist) and
    immediately queues a citation extraction job. Useful when you just want to harvest
    citations from the main English edition without discovering translations.
    """
    from .services.job_worker import create_extract_citations_job

    try:
        # Get the paper
        result = await db.execute(select(Paper).where(Paper.id == paper_id))
        paper = result.scalar_one_or_none()
        if not paper:
            raise HTTPException(status_code=404, detail="Paper not found")

        if paper.status != "resolved":
            raise HTTPException(status_code=400, detail="Paper must be resolved first")

        if not paper.scholar_id:
            raise HTTPException(status_code=400, detail="Paper has no Scholar ID - cannot harvest")

        # Check if an edition already exists for this paper's scholar_id
        edition_result = await db.execute(
            select(Edition).where(
                Edition.paper_id == paper_id,
                Edition.scholar_id == paper.scholar_id
            )
        )
        existing_edition = edition_result.scalar_one_or_none()
        edition_created = False

        if existing_edition:
            edition = existing_edition
            # Make sure it's selected
            if not edition.selected:
                edition.selected = True
        else:
            # Create a new edition from the paper data
            edition = Edition(
                paper_id=paper_id,
                scholar_id=paper.scholar_id,
                cluster_id=paper.cluster_id,
                title=paper.title,
                authors=paper.authors,
                year=paper.year,
                venue=paper.venue,
                abstract=paper.abstract,
                link=paper.link,
                citation_count=paper.citation_count,
                language=paper.language or "English",
                confidence="high",
                auto_selected=True,
                selected=True,
                is_supplementary=False,
                found_by_query="Quick harvest from resolved paper",
            )
            db.add(edition)
            edition_created = True

        await db.commit()
        await db.refresh(edition)

        # Check for existing pending/running job for this edition
        existing_job_result = await db.execute(
            select(Job).where(
                Job.paper_id == paper_id,
                Job.job_type == "extract_citations",
                Job.status.in_(["pending", "running"])
            )
        )
        existing_job = existing_job_result.scalar_one_or_none()

        estimated = edition.citation_count or 0

        if existing_job:
            return QuickHarvestResponse(
                job_id=existing_job.id,
                paper_id=paper_id,
                edition_id=edition.id,
                edition_created=edition_created,
                estimated_citations=estimated,
                message=f"Citation extraction already in progress (job {existing_job.id})"
            )

        # Queue citation extraction job
        job_id = await create_extract_citations_job(
            db=db,
            paper_id=paper_id,
            edition_ids=[edition.id],
            max_citations_per_edition=1000,
            skip_threshold=50000,
        )

        return QuickHarvestResponse(
            job_id=job_id,
            paper_id=paper_id,
            edition_id=edition.id,
            edition_created=edition_created,
            estimated_citations=estimated,
            message=f"Queued citation extraction for {estimated:,} citations"
        )

    except HTTPException:
        raise  # Re-raise HTTP exceptions as-is
    except Exception as e:
        # Log the error and return a proper error response
        import traceback
        print(f"[quick-harvest] Error for paper {paper_id}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Quick harvest failed: {str(e)}")


class EditionHarvestResponse(BaseModel):
    """Response from edition harvest endpoint"""
    job_id: int
    paper_id: int
    edition_id: int
    estimated_citations: int
    message: str


@app.post("/api/editions/{edition_id}/harvest", response_model=EditionHarvestResponse)
async def harvest_edition(
    edition_id: int,
    db: AsyncSession = Depends(get_db)
):
    """
    Start or resume citation harvesting for a specific edition.

    This is the manual "resume harvest" button - it queues a citation extraction
    job for just this one edition. If a job is already running, returns that job.
    """
    from .services.job_worker import create_extract_citations_job

    # Get the edition
    result = await db.execute(select(Edition).where(Edition.id == edition_id))
    edition = result.scalar_one_or_none()
    if not edition:
        raise HTTPException(status_code=404, detail="Edition not found")

    if not edition.scholar_id:
        raise HTTPException(status_code=400, detail="Edition has no Scholar ID - cannot harvest")

    paper_id = edition.paper_id
    estimated = edition.citation_count or 0

    # Check for existing pending/running job for this edition's paper
    existing_job_result = await db.execute(
        select(Job).where(
            Job.paper_id == paper_id,
            Job.job_type == "extract_citations",
            Job.status.in_(["pending", "running"])
        )
    )
    existing_job = existing_job_result.scalar_one_or_none()

    if existing_job:
        return EditionHarvestResponse(
            job_id=existing_job.id,
            paper_id=paper_id,
            edition_id=edition_id,
            estimated_citations=estimated,
            message=f"Citation extraction already in progress (job {existing_job.id})"
        )

    # Queue citation extraction job for just this edition
    job_id = await create_extract_citations_job(
        db=db,
        paper_id=paper_id,
        edition_ids=[edition_id],
        max_citations_per_edition=1000,
        skip_threshold=50000,
    )

    return EditionHarvestResponse(
        job_id=job_id,
        paper_id=paper_id,
        edition_id=edition_id,
        estimated_citations=estimated,
        message=f"Queued citation extraction for edition ({estimated:,} citations)"
    )


@app.post("/api/jobs/process")
async def process_pending_jobs_endpoint(
    max_jobs: int = 5,
    db: AsyncSession = Depends(get_db)
):
    """Manually trigger processing of pending jobs"""
    from .services.paper_resolution import process_pending_jobs

    try:
        result = await process_pending_jobs(db, max_jobs=max_jobs)
        await db.commit()
        return result

    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Job processing failed: {str(e)}")


# ============== Stats Endpoint ==============

@app.get("/api/stats")
async def get_stats(db: AsyncSession = Depends(get_db)):
    """Get system statistics"""
    collections_count = await db.execute(select(func.count(Collection.id)))
    papers_count = await db.execute(select(func.count(Paper.id)))
    editions_count = await db.execute(select(func.count(Edition.id)))
    citations_count = await db.execute(select(func.count(Citation.id)))
    pending_jobs = await db.execute(select(func.count(Job.id)).where(Job.status == "pending"))
    running_jobs = await db.execute(select(func.count(Job.id)).where(Job.status == "running"))

    return {
        "collections": collections_count.scalar() or 0,
        "papers": papers_count.scalar() or 0,
        "editions": editions_count.scalar() or 0,
        "citations": citations_count.scalar() or 0,
        "jobs": {
            "pending": pending_jobs.scalar() or 0,
            "running": running_jobs.scalar() or 0,
        },
    }


# ============== Harvest Dashboard ==============

from datetime import timedelta

MAX_CONCURRENT_JOBS = 20  # Must match job_worker.py


@app.get("/api/dashboard/harvest-stats", response_model=HarvestDashboardResponse)
async def get_harvest_dashboard(db: AsyncSession = Depends(get_db)):
    """Get comprehensive harvesting dashboard data"""
    # Use naive UTC datetimes to match database TIMESTAMP WITHOUT TIME ZONE columns
    now = datetime.utcnow()
    one_hour_ago = now - timedelta(hours=1)
    six_hours_ago = now - timedelta(hours=6)
    twenty_four_hours_ago = now - timedelta(hours=24)

    # === System Health ===

    # Active jobs count
    active_jobs_result = await db.execute(
        select(func.count(Job.id)).where(Job.status.in_(["pending", "running"]))
    )
    active_jobs = active_jobs_result.scalar() or 0

    # Citations in last hour
    citations_last_hour_result = await db.execute(
        select(func.count(Citation.id)).where(Citation.created_at >= one_hour_ago)
    )
    citations_last_hour = citations_last_hour_result.scalar() or 0

    # Papers with active jobs
    papers_with_jobs_result = await db.execute(
        select(func.count(func.distinct(Job.paper_id))).where(
            Job.status.in_(["pending", "running"]),
            Job.paper_id.isnot(None)
        )
    )
    papers_with_active_jobs = papers_with_jobs_result.scalar() or 0

    # Job history counts for different time periods
    async def get_job_counts(since: datetime) -> dict:
        completed = await db.execute(
            select(func.count(Job.id)).where(
                Job.status == "completed",
                Job.completed_at >= since
            )
        )
        failed = await db.execute(
            select(func.count(Job.id)).where(
                Job.status == "failed",
                Job.completed_at >= since
            )
        )
        cancelled = await db.execute(
            select(func.count(Job.id)).where(
                Job.status == "cancelled",
                Job.completed_at >= since
            )
        )
        return {
            "completed": completed.scalar() or 0,
            "failed": failed.scalar() or 0,
            "cancelled": cancelled.scalar() or 0,
        }

    jobs_24h = await get_job_counts(twenty_four_hours_ago)
    jobs_6h = await get_job_counts(six_hours_ago)
    jobs_1h = await get_job_counts(one_hour_ago)

    # Calculate average duplicate rate from recent completed jobs
    # Look at jobs completed in last hour with results containing duplicates info
    recent_jobs_result = await db.execute(
        select(Job).where(
            Job.status == "completed",
            Job.completed_at >= one_hour_ago,
            Job.job_type == "extract_citations",
            Job.result.isnot(None)
        ).limit(20)
    )
    recent_jobs = recent_jobs_result.scalars().all()

    total_saved = 0
    total_duplicates = 0
    for job in recent_jobs:
        try:
            result = json.loads(job.result) if isinstance(job.result, str) else job.result
            if result:
                total_saved += result.get("citations_saved", 0)
                total_duplicates += result.get("duplicates_found", 0)
        except:
            pass

    avg_duplicate_rate = 0.0
    if total_saved + total_duplicates > 0:
        avg_duplicate_rate = total_duplicates / (total_saved + total_duplicates)

    system_health = SystemHealthStats(
        active_jobs=active_jobs,
        max_concurrent_jobs=MAX_CONCURRENT_JOBS,
        citations_last_hour=citations_last_hour,
        papers_with_active_jobs=papers_with_active_jobs,
        jobs_24h=JobHistorySummary(**jobs_24h),
        avg_duplicate_rate_1h=round(avg_duplicate_rate, 3),
    )

    # === Active Harvests (running jobs with details) ===
    active_harvests = []
    running_jobs_result = await db.execute(
        select(Job).where(
            Job.status == "running",
            Job.job_type == "extract_citations"
        ).order_by(Job.started_at.desc())
    )
    running_jobs_list = running_jobs_result.scalars().all()

    for job in running_jobs_list:
        # Get paper info
        paper_result = await db.execute(select(Paper).where(Paper.id == job.paper_id))
        paper = paper_result.scalar_one_or_none()
        if not paper:
            continue

        # Parse job params for progress details
        try:
            params = json.loads(job.params) if isinstance(job.params, str) else (job.params or {})
        except:
            params = {}

        progress_details = params.get("progress_details", {})

        # Get edition info for this paper
        editions_result = await db.execute(
            select(Edition).where(
                Edition.paper_id == paper.id,
                Edition.selected == True
            )
        )
        editions = editions_result.scalars().all()
        edition_count = len(editions)

        # Sum up expected and harvested across editions
        expected_total = sum(e.citation_count or 0 for e in editions)
        harvested_total = sum(e.harvested_citation_count or 0 for e in editions)
        stall_count = max((e.harvest_stall_count or 0) for e in editions) if editions else 0

        # Calculate citations saved this hour for this paper
        citations_hour_result = await db.execute(
            select(func.count(Citation.id)).where(
                Citation.paper_id == paper.id,
                Citation.created_at >= one_hour_ago
            )
        )
        citations_saved_hour = citations_hour_result.scalar() or 0

        # Parse result for saved/duplicates this job
        try:
            result = json.loads(job.result) if isinstance(job.result, str) else (job.result or {})
        except:
            result = {}

        citations_saved_job = result.get("citations_saved", progress_details.get("citations_saved", 0))
        duplicates_job = result.get("duplicates_found", 0)

        duplicate_rate = 0.0
        if citations_saved_job + duplicates_job > 0:
            duplicate_rate = duplicates_job / (citations_saved_job + duplicates_job)

        # Calculate running time (both now and started_at are naive UTC)
        running_minutes = 0
        if job.started_at:
            running_minutes = int((now - job.started_at).total_seconds() / 60)

        active_harvests.append(ActiveHarvestInfo(
            paper_id=paper.id,
            paper_title=paper.title[:80] if paper.title else f"Paper #{paper.id}",
            job_id=job.id,
            job_progress=job.progress or 0,
            current_year=progress_details.get("current_year"),
            current_page=progress_details.get("current_page"),
            citations_saved_job=citations_saved_job,
            citations_saved_hour=citations_saved_hour,
            duplicates_job=duplicates_job,
            duplicate_rate=round(duplicate_rate, 3),
            gap_remaining=max(0, expected_total - harvested_total),
            expected_total=expected_total,
            harvested_total=harvested_total,
            running_minutes=running_minutes,
            stall_count=stall_count,
            edition_count=edition_count,
        ))

    # === Recently Completed Papers (last 24h where gap < 5%) ===
    recently_completed = []

    # Find papers where the most recent extract_citations job completed in last 24h
    # and the paper is "mostly done" (gap < 5%)
    recent_completed_jobs = await db.execute(
        select(Job).where(
            Job.status == "completed",
            Job.job_type == "extract_citations",
            Job.completed_at >= twenty_four_hours_ago,
            Job.paper_id.isnot(None)
        ).order_by(Job.completed_at.desc()).limit(100)
    )
    completed_jobs = recent_completed_jobs.scalars().all()

    seen_papers = set()
    for job in completed_jobs:
        if job.paper_id in seen_papers:
            continue
        seen_papers.add(job.paper_id)

        # Get paper info
        paper_result = await db.execute(select(Paper).where(Paper.id == job.paper_id))
        paper = paper_result.scalar_one_or_none()
        if not paper:
            continue

        # Get edition totals
        editions_result = await db.execute(
            select(Edition).where(
                Edition.paper_id == paper.id,
                Edition.selected == True
            )
        )
        editions = editions_result.scalars().all()
        expected_total = sum(e.citation_count or 0 for e in editions)
        harvested_total = sum(e.harvested_citation_count or 0 for e in editions)

        if expected_total == 0:
            continue

        gap_percent = harvested_total / expected_total if expected_total > 0 else 0

        # Only include if >= 95% complete
        if gap_percent >= 0.95:
            recently_completed.append(RecentlyCompletedPaper(
                paper_id=paper.id,
                paper_title=paper.title[:80] if paper.title else f"Paper #{paper.id}",
                total_harvested=harvested_total,
                expected_total=expected_total,
                gap_percent=round(gap_percent, 3),
                completed_at=job.completed_at,
            ))

        if len(recently_completed) >= 20:
            break

    # === Alerts ===
    alerts = []

    # Alert: High duplicate rate papers (from active harvests)
    for harvest in active_harvests:
        if harvest.duplicate_rate > 0.6:
            alerts.append(DashboardAlert(
                type="high_duplicate_rate",
                paper_id=harvest.paper_id,
                paper_title=harvest.paper_title,
                job_id=harvest.job_id,
                value=harvest.duplicate_rate,
                message=f"{int(harvest.duplicate_rate * 100)}% duplicates - possible resume bug",
            ))

    # Alert: Stalled papers (stall_count > 2) - no limit, show all stalled
    # Only show papers with genuine gaps (> 5% remaining)
    # Skip editions already marked as harvest_complete
    stalled_editions_result = await db.execute(
        select(Edition).where(
            Edition.harvest_stall_count > 2,
            Edition.selected == True,
            # Handle case where harvest_complete column may not exist yet (migration pending)
            or_(
                Edition.harvest_complete.is_(None),
                Edition.harvest_complete == False
            )
        ).order_by(Edition.harvest_stall_count.desc())
    )
    stalled_editions = stalled_editions_result.scalars().all()
    for edition in stalled_editions:
        paper_result = await db.execute(select(Paper).where(Paper.id == edition.paper_id))
        paper = paper_result.scalar_one_or_none()
        if paper:
            # Get harvest stats
            harvested = paper.total_harvested_citations or 0
            expected = edition.citation_count or 0
            gap = max(0, expected - harvested)

            # Skip papers that are essentially complete (< 5% gap)
            gap_percent = (gap / expected * 100) if expected > 0 else 0
            if gap_percent < 5:
                # Paper is complete, reset stall count and mark as complete
                edition.harvest_stall_count = 0
                edition.harvest_complete = True
                edition.harvest_complete_reason = "exhausted"
                continue

            # Get year completion breakdown from HarvestTargets
            targets_result = await db.execute(
                select(HarvestTarget).where(HarvestTarget.edition_id == edition.id)
            )
            targets = targets_result.scalars().all()

            years_complete = 0
            years_incomplete = 0
            years_harvesting = 0
            has_overflow = False

            for target in targets:
                if target.year is not None:  # Skip the "all years" aggregate entry
                    if target.status == "complete":
                        years_complete += 1
                    elif target.status == "incomplete":
                        years_incomplete += 1
                    else:  # harvesting
                        years_harvesting += 1
                    if target.expected_count > 1000:
                        has_overflow = True

            years_total = years_complete + years_incomplete + years_harvesting

            # Determine diagnosis
            if years_total == 0:
                # No HarvestTargets - need to run harvest first
                diagnosis = "no_data"
            elif years_incomplete > 0 or years_harvesting > 0:
                # Some years haven't been fully scraped - our fault
                diagnosis = "needs_scraping"
            else:
                # All years complete but still have gap - GS's fault
                diagnosis = "gs_fault"

            alerts.append(DashboardAlert(
                type="stalled_paper",
                paper_id=paper.id,
                paper_title=paper.title[:60] if paper.title else f"Paper #{paper.id}",
                edition_id=edition.id,
                value=float(edition.harvest_stall_count),
                message=f"{edition.harvest_stall_count} consecutive zero-progress jobs",
                harvested_count=harvested,
                expected_count=expected,
                gap_remaining=gap,
                stall_count=edition.harvest_stall_count,
                years_complete=years_complete,
                years_incomplete=years_incomplete,
                years_harvesting=years_harvesting,
                years_total=years_total,
                has_overflow_years=has_overflow,
                diagnosis=diagnosis,
            ))

    # Alert: Long-running jobs (> 45 min)
    for harvest in active_harvests:
        if harvest.running_minutes > 45:
            alerts.append(DashboardAlert(
                type="long_running_job",
                paper_id=harvest.paper_id,
                paper_title=harvest.paper_title,
                job_id=harvest.job_id,
                value=float(harvest.running_minutes),
                message=f"Running for {harvest.running_minutes} minutes",
            ))

    # Alert: Repeated failures (3+ failed jobs for same paper in 24h)
    failed_counts_result = await db.execute(
        select(Job.paper_id, func.count(Job.id).label("fail_count")).where(
            Job.status == "failed",
            Job.completed_at >= twenty_four_hours_ago,
            Job.paper_id.isnot(None)
        ).group_by(Job.paper_id).having(func.count(Job.id) >= 3)
    )
    failed_papers = failed_counts_result.all()
    for paper_id, fail_count in failed_papers:
        paper_result = await db.execute(select(Paper).where(Paper.id == paper_id))
        paper = paper_result.scalar_one_or_none()
        if paper:
            alerts.append(DashboardAlert(
                type="repeated_failures",
                paper_id=paper.id,
                paper_title=paper.title[:60] if paper.title else f"Paper #{paper.id}",
                value=float(fail_count),
                message=f"{fail_count} failed jobs in last 24h",
            ))

    return HarvestDashboardResponse(
        system_health=system_health,
        active_harvests=active_harvests,
        recently_completed=recently_completed,
        alerts=alerts,
        job_history_summary={
            "last_hour": jobs_1h,
            "last_6h": jobs_6h,
            "last_24h": jobs_24h,
        },
    )


@app.get("/api/dashboard/job-history", response_model=JobHistoryResponse)
async def get_job_history(
    hours: int = 6,
    status: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    db: AsyncSession = Depends(get_db)
):
    """Get paginated job history with optional filters"""
    # Use naive UTC datetimes to match database TIMESTAMP WITHOUT TIME ZONE columns
    now = datetime.utcnow()
    since = now - timedelta(hours=hours)

    # Base query
    query = select(Job).where(
        Job.created_at >= since
    )

    if status:
        query = query.where(Job.status == status)

    # Get total count
    count_query = select(func.count()).select_from(query.subquery())
    total_result = await db.execute(count_query)
    total = total_result.scalar() or 0

    # Get paginated results
    query = query.order_by(Job.created_at.desc()).offset(offset).limit(limit)
    result = await db.execute(query)
    jobs = result.scalars().all()

    # Build response with paper titles
    items = []
    for job in jobs:
        # Get paper title
        paper_title = None
        if job.paper_id:
            paper_result = await db.execute(select(Paper.title).where(Paper.id == job.paper_id))
            paper_title = paper_result.scalar()

        # Parse result for citations/duplicates
        citations_saved = 0
        duplicates_found = 0
        try:
            if job.result:
                result_data = json.loads(job.result) if isinstance(job.result, str) else job.result
                citations_saved = result_data.get("citations_saved", 0)
                duplicates_found = result_data.get("duplicates_found", 0)
        except:
            pass

        # Calculate duration
        duration_seconds = None
        if job.started_at and job.completed_at:
            duration_seconds = int((job.completed_at - job.started_at).total_seconds())

        items.append(JobHistoryItem(
            id=job.id,
            paper_id=job.paper_id,
            paper_title=paper_title[:60] if paper_title else None,
            job_type=job.job_type,
            status=job.status,
            citations_saved=citations_saved,
            duplicates_found=duplicates_found,
            duration_seconds=duration_seconds,
            started_at=job.started_at,
            completed_at=job.completed_at,
            error=job.error[:200] if job.error else None,
        ))

    return JobHistoryResponse(
        jobs=items,
        total=total,
        has_more=offset + len(items) < total,
    )


class RestartStalledRequest(BaseModel):
    """Request to restart stalled papers"""
    edition_ids: List[int]


class RestartStalledResponse(BaseModel):
    """Response from restarting stalled papers"""
    restarted: int
    jobs_created: List[int]
    errors: List[str]


@app.post("/api/dashboard/restart-stalled", response_model=RestartStalledResponse)
async def restart_stalled_papers(
    request: RestartStalledRequest,
    db: AsyncSession = Depends(get_db)
):
    """Reset stall count and create new harvest jobs for stalled editions"""
    from .services.job_worker import create_extract_citations_job

    restarted = 0
    jobs_created = []
    errors = []

    for edition_id in request.edition_ids:
        try:
            # Get edition
            edition_result = await db.execute(
                select(Edition).where(Edition.id == edition_id)
            )
            edition = edition_result.scalar_one_or_none()

            if not edition:
                errors.append(f"Edition {edition_id} not found")
                continue

            # Reset stall count
            old_stall = edition.harvest_stall_count
            edition.harvest_stall_count = 0
            await db.flush()

            # Check for existing running job
            running_job_result = await db.execute(
                select(Job).where(
                    Job.paper_id == edition.paper_id,
                    Job.job_type == "extract_citations",
                    Job.status.in_(["pending", "running"])
                )
            )
            if running_job_result.scalar_one_or_none():
                errors.append(f"Paper {edition.paper_id} already has a running job")
                restarted += 1  # Still count as restarted since we reset stall count
                continue

            # Create new harvest job
            job_id = await create_extract_citations_job(
                db,
                edition.paper_id,
                force_full_refresh=False  # Resume from where we left off
            )
            if job_id:
                jobs_created.append(job_id)
                restarted += 1
                logger.info(f"Restarted edition {edition_id} (paper {edition.paper_id}), "
                           f"stall count {old_stall} -> 0, created job {job_id}")
            else:
                errors.append(f"Failed to create job for paper {edition.paper_id}")
                restarted += 1  # Still count since we reset stall count

        except Exception as e:
            errors.append(f"Error processing edition {edition_id}: {str(e)}")
            logger.error(f"Error restarting edition {edition_id}: {e}")

    await db.commit()

    return RestartStalledResponse(
        restarted=restarted,
        jobs_created=jobs_created,
        errors=errors
    )


@app.post("/api/dashboard/restart-all-stalled", response_model=RestartStalledResponse)
async def restart_all_stalled_papers(
    db: AsyncSession = Depends(get_db)
):
    """Reset stall count and create harvest jobs for ALL stalled editions"""
    # Get all stalled editions
    stalled_result = await db.execute(
        select(Edition.id).where(
            Edition.harvest_stall_count > 2,
            Edition.selected == True
        )
    )
    edition_ids = [row[0] for row in stalled_result.all()]

    # Reuse the batch restart endpoint
    request = RestartStalledRequest(edition_ids=edition_ids)
    return await restart_stalled_papers(request, db)


# ============== AI Diagnosis Actions ==============

class ExecuteAIActionRequest(BaseModel):
    action_type: str  # RESET, RESUME, PARTITION_REHARVEST, MARK_COMPLETE
    specific_params: dict = {}  # start_year, start_page, keep_completed_years, mode, continue_backwards_to

class ExecuteAIActionResponse(BaseModel):
    success: bool
    action_type: str
    edition_id: int
    message: str
    job_id: int = None
    resume_state: dict = None

@app.post("/api/editions/{edition_id}/execute-ai-action", response_model=ExecuteAIActionResponse)
async def execute_ai_action(
    edition_id: int,
    request: ExecuteAIActionRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Execute an action recommended by AI diagnosis.

    Supported action types:
    - RESET: Reset resume state to specific year/page and queue new job
    - RESUME: Just queue a new harvest job (simple restart)
    - PARTITION_REHARVEST: Reset overflow years and queue partition harvest
    - MARK_COMPLETE: Mark edition as complete (gap is GS fault)
    """
    from .services.job_worker import create_extract_citations_job

    logger.info(f"Executing AI action {request.action_type} on edition {edition_id}")
    logger.info(f"Params: {request.specific_params}")

    # Get edition
    result = await db.execute(select(Edition).where(Edition.id == edition_id))
    edition = result.scalar_one_or_none()
    if not edition:
        raise HTTPException(status_code=404, detail="Edition not found")

    # Get paper for title
    paper_result = await db.execute(select(Paper).where(Paper.id == edition.paper_id))
    paper = paper_result.scalar_one_or_none()

    action_type = request.action_type.upper()
    params = request.specific_params

    if action_type == "RESET":
        # Build new resume state
        start_year = params.get("start_year")
        start_page = params.get("start_page", 0)
        keep_completed_years = params.get("keep_completed_years", [])
        mode = params.get("mode", "year_by_year")
        continue_backwards_to = params.get("continue_backwards_to")

        # ALWAYS preserve completed years from harvest_targets - this is the authoritative source
        # Query harvest_targets for years with status='complete'
        from app.models import HarvestTarget
        completed_targets_result = await db.execute(
            select(HarvestTarget.year)
            .where(HarvestTarget.edition_id == edition_id)
            .where(HarvestTarget.status == 'complete')
        )
        completed_from_targets = {row.year for row in completed_targets_result.fetchall()}

        # Merge with any explicitly kept years (union)
        all_completed_years = completed_from_targets.union(set(keep_completed_years))

        new_resume_state = {
            "mode": mode,
            "current_year": start_year,
            "current_page": start_page,
            "completed_years": sorted(all_completed_years, reverse=True),
            "ai_reset": {
                "reset_at": datetime.utcnow().isoformat(),
                "start_year": start_year,
                "continue_backwards_to": continue_backwards_to,
                "preserved_from_harvest_targets": len(completed_from_targets)
            }
        }

        # Update edition
        edition.harvest_resume_state = json.dumps(new_resume_state)
        edition.harvest_stall_count = 0
        edition.harvest_complete = False
        edition.harvest_complete_reason = None
        await db.commit()

        # Queue new harvest job
        job = await create_extract_citations_job(
            db,
            edition.paper_id
        )

        return ExecuteAIActionResponse(
            success=True,
            action_type=action_type,
            edition_id=edition_id,
            message=f"Reset to year {start_year}, page {start_page}. Preserved {len(completed_from_targets)} completed years. Queued job #{job.id}.",
            job_id=job.id,
            resume_state=new_resume_state
        )

    elif action_type == "RESUME":
        # Simple restart - just reset stall count and queue job
        edition.harvest_stall_count = 0
        edition.harvest_complete = False
        edition.harvest_complete_reason = None
        await db.commit()

        job = await create_extract_citations_job(
            db,
            edition.paper_id
        )

        return ExecuteAIActionResponse(
            success=True,
            action_type=action_type,
            edition_id=edition_id,
            message=f"Resumed harvest. Queued job #{job.id}.",
            job_id=job.id
        )

    elif action_type == "PARTITION_REHARVEST":
        # Reset overflow years from completed_years
        overflow_years = params.get("overflow_years", [])

        resume_state = {}
        if edition.harvest_resume_state:
            try:
                resume_state = json.loads(edition.harvest_resume_state)
            except json.JSONDecodeError:
                resume_state = {}

        completed_years = set(resume_state.get("completed_years", []))
        completed_years -= set(overflow_years)

        resume_state["completed_years"] = sorted(list(completed_years), reverse=True)
        resume_state["mode"] = "year_by_year"
        resume_state["overflow_reharvest"] = {
            "years_reset": overflow_years,
            "reset_at": datetime.utcnow().isoformat()
        }

        edition.harvest_resume_state = json.dumps(resume_state)
        edition.harvest_stall_count = 0
        edition.harvest_complete = False
        edition.harvest_complete_reason = None
        await db.commit()

        job = await create_extract_citations_job(
            db,
            edition.paper_id
        )

        return ExecuteAIActionResponse(
            success=True,
            action_type=action_type,
            edition_id=edition_id,
            message=f"Reset {len(overflow_years)} overflow years for partition reharvest. Queued job #{job.id}.",
            job_id=job.id,
            resume_state=resume_state
        )

    elif action_type == "MARK_COMPLETE":
        # Mark as complete - gap is GS's fault
        reason = params.get("reason", "ai_diagnosis_gs_fault")
        edition.harvest_complete = True
        edition.harvest_complete_reason = reason
        edition.harvest_stall_count = 0
        await db.commit()

        return ExecuteAIActionResponse(
            success=True,
            action_type=action_type,
            edition_id=edition_id,
            message=f"Marked edition as complete (reason: {reason})."
        )

    else:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown action type: {action_type}. Supported: RESET, RESUME, PARTITION_REHARVEST, MARK_COMPLETE"
        )


# ============== Add Missing Year Targets ==============

class AddYearTargetsRequest(BaseModel):
    start_year: int
    end_year: int

class AddYearTargetsResponse(BaseModel):
    success: bool
    edition_id: int
    years_added: list[int]
    message: str

@app.post("/api/editions/{edition_id}/add-year-targets", response_model=AddYearTargetsResponse)
async def add_year_targets(edition_id: int, request: AddYearTargetsRequest, db: AsyncSession = Depends(get_db)):
    """Add harvest_targets for missing years in a range"""
    from app.models import HarvestTarget

    # Get edition
    result = await db.execute(select(Edition).where(Edition.id == edition_id))
    edition = result.scalar_one_or_none()
    if not edition:
        raise HTTPException(status_code=404, detail="Edition not found")

    # Get existing years
    existing = await db.execute(
        select(HarvestTarget.year).where(HarvestTarget.edition_id == edition_id)
    )
    existing_years = set(r[0] for r in existing.fetchall())

    # Add missing years
    years_added = []
    for year in range(request.start_year, request.end_year + 1):
        if year not in existing_years:
            target = HarvestTarget(
                edition_id=edition_id,
                year=year,
                expected_count=0,
                actual_count=0,
                status='pending'
            )
            db.add(target)
            years_added.append(year)

    await db.commit()

    return AddYearTargetsResponse(
        success=True,
        edition_id=edition_id,
        years_added=years_added,
        message=f"Added {len(years_added)} year targets ({request.start_year}-{request.end_year})"
    )


# ============== Sync Harvest Targets with Actual Citations ==============

class SyncHarvestTargetsResponse(BaseModel):
    success: bool
    edition_id: int
    years_synced: int
    years_marked_complete: int
    message: str

@app.post("/api/editions/{edition_id}/sync-harvest-targets", response_model=SyncHarvestTargetsResponse)
async def sync_harvest_targets(edition_id: int, db: AsyncSession = Depends(get_db)):
    """
    Sync harvest_targets with actual citation counts from the database.

    This fixes the issue where initial non-year-by-year harvests left actual_count at 0
    even though citations exist. Years with enough actual citations are marked 'complete'.
    """
    from app.models import HarvestTarget, Citation
    from sqlalchemy import func

    # Get edition
    result = await db.execute(select(Edition).where(Edition.id == edition_id))
    edition = result.scalar_one_or_none()
    if not edition:
        raise HTTPException(status_code=404, detail="Edition not found")

    # Get actual citation counts per year from the database
    citation_counts = await db.execute(
        select(Citation.year, func.count(Citation.id).label('count'))
        .where(Citation.edition_id == edition_id)
        .where(Citation.year.isnot(None))
        .group_by(Citation.year)
    )
    year_counts = {row.year: row.count for row in citation_counts.fetchall()}

    # Get all harvest_targets for this edition
    targets_result = await db.execute(
        select(HarvestTarget).where(HarvestTarget.edition_id == edition_id)
    )
    targets = targets_result.scalars().all()

    years_synced = 0
    years_marked_complete = 0

    for target in targets:
        actual_in_db = year_counts.get(target.year, 0)

        # Update actual_count if database has more
        if actual_in_db > target.actual_count:
            target.actual_count = actual_in_db
            years_synced += 1

        # Mark as complete if we have >= 80% of expected, or if actual >= expected
        # Also mark complete if expected is 0 (no citations expected)
        if target.status in ('incomplete', 'harvesting', 'pending'):
            expected = target.expected_count or 0
            if expected == 0 or actual_in_db >= expected or (expected > 0 and actual_in_db >= expected * 0.8):
                target.status = 'complete'
                target.completed_at = datetime.utcnow()
                years_marked_complete += 1

    await db.commit()

    return SyncHarvestTargetsResponse(
        success=True,
        edition_id=edition_id,
        years_synced=years_synced,
        years_marked_complete=years_marked_complete,
        message=f"Synced {years_synced} years, marked {years_marked_complete} complete"
    )


# ============== Fix Incorrectly Complete Targets ==============

class FixIncompleteTargetsResponse(BaseModel):
    success: bool
    targets_fixed: int
    editions_affected: int
    total_missing_citations: int
    message: str

@app.post("/api/admin/fix-incomplete-harvest-targets", response_model=FixIncompleteTargetsResponse)
async def fix_incomplete_harvest_targets(
    threshold: float = 0.95,
    db: AsyncSession = Depends(get_db)
):
    """
    Fix harvest_targets that are incorrectly marked 'complete' when actual < expected * threshold.

    This fixes the bug where years were marked complete even when they had gaps.
    Resets such targets to 'incomplete' so they get retried by the harvester.
    """
    from app.models import HarvestTarget
    from sqlalchemy import func, and_

    # Find all incorrectly complete targets
    # Must have expected_count > 0 and actual_count < expected_count * threshold
    result = await db.execute(
        select(HarvestTarget)
        .where(HarvestTarget.status == 'complete')
        .where(HarvestTarget.expected_count > 0)
        .where(HarvestTarget.actual_count < HarvestTarget.expected_count * threshold)
    )
    targets = result.scalars().all()

    if not targets:
        return FixIncompleteTargetsResponse(
            success=True,
            targets_fixed=0,
            editions_affected=0,
            total_missing_citations=0,
            message="No incorrectly complete targets found"
        )

    # Calculate stats
    editions_affected = len(set(t.edition_id for t in targets))
    total_missing = sum((t.expected_count or 0) - (t.actual_count or 0) for t in targets)

    # Reset to incomplete
    for target in targets:
        target.status = 'incomplete'
        target.completed_at = None

    await db.commit()

    return FixIncompleteTargetsResponse(
        success=True,
        targets_fixed=len(targets),
        editions_affected=editions_affected,
        total_missing_citations=total_missing,
        message=f"Reset {len(targets)} targets across {editions_affected} editions to 'incomplete' (~{total_missing:,} missing citations)"
    )


# ============== Auto-Unstall Editions ==============

class AutoUnstallResponse(BaseModel):
    success: bool
    editions_unstalled: int
    editions_auto_completed: int
    targets_auto_completed: int
    details: list[dict]
    message: str

@app.post("/api/admin/auto-unstall-editions", response_model=AutoUnstallResponse)
async def auto_unstall_editions(
    completion_threshold: float = 0.95,
    gap_threshold: int = 100,
    db: AsyncSession = Depends(get_db)
):
    """
    Auto-unstall editions that are stalled but mostly complete.

    For each stalled edition (stall_count > 2):
    1. Calculate overall completion percentage
    2. If >= 95% complete OR total gap < 100 citations:
       - Mark remaining incomplete targets as complete
       - Reset stall_count to 0
    3. If not mostly complete, just reset stall_count to allow retry

    This fixes the issue where editions stall on small unfetchable gaps
    due to Google Scholar inconsistency.
    """
    from app.models import HarvestTarget
    from sqlalchemy import func

    # Find all stalled editions
    result = await db.execute(
        select(Edition).where(Edition.harvest_stall_count > 2)
    )
    stalled_editions = result.scalars().all()

    if not stalled_editions:
        return AutoUnstallResponse(
            success=True,
            editions_unstalled=0,
            editions_auto_completed=0,
            targets_auto_completed=0,
            details=[],
            message="No stalled editions found"
        )

    editions_unstalled = 0
    editions_auto_completed = 0
    targets_auto_completed = 0
    details = []

    for edition in stalled_editions:
        # Get all harvest targets with expected_count > 0
        targets_result = await db.execute(
            select(HarvestTarget)
            .where(HarvestTarget.edition_id == edition.id)
            .where(HarvestTarget.expected_count > 0)
        )
        all_targets = list(targets_result.scalars().all())

        if not all_targets:
            # No targets - just reset stall count
            old_stall = edition.harvest_stall_count
            edition.harvest_stall_count = 0
            editions_unstalled += 1
            details.append({
                "edition_id": edition.id,
                "title": edition.title[:50],
                "action": "unstalled",
                "old_stall_count": old_stall
            })
            continue

        # Calculate completion stats
        total_expected = sum(t.expected_count or 0 for t in all_targets)
        total_actual = sum(t.actual_count or 0 for t in all_targets)
        completion_pct = total_actual / total_expected if total_expected > 0 else 0
        total_gap = total_expected - total_actual

        incomplete_targets = [t for t in all_targets if t.status != 'complete']

        # Check if we should auto-complete
        if completion_pct >= completion_threshold or total_gap < gap_threshold:
            # Auto-complete remaining targets
            for target in incomplete_targets:
                target.status = 'complete'
                target.completed_at = datetime.utcnow()
                targets_auto_completed += 1

            old_stall = edition.harvest_stall_count
            edition.harvest_stall_count = 0
            editions_auto_completed += 1
            editions_unstalled += 1

            details.append({
                "edition_id": edition.id,
                "title": edition.title[:50],
                "action": "auto_completed",
                "completion_pct": round(completion_pct * 100, 1),
                "gap": total_gap,
                "targets_completed": len(incomplete_targets),
                "old_stall_count": old_stall
            })
        else:
            # Not mostly complete - just reset stall count to allow retry
            old_stall = edition.harvest_stall_count
            edition.harvest_stall_count = 0
            editions_unstalled += 1

            details.append({
                "edition_id": edition.id,
                "title": edition.title[:50],
                "action": "unstalled_for_retry",
                "completion_pct": round(completion_pct * 100, 1),
                "gap": total_gap,
                "incomplete_targets": len(incomplete_targets),
                "old_stall_count": old_stall
            })

    await db.commit()

    return AutoUnstallResponse(
        success=True,
        editions_unstalled=editions_unstalled,
        editions_auto_completed=editions_auto_completed,
        targets_auto_completed=targets_auto_completed,
        details=details,
        message=f"Unstalled {editions_unstalled} editions: {editions_auto_completed} auto-completed, {editions_unstalled - editions_auto_completed} reset for retry"
    )


# ============== Sync Actual Counts with Real Citation Data ==============

class SyncActualCountsResponse(BaseModel):
    success: bool
    targets_synced: int
    targets_status_changed: int
    editions_affected: int
    details: list[dict]
    message: str

@app.post("/api/admin/sync-actual-counts", response_model=SyncActualCountsResponse)
async def sync_actual_counts(db: AsyncSession = Depends(get_db)):
    """
    Sync harvest_targets.actual_count with real citation counts from the database.

    This fixes the ROOT CAUSE of stalling: actual_count=0 while citations actually exist.
    After syncing, it re-evaluates completion status based on real data.
    """
    import traceback
    import logging
    logger = logging.getLogger(__name__)

    try:
        from app.models import HarvestTarget, Citation
        from sqlalchemy import func

        logger.info("[SYNC] Step 1: Query real citation counts...")
        # Get real citation counts per edition+year
        real_counts_query = (
            select(
                Citation.edition_id,
                Citation.year,
                func.count().label('real_count')
            )
            .group_by(Citation.edition_id, Citation.year)
        )
        real_counts_result = await db.execute(real_counts_query)
        real_counts = {(row.edition_id, row.year): row.real_count for row in real_counts_result}
        logger.info(f"[SYNC] Found {len(real_counts)} edition+year combinations with citations")

        logger.info("[SYNC] Step 2: Query all harvest_targets...")
        # Get all harvest_targets (no relationship to Edition, just edition_id FK)
        targets_result = await db.execute(select(HarvestTarget))
        targets = list(targets_result.scalars().all())
        logger.info(f"[SYNC] Found {len(targets)} harvest targets")

        targets_synced = 0
        targets_status_changed = 0
        editions_affected = set()
        details = []

        logger.info("[SYNC] Step 3: Processing targets...")
        for target in targets:
            real_count = real_counts.get((target.edition_id, target.year), 0)
            old_actual = target.actual_count or 0
            old_status = target.status

            if old_actual != real_count:
                target.actual_count = real_count
                targets_synced += 1
                editions_affected.add(target.edition_id)

                # Re-evaluate status based on synced data
                expected = target.expected_count or 0
                if expected > 0:
                    completion_ratio = real_count / expected
                    # Mark as complete if >= 95% or if we got more than expected
                    if completion_ratio >= 0.95 or real_count >= expected:
                        if old_status != 'complete':
                            target.status = 'complete'
                            target.completed_at = datetime.utcnow()
                            targets_status_changed += 1
                    else:
                        if old_status == 'complete':
                            target.status = 'incomplete'
                            target.completed_at = None
                            targets_status_changed += 1

                details.append({
                    "edition_id": target.edition_id,
                    "year": target.year,
                    "old_actual": old_actual,
                    "new_actual": real_count,
                    "expected": expected,
                    "old_status": old_status,
                    "new_status": target.status
                })

        logger.info(f"[SYNC] Processed {len(targets)} targets, {targets_synced} need syncing")
        logger.info("[SYNC] Step 4: Committing changes...")
        await db.commit()

        # Also reset stall counts for editions that now have correct data
        stalled_editions = []
        if editions_affected:
            logger.info(f"[SYNC] Step 5: Reset stall counts for {len(editions_affected)} editions...")
            reset_result = await db.execute(
                select(Edition).where(
                    Edition.id.in_(editions_affected),
                    Edition.harvest_stall_count > 0
                )
            )
            stalled_editions = list(reset_result.scalars().all())
            for edition in stalled_editions:
                edition.harvest_stall_count = 0
            await db.commit()

        logger.info(f"[SYNC] Complete! Synced {targets_synced} targets, {targets_status_changed} status changes")
        return SyncActualCountsResponse(
            success=True,
            targets_synced=targets_synced,
            targets_status_changed=targets_status_changed,
            editions_affected=len(editions_affected),
            details=details[:100],  # Limit details to first 100
            message=f"Synced {targets_synced} targets across {len(editions_affected)} editions. {targets_status_changed} status changes. Reset stall counts for {len(stalled_editions)} editions."
        )

    except Exception as e:
        logger.error(f"[SYNC] ERROR: {e}")
        logger.error(f"[SYNC] Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Sync failed: {str(e)}")


# ============== Refresh Expected Counts from GS ==============

class RefreshExpectedCountsRequest(BaseModel):
    start_year: int
    end_year: int
    only_zero: bool = True  # Only refresh years with expected_count=0

class RefreshExpectedCountsResponse(BaseModel):
    success: bool
    edition_id: int
    years_refreshed: int
    results: list[dict]
    message: str

@app.post("/api/editions/{edition_id}/refresh-expected-counts", response_model=RefreshExpectedCountsResponse)
async def refresh_expected_counts(edition_id: int, request: RefreshExpectedCountsRequest, db: AsyncSession = Depends(get_db)):
    """
    Query Google Scholar to refresh expected_count for harvest_targets.

    This fixes the issue where years have expected_count=0 because they were
    never properly scanned. It queries GS for each year and updates the
    expected_count in harvest_targets.
    """
    from app.models import HarvestTarget
    from app.services.scholar_search import ScholarSearchService
    import asyncio

    # Get edition
    result = await db.execute(select(Edition).where(Edition.id == edition_id))
    edition = result.scalar_one_or_none()
    if not edition:
        raise HTTPException(status_code=404, detail="Edition not found")

    scholar_id = edition.cluster_id or edition.scholar_id
    if not scholar_id:
        raise HTTPException(status_code=400, detail="Edition has no scholar_id/cluster_id")

    # Get harvest_targets for the year range
    query = select(HarvestTarget).where(
        HarvestTarget.edition_id == edition_id,
        HarvestTarget.year >= request.start_year,
        HarvestTarget.year <= request.end_year
    )
    if request.only_zero:
        query = query.where(HarvestTarget.expected_count == 0)

    result = await db.execute(query.order_by(HarvestTarget.year))
    targets = result.scalars().all()

    if not targets:
        return RefreshExpectedCountsResponse(
            success=True,
            edition_id=edition_id,
            years_refreshed=0,
            results=[],
            message="No years to refresh (all have expected_count > 0)"
        )

    # Query GS for each year
    scholar_service = ScholarSearchService()
    results = []
    years_refreshed = 0

    for target in targets:
        try:
            expected = await scholar_service.get_year_citation_count(scholar_id, target.year)
            if expected is not None:
                old_expected = target.expected_count
                target.expected_count = expected
                # Update status if now has citations to harvest
                if expected > target.actual_count and target.status == 'complete':
                    target.status = 'incomplete'
                years_refreshed += 1
                results.append({
                    "year": target.year,
                    "old_expected": old_expected,
                    "new_expected": expected,
                    "actual": target.actual_count,
                    "status": target.status
                })
            else:
                results.append({
                    "year": target.year,
                    "error": "Failed to get count from GS"
                })
            # Rate limit - wait between requests
            await asyncio.sleep(2)
        except Exception as e:
            results.append({
                "year": target.year,
                "error": str(e)
            })

    await db.commit()

    return RefreshExpectedCountsResponse(
        success=True,
        edition_id=edition_id,
        years_refreshed=years_refreshed,
        results=results,
        message=f"Refreshed {years_refreshed}/{len(targets)} years from GS"
    )


# ============== Populate Missing Harvest Targets ==============

class PopulateMissingTargetsRequest(BaseModel):
    start_year: int = 1950  # Go back to 1950 by default
    end_year: int = None    # Defaults to current year
    dry_run: bool = False   # If true, only report what would be created
    skip_gs_query: bool = True  # If true, skip GS query and create with expected=0 (fast mode)

class PopulateMissingTargetsResponse(BaseModel):
    success: bool
    edition_id: int
    years_checked: int
    targets_created: int
    total_expected_added: int
    results: list[dict]
    message: str

@app.post("/api/editions/{edition_id}/populate-missing-targets", response_model=PopulateMissingTargetsResponse)
async def populate_missing_targets(edition_id: int, request: PopulateMissingTargetsRequest, db: AsyncSession = Depends(get_db)):
    """
    Create harvest_targets for years that don't exist yet.

    This fixes the issue where harvest_targets only exist from edition.year onwards,
    but the work was published much earlier. Queries GS for expected counts and
    creates new harvest_target records.
    """
    from app.models import HarvestTarget
    from app.services.scholar_search import ScholarSearchService
    import asyncio
    from datetime import datetime

    # Get edition
    result = await db.execute(select(Edition).where(Edition.id == edition_id))
    edition = result.scalar_one_or_none()
    if not edition:
        raise HTTPException(status_code=404, detail="Edition not found")

    scholar_id = edition.cluster_id or edition.scholar_id
    if not scholar_id:
        raise HTTPException(status_code=400, detail="Edition has no scholar_id/cluster_id")

    end_year = request.end_year or datetime.now().year

    # Get existing harvest_target years
    existing_result = await db.execute(
        select(HarvestTarget.year)
        .where(HarvestTarget.edition_id == edition_id)
        .where(HarvestTarget.year.isnot(None))
    )
    existing_years = {row[0] for row in existing_result.fetchall()}

    # Find years that are missing
    all_years = set(range(request.start_year, end_year + 1))
    missing_years = sorted(all_years - existing_years)

    if not missing_years:
        return PopulateMissingTargetsResponse(
            success=True,
            edition_id=edition_id,
            years_checked=len(all_years),
            targets_created=0,
            total_expected_added=0,
            results=[],
            message=f"No missing years found between {request.start_year} and {end_year}"
        )

    # Query GS for each missing year (unless skip_gs_query is True)
    results = []
    targets_created = 0
    total_expected_added = 0

    if request.skip_gs_query:
        # Fast mode: create all targets with expected_count=0
        # The harvester will query GS when it runs
        for year in missing_years:
            if not request.dry_run:
                new_target = HarvestTarget(
                    edition_id=edition_id,
                    year=year,
                    expected_count=0,  # Will be updated by harvester
                    actual_count=0,
                    status='incomplete',
                    pages_attempted=0,
                    pages_succeeded=0,
                    pages_failed=0
                )
                db.add(new_target)
            targets_created += 1
            results.append({
                "year": year,
                "expected_count": 0,
                "action": "would_create" if request.dry_run else "created"
            })
    else:
        # Slow mode: query GS for each year
        scholar_service = ScholarSearchService()
        for year in missing_years:
            try:
                expected = await scholar_service.get_year_citation_count(scholar_id, year)

                result_entry = {
                    "year": year,
                    "expected_count": expected or 0,
                    "action": "skipped" if expected == 0 else ("would_create" if request.dry_run else "created")
                }

                # Only create target if there are expected citations
                if expected and expected > 0:
                    if not request.dry_run:
                        new_target = HarvestTarget(
                            edition_id=edition_id,
                            year=year,
                            expected_count=expected,
                            actual_count=0,
                            status='incomplete',
                            pages_attempted=0,
                            pages_succeeded=0,
                            pages_failed=0
                        )
                        db.add(new_target)
                    targets_created += 1
                    total_expected_added += expected

                results.append(result_entry)

                # Rate limit - wait between requests
                await asyncio.sleep(2)
            except Exception as e:
                results.append({
                    "year": year,
                    "error": str(e)
                })

    if not request.dry_run:
        await db.commit()

        # Also reset stall count if we added targets
        if targets_created > 0:
            edition.harvest_stall_count = 0
            await db.commit()

    return PopulateMissingTargetsResponse(
        success=True,
        edition_id=edition_id,
        years_checked=len(missing_years),
        targets_created=targets_created,
        total_expected_added=total_expected_added,
        results=results,
        message=f"{'Would create' if request.dry_run else 'Created'} {targets_created} new harvest targets with {total_expected_added} total expected citations"
    )


# ============== Bulk Populate Missing Targets ==============

class BulkPopulateMissingTargetsRequest(BaseModel):
    coverage_threshold: float = 0.7  # Only editions with < 70% coverage
    min_citation_count: int = 1000   # Only editions with > 1000 citations
    start_year: int = 1950
    limit: int = 10  # Max editions to process
    dry_run: bool = True  # Default to dry run for safety
    skip_gs_query: bool = True  # If true, skip GS query and create with expected=0 (fast mode)

class BulkPopulateMissingTargetsResponse(BaseModel):
    success: bool
    editions_checked: int
    editions_needing_fix: int
    editions_processed: int
    total_targets_created: int
    total_expected_added: int
    results: list[dict]
    message: str

@app.post("/api/admin/bulk-populate-missing-targets", response_model=BulkPopulateMissingTargetsResponse)
async def bulk_populate_missing_targets(request: BulkPopulateMissingTargetsRequest, db: AsyncSession = Depends(get_db)):
    """
    Find editions with low harvest_target coverage and populate missing years.

    This helps fix the systemic issue where edition.year is a reprint year
    and harvest_targets are missing for earlier years.
    """
    from app.models import HarvestTarget
    from sqlalchemy import func
    from datetime import datetime

    # Find editions with low coverage
    subq = (
        select(
            HarvestTarget.edition_id,
            func.sum(HarvestTarget.expected_count).label('sum_expected')
        )
        .where(HarvestTarget.year.isnot(None))
        .group_by(HarvestTarget.edition_id)
        .subquery()
    )

    result = await db.execute(
        select(Edition, subq.c.sum_expected)
        .outerjoin(subq, Edition.id == subq.c.edition_id)
        .where(Edition.citation_count > request.min_citation_count)
        .where(
            (subq.c.sum_expected.is_(None)) |
            (subq.c.sum_expected < Edition.citation_count * request.coverage_threshold)
        )
        .order_by((Edition.citation_count - func.coalesce(subq.c.sum_expected, 0)).desc())
        .limit(request.limit * 2)  # Get extra to filter
    )

    editions_needing_fix = []
    for edition, sum_expected in result:
        sum_expected = sum_expected or 0
        coverage = sum_expected / edition.citation_count if edition.citation_count > 0 else 1
        editions_needing_fix.append({
            'edition': edition,
            'sum_expected': sum_expected,
            'coverage': coverage,
            'gap': edition.citation_count - sum_expected
        })

    if not editions_needing_fix:
        return BulkPopulateMissingTargetsResponse(
            success=True,
            editions_checked=0,
            editions_needing_fix=0,
            editions_processed=0,
            total_targets_created=0,
            total_expected_added=0,
            results=[],
            message="No editions found needing target population"
        )

    # Process editions (limited)
    editions_to_process = editions_needing_fix[:request.limit]
    results = []
    total_targets_created = 0
    total_expected_added = 0

    for item in editions_to_process:
        edition = item['edition']
        try:
            # Call the single-edition endpoint logic
            from app.services.scholar_search import ScholarSearchService
            import asyncio

            scholar_id = edition.cluster_id or edition.scholar_id
            if not scholar_id:
                results.append({
                    'edition_id': edition.id,
                    'title': edition.title[:50] if edition.title else 'Unknown',
                    'error': 'No scholar_id'
                })
                continue

            end_year = datetime.now().year

            # Get existing years
            existing_result = await db.execute(
                select(HarvestTarget.year)
                .where(HarvestTarget.edition_id == edition.id)
                .where(HarvestTarget.year.isnot(None))
            )
            existing_years = {row[0] for row in existing_result.fetchall()}

            # Find missing years
            all_years = set(range(request.start_year, end_year + 1))
            missing_years = sorted(all_years - existing_years)

            if not missing_years:
                results.append({
                    'edition_id': edition.id,
                    'title': edition.title[:50] if edition.title else 'Unknown',
                    'coverage': round(item['coverage'] * 100, 1),
                    'message': 'All years already have targets'
                })
                continue

            # Create targets for missing years
            edition_targets_created = 0
            edition_expected_added = 0

            if request.skip_gs_query:
                # Fast mode: create all targets with expected_count=0
                for year in missing_years:
                    if not request.dry_run:
                        new_target = HarvestTarget(
                            edition_id=edition.id,
                            year=year,
                            expected_count=0,  # Will be updated by harvester
                            actual_count=0,
                            status='incomplete',
                            pages_attempted=0,
                            pages_succeeded=0,
                            pages_failed=0
                        )
                        db.add(new_target)
                    edition_targets_created += 1
            else:
                # Slow mode: query GS for each year
                scholar_service = ScholarSearchService()
                for year in missing_years:
                    try:
                        expected = await scholar_service.get_year_citation_count(scholar_id, year)

                        if expected and expected > 0:
                            if not request.dry_run:
                                new_target = HarvestTarget(
                                    edition_id=edition.id,
                                    year=year,
                                    expected_count=expected,
                                    actual_count=0,
                                    status='incomplete',
                                    pages_attempted=0,
                                    pages_succeeded=0,
                                    pages_failed=0
                                )
                                db.add(new_target)
                            edition_targets_created += 1
                            edition_expected_added += expected

                        await asyncio.sleep(2)  # Rate limit
                    except Exception as e:
                        pass  # Skip failed years

            if not request.dry_run and edition_targets_created > 0:
                edition.harvest_stall_count = 0

            total_targets_created += edition_targets_created
            total_expected_added += edition_expected_added

            results.append({
                'edition_id': edition.id,
                'title': edition.title[:50] if edition.title else 'Unknown',
                'old_coverage': round(item['coverage'] * 100, 1),
                'targets_created': edition_targets_created,
                'expected_added': edition_expected_added,
                'new_coverage_estimate': round(100 * (item['sum_expected'] + edition_expected_added) / edition.citation_count, 1) if edition.citation_count > 0 else 0
            })

        except Exception as e:
            results.append({
                'edition_id': edition.id,
                'title': edition.title[:50] if edition.title else 'Unknown',
                'error': str(e)
            })

    if not request.dry_run:
        await db.commit()

    return BulkPopulateMissingTargetsResponse(
        success=True,
        editions_checked=len(editions_needing_fix),
        editions_needing_fix=len(editions_needing_fix),
        editions_processed=len(editions_to_process),
        total_targets_created=total_targets_created,
        total_expected_added=total_expected_added,
        results=results,
        message=f"{'Would create' if request.dry_run else 'Created'} {total_targets_created} targets across {len(editions_to_process)} editions ({total_expected_added} expected citations)"
    )


# ============== Reset Stall Counts ==============

@app.post("/api/admin/reset-stall-counts")
async def reset_stall_counts(db: AsyncSession = Depends(get_db)):
    """
    Reset harvest_stall_count for editions that have new incomplete targets.

    This is needed after running bulk-populate-missing-targets, because:
    1. Old stall counts block harvesting
    2. New targets with expected_count=0 need to be processed
    """
    from app.models import HarvestTarget
    from sqlalchemy import func, and_, or_

    # Find editions with:
    # - harvest_stall_count >= 5
    # - At least one incomplete target (status != 'complete')
    subq = (
        select(HarvestTarget.edition_id)
        .where(
            HarvestTarget.status != 'complete',
            HarvestTarget.year.isnot(None)
        )
        .group_by(HarvestTarget.edition_id)
    )

    result = await db.execute(
        select(Edition)
        .where(
            Edition.harvest_stall_count >= 5,
            Edition.id.in_(subq)
        )
    )
    editions = list(result.scalars().all())

    reset_count = 0
    reset_editions = []
    for edition in editions:
        old_stall_count = edition.harvest_stall_count or 0
        edition.harvest_stall_count = 0
        # Increment reset count for tracking how many times we've reset
        edition.harvest_reset_count = (edition.harvest_reset_count or 0) + 1
        reset_count += 1
        reset_editions.append({
            "id": edition.id,
            "title": edition.title[:50] if edition.title else "Unknown",
            "old_stall_count": old_stall_count,
            "total_resets": edition.harvest_reset_count
        })

    await db.commit()

    return {
        "success": True,
        "reset_count": reset_count,
        "editions": reset_editions,
        "message": f"Reset stall count for {reset_count} editions"
    }


# ============== Mark Near-Complete Targets ==============

class MarkNearCompleteRequest(BaseModel):
    threshold_percent: float = 95.0  # Mark as complete if actual >= threshold% of expected
    min_expected: int = 10  # Only consider targets with at least this many expected
    dry_run: bool = True

@app.post("/api/admin/mark-near-complete-targets")
async def mark_near_complete_targets(
    request: MarkNearCompleteRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Mark harvest_targets as 'complete' when they're nearly done.

    This helps prevent stalls for targets that are 95%+ complete but
    Google Scholar keeps returning duplicates for the remaining citations.

    Parameters:
    - threshold_percent: Mark as complete if actual >= this % of expected (default: 95)
    - min_expected: Only process targets with at least this many expected (default: 10)
    - dry_run: If True, just report what would be marked (default: True)
    """
    from app.models import HarvestTarget
    from sqlalchemy import func

    # Find targets that are:
    # - Not complete
    # - Have expected_count >= min_expected
    # - Have actual_count >= threshold_percent of expected_count
    result = await db.execute(
        select(HarvestTarget, Edition.title)
        .join(Edition, Edition.id == HarvestTarget.edition_id)
        .where(
            HarvestTarget.status != 'complete',
            HarvestTarget.expected_count >= request.min_expected,
            HarvestTarget.actual_count >= (HarvestTarget.expected_count * request.threshold_percent / 100.0)
        )
        .order_by(HarvestTarget.edition_id, HarvestTarget.year)
    )
    rows = result.all()

    targets_to_mark = []
    for target, title in rows:
        pct = (target.actual_count / target.expected_count * 100) if target.expected_count > 0 else 0
        targets_to_mark.append({
            "edition_id": target.edition_id,
            "title": title[:50] if title else "Unknown",
            "year": target.year,
            "expected": target.expected_count,
            "actual": target.actual_count,
            "percent_complete": round(pct, 1),
            "gap": target.expected_count - target.actual_count
        })

        if not request.dry_run:
            target.status = 'complete'

    if not request.dry_run:
        await db.commit()

    return {
        "success": True,
        "dry_run": request.dry_run,
        "threshold_percent": request.threshold_percent,
        "min_expected": request.min_expected,
        "targets_count": len(targets_to_mark),
        "targets": targets_to_mark,
        "message": f"{'Would mark' if request.dry_run else 'Marked'} {len(targets_to_mark)} targets as complete"
    }


# ============== Harvest Report ==============

@app.get("/api/admin/harvest-report")
async def get_harvest_report(
    include_all: bool = False,
    db: AsyncSession = Depends(get_db)
):
    """
    Get comprehensive harvest diagnostics for all editions.

    Returns data suitable for Excel export with:
    - Edition info (id, title, scholar_id, citation_count)
    - Harvest status (stall_count, completion %)
    - Target breakdown (expected vs actual, incomplete years)
    - Google Scholar URLs for debugging

    Parameters:
    - include_all: If True, include all editions with harvesting activity. If False (default), only stalled.
    """
    from app.models import HarvestTarget
    from sqlalchemy import func, case
    from datetime import datetime

    # Build target summary subquery
    target_summary = (
        select(
            HarvestTarget.edition_id,
            func.sum(HarvestTarget.expected_count).label('total_expected'),
            func.sum(HarvestTarget.actual_count).label('total_actual'),
            func.count().filter(HarvestTarget.status == 'complete').label('complete_years'),
            func.count().filter(
                HarvestTarget.status != 'complete',
                HarvestTarget.expected_count > 0
            ).label('incomplete_years'),
            func.min(case(
                (HarvestTarget.status != 'complete', HarvestTarget.year),
                else_=None
            )).label('min_incomplete_year'),
            func.max(case(
                (HarvestTarget.status != 'complete', HarvestTarget.year),
                else_=None
            )).label('max_incomplete_year'),
        )
        .group_by(HarvestTarget.edition_id)
        .subquery()
    )

    # Main query
    query = (
        select(
            Edition.id,
            Edition.title,
            Edition.scholar_id,
            Edition.year.label('publication_year'),
            Edition.citation_count,
            Edition.harvest_stall_count,
            Edition.harvest_complete,
            Edition.last_harvested_at,
            # Stall tracking fields
            Edition.harvest_reset_count,
            Edition.last_stall_year,
            Edition.last_stall_offset,
            Edition.last_stall_reason,
            Edition.last_stall_at,
            target_summary.c.total_expected,
            target_summary.c.total_actual,
            target_summary.c.complete_years,
            target_summary.c.incomplete_years,
            target_summary.c.min_incomplete_year,
            target_summary.c.max_incomplete_year,
        )
        .outerjoin(target_summary, target_summary.c.edition_id == Edition.id)
    )

    if not include_all:
        # Only stalled editions
        query = query.where(Edition.harvest_stall_count >= 5)
    else:
        # All with activity
        query = query.where(
            (Edition.harvest_stall_count > 0) |
            (target_summary.c.incomplete_years > 0)
        )

    query = query.order_by(Edition.harvest_stall_count.desc())

    result = await db.execute(query)
    rows = result.all()

    editions = []
    for row in rows:
        total_exp = row.total_expected or 0
        total_act = row.total_actual or 0
        pct = round(100 * total_act / max(total_exp, 1), 1)

        # Determine status category
        if row.harvest_complete:
            status = 'Complete'
        elif row.harvest_stall_count >= 5:
            status = 'Stalled'
        elif (row.incomplete_years or 0) > 0:
            status = 'Has Work'
        else:
            status = 'Unknown'

        # Determine likely stall reason
        if row.harvest_stall_count >= 5:
            if pct > 90:
                reason = 'Near complete - likely duplicates'
            elif pct > 70:
                reason = 'High progress - pagination issue?'
            elif pct > 50:
                reason = 'Medium progress - rate limit?'
            else:
                reason = 'Low progress - needs investigation'
        else:
            reason = ''

        editions.append({
            "id": row.id,
            "title": row.title[:80] if row.title else None,
            "scholar_id": row.scholar_id,
            "publication_year": row.publication_year,
            "citation_count": row.citation_count,
            "harvest_stall_count": row.harvest_stall_count,
            "harvest_complete": row.harvest_complete,
            "last_harvested_at": row.last_harvested_at.isoformat() if row.last_harvested_at else None,
            # Stall tracking fields
            "harvest_reset_count": row.harvest_reset_count or 0,
            "last_stall_year": row.last_stall_year,
            "last_stall_offset": row.last_stall_offset,
            "last_stall_reason": row.last_stall_reason,
            "last_stall_at": row.last_stall_at.isoformat() if row.last_stall_at else None,
            "total_expected": total_exp,
            "total_actual": total_act,
            "gap": total_exp - total_act,
            "pct_complete": pct,
            "complete_years": row.complete_years or 0,
            "incomplete_years": row.incomplete_years or 0,
            "min_incomplete_year": row.min_incomplete_year,
            "max_incomplete_year": row.max_incomplete_year,
            "status": status,
            "likely_reason": reason,
            "gs_url": f"https://scholar.google.com/scholar?cites={row.scholar_id}&hl=en" if row.scholar_id else None,
            # Add year-specific GS URL if there's a known stall year
            "gs_stall_year_url": f"https://scholar.google.com/scholar?cites={row.scholar_id}&hl=en&as_ylo={row.last_stall_year}&as_yhi={row.last_stall_year}" if row.scholar_id and row.last_stall_year else None
        })

    # Summary stats
    stalled_count = len([e for e in editions if e['status'] == 'Stalled'])
    has_work_count = len([e for e in editions if e['status'] == 'Has Work'])
    total_expected = sum(e['total_expected'] for e in editions)
    total_actual = sum(e['total_actual'] for e in editions)

    return {
        "success": True,
        "summary": {
            "total_editions": len(editions),
            "stalled": stalled_count,
            "has_work": has_work_count,
            "total_expected": total_expected,
            "total_actual": total_actual,
            "overall_pct": round(100 * total_actual / max(total_expected, 1), 1)
        },
        "editions": editions,
        "generated_at": datetime.utcnow().isoformat()
    }


@app.get("/api/admin/harvest-report/year-details/{edition_id}")
async def get_harvest_year_details(
    edition_id: int,
    db: AsyncSession = Depends(get_db)
):
    """
    Get detailed year-by-year breakdown for a specific edition.
    Useful for diagnosing exactly where harvesting got stuck.
    """
    from app.models import HarvestTarget

    # Get edition info
    result = await db.execute(
        select(Edition).where(Edition.id == edition_id)
    )
    edition = result.scalar_one_or_none()
    if not edition:
        raise HTTPException(status_code=404, detail="Edition not found")

    # Get all targets for this edition
    result = await db.execute(
        select(HarvestTarget)
        .where(HarvestTarget.edition_id == edition_id)
        .order_by(HarvestTarget.year.desc())
    )
    targets = result.scalars().all()

    years = []
    for t in targets:
        pct = round(100 * t.actual_count / max(t.expected_count, 1), 1)
        years.append({
            "year": t.year,
            "expected": t.expected_count,
            "actual": t.actual_count,
            "gap": t.expected_count - t.actual_count,
            "pct": pct,
            "status": t.status,
            "pages_attempted": t.pages_attempted,
            "pages_succeeded": t.pages_succeeded,
            "pages_failed": t.pages_failed,
            "gs_year_url": f"https://scholar.google.com/scholar?cites={edition.scholar_id}&hl=en&as_ylo={t.year}&as_yhi={t.year}" if edition.scholar_id and t.year else None
        })

    return {
        "success": True,
        "edition": {
            "id": edition.id,
            "title": edition.title,
            "scholar_id": edition.scholar_id,
            "harvest_stall_count": edition.harvest_stall_count,
            "gs_url": f"https://scholar.google.com/scholar?cites={edition.scholar_id}&hl=en" if edition.scholar_id else None
        },
        "years": years
    }


# ============== Gap Diagnostics ==============

@app.get("/api/admin/gap-diagnostics")
async def get_gap_diagnostics(
    include_explained: bool = False,
    min_gap: int = 1,
    db: AsyncSession = Depends(get_db)
):
    """
    Get harvest targets with unexplained gaps for manual diagnosis.

    By default, excludes gaps where gap_reason = 'gs_estimate_changed' (these are acceptable).
    Use include_explained=true to see all gaps including those with known causes.

    Returns targets grouped by edition with gap details for investigation.
    """
    # Build query for harvest targets with gaps
    query = (
        select(HarvestTarget, Edition)
        .join(Edition, HarvestTarget.edition_id == Edition.id)
        .where(HarvestTarget.status == "incomplete")
        .where(HarvestTarget.expected_count > 0)
    )

    result = await db.execute(query)
    all_targets = result.all()

    # Filter and organize targets
    editions_with_gaps = {}

    for target, edition in all_targets:
        # Calculate gap
        actual = target.actual_count or 0
        expected = target.expected_count or 0
        gap = expected - actual

        # Skip if gap too small
        if gap < min_gap:
            continue

        # Skip explained gaps unless requested
        if not include_explained and target.gap_reason == "gs_estimate_changed":
            continue

        # Group by edition
        if edition.id not in editions_with_gaps:
            editions_with_gaps[edition.id] = {
                "id": edition.id,
                "title": edition.title[:80] if edition.title else "Unknown",
                "scholar_id": edition.scholar_id,
                "gs_url": f"https://scholar.google.com/scholar?cites={edition.scholar_id}&hl=en" if edition.scholar_id else None,
                "harvest_stall_count": edition.harvest_stall_count,
                "targets": [],
                "total_gap": 0,
                "unexplained_gap": 0,
            }

        # Determine if gap is explained or unexplained
        is_explained = target.gap_reason == "gs_estimate_changed"

        target_info = {
            "year": target.year,
            "expected": expected,
            "actual": actual,
            "gap": gap,
            "gap_pct": round((gap / expected) * 100, 1) if expected > 0 else 0,
            "original_expected": target.original_expected,
            "final_gs_count": target.final_gs_count,
            "gap_reason": target.gap_reason,
            "gap_details": target.gap_details,
            "last_scraped_page": target.last_scraped_page,
            "pages_attempted": target.pages_attempted,
            "pages_succeeded": target.pages_succeeded,
            "pages_failed": target.pages_failed,
            "is_explained": is_explained,
            "gap_reviewed": target.gap_reviewed,
            "gap_review_notes": target.gap_review_notes,
            "gs_year_url": f"https://scholar.google.com/scholar?cites={edition.scholar_id}&hl=en&as_ylo={target.year}&as_yhi={target.year}" if edition.scholar_id and target.year else None
        }

        editions_with_gaps[edition.id]["targets"].append(target_info)
        editions_with_gaps[edition.id]["total_gap"] += gap
        if not is_explained:
            editions_with_gaps[edition.id]["unexplained_gap"] += gap

    # Sort editions by unexplained gap (descending)
    sorted_editions = sorted(
        editions_with_gaps.values(),
        key=lambda e: e["unexplained_gap"],
        reverse=True
    )

    # Summary stats
    total_targets = sum(len(e["targets"]) for e in sorted_editions)
    total_gap = sum(e["total_gap"] for e in sorted_editions)
    total_unexplained = sum(e["unexplained_gap"] for e in sorted_editions)

    return {
        "success": True,
        "summary": {
            "editions_with_gaps": len(sorted_editions),
            "total_targets_with_gaps": total_targets,
            "total_gap": total_gap,
            "total_unexplained_gap": total_unexplained,
            "include_explained": include_explained,
            "min_gap_filter": min_gap,
        },
        "editions": sorted_editions,
        "generated_at": datetime.utcnow().isoformat()
    }


@app.post("/api/admin/gap-diagnostics/mark-reviewed")
async def mark_gap_reviewed(
    edition_id: int,
    year: int = None,
    reviewed: bool = True,
    notes: str = None,
    db: AsyncSession = Depends(get_db)
):
    """
    Mark a harvest target gap as manually reviewed.

    This allows tracking which gaps have been investigated and what was found.
    """
    query = select(HarvestTarget).where(
        HarvestTarget.edition_id == edition_id,
        HarvestTarget.year == year
    )
    result = await db.execute(query)
    target = result.scalar_one_or_none()

    if not target:
        raise HTTPException(status_code=404, detail=f"HarvestTarget not found for edition {edition_id}, year {year}")

    target.gap_reviewed = reviewed
    if notes:
        target.gap_review_notes = notes
    target.updated_at = datetime.utcnow()

    await db.commit()

    return {
        "success": True,
        "message": f"Marked gap for edition {edition_id}, year {year} as {'reviewed' if reviewed else 'unreviewed'}",
        "gap_reviewed": target.gap_reviewed,
        "gap_review_notes": target.gap_review_notes,
    }


@app.post("/api/admin/gap-diagnostics/set-reason")
async def set_gap_reason(
    edition_id: int,
    year: int = None,
    gap_reason: str = None,
    gap_details: dict = None,
    db: AsyncSession = Depends(get_db)
):
    """
    Manually set the gap reason for a harvest target.

    Valid gap reasons:
    - gs_estimate_changed: GS's count changed during pagination (acceptable)
    - rate_limit: We got rate-limited
    - parse_error: Page parsing failed
    - max_pages_reached: Hit the 100-page limit
    - blocked: Got blocked by CAPTCHA
    - empty_page: Page returned no results unexpectedly
    - pagination_ended: GS stopped returning results early
    - unknown: Needs investigation
    """
    query = select(HarvestTarget).where(
        HarvestTarget.edition_id == edition_id,
        HarvestTarget.year == year
    )
    result = await db.execute(query)
    target = result.scalar_one_or_none()

    if not target:
        raise HTTPException(status_code=404, detail=f"HarvestTarget not found for edition {edition_id}, year {year}")

    if gap_reason:
        target.gap_reason = gap_reason
    if gap_details:
        target.gap_details = gap_details
    target.updated_at = datetime.utcnow()

    await db.commit()

    return {
        "success": True,
        "message": f"Updated gap reason for edition {edition_id}, year {year}",
        "gap_reason": target.gap_reason,
        "gap_details": target.gap_details,
    }


@app.post("/api/admin/gap-diagnostics/auto-complete-estimate-changes")
async def auto_complete_estimate_changes(
    dry_run: bool = True,
    completion_threshold: float = 0.95,
    db: AsyncSession = Depends(get_db)
):
    """
    Auto-complete harvest targets where the gap is due to GS estimate changes.

    This finds all incomplete targets where:
    1. We have gap tracking data (first_gs_count, last_gs_count)
    2. The GS count changed (first != last)
    3. We got >= 95% of what GS now reports (actual >= last_gs_count * threshold)

    These targets are "complete" in the sense that we can't get more results -
    GS simply reduced its count during pagination.

    Args:
        dry_run: If true, just return what would be completed without making changes
        completion_threshold: What % of final_gs_count we need to consider it complete (default 0.95)

    Returns:
        List of targets that were/would be auto-completed
    """
    # Find incomplete targets with gap tracking data
    query = (
        select(HarvestTarget, Edition)
        .join(Edition, HarvestTarget.edition_id == Edition.id)
        .where(HarvestTarget.status == "incomplete")
        .where(HarvestTarget.original_expected.isnot(None))
        .where(HarvestTarget.final_gs_count.isnot(None))
    )

    result = await db.execute(query)
    all_targets = result.all()

    candidates = []
    auto_completed = []

    for target, edition in all_targets:
        # Check if GS count changed
        if target.original_expected == target.final_gs_count:
            continue  # No estimate change

        # Check if we got enough of the final count
        actual = target.actual_count or 0
        final = target.final_gs_count or 0
        if final <= 0:
            continue

        ratio = actual / final
        if ratio >= completion_threshold:
            candidates.append({
                "edition_id": edition.id,
                "edition_title": edition.title[:60] if edition.title else "Unknown",
                "year": target.year,
                "original_expected": target.original_expected,
                "final_gs_count": target.final_gs_count,
                "actual_count": actual,
                "completion_ratio": round(ratio * 100, 1),
                "estimate_change": target.final_gs_count - target.original_expected,
            })

            if not dry_run:
                # Mark as complete
                target.status = "complete"
                target.gap_reason = "gs_estimate_changed"
                target.gap_details = {
                    "original_expected": target.original_expected,
                    "final_gs_count": target.final_gs_count,
                    "auto_completed": True,
                    "completion_ratio": ratio,
                }
                target.completed_at = datetime.utcnow()
                auto_completed.append(target)

    if not dry_run and auto_completed:
        await db.commit()

    return {
        "success": True,
        "dry_run": dry_run,
        "completion_threshold": completion_threshold,
        "candidates_found": len(candidates),
        "auto_completed_count": len(auto_completed) if not dry_run else 0,
        "candidates": candidates,
        "message": f"{'Would auto-complete' if dry_run else 'Auto-completed'} {len(candidates)} targets where GS estimate changed"
    }


@app.post("/api/admin/gap-diagnostics/analyze-gaps")
async def analyze_existing_gaps(
    db: AsyncSession = Depends(get_db)
):
    """
    Analyze all incomplete harvest targets to determine gap reasons.

    This is a one-time remediation that looks at existing data and tries to
    categorize gaps based on available information:

    1. If actual >= expected * 0.95: Mark as complete (within tolerance)
    2. If original_expected != final_gs_count: Gap is "gs_estimate_changed"
    3. If pages_failed > 0: Gap might be due to rate_limit or parse_error
    4. Otherwise: Mark as "unknown" for manual investigation

    Returns summary of what was found and updated.
    """
    # Find all incomplete targets
    query = (
        select(HarvestTarget)
        .where(HarvestTarget.status == "incomplete")
        .where(HarvestTarget.expected_count > 0)
    )

    result = await db.execute(query)
    targets = result.scalars().all()

    stats = {
        "total_incomplete": len(targets),
        "already_categorized": 0,
        "auto_completed": 0,
        "gs_estimate_changed": 0,
        "with_failed_pages": 0,
        "unknown": 0,
    }

    updated_targets = []

    for target in targets:
        expected = target.expected_count or 0
        actual = target.actual_count or 0

        # Skip if already has a gap reason
        if target.gap_reason:
            stats["already_categorized"] += 1
            continue

        # Check if within completion tolerance (95%)
        if expected > 0 and actual >= expected * 0.95:
            target.status = "complete"
            target.completed_at = datetime.utcnow()
            stats["auto_completed"] += 1
            updated_targets.append(target)
            continue

        # Check for GS estimate changes (using expected_count difference from actual)
        # Note: This is a heuristic since we don't have first/last counts for old data
        gap_pct = ((expected - actual) / expected * 100) if expected > 0 else 0
        if 5 <= gap_pct <= 25:
            # Typical GS estimate drift is 10-20%
            target.gap_reason = "gs_estimate_changed"
            target.gap_details = {
                "heuristic": True,
                "note": "Categorized based on gap percentage typical of GS estimate drift"
            }
            stats["gs_estimate_changed"] += 1
            updated_targets.append(target)
            continue

        # Check for failed pages
        if target.pages_failed and target.pages_failed > 0:
            target.gap_reason = "rate_limit"
            target.gap_details = {
                "pages_failed": target.pages_failed,
                "note": "Has failed pages that may need retry"
            }
            stats["with_failed_pages"] += 1
            updated_targets.append(target)
            continue

        # Otherwise unknown
        target.gap_reason = "unknown"
        target.gap_details = {
            "expected": expected,
            "actual": actual,
            "gap": expected - actual,
            "gap_pct": gap_pct,
            "note": "Needs manual investigation"
        }
        stats["unknown"] += 1
        updated_targets.append(target)

    # Commit changes
    if updated_targets:
        await db.commit()

    return {
        "success": True,
        "stats": stats,
        "updated_count": len(updated_targets),
        "message": f"Analyzed {len(targets)} incomplete targets, updated {len(updated_targets)}"
    }


# ============== Bibliography Parsing ==============

class BibliographyParseRequest(BaseModel):
    text: str

class BibliographyParseResponse(BaseModel):
    success: bool
    parsed: dict = None
    error: str = None

@app.post("/api/bibliography/parse", response_model=BibliographyParseResponse)
async def parse_bibliography(request: BibliographyParseRequest):
    """Parse bibliography text using Claude to extract structured metadata"""
    if not request.text.strip():
        return BibliographyParseResponse(success=False, error="text is required")

    try:
        import anthropic

        client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

        response = client.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=16000,
            messages=[{
                "role": "user",
                "content": f"""You are a bibliographic metadata extraction assistant. Parse the following bibliography text and extract structured metadata.

Extract:
1. Author name (use shortest form shown, e.g., "Mao Zedong" or "M Zedong")
2. Work title (clean, no formatting marks)
3. Year of publication (if present)
4. Publisher/venue (if present)
5. Group/Category (if present - e.g., "Group 1: Historical Core", "Group 2", etc.)

IMPORTANT: If the bibliography contains group labels like "Group 1:", "Group 2: Secondary Core", or numbered sections, extract and preserve these as the "group" field for each author.

Group works by author. Return ONLY valid JSON (no markdown, no explanations):

{{
  "authors": [
    {{
      "name": "Author Name",
      "group": "Group 1: Historical Core",
      "works": [
        {{
          "title": "Work Title",
          "year": "1967",
          "publisher": "Publisher Name"
        }}
      ]
    }}
  ]
}}

Rules:
- Preserve group/category labels from the original text (e.g., "Group 1", "Group 2: Secondary Core")
- Ignore subtitles after colons unless critical
- Extract years from parentheses  (1967) → "1967"
- For anthologies/collections, use the collection title
- If author appears in multiple forms, use the most common one
- Skip notes, introductions, or non-primary texts
- Clean markdown formatting (_ * ~~ etc.)

Bibliography text:
{request.text}"""
            }]
        )

        # Extract JSON from response
        json_text = response.content[0].text.strip()

        # Clean JSON (remove markdown code blocks if present)
        if json_text.startswith("```"):
            json_text = json_text.split("```")[1]
            if json_text.startswith("json"):
                json_text = json_text[4:]
        json_text = json_text.strip()

        parsed = json.loads(json_text)

        if not parsed.get("authors") or not isinstance(parsed["authors"], list):
            return BibliographyParseResponse(
                success=False,
                error="Invalid response structure - missing authors array"
            )

        return BibliographyParseResponse(success=True, parsed=parsed)

    except json.JSONDecodeError as e:
        return BibliographyParseResponse(
            success=False,
            error=f"Failed to parse LLM response as JSON: {str(e)}"
        )
    except Exception as e:
        return BibliographyParseResponse(
            success=False,
            error=f"Failed to parse bibliography: {str(e)}"
        )


# ============== Citation Refresh/Auto-Updater Endpoints ==============

import uuid
from datetime import timedelta

STALENESS_THRESHOLD_DAYS = 90


@app.post("/api/refresh/paper/{paper_id}", response_model=RefreshJobResponse)
async def refresh_paper_citations(
    paper_id: int,
    request: RefreshRequest = None,
    db: AsyncSession = Depends(get_db)
):
    """Queue refresh jobs for a single paper's editions

    This will re-harvest citations for all selected editions of the paper,
    using year-aware filtering to only fetch new citations since the last harvest.
    """
    from .services.job_worker import create_extract_citations_job

    if request is None:
        request = RefreshRequest()

    # Get paper
    result = await db.execute(select(Paper).where(Paper.id == paper_id))
    paper = result.scalar_one_or_none()
    if not paper:
        raise HTTPException(status_code=404, detail="Paper not found")

    # Get selected editions
    editions_result = await db.execute(
        select(Edition).where(
            Edition.paper_id == paper_id,
            Edition.selected == True
        )
    )
    editions = list(editions_result.scalars().all())

    if not editions:
        raise HTTPException(status_code=400, detail="No editions selected for refresh")

    # Generate batch ID
    batch_id = str(uuid.uuid4())

    # Compute year_low from paper's last harvest (or None for full harvest)
    year_low = None
    if not request.force_full_refresh and paper.any_edition_harvested_at:
        year_low = paper.any_edition_harvested_at.year

    # Create refresh job
    # When force_full_refresh=True, set is_refresh=False to bypass year filtering in job_worker
    job = await create_extract_citations_job(
        db=db,
        paper_id=paper_id,
        edition_ids=[e.id for e in editions],
        max_citations_per_edition=request.max_citations_per_edition,
        skip_threshold=request.skip_threshold,
        is_refresh=not request.force_full_refresh,
        year_low=year_low,
        batch_id=batch_id,
    )
    await db.commit()

    return RefreshJobResponse(
        jobs_created=1,
        papers_included=1,
        editions_included=len(editions),
        job_ids=[job.id],
        batch_id=batch_id,
    )


@app.post("/api/refresh/collection/{collection_id}", response_model=RefreshJobResponse)
async def refresh_collection_citations(
    collection_id: int,
    request: RefreshRequest = None,
    db: AsyncSession = Depends(get_db)
):
    """Queue refresh jobs for all papers in a collection

    Creates one job per paper with selected editions.
    """
    from .services.job_worker import create_extract_citations_job

    if request is None:
        request = RefreshRequest()

    # Get collection
    result = await db.execute(select(Collection).where(Collection.id == collection_id))
    collection = result.scalar_one_or_none()
    if not collection:
        raise HTTPException(status_code=404, detail="Collection not found")

    # Get all papers in collection
    papers_result = await db.execute(
        select(Paper).where(Paper.collection_id == collection_id)
    )
    papers = list(papers_result.scalars().all())

    if not papers:
        raise HTTPException(status_code=400, detail="Collection has no papers")

    # Generate batch ID for tracking
    batch_id = str(uuid.uuid4())

    job_ids = []
    papers_included = 0
    editions_included = 0

    for paper in papers:
        # Get selected editions for this paper
        editions_result = await db.execute(
            select(Edition).where(
                Edition.paper_id == paper.id,
                Edition.selected == True
            )
        )
        editions = list(editions_result.scalars().all())

        if not editions:
            continue  # Skip papers with no selected editions

        # Compute year_low from paper's last harvest
        year_low = None
        if not request.force_full_refresh and paper.any_edition_harvested_at:
            year_low = paper.any_edition_harvested_at.year

        # Create refresh job
        job = await create_extract_citations_job(
            db=db,
            paper_id=paper.id,
            edition_ids=[e.id for e in editions],
            max_citations_per_edition=request.max_citations_per_edition,
            skip_threshold=request.skip_threshold,
            is_refresh=True,
            year_low=year_low,
            batch_id=batch_id,
        )
        job_ids.append(job.id)
        papers_included += 1
        editions_included += len(editions)

    await db.commit()

    if not job_ids:
        raise HTTPException(status_code=400, detail="No papers in collection have selected editions")

    return RefreshJobResponse(
        jobs_created=len(job_ids),
        papers_included=papers_included,
        editions_included=editions_included,
        job_ids=job_ids,
        batch_id=batch_id,
    )


@app.post("/api/refresh/global", response_model=RefreshJobResponse)
async def refresh_all_citations(
    request: RefreshRequest = None,
    stale_only: bool = True,
    db: AsyncSession = Depends(get_db)
):
    """Queue refresh jobs for all papers (optionally filtered to stale only)

    Args:
        stale_only: If True, only refresh papers that haven't been harvested in 90+ days
        request: Optional refresh configuration
    """
    from .services.job_worker import create_extract_citations_job

    if request is None:
        request = RefreshRequest()

    # Build query for papers
    query = select(Paper)

    if stale_only:
        # Only papers that are stale (never harvested or >90 days ago)
        threshold_date = datetime.utcnow() - timedelta(days=STALENESS_THRESHOLD_DAYS)
        from sqlalchemy import or_
        query = query.where(
            or_(
                Paper.any_edition_harvested_at.is_(None),
                Paper.any_edition_harvested_at < threshold_date
            )
        )

    papers_result = await db.execute(query)
    papers = list(papers_result.scalars().all())

    if not papers:
        return RefreshJobResponse(
            jobs_created=0,
            papers_included=0,
            editions_included=0,
            job_ids=[],
            batch_id=str(uuid.uuid4()),
        )

    # Generate batch ID
    batch_id = str(uuid.uuid4())

    job_ids = []
    papers_included = 0
    editions_included = 0

    for paper in papers:
        # Get selected editions
        editions_result = await db.execute(
            select(Edition).where(
                Edition.paper_id == paper.id,
                Edition.selected == True
            )
        )
        editions = list(editions_result.scalars().all())

        if not editions:
            continue

        # Compute year_low
        year_low = None
        if not request.force_full_refresh and paper.any_edition_harvested_at:
            year_low = paper.any_edition_harvested_at.year

        # Create job
        job = await create_extract_citations_job(
            db=db,
            paper_id=paper.id,
            edition_ids=[e.id for e in editions],
            max_citations_per_edition=request.max_citations_per_edition,
            skip_threshold=request.skip_threshold,
            is_refresh=True,
            year_low=year_low,
            batch_id=batch_id,
        )
        job_ids.append(job.id)
        papers_included += 1
        editions_included += len(editions)

    await db.commit()

    return RefreshJobResponse(
        jobs_created=len(job_ids),
        papers_included=papers_included,
        editions_included=editions_included,
        job_ids=job_ids,
        batch_id=batch_id,
    )


@app.get("/api/refresh/status", response_model=RefreshStatusResponse)
async def get_refresh_status(
    batch_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Get status of refresh operation by batch ID"""
    # Find all jobs with this batch_id in params
    jobs_result = await db.execute(select(Job))
    all_jobs = jobs_result.scalars().all()

    matching_jobs = []
    for job in all_jobs:
        if job.params:
            try:
                params = json.loads(job.params)
                if params.get("batch_id") == batch_id:
                    matching_jobs.append(job)
            except:
                pass

    if not matching_jobs:
        raise HTTPException(status_code=404, detail="Batch not found")

    # Count by status
    completed = sum(1 for j in matching_jobs if j.status == "completed")
    failed = sum(1 for j in matching_jobs if j.status == "failed")
    running = sum(1 for j in matching_jobs if j.status == "running")
    pending = sum(1 for j in matching_jobs if j.status == "pending")

    # Sum new citations from completed jobs
    new_citations = 0
    for job in matching_jobs:
        if job.status == "completed" and job.result:
            try:
                result = json.loads(job.result)
                new_citations += result.get("new_citations_added", 0)
            except:
                pass

    return RefreshStatusResponse(
        batch_id=batch_id,
        total_jobs=len(matching_jobs),
        completed_jobs=completed,
        failed_jobs=failed,
        running_jobs=running,
        pending_jobs=pending,
        new_citations_added=new_citations,
        is_complete=(completed + failed == len(matching_jobs)),
    )


@app.get("/api/staleness", response_model=StalenessReportResponse)
async def get_staleness_report(
    collection_id: Optional[int] = None,
    threshold_days: int = STALENESS_THRESHOLD_DAYS,
    db: AsyncSession = Depends(get_db)
):
    """Get report on stale papers and editions

    Args:
        collection_id: Optional - filter to a specific collection
        threshold_days: Days before considering a paper stale (default: 90)
    """
    from sqlalchemy import or_

    threshold_date = datetime.utcnow() - timedelta(days=threshold_days)

    # Build paper query
    paper_query = select(Paper)
    if collection_id is not None:
        paper_query = paper_query.where(Paper.collection_id == collection_id)

    papers_result = await db.execute(paper_query)
    papers = list(papers_result.scalars().all())

    # Count paper staleness
    total_papers = len(papers)
    never_harvested_papers = sum(1 for p in papers if p.any_edition_harvested_at is None)
    stale_papers = sum(
        1 for p in papers
        if p.any_edition_harvested_at is not None and p.any_edition_harvested_at < threshold_date
    )

    # Get editions (only selected ones)
    paper_ids = [p.id for p in papers] if papers else []

    if paper_ids:
        editions_result = await db.execute(
            select(Edition).where(
                Edition.paper_id.in_(paper_ids),
                Edition.selected == True
            )
        )
        editions = list(editions_result.scalars().all())
    else:
        editions = []

    # Count edition staleness
    total_editions = len(editions)
    never_harvested_editions = sum(1 for e in editions if e.last_harvested_at is None)
    stale_editions = sum(
        1 for e in editions
        if e.last_harvested_at is not None and e.last_harvested_at < threshold_date
    )

    # Find oldest harvest date
    oldest_harvest = None
    for p in papers:
        if p.any_edition_harvested_at:
            if oldest_harvest is None or p.any_edition_harvested_at < oldest_harvest:
                oldest_harvest = p.any_edition_harvested_at

    return StalenessReportResponse(
        total_papers=total_papers,
        stale_papers=stale_papers,
        never_harvested_papers=never_harvested_papers,
        total_editions=total_editions,
        stale_editions=stale_editions,
        never_harvested_editions=never_harvested_editions,
        oldest_harvest_date=oldest_harvest,
        staleness_threshold_days=threshold_days,
    )


# ============== Harvest Completeness ==============

@app.get("/api/harvest-completeness/edition/{edition_id}", response_model=HarvestCompletenessResponse)
async def get_edition_harvest_completeness(
    edition_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Get harvest completeness report for a specific edition.

    Shows expected vs actual citation counts per year, any incomplete years,
    and failed page fetches that need retry.
    """
    # Get the edition
    edition_result = await db.execute(
        select(Edition).where(Edition.id == edition_id)
    )
    edition = edition_result.scalar_one_or_none()
    if not edition:
        raise HTTPException(status_code=404, detail="Edition not found")

    # Get harvest targets for this edition
    targets_result = await db.execute(
        select(HarvestTarget)
        .where(HarvestTarget.edition_id == edition_id)
        .order_by(HarvestTarget.year.desc())
    )
    targets = list(targets_result.scalars().all())

    # Get failed fetches for this edition
    failed_result = await db.execute(
        select(FailedFetch)
        .where(FailedFetch.edition_id == edition_id)
        .order_by(FailedFetch.year.desc(), FailedFetch.page_number.asc())
    )
    failed_fetches = list(failed_result.scalars().all())

    # Build response
    total_expected = sum(t.expected_count for t in targets)
    total_actual = sum(t.actual_count for t in targets)
    total_missing = total_expected - total_actual
    completion_percent = (total_actual / total_expected * 100) if total_expected > 0 else 0

    # Find incomplete years
    incomplete_years = [
        t.year for t in targets
        if t.status == "incomplete" or (t.pages_failed > 0)
    ]

    # Format targets
    formatted_targets = [
        HarvestTargetResponse(
            id=t.id,
            edition_id=t.edition_id,
            year=t.year,
            expected_count=t.expected_count,
            actual_count=t.actual_count,
            status=t.status,
            pages_attempted=t.pages_attempted,
            pages_succeeded=t.pages_succeeded,
            pages_failed=t.pages_failed,
            created_at=t.created_at,
            completed_at=t.completed_at,
            missing_count=t.expected_count - t.actual_count,
            completion_percent=(t.actual_count / t.expected_count * 100) if t.expected_count > 0 else 0,
        )
        for t in targets
    ]

    # Format failed fetches
    formatted_failed = [
        FailedFetchResponse(
            id=f.id,
            edition_id=f.edition_id,
            url=f.url,
            year=f.year,
            page_number=f.page_number,
            retry_count=f.retry_count,
            last_retry_at=f.last_retry_at,
            last_error=f.last_error,
            status=f.status,
            recovered_citations=f.recovered_citations,
            created_at=f.created_at,
            resolved_at=f.resolved_at,
            edition_title=edition.title,
            paper_id=edition.paper_id,
        )
        for f in failed_fetches
    ]

    return HarvestCompletenessResponse(
        edition_id=edition_id,
        paper_id=edition.paper_id,
        total_expected=total_expected,
        total_actual=total_actual,
        total_missing=total_missing,
        completion_percent=completion_percent,
        targets=formatted_targets,
        failed_fetches=formatted_failed,
        incomplete_years=incomplete_years,
    )


@app.get("/api/harvest-completeness/paper/{paper_id}", response_model=HarvestCompletenessResponse)
async def get_paper_harvest_completeness(
    paper_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Get harvest completeness report for all editions of a paper.

    Aggregates data across all selected editions.
    """
    # Get all selected editions for this paper
    editions_result = await db.execute(
        select(Edition).where(
            Edition.paper_id == paper_id,
            Edition.selected == True
        )
    )
    editions = list(editions_result.scalars().all())
    edition_ids = [e.id for e in editions]

    if not edition_ids:
        return HarvestCompletenessResponse(
            paper_id=paper_id,
            total_expected=0,
            total_actual=0,
            total_missing=0,
            completion_percent=0,
        )

    # Get harvest targets for all editions
    targets_result = await db.execute(
        select(HarvestTarget)
        .where(HarvestTarget.edition_id.in_(edition_ids))
        .order_by(HarvestTarget.edition_id, HarvestTarget.year.desc())
    )
    targets = list(targets_result.scalars().all())

    # Get failed fetches for all editions
    failed_result = await db.execute(
        select(FailedFetch)
        .where(FailedFetch.edition_id.in_(edition_ids))
        .order_by(FailedFetch.edition_id, FailedFetch.year.desc())
    )
    failed_fetches = list(failed_result.scalars().all())

    # Build edition lookup
    edition_map = {e.id: e for e in editions}

    # Calculate totals
    total_expected = sum(t.expected_count for t in targets)
    total_actual = sum(t.actual_count for t in targets)
    total_missing = total_expected - total_actual
    completion_percent = (total_actual / total_expected * 100) if total_expected > 0 else 0

    # Find incomplete years (deduplicated)
    incomplete_years = list(set(
        t.year for t in targets
        if t.year and (t.status == "incomplete" or t.pages_failed > 0)
    ))
    incomplete_years.sort(reverse=True)

    # Format targets
    formatted_targets = [
        HarvestTargetResponse(
            id=t.id,
            edition_id=t.edition_id,
            year=t.year,
            expected_count=t.expected_count,
            actual_count=t.actual_count,
            status=t.status,
            pages_attempted=t.pages_attempted,
            pages_succeeded=t.pages_succeeded,
            pages_failed=t.pages_failed,
            created_at=t.created_at,
            completed_at=t.completed_at,
            missing_count=t.expected_count - t.actual_count,
            completion_percent=(t.actual_count / t.expected_count * 100) if t.expected_count > 0 else 0,
        )
        for t in targets
    ]

    # Format failed fetches
    formatted_failed = [
        FailedFetchResponse(
            id=f.id,
            edition_id=f.edition_id,
            url=f.url,
            year=f.year,
            page_number=f.page_number,
            retry_count=f.retry_count,
            last_retry_at=f.last_retry_at,
            last_error=f.last_error,
            status=f.status,
            recovered_citations=f.recovered_citations,
            created_at=f.created_at,
            resolved_at=f.resolved_at,
            edition_title=edition_map.get(f.edition_id, Edition()).title if f.edition_id in edition_map else None,
            paper_id=paper_id,
        )
        for f in failed_fetches
    ]

    return HarvestCompletenessResponse(
        paper_id=paper_id,
        total_expected=total_expected,
        total_actual=total_actual,
        total_missing=total_missing,
        completion_percent=completion_percent,
        targets=formatted_targets,
        failed_fetches=formatted_failed,
        incomplete_years=incomplete_years,
    )


@app.get("/api/failed-fetches", response_model=FailedFetchesSummary)
async def get_failed_fetches(
    status: Optional[str] = None,
    edition_id: Optional[int] = None,
    limit: int = 100,
    db: AsyncSession = Depends(get_db)
):
    """Get failed page fetches with optional filtering.

    Args:
        status: Filter by status (pending, retrying, succeeded, abandoned)
        edition_id: Filter to a specific edition
        limit: Max number to return (default 100)
    """
    # Build query
    query = select(FailedFetch)
    if status:
        query = query.where(FailedFetch.status == status)
    if edition_id:
        query = query.where(FailedFetch.edition_id == edition_id)

    query = query.order_by(FailedFetch.created_at.desc()).limit(limit)

    result = await db.execute(query)
    failed_fetches = list(result.scalars().all())

    # Get edition info for display
    edition_ids = list(set(f.edition_id for f in failed_fetches))
    if edition_ids:
        editions_result = await db.execute(
            select(Edition).where(Edition.id.in_(edition_ids))
        )
        editions = {e.id: e for e in editions_result.scalars().all()}
    else:
        editions = {}

    # Count by status
    all_result = await db.execute(select(FailedFetch))
    all_fetches = list(all_result.scalars().all())

    total_pending = sum(1 for f in all_fetches if f.status == "pending")
    total_retrying = sum(1 for f in all_fetches if f.status == "retrying")
    total_succeeded = sum(1 for f in all_fetches if f.status == "succeeded")
    total_abandoned = sum(1 for f in all_fetches if f.status == "abandoned")
    total_recovered = sum(f.recovered_citations for f in all_fetches if f.status == "succeeded")

    # Format failed fetches
    formatted = [
        FailedFetchResponse(
            id=f.id,
            edition_id=f.edition_id,
            url=f.url,
            year=f.year,
            page_number=f.page_number,
            retry_count=f.retry_count,
            last_retry_at=f.last_retry_at,
            last_error=f.last_error,
            status=f.status,
            recovered_citations=f.recovered_citations,
            created_at=f.created_at,
            resolved_at=f.resolved_at,
            edition_title=editions.get(f.edition_id, Edition()).title if f.edition_id in editions else None,
            paper_id=editions.get(f.edition_id, Edition()).paper_id if f.edition_id in editions else None,
        )
        for f in failed_fetches
    ]

    return FailedFetchesSummary(
        total_pending=total_pending,
        total_retrying=total_retrying,
        total_succeeded=total_succeeded,
        total_abandoned=total_abandoned,
        total_recovered_citations=total_recovered,
        failed_fetches=formatted,
    )


@app.post("/api/failed-fetches/retry")
async def trigger_retry_failed_fetches(
    max_retries: int = 50,
    db: AsyncSession = Depends(get_db)
):
    """Manually trigger a retry job for pending failed fetches.

    This queues a retry_failed_fetches job that will attempt to re-fetch
    pages that previously failed.
    """
    # Check if there's already a pending/running retry job
    existing = await db.execute(
        select(Job).where(
            Job.job_type == "retry_failed_fetches",
            Job.status.in_(["pending", "running"])
        )
    )
    if existing.scalar_one_or_none():
        raise HTTPException(
            status_code=409,
            detail="A retry job is already pending or running"
        )

    # Check if there are pending failed fetches
    pending_result = await db.execute(
        select(func.count(FailedFetch.id)).where(FailedFetch.status == "pending")
    )
    pending_count = pending_result.scalar() or 0

    if pending_count == 0:
        return {"message": "No pending failed fetches to retry", "job_id": None}

    # Create the job
    job = Job(
        job_type="retry_failed_fetches",
        status="pending",
        params=json.dumps({"max_retries": max_retries}),
        progress=0,
        progress_message=f"Queued: Retry {min(pending_count, max_retries)} failed fetches",
    )
    db.add(job)
    await db.commit()
    await db.refresh(job)

    return {
        "message": f"Retry job queued for {min(pending_count, max_retries)} failed fetches",
        "job_id": job.id,
        "pending_failed_fetches": pending_count,
    }


# ============== AI Gap Analysis ==============

@app.get("/api/papers/{paper_id}/analyze-gaps", response_model=AIGapAnalysisResponse)
async def analyze_harvest_gaps_with_ai(
    paper_id: int,
    edition_id: Optional[int] = None,
    db: AsyncSession = Depends(get_db)
):
    """Analyze harvest gaps for a paper using AI.

    This endpoint:
    1. Collects all harvest data (targets, failed fetches, editions)
    2. Identifies gaps: missing years, incomplete years, failed pages
    3. Uses LLM to generate human-readable analysis and recommendations
    4. Returns actionable fixes with API endpoints to execute them

    Args:
        paper_id: Paper ID
        edition_id: Optional - if provided, analyze gaps for this specific edition only
    """
    import anthropic
    import asyncio
    from .services.scholar_search import get_scholar_service

    # Get paper
    paper_result = await db.execute(
        select(Paper).where(Paper.id == paper_id)
    )
    paper = paper_result.scalar_one_or_none()
    if not paper:
        raise HTTPException(status_code=404, detail="Paper not found")

    # Get editions - either specific edition or all selected editions
    if edition_id:
        # Analyze specific edition
        editions_result = await db.execute(
            select(Edition).where(
                Edition.paper_id == paper_id,
                Edition.id == edition_id
            )
        )
        editions = list(editions_result.scalars().all())
        if not editions:
            raise HTTPException(status_code=404, detail=f"Edition {edition_id} not found for paper {paper_id}")
        analyzing_single_edition = True
    else:
        # Analyze all selected editions
        editions_result = await db.execute(
            select(Edition).where(
                Edition.paper_id == paper_id,
                Edition.selected == True
            )
        )
        editions = list(editions_result.scalars().all())
        analyzing_single_edition = False

    edition_ids = [e.id for e in editions]
    edition_map = {e.id: e for e in editions}

    if not edition_ids:
        return AIGapAnalysisResponse(
            paper_id=paper_id,
            paper_title=paper.title,
            analysis_timestamp=datetime.utcnow(),
            total_editions=0,
            selected_editions=0,
            ai_summary="No editions selected for harvesting.",
            ai_recommendations="Select editions to harvest before analyzing gaps.",
        )

    # Get harvest targets for all editions
    targets_result = await db.execute(
        select(HarvestTarget)
        .where(HarvestTarget.edition_id.in_(edition_ids))
        .order_by(HarvestTarget.edition_id, HarvestTarget.year.desc())
    )
    targets = list(targets_result.scalars().all())

    # Get failed fetches
    failed_result = await db.execute(
        select(FailedFetch)
        .where(
            FailedFetch.edition_id.in_(edition_ids),
            FailedFetch.status.in_(["pending", "retrying"])
        )
    )
    failed_fetches = list(failed_result.scalars().all())

    # Get ACTUAL harvested citation counts by querying the database
    # (don't use cached harvested_citation_count which can be stale)
    citation_counts_result = await db.execute(
        select(Citation.edition_id, func.count(Citation.id).label('count'))
        .where(Citation.edition_id.in_(edition_ids))
        .group_by(Citation.edition_id)
    )
    harvested_map = {row.edition_id: row.count for row in citation_counts_result}

    # Build analysis data - use edition's current citation_count (from Scholar) as expected
    # and actual DB citation count as harvested
    total_expected = sum(e.citation_count or 0 for e in editions)
    total_harvested = sum(harvested_map.get(e.id, 0) for e in editions)
    total_missing = total_expected - total_harvested
    completion_percent = (total_harvested / total_expected * 100) if total_expected > 0 else 0

    # Also track harvest_targets totals for gap detection
    targets_expected = sum(t.expected_count for t in targets)
    targets_actual = sum(t.actual_count for t in targets)

    # Identify gaps
    gaps: List[GapDetail] = []
    recommended_fixes: List[GapFix] = []

    # Group targets by edition for analysis
    targets_by_edition = {}
    for t in targets:
        if t.edition_id not in targets_by_edition:
            targets_by_edition[t.edition_id] = []
        targets_by_edition[t.edition_id].append(t)

    # Check each edition for gaps
    for edition_id, edition_targets in targets_by_edition.items():
        edition = edition_map.get(edition_id)
        edition_title = edition.title if edition else f"Edition {edition_id}"

        # Find year range from targets
        years_harvested = set(t.year for t in edition_targets if t.year)
        if years_harvested:
            min_year = min(years_harvested)
            max_year = max(years_harvested)

            # Check for missing years in the range
            expected_years = set(range(min_year, max_year + 1))
            missing_years = expected_years - years_harvested

            # Query Scholar for actual counts (limit to 5 years to avoid rate limiting)
            MAX_YEAR_LOOKUPS = 5
            scholar_service = get_scholar_service()
            years_to_check = sorted(missing_years, reverse=True)[:MAX_YEAR_LOOKUPS]

            for year in sorted(missing_years, reverse=True):
                # Query Scholar for actual expected count if edition has scholar_id
                expected_count = None
                if edition and edition.scholar_id and year in years_to_check:
                    try:
                        expected_count = await scholar_service.get_year_citation_count(
                            edition.scholar_id, year
                        )
                        await asyncio.sleep(0.3)  # Small delay between requests
                    except Exception as e:
                        logger.warning(f"Failed to get year count for {year}: {e}")

                # Build description based on whether we have count
                if expected_count:
                    description = f"Year {year}: {expected_count} citations never harvested"
                    severity = "critical" if expected_count > 100 else "high" if expected_count > 20 else "medium"
                else:
                    description = f"Year {year} was never harvested for {edition_title}"
                    severity = "high"

                gaps.append(GapDetail(
                    gap_type="missing_year",
                    year=year,
                    edition_id=edition_id,
                    edition_title=edition_title,
                    expected_count=expected_count or 0,
                    actual_count=0,
                    missing_count=expected_count or 0,
                    description=description,
                    severity=severity,
                ))
                recommended_fixes.append(GapFix(
                    fix_type="harvest_year",
                    priority=1,
                    year=year,
                    edition_id=edition_id,
                    edition_title=edition_title,
                    estimated_citations=expected_count or 0,
                    description=f"Harvest citations from year {year}" + (f" (~{expected_count} citations)" if expected_count else ""),
                    action_url=f"/api/papers/{paper_id}/verify-repair",
                ))

        # Check for incomplete years
        for target in edition_targets:
            if target.status == "incomplete" or (target.expected_count > target.actual_count and target.expected_count - target.actual_count > 5):
                missing = target.expected_count - target.actual_count
                severity = "critical" if missing > 100 else "high" if missing > 20 else "medium"

                gaps.append(GapDetail(
                    gap_type="incomplete_year",
                    year=target.year,
                    edition_id=edition_id,
                    edition_title=edition_title,
                    expected_count=target.expected_count,
                    actual_count=target.actual_count,
                    missing_count=missing,
                    description=f"Year {target.year}: Expected {target.expected_count}, got {target.actual_count} ({missing} missing)",
                    severity=severity,
                ))

                if target.pages_failed > 0:
                    gaps[-1].failed_pages = list(range(1, target.pages_failed + 1))  # Approximation
                    recommended_fixes.append(GapFix(
                        fix_type="retry_failed_pages",
                        priority=2,
                        year=target.year,
                        edition_id=edition_id,
                        edition_title=edition_title,
                        estimated_citations=missing,
                        description=f"Retry {target.pages_failed} failed pages for year {target.year}",
                        action_url="/api/failed-fetches/retry",
                    ))

    # Check for failed fetches
    failed_by_year = {}
    for ff in failed_fetches:
        key = (ff.edition_id, ff.year)
        if key not in failed_by_year:
            failed_by_year[key] = []
        failed_by_year[key].append(ff.page_number)

    for (edition_id, year), pages in failed_by_year.items():
        edition_title = edition_map.get(edition_id, Edition()).title if edition_id in edition_map else f"Edition {edition_id}"
        gaps.append(GapDetail(
            gap_type="failed_pages",
            year=year,
            edition_id=edition_id,
            edition_title=edition_title,
            failed_pages=sorted(pages),
            description=f"Year {year}: {len(pages)} page(s) failed to fetch: {sorted(pages)[:5]}{'...' if len(pages) > 5 else ''}",
            severity="medium",
        ))

    # Sort gaps by severity
    severity_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    gaps.sort(key=lambda g: (severity_order.get(g.severity, 99), -(g.year or 0)))

    # Sort fixes by priority
    recommended_fixes.sort(key=lambda f: f.priority)

    # Generate AI summary
    ai_summary = ""
    ai_recommendations = ""

    if settings.anthropic_api_key:
        try:
            client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

            # Build analysis context for LLM
            gap_descriptions = "\n".join([
                f"- {g.description} (severity: {g.severity})"
                for g in gaps[:20]  # Limit to top 20 gaps
            ]) or "No gaps detected."

            fix_descriptions = "\n".join([
                f"- Priority {f.priority}: {f.description}"
                for f in recommended_fixes[:10]  # Limit to top 10 fixes
            ]) or "No fixes needed."

            # Build context header based on single/multiple edition analysis
            if analyzing_single_edition:
                edition = editions[0]
                context_header = f"""Analyze this citation harvest report for the {edition.language or 'Unknown'} edition "{edition.title}" of the academic paper "{paper.title}".
This is an analysis of ONE SPECIFIC EDITION (not all editions of the work)."""
            else:
                context_header = f"""Analyze this citation harvest report for the academic paper "{paper.title}" (across all {len(editions)} selected editions)."""

            prompt = f"""{context_header}

Provide:
1. A brief summary of the harvest status (2-3 sentences)
2. Specific recommendations for filling gaps (bullet points)

HARVEST STATUS:
- Editions analyzed: {len(editions)}{f" ({editions[0].language} edition)" if analyzing_single_edition else ""}
- Total expected citations (from Google Scholar): {total_expected}
- Total harvested (unique citations in DB): {total_harvested}
- Missing: {total_missing} ({100 - completion_percent:.1f}% gap)

DETECTED GAPS:
{gap_descriptions}

RECOMMENDED FIXES:
{fix_descriptions}

Respond in this exact format:
SUMMARY:
[Your 2-3 sentence summary]

RECOMMENDATIONS:
[Your bullet-point recommendations]"""

            response = client.messages.create(
                model="claude-sonnet-4-5-20250929",
                max_tokens=1000,
                messages=[{"role": "user", "content": prompt}]
            )

            response_text = response.content[0].text

            # Parse response
            if "SUMMARY:" in response_text and "RECOMMENDATIONS:" in response_text:
                parts = response_text.split("RECOMMENDATIONS:")
                ai_summary = parts[0].replace("SUMMARY:", "").strip()
                ai_recommendations = parts[1].strip() if len(parts) > 1 else ""
            else:
                ai_summary = response_text

        except Exception as e:
            logger.warning(f"AI analysis failed: {e}")
            ai_summary = f"Harvest is {completion_percent:.1f}% complete. {len(gaps)} gaps detected across {len(editions)} editions."
            ai_recommendations = "Run verify-repair to fill gaps."
    else:
        ai_summary = f"Harvest is {completion_percent:.1f}% complete. {len(gaps)} gaps detected across {len(editions)} editions."
        if gaps:
            ai_recommendations = f"Found {len([g for g in gaps if g.gap_type == 'missing_year'])} missing years, {len([g for g in gaps if g.gap_type == 'incomplete_year'])} incomplete years, {len([g for g in gaps if g.gap_type == 'failed_pages'])} years with failed pages. Use verify-repair to fill gaps."
        else:
            ai_recommendations = "Harvest appears complete. No action needed."

    # Build response with edition info if analyzing single edition
    response_kwargs = {
        "paper_id": paper_id,
        "paper_title": paper.title,
        "analysis_timestamp": datetime.utcnow(),
        "total_editions": len(editions),
        "selected_editions": len(editions),
        "total_expected_citations": total_expected,
        "total_harvested_citations": total_harvested,
        "total_missing_citations": total_missing,
        "completion_percent": completion_percent,
        "gaps": gaps,
        "recommended_fixes": recommended_fixes,
        "ai_summary": ai_summary,
        "ai_recommendations": ai_recommendations,
    }

    if analyzing_single_edition:
        edition = editions[0]
        response_kwargs["edition_id"] = edition.id
        response_kwargs["edition_title"] = edition.title
        response_kwargs["edition_language"] = edition.language

    return AIGapAnalysisResponse(**response_kwargs)


# ============== Verify and Repair Harvest ==============

class VerifyRepairRequest(BaseModel):
    """Request to verify and repair harvest gaps"""
    year_start: int = 2025  # Start from most recent
    year_end: int = 1932    # Go back to publication year
    fix_gaps: bool = True   # If True, fetch missing pages. If False, just report.


class VerifyRepairResponse(BaseModel):
    """Response from verify/repair endpoint"""
    job_id: int
    paper_id: int
    years_to_check: int
    message: str


@app.post("/api/papers/{paper_id}/verify-repair", response_model=VerifyRepairResponse)
async def verify_and_repair_harvest(
    paper_id: int,
    request: VerifyRepairRequest,
    db: AsyncSession = Depends(get_db)
):
    """Verify harvest completeness and repair gaps for a paper.

    This queues a verify_and_repair job that will:
    1. For each year in the specified range, verify that the last page exists
    2. Compare Scholar's actual count to our harvested count
    3. Identify missing pages between what we have and what Scholar reports
    4. If fix_gaps=True, fetch the missing pages and save citations

    Use this to recover citations that may have been lost during original harvest.
    """
    # Get paper with its editions
    result = await db.execute(
        select(Paper).where(Paper.id == paper_id, Paper.deleted_at.is_(None))
    )
    paper = result.scalar_one_or_none()
    if not paper:
        raise HTTPException(status_code=404, detail="Paper not found")

    if not paper.scholar_id:
        raise HTTPException(status_code=400, detail="Paper has no Scholar ID - cannot verify")

    # Get selected editions
    editions_result = await db.execute(
        select(Edition)
        .where(Edition.paper_id == paper_id, Edition.selected == True, Edition.excluded == False)
    )
    editions = list(editions_result.scalars().all())

    if not editions:
        raise HTTPException(status_code=400, detail="Paper has no selected editions to verify")

    # Check if there's already a pending/running verify job for this paper
    existing = await db.execute(
        select(Job).where(
            Job.paper_id == paper_id,
            Job.job_type == "verify_and_repair",
            Job.status.in_(["pending", "running"])
        )
    )
    if existing.scalar_one_or_none():
        raise HTTPException(
            status_code=409,
            detail="A verify/repair job is already pending or running for this paper"
        )

    # Calculate years to check
    years_to_check = request.year_start - request.year_end + 1

    # Create the job
    job = Job(
        paper_id=paper_id,
        job_type="verify_and_repair",
        status="pending",
        params=json.dumps({
            "paper_id": paper_id,
            "year_start": request.year_start,
            "year_end": request.year_end,
            "fix_gaps": request.fix_gaps,
            "edition_ids": [e.id for e in editions],
        }),
        progress=0,
        progress_message=f"Queued: Verify/repair years {request.year_start} to {request.year_end} ({years_to_check} years)",
    )
    db.add(job)
    await db.commit()
    await db.refresh(job)

    return VerifyRepairResponse(
        job_id=job.id,
        paper_id=paper_id,
        years_to_check=years_to_check,
        message=f"Verify/repair job queued for {years_to_check} years across {len(editions)} editions"
    )


# ============== TEST: Single-Year Partition Harvest ==============

class PartitionTestRequest(BaseModel):
    """Request to test partition harvest on a single year"""
    edition_id: int
    year: int


@app.post("/api/test/partition-harvest")
async def test_partition_harvest(
    request: PartitionTestRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    TEST ENDPOINT: Trigger partition harvest for a single year.

    This is for testing the overflow partition strategy.
    The harvest runs in the background.
    """
    from .services.overflow_harvester import harvest_partition
    from .services.scholar_search import get_scholar_service

    # Get edition
    result = await db.execute(select(Edition).where(Edition.id == request.edition_id))
    edition = result.scalar_one_or_none()
    if not edition:
        raise HTTPException(status_code=404, detail="Edition not found")

    if not edition.scholar_id:
        raise HTTPException(status_code=400, detail="Edition has no scholar_id")

    # Get paper for the edition
    paper_result = await db.execute(select(Paper).where(Paper.id == edition.paper_id))
    paper = paper_result.scalar_one_or_none()

    # Check count for this year first
    scholar_service = get_scholar_service()
    count_result = await scholar_service.get_cited_by(
        scholar_id=edition.scholar_id,
        max_results=10,
        year_low=request.year,
        year_high=request.year,
    )
    total_count = count_result.get('totalResults', 0)

    if total_count <= 1000:
        return {
            "status": "skipped",
            "reason": f"Year {request.year} only has {total_count} citations (<=1000), no partition needed",
            "edition_id": request.edition_id,
            "year": request.year,
            "total_count": total_count
        }

    # Create a job to track this - job worker will pick it up
    job = Job(
        paper_id=edition.paper_id,
        job_type="partition_harvest_test",
        status="pending",
        params=json.dumps({
            "edition_id": request.edition_id,
            "year": request.year,
            "total_count": total_count,
        }),
        progress=0,
        progress_message=f"Queued: partition harvest for year {request.year} ({total_count} citations)",
    )
    db.add(job)
    await db.commit()
    await db.refresh(job)

    # Job worker will automatically pick this up - no need for background task
    return {
        "status": "started",
        "job_id": job.id,
        "edition_id": request.edition_id,
        "year": request.year,
        "total_count": total_count,
        "message": f"Partition harvest queued for year {request.year} with {total_count} citations"
    }


# ============== Overflow Year Re-Harvest ==============

class ReharvestOverflowRequest(BaseModel):
    """Request to re-harvest overflow years (>1000 citations) for a paper"""
    paper_id: int
    year_start: int  # Start of year range (inclusive)
    year_end: int    # End of year range (inclusive)


@app.post("/api/papers/{paper_id}/reharvest-overflow")
async def reharvest_overflow_years(
    paper_id: int,
    request: ReharvestOverflowRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Re-harvest overflow years (>1000 citations) for a paper using partition strategy.

    This endpoint:
    1. Checks each selected edition of the paper
    2. For each year in the range, checks if Google Scholar reports >1000 citations
    3. Removes those years from completed_years in harvest_resume_state
    4. Creates a harvest job that will use partition strategy for overflow years

    Use this to recover citations that were truncated before partition harvesting was implemented.
    """
    from .services.scholar_search import get_scholar_service

    # Validate paper exists
    result = await db.execute(select(Paper).where(Paper.id == paper_id))
    paper = result.scalar_one_or_none()
    if not paper:
        raise HTTPException(status_code=404, detail="Paper not found")

    # Get selected editions with scholar_id
    editions_result = await db.execute(
        select(Edition)
        .where(Edition.paper_id == paper_id)
        .where(Edition.selected == True)
        .where(Edition.scholar_id.isnot(None))
    )
    editions = editions_result.scalars().all()

    if not editions:
        raise HTTPException(status_code=400, detail="No selected editions with scholar_id found")

    scholar_service = get_scholar_service()
    overflow_years_found = []
    editions_updated = []

    # Check each edition
    for edition in editions:
        edition_overflow_years = []

        # Check each year in range for overflow
        for year in range(request.year_start, request.year_end + 1):
            try:
                count_result = await scholar_service.get_cited_by(
                    scholar_id=edition.scholar_id,
                    max_results=10,
                    year_low=year,
                    year_high=year,
                )
                total_count = count_result.get('totalResults', 0)

                if total_count > 1000:
                    edition_overflow_years.append({
                        "year": year,
                        "count": total_count
                    })

            except Exception as e:
                logger.warning(f"Error checking year {year} for edition {edition.id}: {e}")
                continue

        if edition_overflow_years:
            # Remove these years from completed_years
            years_to_remove = {y["year"] for y in edition_overflow_years}

            resume_state = {}
            if edition.harvest_resume_state:
                try:
                    resume_state = json.loads(edition.harvest_resume_state)
                except json.JSONDecodeError:
                    resume_state = {}

            completed_years = set(resume_state.get("completed_years", []))
            original_count = len(completed_years)
            completed_years -= years_to_remove

            # Update the resume state
            resume_state["completed_years"] = sorted(list(completed_years), reverse=True)
            resume_state["mode"] = "year_by_year"
            resume_state["overflow_reharvest"] = {
                "years_reset": list(years_to_remove),
                "reset_at": datetime.utcnow().isoformat()
            }

            await db.execute(
                update(Edition)
                .where(Edition.id == edition.id)
                .values(harvest_resume_state=json.dumps(resume_state))
            )

            editions_updated.append({
                "edition_id": edition.id,
                "title": edition.title,
                "overflow_years": edition_overflow_years,
                "years_removed_from_completed": list(years_to_remove),
                "completed_years_before": original_count,
                "completed_years_after": len(completed_years)
            })

            overflow_years_found.extend([y["year"] for y in edition_overflow_years])

    await db.commit()

    if not overflow_years_found:
        return {
            "status": "no_overflow",
            "message": f"No years with >1000 citations found in range {request.year_start}-{request.year_end}",
            "paper_id": paper_id,
            "editions_checked": len(editions)
        }

    # Check for existing pending/running job (duplicate prevention)
    existing_result = await db.execute(
        select(Job).where(
            Job.paper_id == paper_id,
            Job.job_type == "extract_citations",
            Job.status.in_(["pending", "running"])
        )
    )
    existing_job = existing_result.scalar_one_or_none()
    if existing_job:
        return {
            "status": "already_running",
            "job_id": existing_job.id,
            "paper_id": paper_id,
            "message": f"Existing job {existing_job.id} already running for this paper"
        }

    # Create a harvest job to process these editions
    job = Job(
        paper_id=paper_id,
        job_type="extract_citations",  # Standard harvest job type
        status="pending",
        params=json.dumps({
            "overflow_reharvest": True,
            "year_range": [request.year_start, request.year_end],
            "overflow_years": list(set(overflow_years_found))
        }),
        progress=0,
        progress_message=f"Queued: re-harvest overflow years {request.year_start}-{request.year_end}",
    )
    db.add(job)
    await db.commit()
    await db.refresh(job)

    return {
        "status": "started",
        "job_id": job.id,
        "paper_id": paper_id,
        "year_range": [request.year_start, request.year_end],
        "overflow_years_found": list(set(overflow_years_found)),
        "editions_updated": editions_updated,
        "message": f"Re-harvest queued for {len(set(overflow_years_found))} overflow years across {len(editions_updated)} editions"
    }


# ============== Orphan/Incomplete Work Detection ==============

@app.get("/api/incomplete-harvests")
async def get_incomplete_harvests(
    db: AsyncSession = Depends(get_db)
):
    """Scan for editions with incomplete harvests that should be resumed.

    Returns editions where:
    - selected = True (we want their citations)
    - harvested_citation_count < citation_count (incomplete)
    - No pending/running job for that paper
    - Paper is not paused
    - Harvest is not stalled
    """
    from app.services.job_worker import find_incomplete_harvests

    incomplete = await find_incomplete_harvests(db)

    # Group by paper
    editions_by_paper = {}
    for e in incomplete:
        if e.paper_id not in editions_by_paper:
            editions_by_paper[e.paper_id] = []
        editions_by_paper[e.paper_id].append(e)

    # Build response
    papers_with_incomplete = []
    for paper_id, editions in editions_by_paper.items():
        # Get paper info
        paper_result = await db.execute(select(Paper).where(Paper.id == paper_id))
        paper = paper_result.scalar_one_or_none()

        total_missing = sum((e.citation_count or 0) - (e.harvested_citation_count or 0) for e in editions)
        total_expected = sum(e.citation_count or 0 for e in editions)

        papers_with_incomplete.append({
            "paper_id": paper_id,
            "paper_title": paper.title if paper else f"Paper #{paper_id}",
            "editions_count": len(editions),
            "total_missing_citations": total_missing,
            "total_expected_citations": total_expected,
            "completion_percent": round((total_expected - total_missing) / total_expected * 100, 1) if total_expected > 0 else 0,
            "editions": [
                {
                    "edition_id": e.id,
                    "title": e.title[:60] if e.title else "Unknown",
                    "language": e.language,
                    "citation_count": e.citation_count,
                    "harvested_count": e.harvested_citation_count or 0,
                    "missing": (e.citation_count or 0) - (e.harvested_citation_count or 0),
                    "stall_count": e.harvest_stall_count or 0,
                    "last_harvested": e.last_harvested_at.isoformat() if e.last_harvested_at else None,
                }
                for e in editions
            ]
        })

    # Sort by total missing (most work needed first)
    papers_with_incomplete.sort(key=lambda x: x["total_missing_citations"], reverse=True)

    return {
        "total_papers_with_incomplete": len(papers_with_incomplete),
        "total_editions_with_incomplete": len(incomplete),
        "total_missing_citations": sum(p["total_missing_citations"] for p in papers_with_incomplete),
        "papers": papers_with_incomplete
    }


@app.post("/api/incomplete-harvests/queue-all")
async def queue_all_incomplete_harvests(
    db: AsyncSession = Depends(get_db)
):
    """Manually trigger queueing of jobs for all incomplete harvests.

    Creates one job per paper with incomplete editions. This is useful when
    auto-resume isn't picking things up due to timing or other issues.

    Uses duplicate prevention - won't create jobs for papers that already have
    pending/running jobs.
    """
    from app.services.job_worker import find_incomplete_harvests, create_extract_citations_job

    incomplete = await find_incomplete_harvests(db)

    if not incomplete:
        return {
            "status": "ok",
            "message": "No incomplete harvests found",
            "jobs_queued": 0
        }

    # Group by paper
    editions_by_paper = {}
    for e in incomplete:
        if e.paper_id not in editions_by_paper:
            editions_by_paper[e.paper_id] = []
        editions_by_paper[e.paper_id].append(e)

    # Create jobs using create_extract_citations_job (with duplicate prevention)
    jobs_created = []
    jobs_skipped = []
    for paper_id, editions in editions_by_paper.items():
        total_missing = sum((e.citation_count or 0) - (e.harvested_citation_count or 0) for e in editions)
        edition_ids = [e.id for e in editions]

        before_create = datetime.utcnow()
        job = await create_extract_citations_job(
            db=db,
            paper_id=paper_id,
            edition_ids=edition_ids,
            is_resume=True,
            resume_message=f"Manual queue: {len(editions)} editions, {total_missing:,} citations remaining",
        )

        # Check if job was newly created or existing
        is_new = job.created_at and job.created_at >= before_create
        if is_new:
            jobs_created.append({
                "paper_id": paper_id,
                "job_id": job.id,
                "editions_count": len(editions),
                "missing_citations": total_missing
            })
        else:
            jobs_skipped.append({
                "paper_id": paper_id,
                "existing_job_id": job.id,
                "reason": f"Job {job.id} already {job.status}"
            })

    await db.commit()

    return {
        "status": "ok",
        "message": f"Queued {len(jobs_created)} jobs, skipped {len(jobs_skipped)} (already have pending/running jobs)",
        "jobs_queued": len(jobs_created),
        "jobs_skipped": len(jobs_skipped),
        "jobs": jobs_created,
        "skipped": jobs_skipped
    }


class TargetedYearHarvestRequest(BaseModel):
    year_start: int
    year_end: int


@app.post("/api/papers/{paper_id}/harvest-years")
async def harvest_specific_years(
    paper_id: int,
    request: TargetedYearHarvestRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Harvest citations for a specific year range only.

    This is useful when:
    - Initial harvest stopped at 1990 but work is older
    - Some years were skipped or failed
    - You want to fill in gaps without re-harvesting everything

    The endpoint updates the edition's resume_state to mark all years
    OUTSIDE the requested range as "completed", then queues a harvest job.
    """
    # Validate paper exists
    result = await db.execute(select(Paper).where(Paper.id == paper_id))
    paper = result.scalar_one_or_none()
    if not paper:
        raise HTTPException(status_code=404, detail="Paper not found")

    # Get selected editions with scholar_id
    editions_result = await db.execute(
        select(Edition)
        .where(Edition.paper_id == paper_id)
        .where(Edition.selected == True)
        .where(Edition.scholar_id.isnot(None))
    )
    editions = editions_result.scalars().all()

    if not editions:
        raise HTTPException(status_code=400, detail="No selected editions with scholar_id found")

    current_year = datetime.now().year

    # Build completed_years list (all years EXCEPT the requested range)
    # This tells the harvester to skip these years
    completed_years = []
    for year in range(1950, current_year + 1):
        if year < request.year_start or year > request.year_end:
            completed_years.append(year)

    # Update each edition's resume_state
    editions_updated = []
    for edition in editions:
        resume_state = {
            "mode": "year_by_year",
            "completed_years": completed_years,
            "current_year": request.year_end,  # Start from end of range and go backwards
            "current_page": 0
        }
        edition.harvest_resume_state = json.dumps(resume_state)
        editions_updated.append({
            "id": edition.id,
            "title": edition.title,
            "will_harvest_years": list(range(request.year_start, request.year_end + 1))
        })

    # Check for existing pending/running job (duplicate prevention)
    existing_result = await db.execute(
        select(Job).where(
            Job.paper_id == paper_id,
            Job.job_type == "extract_citations",
            Job.status.in_(["pending", "running"])
        )
    )
    existing_job = existing_result.scalar_one_or_none()
    if existing_job:
        await db.commit()  # Commit the resume_state updates
        return {
            "status": "already_running",
            "message": f"Existing job {existing_job.id} already running for this paper. Resume state updated.",
            "paper_id": paper_id,
            "existing_job_id": existing_job.id,
            "editions_updated": editions_updated
        }

    # Queue harvest job
    job = Job(
        job_type="extract_citations",
        status="pending",
        paper_id=paper_id,
        progress=0,
        progress_message=f"Queued: targeted harvest for years {request.year_start}-{request.year_end}",
    )
    db.add(job)

    await db.commit()

    return {
        "status": "ok",
        "message": f"Queued targeted harvest for years {request.year_start}-{request.year_end}",
        "paper_id": paper_id,
        "year_range": [request.year_start, request.year_end],
        "editions_updated": editions_updated,
        "job_id": job.id
    }


# ============== Admin: Citation Deduplication ==============

@app.post("/api/admin/deduplicate-citations")
async def deduplicate_citations(
    batch_size: int = 1000,
    db: AsyncSession = Depends(get_db)
):
    """
    Remove duplicate citations (same paper_id + scholar_id).
    Keeps the oldest citation (lowest id) for each unique pair.
    Runs in batches to avoid long-running transactions.
    """
    from sqlalchemy import text

    # Count duplicates first
    count_result = await db.execute(text("""
        SELECT COUNT(*) FROM citations c1
        WHERE scholar_id IS NOT NULL
        AND EXISTS (
            SELECT 1 FROM citations c2
            WHERE c2.paper_id = c1.paper_id
            AND c2.scholar_id = c1.scholar_id
            AND c2.id < c1.id
        )
    """))
    total_duplicates = count_result.scalar()

    if total_duplicates == 0:
        return {"status": "ok", "message": "No duplicates found", "deleted": 0}

    # Delete in batches
    deleted_total = 0
    batches = 0
    max_batches = 100  # Safety limit

    while batches < max_batches:
        result = await db.execute(text(f"""
            DELETE FROM citations WHERE id IN (
                SELECT c1.id FROM citations c1
                WHERE c1.scholar_id IS NOT NULL
                AND EXISTS (
                    SELECT 1 FROM citations c2
                    WHERE c2.paper_id = c1.paper_id
                    AND c2.scholar_id = c1.scholar_id
                    AND c2.id < c1.id
                )
                LIMIT {batch_size}
            )
        """))
        deleted = result.rowcount
        await db.commit()

        if deleted == 0:
            break

        deleted_total += deleted
        batches += 1

    return {
        "status": "ok",
        "message": f"Deleted {deleted_total} duplicate citations in {batches} batches",
        "deleted": deleted_total,
        "batches": batches,
        "initial_duplicates": total_duplicates
    }


@app.post("/api/admin/create-citation-unique-index")
async def create_citation_unique_index(
    db: AsyncSession = Depends(get_db)
):
    """
    Create unique index on citations(paper_id, scholar_id).
    This is required for UPSERT ON CONFLICT logic to work properly.
    Run this AFTER deduplication is complete.
    """
    from sqlalchemy import text

    # Check if index already exists
    check_result = await db.execute(text("""
        SELECT 1 FROM pg_indexes
        WHERE indexname = 'ix_citations_paper_scholar_unique'
    """))
    if check_result.scalar():
        return {"status": "ok", "message": "Index already exists"}

    # Create the unique index (non-partial - required for ON CONFLICT to work)
    # Note: Cannot use CONCURRENTLY inside a transaction, so we use regular CREATE INDEX
    # This will briefly lock the table but is safe since we've already deduplicated
    try:
        await db.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS ix_citations_paper_scholar_unique
            ON citations(paper_id, scholar_id)
        """))
        await db.commit()
        return {"status": "ok", "message": "Unique index created successfully"}
    except Exception as e:
        await db.rollback()
        return {"status": "error", "message": str(e)}


@app.post("/api/admin/refresh-citation-counts")
async def refresh_citation_counts(
    paper_id: int = None,
    db: AsyncSession = Depends(get_db)
):
    """
    Refresh cached total_harvested_citations for papers.
    If paper_id is provided, only refresh that paper.
    Otherwise refresh all papers.
    """
    from sqlalchemy import text

    if paper_id:
        # Single paper refresh
        result = await db.execute(text("""
            UPDATE papers SET total_harvested_citations = (
                SELECT COUNT(*) FROM citations WHERE paper_id = :paper_id
            )
            WHERE id = :paper_id
            RETURNING id, total_harvested_citations
        """), {"paper_id": paper_id})
        row = result.fetchone()
        await db.commit()

        if row:
            return {
                "status": "ok",
                "paper_id": row[0],
                "total_harvested_citations": row[1]
            }
        return {"status": "error", "message": f"Paper {paper_id} not found"}
    else:
        # Refresh all papers
        result = await db.execute(text("""
            UPDATE papers p SET total_harvested_citations = (
                SELECT COUNT(*) FROM citations c WHERE c.paper_id = p.id
            )
            RETURNING id
        """))
        updated = result.rowcount
        await db.commit()
        return {"status": "ok", "message": f"Refreshed citation counts for {updated} papers"}


@app.get("/api/admin/partition-runs")
async def get_partition_runs(
    edition_id: int = None,
    paper_id: int = None,
    status: str = None,
    limit: int = 50,
    db: AsyncSession = Depends(get_db)
):
    """Get partition run history for traceability."""
    from sqlalchemy import select
    from .models import PartitionRun, Edition

    query = select(PartitionRun).order_by(PartitionRun.created_at.desc())

    if edition_id:
        query = query.where(PartitionRun.edition_id == edition_id)
    if paper_id:
        # Join to get edition's paper
        query = query.join(Edition).where(Edition.paper_id == paper_id)
    if status:
        query = query.where(PartitionRun.status == status)

    query = query.limit(limit)
    result = await db.execute(query)
    runs = result.scalars().all()

    return {
        "partition_runs": [
            {
                "id": r.id,
                "edition_id": r.edition_id,
                "job_id": r.job_id,
                "year": r.year,
                "depth": r.depth,
                "status": r.status,
                "initial_count": r.initial_count,
                "exclusion_set_count": r.exclusion_set_count,
                "inclusion_set_count": r.inclusion_set_count,
                "terms_tried_count": r.terms_tried_count,
                "terms_kept_count": r.terms_kept_count,
                "final_exclusion_terms": json.loads(r.final_exclusion_terms) if r.final_exclusion_terms else None,
                "exclusion_harvested": r.exclusion_harvested,
                "inclusion_harvested": r.inclusion_harvested,
                "total_harvested": r.total_harvested,
                "total_new_unique": r.total_new_unique,
                "error_message": r.error_message,
                "error_stage": r.error_stage,
                "created_at": r.created_at.isoformat() if r.created_at else None,
                "completed_at": r.completed_at.isoformat() if r.completed_at else None,
            }
            for r in runs
        ]
    }


@app.get("/api/admin/partition-runs/{run_id}")
async def get_partition_run_details(run_id: int, db: AsyncSession = Depends(get_db)):
    """Get full details of a partition run including terms, queries, and LLM calls."""
    from sqlalchemy import select
    from sqlalchemy.orm import selectinload
    from .models import PartitionRun, PartitionTermAttempt, PartitionQuery, PartitionLLMCall

    # Get the run
    result = await db.execute(select(PartitionRun).where(PartitionRun.id == run_id))
    run = result.scalar_one_or_none()
    if not run:
        raise HTTPException(status_code=404, detail="Partition run not found")

    # Get related records
    terms_result = await db.execute(
        select(PartitionTermAttempt)
        .where(PartitionTermAttempt.partition_run_id == run_id)
        .order_by(PartitionTermAttempt.order_tried)
    )
    terms = terms_result.scalars().all()

    queries_result = await db.execute(
        select(PartitionQuery)
        .where(PartitionQuery.partition_run_id == run_id)
        .order_by(PartitionQuery.started_at)
    )
    queries = queries_result.scalars().all()

    llm_calls_result = await db.execute(
        select(PartitionLLMCall)
        .where(PartitionLLMCall.partition_run_id == run_id)
        .order_by(PartitionLLMCall.call_number)
    )
    llm_calls = llm_calls_result.scalars().all()

    return {
        "partition_run": {
            "id": run.id,
            "edition_id": run.edition_id,
            "job_id": run.job_id,
            "year": run.year,
            "depth": run.depth,
            "parent_partition_id": run.parent_partition_id,
            "base_query": run.base_query,
            "status": run.status,
            "initial_count": run.initial_count,
            "target_threshold": run.target_threshold,
            "final_exclusion_query": run.final_exclusion_query,
            "final_inclusion_query": run.final_inclusion_query,
            "exclusion_set_count": run.exclusion_set_count,
            "inclusion_set_count": run.inclusion_set_count,
            "terms_tried_count": run.terms_tried_count,
            "terms_kept_count": run.terms_kept_count,
            "final_exclusion_terms": json.loads(run.final_exclusion_terms) if run.final_exclusion_terms else None,
            "exclusion_harvested": run.exclusion_harvested,
            "inclusion_harvested": run.inclusion_harvested,
            "total_harvested": run.total_harvested,
            "total_new_unique": run.total_new_unique,
            "error_message": run.error_message,
            "error_stage": run.error_stage,
            "created_at": run.created_at.isoformat() if run.created_at else None,
            "terms_started_at": run.terms_started_at.isoformat() if run.terms_started_at else None,
            "terms_completed_at": run.terms_completed_at.isoformat() if run.terms_completed_at else None,
            "exclusion_started_at": run.exclusion_started_at.isoformat() if run.exclusion_started_at else None,
            "exclusion_completed_at": run.exclusion_completed_at.isoformat() if run.exclusion_completed_at else None,
            "inclusion_started_at": run.inclusion_started_at.isoformat() if run.inclusion_started_at else None,
            "inclusion_completed_at": run.inclusion_completed_at.isoformat() if run.inclusion_completed_at else None,
            "completed_at": run.completed_at.isoformat() if run.completed_at else None,
        },
        "term_attempts": [
            {
                "id": t.id,
                "term": t.term,
                "order_tried": t.order_tried,
                "source": t.source,
                "count_before": t.count_before,
                "count_after": t.count_after,
                "reduction": t.reduction,
                "reduction_percent": t.reduction_percent,
                "kept": t.kept,
                "skip_reason": t.skip_reason,
                "latency_ms": t.latency_ms,
                "tested_at": t.tested_at.isoformat() if t.tested_at else None,
            }
            for t in terms
        ],
        "queries": [
            {
                "id": q.id,
                "query_type": q.query_type,
                "scholar_id": q.scholar_id,
                "year": q.year,
                "additional_query": q.additional_query,
                "purpose": q.purpose,
                "expected_count": q.expected_count,
                "actual_count": q.actual_count,
                "pages_requested": q.pages_requested,
                "pages_succeeded": q.pages_succeeded,
                "pages_failed": q.pages_failed,
                "citations_found": q.citations_found,
                "citations_new": q.citations_new,
                "status": q.status,
                "error_message": q.error_message,
                "started_at": q.started_at.isoformat() if q.started_at else None,
                "completed_at": q.completed_at.isoformat() if q.completed_at else None,
                "latency_ms": q.latency_ms,
            }
            for q in queries
        ],
        "llm_calls": [
            {
                "id": c.id,
                "call_number": c.call_number,
                "purpose": c.purpose,
                "model": c.model,
                "edition_title": c.edition_title,
                "year": c.year,
                "current_count": c.current_count,
                "already_excluded_terms": json.loads(c.already_excluded_terms) if c.already_excluded_terms else None,
                "parsed_terms": json.loads(c.parsed_terms) if c.parsed_terms else None,
                "terms_count": c.terms_count,
                "status": c.status,
                "error_message": c.error_message,
                "input_tokens": c.input_tokens,
                "output_tokens": c.output_tokens,
                "started_at": c.started_at.isoformat() if c.started_at else None,
                "completed_at": c.completed_at.isoformat() if c.completed_at else None,
                "latency_ms": c.latency_ms,
            }
            for c in llm_calls
        ],
    }


# ============== External API Endpoints ==============
# These endpoints are designed for service-to-service integration
# They require API key authentication when api_auth_enabled=True


@app.post("/api/external/cross-citations", response_model=BatchCrossResult)
async def batch_cross_citations(
    request: BatchCrossRequest,
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    """
    Get citations that cite multiple papers from the given list.
    Returns papers ranked by how many of the target papers they cite.

    This is the key endpoint for finding papers that are cited by most/all
    of your target papers - useful for identifying foundational works.
    """
    if not request.paper_ids:
        raise HTTPException(status_code=400, detail="paper_ids cannot be empty")

    # Single efficient query to find cross-citations
    # Uses GROUP BY to aggregate citations across papers
    query = text("""
        SELECT
            c.scholar_id,
            MIN(c.title) as title,
            MIN(c.authors) as authors,
            MIN(c.year) as year,
            MIN(c.venue) as venue,
            MIN(c.link) as link,
            MIN(c.citation_count) as citation_count,
            COUNT(DISTINCT c.paper_id) as cites_count,
            ARRAY_AGG(DISTINCT c.paper_id) as cites_papers
        FROM citations c
        WHERE c.paper_id = ANY(:paper_ids)
        AND c.scholar_id IS NOT NULL
        GROUP BY c.scholar_id
        HAVING COUNT(DISTINCT c.paper_id) >= :min_intersection
        ORDER BY cites_count DESC, citation_count DESC
        LIMIT 1000
    """)

    result = await db.execute(
        query,
        {"paper_ids": request.paper_ids, "min_intersection": request.min_intersection}
    )
    rows = result.fetchall()

    cross_citations = [
        CrossCitationItem(
            scholar_id=row.scholar_id,
            title=row.title,
            authors=row.authors,
            year=row.year,
            venue=row.venue,
            link=row.link,
            cites_count=row.cites_count,
            cites_papers=list(row.cites_papers),
            own_citation_count=row.citation_count or 0,
        )
        for row in rows
    ]

    return BatchCrossResult(
        paper_ids=request.paper_ids,
        total_unique_citations=len(cross_citations),
        cross_citations=cross_citations,
    )


@app.post("/api/external/analyze", response_model=ExternalAnalyzeResponse)
async def external_analyze_papers(
    request: ExternalAnalyzeRequest,
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    """
    Submit papers for full analysis pipeline via external API.

    This endpoint:
    1. Creates a collection/dossier if specified
    2. Adds all papers
    3. Queues jobs for edition discovery and citation extraction
    4. Returns job IDs for tracking
    5. Optionally sends webhook when complete

    Options:
    - discover_editions: bool (default True) - Run edition discovery
    - harvest_citations: bool (default True) - Harvest citations after discovery
    - compute_cross_citations: bool (default False) - Compute cross-citations after harvest
    """
    from .services.job_worker import create_extract_citations_job

    options = request.options or {}
    discover_editions = options.get("discover_editions", True)
    harvest_citations = options.get("harvest_citations", True)

    # Create collection/dossier if specified
    collection_id = None
    dossier_id = None

    if request.collection_name:
        # Check if collection exists
        result = await db.execute(
            select(Collection).where(Collection.name == request.collection_name)
        )
        collection = result.scalar_one_or_none()

        if not collection:
            collection = Collection(name=request.collection_name)
            db.add(collection)
            await db.flush()

        collection_id = collection.id

        # Create dossier if specified
        if request.dossier_name:
            result = await db.execute(
                select(Dossier).where(
                    Dossier.collection_id == collection_id,
                    Dossier.name == request.dossier_name
                )
            )
            dossier = result.scalar_one_or_none()

            if not dossier:
                dossier = Dossier(
                    collection_id=collection_id,
                    name=request.dossier_name
                )
                db.add(dossier)
                await db.flush()

            dossier_id = dossier.id

    # Add papers
    paper_ids = []
    for paper_input in request.papers:
        paper = Paper(
            title=paper_input.title,
            authors=paper_input.authors,
            year=paper_input.year,
            status="pending",
            collection_id=collection_id,
            dossier_id=dossier_id,
        )
        db.add(paper)
        await db.flush()
        paper_ids.append(paper.id)

    # Create a master job to track the overall process
    master_job = Job(
        job_type="external_analyze",
        status="pending",
        priority=5,
        params=json.dumps({
            "paper_ids": paper_ids,
            "discover_editions": discover_editions,
            "harvest_citations": harvest_citations,
            "options": options,
        }),
        callback_url=request.callback_url,
        callback_secret=request.callback_secret,
    )
    db.add(master_job)
    await db.flush()

    # Queue edition discovery jobs for each paper
    if discover_editions:
        for paper_id in paper_ids:
            job = Job(
                paper_id=paper_id,
                job_type="discover_editions",
                status="pending",
                priority=3,
                params=json.dumps({
                    "language_strategy": "major_languages",
                    "parent_job_id": master_job.id,
                }),
            )
            db.add(job)

    await db.commit()

    return ExternalAnalyzeResponse(
        job_id=master_job.id,
        paper_ids=paper_ids,
        status="pending",
        message=f"Queued {len(paper_ids)} papers for analysis",
        collection_id=collection_id,
        dossier_id=dossier_id,
    )


@app.get("/api/external/papers/{paper_id}/citations")
async def external_get_paper_citations(
    paper_id: int,
    limit: int = 100,
    offset: int = 0,
    min_citation_count: int = 0,
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    """
    Get all citations for a paper with full Google Scholar data.

    Returns citations sorted by their own citation count (most cited first).
    """
    # Verify paper exists
    result = await db.execute(select(Paper).where(Paper.id == paper_id))
    paper = result.scalar_one_or_none()
    if not paper:
        raise HTTPException(status_code=404, detail=f"Paper {paper_id} not found")

    # Get citations with optional filtering
    query = (
        select(Citation)
        .where(Citation.paper_id == paper_id)
        .where(Citation.citation_count >= min_citation_count)
        .order_by(Citation.citation_count.desc())
        .offset(offset)
        .limit(limit)
    )

    result = await db.execute(query)
    citations = result.scalars().all()

    # Get total count
    count_result = await db.execute(
        select(func.count(Citation.id))
        .where(Citation.paper_id == paper_id)
        .where(Citation.citation_count >= min_citation_count)
    )
    total_count = count_result.scalar() or 0

    return {
        "paper_id": paper_id,
        "paper_title": paper.title,
        "total_citations": total_count,
        "returned_count": len(citations),
        "offset": offset,
        "limit": limit,
        "citations": [
            {
                "scholar_id": c.scholar_id,
                "title": c.title,
                "authors": c.authors,
                "year": c.year,
                "venue": c.venue,
                "link": c.link,
                "citation_count": c.citation_count,
                "abstract": c.abstract,
            }
            for c in citations
        ],
    }


@app.get("/api/external/jobs/{job_id}")
async def external_get_job_status(
    job_id: int,
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(verify_api_key),
):
    """
    Get status of a job, including webhook callback status.
    """
    result = await db.execute(select(Job).where(Job.id == job_id))
    job = result.scalar_one_or_none()

    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    return {
        "id": job.id,
        "job_type": job.job_type,
        "status": job.status,
        "progress": job.progress,
        "progress_message": job.progress_message,
        "paper_id": job.paper_id,
        "result": json.loads(job.result) if job.result else None,
        "error": job.error,
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "started_at": job.started_at.isoformat() if job.started_at else None,
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
        "callback_url": job.callback_url,
        "callback_sent_at": job.callback_sent_at.isoformat() if job.callback_sent_at else None,
        "callback_error": job.callback_error,
    }


@app.get("/api/external/health")
async def external_health_check(api_key: str = Depends(verify_api_key)):
    """
    Health check for external API. Verifies API key is valid.
    """
    return {
        "status": "healthy",
        "service": "the-referee",
        "api_auth_enabled": settings.api_auth_enabled,
        "timestamp": datetime.utcnow().isoformat(),
    }


# ============== Citation Buffer Admin Endpoints ==============

@app.get("/api/admin/citation-buffer/stats")
async def get_citation_buffer_stats():
    """
    Get statistics about the citation buffer (local saves pending DB sync).
    """
    from .services.citation_buffer import get_buffer

    buffer = get_buffer()
    stats = buffer.get_buffer_stats()

    return {
        "buffer_stats": stats,
        "description": "Citation buffer holds pages that failed DB save for retry",
    }


@app.post("/api/admin/citation-buffer/retry")
async def retry_citation_buffer():
    """
    Manually trigger retry of failed citation saves from the buffer.
    """
    from .services.citation_buffer import retry_failed_saves

    retried = await retry_failed_saves()

    return {
        "success": True,
        "pages_retried": retried,
        "message": f"Retried {retried} pages from citation buffer",
    }


@app.post("/api/admin/citation-buffer/cleanup")
async def cleanup_citation_buffer(max_age_hours: int = 24):
    """
    Clean up old buffer files (default: older than 24 hours).
    """
    from .services.citation_buffer import get_buffer

    buffer = get_buffer()
    removed = buffer.cleanup_old_buffers(max_age_hours=max_age_hours)

    return {
        "success": True,
        "files_removed": removed,
        "message": f"Removed {removed} buffer files older than {max_age_hours} hours",
    }


@app.get("/api/dashboard/activity-stats")
async def get_activity_stats_endpoint(db: AsyncSession = Depends(get_db)):
    """
    Get activity statistics for the dashboard.

    Returns counts of Oxylabs API calls, pages fetched, and citations saved
    for 15min, 1hr, 6hr, and 24hr time periods.
    """
    from .services.api_logger import get_activity_stats

    stats = await get_activity_stats(db)

    return {
        "success": True,
        "stats": stats,
    }


# ============== Health Monitor Endpoints ==============

@app.get("/api/health-monitor/status")
async def health_monitor_status(
    limit: int = 20,
    db: AsyncSession = Depends(get_db)
):
    """
    Get recent health monitor logs and current status.
    """
    from .services.health_monitor import get_recent_logs
    from .config import get_settings

    settings = get_settings()
    logs = await get_recent_logs(db, limit=limit)

    return {
        "enabled": settings.health_monitor_enabled,
        "dry_run": settings.health_monitor_dry_run,
        "interval_minutes": settings.health_monitor_interval_minutes,
        "recent_logs": [
            {
                "id": log.id,
                "trigger_reason": log.trigger_reason,
                "active_jobs": log.active_jobs_count,
                "citations_15min": log.citations_15min,
                "diagnosis": log.llm_diagnosis,
                "root_cause": log.llm_root_cause,
                "confidence": log.llm_confidence,
                "action": log.action_type,
                "action_executed": log.action_executed,
                "action_result": log.action_result,
                "action_error": log.action_error,
                "llm_duration_ms": log.llm_call_duration_ms,
                "created_at": log.created_at.isoformat() if log.created_at else None,
            }
            for log in logs
        ],
    }


@app.post("/api/health-monitor/trigger")
async def health_monitor_trigger():
    """
    Manually trigger a health check and LLM diagnosis.
    Useful for testing or forcing an immediate diagnosis.
    """
    from .services.health_monitor import trigger_manual_check

    log_entry = await trigger_manual_check()

    if log_entry:
        return {
            "success": True,
            "log_id": log_entry.id,
            "diagnosis": log_entry.llm_diagnosis,
            "root_cause": log_entry.llm_root_cause,
            "confidence": log_entry.llm_confidence,
            "action": log_entry.action_type,
            "action_executed": log_entry.action_executed,
            "action_result": log_entry.action_result,
            "action_error": log_entry.action_error,
        }
    else:
        return {
            "success": False,
            "error": "Failed to run health check",
        }


@app.post("/api/health-monitor/toggle")
async def health_monitor_toggle(enabled: bool = True):
    """
    Enable or disable the health monitor.
    Note: This only affects the current runtime; restart will reset to config value.
    """
    from .config import get_settings

    settings = get_settings()
    # Note: This modifies the cached settings object
    settings.health_monitor_enabled = enabled

    return {
        "success": True,
        "enabled": settings.health_monitor_enabled,
        "message": f"Health monitor {'enabled' if enabled else 'disabled'} (runtime only)",
    }


# ============== Thinker Bibliographies Endpoints ==============

@app.post("/api/thinkers", response_model=ThinkerResponse)
async def create_thinker(
    request: ThinkerCreate,
    db: AsyncSession = Depends(get_db)
):
    """
    Create a new thinker for bibliography harvesting.

    This will:
    1. Use LLM to disambiguate the thinker (e.g., "Marcuse" → "Herbert Marcuse")
    2. Create the thinker record with biographical info
    3. Return disambiguation results if confirmation is needed
    """
    from .services.thinker_service import get_thinker_service

    service = get_thinker_service(db)
    result = await service.create_thinker(request.name)

    if not result.get("success"):
        if result.get("existing_thinker_id"):
            raise HTTPException(
                status_code=409,
                detail=result.get("error", "Thinker already exists")
            )
        raise HTTPException(status_code=500, detail=result.get("error", "Failed to create thinker"))

    thinker = await service.get_thinker(result["thinker_id"])
    return await service.thinker_to_response(thinker)


@app.get("/api/thinkers", response_model=List[ThinkerResponse])
async def list_thinkers(db: AsyncSession = Depends(get_db)):
    """List all thinkers"""
    from .services.thinker_service import get_thinker_service

    service = get_thinker_service(db)
    thinkers = await service.list_thinkers()

    return [await service.thinker_to_response(t) for t in thinkers]


@app.get("/api/thinkers/{thinker_id}", response_model=ThinkerDetail)
async def get_thinker(thinker_id: int, db: AsyncSession = Depends(get_db)):
    """Get thinker details with works and harvest runs"""
    from .services.thinker_service import get_thinker_service

    service = get_thinker_service(db)
    thinker = await service.get_thinker(thinker_id)

    if not thinker:
        raise HTTPException(status_code=404, detail="Thinker not found")

    # Get basic response
    response = await service.thinker_to_response(thinker)

    # Add works
    works_result = await db.execute(
        select(ThinkerWork)
        .where(ThinkerWork.thinker_id == thinker_id)
        .order_by(ThinkerWork.citation_count.desc())
    )
    works = works_result.scalars().all()

    # Add harvest runs
    runs_result = await db.execute(
        select(ThinkerHarvestRun)
        .where(ThinkerHarvestRun.thinker_id == thinker_id)
        .order_by(ThinkerHarvestRun.started_at.desc())
    )
    harvest_runs = runs_result.scalars().all()

    # Add recent LLM calls
    llm_result = await db.execute(
        select(ThinkerLLMCall)
        .where(ThinkerLLMCall.thinker_id == thinker_id)
        .order_by(ThinkerLLMCall.started_at.desc())
        .limit(10)
    )
    llm_calls = llm_result.scalars().all()

    return ThinkerDetail(
        **response,
        works=[ThinkerWorkResponse(
            id=w.id,
            thinker_id=w.thinker_id,
            paper_id=w.paper_id,
            scholar_id=w.scholar_id,
            title=w.title,
            authors_raw=w.authors_raw,
            year=w.year,
            citation_count=w.citation_count,
            decision=w.decision,
            confidence=w.confidence,
            reason=w.reason,
            is_translation=w.is_translation,
            canonical_work_id=w.canonical_work_id,
            original_language=w.original_language,
            detected_language=w.detected_language,
            citations_harvested=w.citations_harvested,
            harvest_job_id=w.harvest_job_id,
            created_at=w.created_at,
        ) for w in works],
        work_groups=[],  # TODO: Group by canonical_work_id
        harvest_runs=[ThinkerHarvestRunResponse(
            id=r.id,
            thinker_id=r.thinker_id,
            query_used=r.query_used,
            variant_type=r.variant_type,
            pages_fetched=r.pages_fetched,
            results_processed=r.results_processed,
            results_accepted=r.results_accepted,
            results_rejected=r.results_rejected,
            results_uncertain=r.results_uncertain,
            status=r.status,
            started_at=r.started_at,
            completed_at=r.completed_at,
        ) for r in harvest_runs],
        recent_llm_calls=[ThinkerLLMCallResponse(
            id=c.id,
            thinker_id=c.thinker_id,
            workflow=c.workflow,
            model=c.model,
            status=c.status,
            input_tokens=c.input_tokens,
            output_tokens=c.output_tokens,
            thinking_tokens=c.thinking_tokens,
            latency_ms=c.latency_ms,
            started_at=c.started_at,
            completed_at=c.completed_at,
        ) for c in llm_calls],
    )


@app.delete("/api/thinkers/{thinker_id}")
async def delete_thinker(thinker_id: int, db: AsyncSession = Depends(get_db)):
    """Delete a thinker and all related data"""
    from .services.thinker_service import get_thinker_service

    service = get_thinker_service(db)
    result = await service.delete_thinker(thinker_id)

    if not result.get("success"):
        raise HTTPException(status_code=404, detail=result.get("error", "Thinker not found"))

    return result


@app.patch("/api/thinkers/{thinker_id}", response_model=ThinkerResponse)
async def update_thinker(
    thinker_id: int,
    request: ThinkerUpdate,
    db: AsyncSession = Depends(get_db)
):
    """Update thinker fields (status, name, bio, domains)"""
    from sqlalchemy import select
    from .models import Thinker
    from datetime import datetime
    import json

    result = await db.execute(select(Thinker).where(Thinker.id == thinker_id))
    thinker = result.scalar_one_or_none()

    if not thinker:
        raise HTTPException(status_code=404, detail="Thinker not found")

    # Update fields if provided
    if request.status is not None:
        valid_statuses = ["pending", "disambiguated", "harvesting", "complete"]
        if request.status not in valid_statuses:
            raise HTTPException(status_code=400, detail=f"Invalid status. Must be one of: {valid_statuses}")
        thinker.status = request.status
        # Set timestamp for complete status
        if request.status == "complete" and not thinker.harvest_completed_at:
            thinker.harvest_completed_at = datetime.utcnow()

    if request.canonical_name is not None:
        thinker.canonical_name = request.canonical_name

    if request.bio is not None:
        thinker.bio = request.bio

    if request.domains is not None:
        thinker.domains = json.dumps(request.domains)

    await db.commit()
    await db.refresh(thinker)

    # Format response
    return ThinkerResponse(
        id=thinker.id,
        canonical_name=thinker.canonical_name,
        birth_death=thinker.birth_death,
        bio=thinker.bio,
        domains=json.loads(thinker.domains) if thinker.domains else [],
        notable_works=json.loads(thinker.notable_works) if thinker.notable_works else [],
        name_variants=json.loads(thinker.name_variants) if thinker.name_variants else [],
        status=thinker.status,
        works_discovered=thinker.works_discovered or 0,
        works_harvested=thinker.works_harvested or 0,
        total_citations=thinker.total_citations or 0,
        created_at=thinker.created_at,
        disambiguated_at=thinker.disambiguated_at,
        harvest_started_at=thinker.harvest_started_at,
        harvest_completed_at=thinker.harvest_completed_at,
    )


@app.post("/api/thinkers/{thinker_id}/refresh-stats")
async def refresh_thinker_stats(thinker_id: int, db: AsyncSession = Depends(get_db)):
    """
    Recalculate thinker stats from actual thinker_works data.

    Useful for fixing inconsistent state after job crashes.
    """
    from sqlalchemy import select, func
    from .models import Thinker, ThinkerWork

    # Get thinker
    result = await db.execute(select(Thinker).where(Thinker.id == thinker_id))
    thinker = result.scalar_one_or_none()
    if not thinker:
        raise HTTPException(status_code=404, detail="Thinker not found")

    # Count works by decision
    counts_result = await db.execute(
        select(
            ThinkerWork.decision,
            func.count().label("count")
        )
        .where(ThinkerWork.thinker_id == thinker_id)
        .group_by(ThinkerWork.decision)
    )
    counts = {row.decision: row.count for row in counts_result}

    accepted = counts.get("accepted", 0)
    uncertain = counts.get("uncertain", 0)
    rejected = counts.get("rejected", 0)

    # Update thinker stats
    old_discovered = thinker.works_discovered
    thinker.works_discovered = accepted + uncertain  # Include uncertain for review

    # Count harvested works (those with citations_harvested=True)
    harvested_result = await db.execute(
        select(func.count())
        .where(ThinkerWork.thinker_id == thinker_id)
        .where(ThinkerWork.citations_harvested == True)
    )
    thinker.works_harvested = harvested_result.scalar() or 0

    await db.commit()

    return {
        "thinker_id": thinker_id,
        "works_discovered": thinker.works_discovered,
        "works_harvested": thinker.works_harvested,
        "breakdown": {
            "accepted": accepted,
            "uncertain": uncertain,
            "rejected": rejected,
        },
        "previous_discovered": old_discovered,
    }


@app.get("/api/thinkers/{thinker_id}/analytics")
async def get_thinker_analytics(thinker_id: int, db: AsyncSession = Depends(get_db)):
    """
    Get comprehensive citation analytics for a thinker.

    Returns:
    - Top citing papers (most influential papers citing this thinker)
    - Top citing authors (scholars who cite this thinker most)
    - Most cited works (this thinker's "greatest hits")
    - Top venues (where this thinker is cited - shows disciplinary reach)
    - Citations by year (trend over time)
    """
    from sqlalchemy import select, func, distinct
    from .models import Thinker, ThinkerWork, Paper, Citation
    from .schemas import ThinkerAnalyticsResponse, CitingPaper, CitingAuthor, MostCitedWork, TopVenue, YearCitations

    # Get thinker
    result = await db.execute(select(Thinker).where(Thinker.id == thinker_id))
    thinker = result.scalar_one_or_none()
    if not thinker:
        raise HTTPException(status_code=404, detail="Thinker not found")

    # Get paper IDs for this thinker's accepted works
    paper_ids_result = await db.execute(
        select(ThinkerWork.paper_id)
        .where(ThinkerWork.thinker_id == thinker_id)
        .where(ThinkerWork.decision == "accepted")
        .where(ThinkerWork.paper_id.isnot(None))
    )
    paper_ids = [r[0] for r in paper_ids_result.fetchall()]

    if not paper_ids:
        # No papers harvested yet
        return ThinkerAnalyticsResponse(
            thinker_id=thinker_id,
            thinker_name=thinker.canonical_name,
            total_citations=0,
            total_works=0,
            unique_citing_papers=0,
            unique_citing_authors=0,
            unique_venues=0,
        )

    # 1. Top Citing Papers (most influential papers that cite this thinker)
    top_papers_result = await db.execute(
        select(
            Citation.id,
            Citation.title,
            Citation.authors,
            Citation.year,
            Citation.venue,
            Citation.link,
            Citation.citation_count,
            func.count(distinct(Citation.paper_id)).label("cites_works")
        )
        .where(Citation.paper_id.in_(paper_ids))
        .where(Citation.scholar_id.isnot(None))
        .group_by(Citation.id, Citation.scholar_id, Citation.title, Citation.authors, Citation.year, Citation.venue, Citation.link, Citation.citation_count)
        .order_by(Citation.citation_count.desc().nulls_last())
        .limit(20)
    )
    # Parse author and venue from Google Scholar format: "Author Names - Venue, Year - source.com"
    def parse_citation_parts(raw_authors: str) -> tuple:
        """Returns (authors, venue) parsed from Google Scholar author string"""
        if not raw_authors:
            return ("Unknown", None)
        parts = raw_authors.split(" - ")
        authors = parts[0].strip() if parts else "Unknown"
        venue = parts[1].strip() if len(parts) > 1 else None
        # Clean up venue - remove year and trailing parts
        if venue and ", " in venue:
            venue = venue.split(", ")[0]  # Take first part before year
        return (authors, venue)

    top_citing_papers = []
    for r in top_papers_result.fetchall():
        parsed_authors, parsed_venue = parse_citation_parts(r.authors)
        top_citing_papers.append(CitingPaper(
            citation_id=r.id,
            title=r.title,
            authors=parsed_authors,
            year=r.year,
            venue=parsed_venue or r.venue,  # Use parsed venue or original
            link=r.link,
            citation_count=r.citation_count or 0,
            cites_works=r.cites_works
        ))

    # 2. Top Citing Authors (with LLM-powered disaggregation and self-citation detection)
    from .services.author_analytics import process_citing_authors
    from sqlalchemy.orm import aliased
    from sqlalchemy import text

    # Parse author names from Google Scholar format: "Author Names - Venue, Year - source.com"
    def parse_author_name(raw_authors: str) -> str:
        if not raw_authors:
            return "Unknown"
        parts = raw_authors.split(" - ")
        author_part = parts[0].strip()
        if author_part.endswith("…"):
            author_part = author_part[:-1] + "et al."
        return author_part if author_part else "Unknown"

    # Fetch citations grouped by parsed author name, including citation IDs for paper lookup
    top_authors_result = await db.execute(
        select(
            Citation.authors,
            func.count().label("citation_count"),
            func.count(distinct(Citation.scholar_id)).label("papers_count"),
            func.array_agg(Citation.id).label("citation_ids")
        )
        .where(Citation.paper_id.in_(paper_ids))
        .where(Citation.authors.isnot(None))
        .where(Citation.authors != "")
        .group_by(Citation.authors)
        .order_by(func.count().desc())
        .limit(100)  # Back to 100 now that we use heuristics instead of LLM
    )

    # Pre-parse author names and collect data for LLM processing
    raw_author_groups = []
    for r in top_authors_result.fetchall():
        parsed_name = parse_author_name(r.authors)
        raw_author_groups.append({
            "authors": parsed_name,
            "citation_count": r.citation_count,
            "papers_count": r.papers_count,
            "citation_ids": list(r.citation_ids) if r.citation_ids else []
        })

    # Use LLM to disaggregate multi-author entries and detect self-citations
    import logging
    analytics_logger = logging.getLogger("analytics")
    analytics_logger.info(f"Calling LLM for {len(raw_author_groups)} author groups, thinker: {thinker.canonical_name}")

    llm_result = await process_citing_authors(
        thinker_name=thinker.canonical_name,
        raw_author_groups=raw_author_groups
    )

    analytics_logger.info(f"LLM result: processed={llm_result.get('llm_processed')}, error={llm_result.get('error')}, authors={len(llm_result.get('individual_authors', []))}")

    # Build final author list from LLM results
    if llm_result.get("llm_processed") and "individual_authors" in llm_result:
        # LLM successfully processed - use disaggregated/normalized authors
        sorted_authors = sorted(
            llm_result["individual_authors"],
            key=lambda x: x.get("total_citation_count", x.get("citation_count", 0)),
            reverse=True
        )[:20]
        top_citing_authors = [
            CitingAuthor(
                author=a.get("normalized_name", a.get("authors", "Unknown")),
                citation_count=a.get("total_citation_count", a.get("citation_count", 0)),
                papers_count=a.get("total_papers_count", a.get("papers_count", 0)),
                is_self_citation=a.get("is_self_citation", False),
                confidence=a.get("confidence", 1.0),
                citation_ids=a.get("citation_ids", [])[:100]  # Limit to prevent huge payloads
            )
            for a in sorted_authors
        ]
    else:
        # Fallback: aggregate by parsed name without LLM
        author_aggregates = {}
        for g in raw_author_groups:
            name = g["authors"]
            if name not in author_aggregates:
                author_aggregates[name] = {"citation_count": 0, "papers_count": 0, "citation_ids": []}
            author_aggregates[name]["citation_count"] += g["citation_count"]
            author_aggregates[name]["papers_count"] += g["papers_count"]
            author_aggregates[name]["citation_ids"].extend(g["citation_ids"])

        sorted_authors = sorted(author_aggregates.items(), key=lambda x: x[1]["citation_count"], reverse=True)[:20]
        top_citing_authors = [
            CitingAuthor(
                author=name,
                citation_count=data["citation_count"],
                papers_count=data["papers_count"],
                citation_ids=data["citation_ids"][:100]
            )
            for name, data in sorted_authors
        ]

    # 3. Most Cited Works (this thinker's works ranked by citations received)
    most_cited_result = await db.execute(
        select(
            ThinkerWork.id,
            ThinkerWork.paper_id,
            ThinkerWork.scholar_id,
            ThinkerWork.title,
            ThinkerWork.authors_raw,
            ThinkerWork.year,
            ThinkerWork.link,
            func.count(Citation.id).label("citations_received")
        )
        .outerjoin(Paper, ThinkerWork.paper_id == Paper.id)
        .outerjoin(Citation, Citation.paper_id == Paper.id)
        .where(ThinkerWork.thinker_id == thinker_id)
        .where(ThinkerWork.decision == "accepted")
        .group_by(ThinkerWork.id, ThinkerWork.paper_id, ThinkerWork.scholar_id, ThinkerWork.title, ThinkerWork.authors_raw, ThinkerWork.year, ThinkerWork.link)
        .order_by(func.count(Citation.id).desc())
        .limit(20)
    )
    most_cited_works = [
        MostCitedWork(
            work_id=r.id,
            paper_id=r.paper_id,
            scholar_id=r.scholar_id,
            title=r.title,
            authors=r.authors_raw,
            year=r.year,
            link=r.link,
            citations_received=r.citations_received
        )
        for r in most_cited_result.fetchall()
    ]

    # 4. Top Venues (where is this thinker cited)
    top_venues_result = await db.execute(
        select(
            Citation.venue,
            func.count().label("citation_count"),
            func.count(distinct(Citation.scholar_id)).label("papers_count")
        )
        .where(Citation.paper_id.in_(paper_ids))
        .where(Citation.venue.isnot(None))
        .where(Citation.venue != "")
        .group_by(Citation.venue)
        .order_by(func.count().desc())
        .limit(20)
    )
    top_venues = [
        TopVenue(
            venue=r.venue,
            citation_count=r.citation_count,
            papers_count=r.papers_count
        )
        for r in top_venues_result.fetchall()
    ]

    # 5. Citations by Year
    by_year_result = await db.execute(
        select(
            Citation.year,
            func.count().label("count")
        )
        .where(Citation.paper_id.in_(paper_ids))
        .where(Citation.year.isnot(None))
        .group_by(Citation.year)
        .order_by(Citation.year)
    )
    citations_by_year = [
        YearCitations(year=r.year, count=r.count)
        for r in by_year_result.fetchall()
    ]

    # Summary stats
    total_citations_result = await db.execute(
        select(func.count()).where(Citation.paper_id.in_(paper_ids))
    )
    total_citations = total_citations_result.scalar() or 0

    unique_papers_result = await db.execute(
        select(func.count(distinct(Citation.scholar_id)))
        .where(Citation.paper_id.in_(paper_ids))
        .where(Citation.scholar_id.isnot(None))
    )
    unique_citing_papers = unique_papers_result.scalar() or 0

    unique_authors_result = await db.execute(
        select(func.count(distinct(Citation.authors)))
        .where(Citation.paper_id.in_(paper_ids))
        .where(Citation.authors.isnot(None))
    )
    unique_citing_authors = unique_authors_result.scalar() or 0

    unique_venues_result = await db.execute(
        select(func.count(distinct(Citation.venue)))
        .where(Citation.paper_id.in_(paper_ids))
        .where(Citation.venue.isnot(None))
        .where(Citation.venue != "")
    )
    unique_venues = unique_venues_result.scalar() or 0

    return ThinkerAnalyticsResponse(
        thinker_id=thinker_id,
        thinker_name=thinker.canonical_name,
        total_citations=total_citations,
        total_works=len(paper_ids),
        unique_citing_papers=unique_citing_papers,
        unique_citing_authors=unique_citing_authors,
        unique_venues=unique_venues,
        top_citing_papers=top_citing_papers,
        top_citing_authors=top_citing_authors,
        most_cited_works=most_cited_works,
        top_venues=top_venues,
        citations_by_year=citations_by_year,
        debug_llm_processed=llm_result.get("llm_processed"),
        debug_llm_error=llm_result.get("error"),
    )


class AuthorPapersRequest(BaseModel):
    """Request to get papers by a citing author"""
    citation_ids: List[int]


class AuthorPaper(BaseModel):
    """A paper written by a citing author"""
    citation_id: int
    title: Optional[str]
    authors: Optional[str]
    year: Optional[int]
    venue: Optional[str]
    citation_count: Optional[int]
    scholar_id: Optional[str]
    url: Optional[str]


@app.post("/api/thinkers/{thinker_id}/author-papers")
async def get_author_papers(
    thinker_id: int,
    request: AuthorPapersRequest,
    db: AsyncSession = Depends(get_db)
) -> List[AuthorPaper]:
    """
    Get papers for a specific citing author using citation IDs.

    This endpoint takes citation IDs from the analytics response and returns
    the actual paper details so users can see what specific papers an author
    wrote that cite the thinker's work.
    """
    from sqlalchemy import select
    from .models import Citation

    if not request.citation_ids:
        return []

    # Limit to prevent abuse
    citation_ids = request.citation_ids[:100]

    result = await db.execute(
        select(Citation)
        .where(Citation.id.in_(citation_ids))
        .order_by(Citation.citation_count.desc().nulls_last())
    )

    papers = []
    seen_scholar_ids = set()
    for citation in result.scalars().all():
        # Dedupe by scholar_id if available
        if citation.scholar_id and citation.scholar_id in seen_scholar_ids:
            continue
        if citation.scholar_id:
            seen_scholar_ids.add(citation.scholar_id)

        # Parse authors from raw string
        authors = citation.authors
        if authors and " - " in authors:
            authors = authors.split(" - ")[0].strip()

        papers.append(AuthorPaper(
            citation_id=citation.id,
            title=citation.title,
            authors=authors,
            year=citation.year,
            venue=citation.venue,
            citation_count=citation.citation_count,
            scholar_id=citation.scholar_id,
            url=citation.link  # Note: model has 'link' not 'url'
        ))

    return papers


@app.post("/api/thinkers/{thinker_id}/confirm")
async def confirm_thinker_disambiguation(
    thinker_id: int,
    request: ThinkerConfirmRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Confirm disambiguation choice for a thinker.

    Call this after creating a thinker if disambiguation required confirmation.
    """
    from .services.thinker_service import get_thinker_service

    service = get_thinker_service(db)
    result = await service.confirm_disambiguation(
        thinker_id,
        candidate_index=request.candidate_index,
        custom_domains=request.custom_domains,
    )

    if not result.get("success"):
        raise HTTPException(status_code=400, detail=result.get("error", "Failed to confirm"))

    return result


@app.post("/api/thinkers/{thinker_id}/generate-variants", response_model=NameVariantsResponse)
async def generate_thinker_variants(
    thinker_id: int,
    db: AsyncSession = Depends(get_db)
):
    """
    Generate name variants for author search queries.

    This uses LLM to generate search query variants like:
    - author:"Herbert Marcuse" (full name)
    - author:"H Marcuse" (initial + surname)
    - author:"马尔库塞" (Chinese transliteration)
    - etc.
    """
    from .services.thinker_service import get_thinker_service

    service = get_thinker_service(db)
    thinker = await service.get_thinker(thinker_id)

    if not thinker:
        raise HTTPException(status_code=404, detail="Thinker not found")

    if thinker.status == "pending":
        raise HTTPException(
            status_code=400,
            detail="Thinker disambiguation not confirmed. Call /confirm first."
        )

    result = await service.generate_name_variants(thinker)

    return NameVariantsResponse(
        thinker_id=result["thinker_id"],
        canonical_name=result["canonical_name"],
        variants=[
            {
                "query": v.get("query", ""),
                "variant_type": v.get("variant_type", "unknown"),
                "language": v.get("language"),
            }
            for v in result.get("variants", [])
        ],
    )


@app.get("/api/thinkers/{thinker_id}/works", response_model=List[ThinkerWorkResponse])
async def list_thinker_works(
    thinker_id: int,
    decision: Optional[str] = None,  # Filter: accepted, rejected, uncertain
    db: AsyncSession = Depends(get_db)
):
    """List works discovered for a thinker"""
    # Verify thinker exists
    thinker = await db.get(Thinker, thinker_id)
    if not thinker:
        raise HTTPException(status_code=404, detail="Thinker not found")

    # Build query
    query = select(ThinkerWork).where(ThinkerWork.thinker_id == thinker_id)
    if decision:
        query = query.where(ThinkerWork.decision == decision)
    query = query.order_by(ThinkerWork.citation_count.desc())

    result = await db.execute(query)
    works = result.scalars().all()

    return [ThinkerWorkResponse(
        id=w.id,
        thinker_id=w.thinker_id,
        paper_id=w.paper_id,
        scholar_id=w.scholar_id,
        title=w.title,
        authors_raw=w.authors_raw,
        year=w.year,
        citation_count=w.citation_count,
        decision=w.decision,
        confidence=w.confidence,
        reason=w.reason,
        is_translation=w.is_translation,
        canonical_work_id=w.canonical_work_id,
        original_language=w.original_language,
        detected_language=w.detected_language,
        citations_harvested=w.citations_harvested,
        harvest_job_id=w.harvest_job_id,
        created_at=w.created_at,
    ) for w in works]


@app.post("/api/thinkers/quick-add", response_model=ThinkerQuickAddResponse)
async def quick_add_thinker(
    request: ThinkerQuickAddRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Quick-add a thinker from natural language input.

    Examples:
    - "harvest works by Marcuse"
    - "Herbert Marcuse"
    - "add thinker Jürgen Habermas"
    """
    from .services.thinker_service import get_thinker_service
    import re

    service = get_thinker_service(db)

    # Extract name from natural language input
    input_text = request.input.strip()

    # Remove common prefixes
    patterns = [
        r"^harvest\s+(?:works\s+)?(?:by\s+)?",
        r"^add\s+(?:thinker\s+)?",
        r"^(?:find|get|search)\s+(?:works\s+)?(?:by\s+)?",
    ]
    name = input_text
    for pattern in patterns:
        name = re.sub(pattern, "", name, flags=re.IGNORECASE).strip()

    if not name:
        raise HTTPException(status_code=400, detail="Could not extract thinker name from input")

    result = await service.create_thinker(name)

    if not result.get("success"):
        if result.get("existing_thinker_id"):
            raise HTTPException(
                status_code=409,
                detail={
                    "message": result.get("error", "Thinker already exists"),
                    "existing_thinker_id": result["existing_thinker_id"],
                }
            )
        raise HTTPException(status_code=500, detail=result.get("error", "Failed to create thinker"))

    disambiguation = result.get("disambiguation")

    return ThinkerQuickAddResponse(
        thinker_id=result["thinker_id"],
        canonical_name=result["canonical_name"],
        disambiguation_required=result.get("requires_confirmation", False),
        disambiguation=DisambiguationResponse(
            is_ambiguous=disambiguation.get("is_ambiguous", False),
            primary_candidate=disambiguation.get("primary_candidate", {}),
            alternatives=disambiguation.get("alternatives", []),
            confidence=disambiguation.get("confidence", 0.0),
            requires_confirmation=disambiguation.get("requires_confirmation", False),
        ) if disambiguation else None,
        message=f"Created thinker '{result['canonical_name']}'" + (
            " - confirmation required" if result.get("requires_confirmation") else ""
        ),
    )


@app.post("/api/thinkers/{thinker_id}/detect-translations", response_model=DetectTranslationsResponse)
async def detect_translations(
    thinker_id: int,
    request: DetectTranslationsRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Detect translations and group works into canonical editions.

    Uses Claude Opus with extended thinking (32k budget) to analyze
    all accepted works and identify translations vs original editions.
    """
    from .services.thinker_service import get_thinker_service

    service = get_thinker_service(db)
    thinker = await service.get_thinker(thinker_id)

    if not thinker:
        raise HTTPException(status_code=404, detail="Thinker not found")

    result = await service.detect_translations(thinker)

    if not result.get("success"):
        raise HTTPException(status_code=500, detail=result.get("error", "Translation detection failed"))

    return DetectTranslationsResponse(
        success=True,
        work_groups=result.get("work_groups", []),
        standalone_work_ids=result.get("standalone_works", []),
        analysis_notes=result.get("analysis_notes", ""),
        thinking_tokens_used=result.get("thinking_tokens"),
    )


@app.post("/api/thinkers/retrospective-match", response_model=RetrospectiveMatchResponse)
async def retrospective_match_papers(
    request: RetrospectiveMatchRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Match existing papers to thinkers (retrospective assignment).

    Analyzes existing papers in the database and determines which ones
    were authored by known thinkers. Useful for:
    - Assigning papers that were added before the thinker was created
    - Bulk-matching papers to multiple thinkers at once
    """
    from .services.thinker_service import get_thinker_service

    service = get_thinker_service(db)
    result = await service.retrospective_match(
        thinker_ids=request.thinker_ids,
        paper_ids=request.paper_ids,
    )

    if not result.get("success") and "error" in result:
        raise HTTPException(status_code=400, detail=result.get("error"))

    return RetrospectiveMatchResponse(
        success=True,
        matches=result.get("matches", []),
        total_papers_analyzed=result.get("total_papers_analyzed", 0),
        total_matches=result.get("total_matches", 0),
        thinkers_checked=result.get("thinkers_checked", 0),
    )


@app.post("/api/thinkers/{thinker_id}/start-discovery", response_model=StartWorkDiscoveryResponse)
async def start_work_discovery(
    thinker_id: int,
    request: StartWorkDiscoveryRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Start a job to discover all works by a thinker.

    This queues a background job that will:
    1. Use all name variants to search Google Scholar
    2. Filter each page of results via LLM
    3. Save accepted works to the database
    """
    from .services.thinker_service import get_thinker_service

    service = get_thinker_service(db)
    thinker = await service.get_thinker(thinker_id)

    if not thinker:
        raise HTTPException(status_code=404, detail="Thinker not found")

    if thinker.status == "pending":
        raise HTTPException(
            status_code=400,
            detail="Thinker disambiguation not confirmed. Call /confirm first."
        )

    # Check if name variants exist
    if not thinker.name_variants:
        # Generate variants first
        await service.generate_name_variants(thinker)
        await db.refresh(thinker)

    # DUPLICATE PREVENTION: Check for existing pending/running discovery job for this thinker
    existing_result = await db.execute(
        select(Job).where(
            Job.job_type == "thinker_discover_works",
            Job.status.in_(["pending", "running"]),
            Job.params.contains(f'"thinker_id": {thinker_id}')
        )
    )
    existing_job = existing_result.scalar_one_or_none()

    if existing_job:
        variants = json.loads(thinker.name_variants) if thinker.name_variants else []
        return StartWorkDiscoveryResponse(
            job_id=existing_job.id,
            thinker_id=thinker_id,
            variants_to_search=len(variants),
            status=existing_job.status,
            message=f"Work discovery already in progress (job {existing_job.id})",
        )

    # Create job
    job = Job(
        job_type="thinker_discover_works",
        status="pending",
        params=json.dumps({
            "thinker_id": thinker_id,
            "max_pages": request.max_pages_per_variant or 50,
        }),
        created_at=datetime.utcnow(),
    )
    db.add(job)
    await db.commit()

    # Count variants
    variants = json.loads(thinker.name_variants) if thinker.name_variants else []

    return StartWorkDiscoveryResponse(
        job_id=job.id,
        thinker_id=thinker_id,
        variants_to_search=len(variants),
        status="queued",
        message=f"Work discovery job queued for {thinker.canonical_name}",
    )


@app.post("/api/thinkers/{thinker_id}/start-harvest", response_model=HarvestCitationsResponse)
async def start_harvest_citations(
    thinker_id: int,
    request: HarvestCitationsRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Start a job to harvest citations for a thinker's accepted works.

    This queues a background job that will:
    1. Create Paper/Edition records for each accepted work
    2. Queue citation extraction jobs for each paper
    """
    from .services.thinker_service import get_thinker_service

    service = get_thinker_service(db)
    thinker = await service.get_thinker(thinker_id)

    if not thinker:
        raise HTTPException(status_code=404, detail="Thinker not found")

    # Check if there are accepted works
    result = await db.execute(
        select(func.count(ThinkerWork.id))
        .where(ThinkerWork.thinker_id == thinker_id)
        .where(ThinkerWork.decision == "accepted")
        .where(ThinkerWork.citations_harvested == False)
    )
    pending_works = result.scalar()

    if not pending_works:
        raise HTTPException(
            status_code=400,
            detail="No accepted works pending harvest. Run work discovery first."
        )

    # DUPLICATE PREVENTION: Check for existing pending/running harvest job for this thinker
    existing_result = await db.execute(
        select(Job).where(
            Job.job_type == "thinker_harvest_citations",
            Job.status.in_(["pending", "running"]),
            Job.params.contains(f'"thinker_id": {thinker_id}')
        )
    )
    existing_job = existing_result.scalar_one_or_none()

    if existing_job:
        return HarvestCitationsResponse(
            job_id=existing_job.id,
            thinker_id=thinker_id,
            works_pending=pending_works,
            message=f"Citation harvest already in progress (job {existing_job.id})",
        )

    # Create job
    job = Job(
        job_type="thinker_harvest_citations",
        status="pending",
        params=json.dumps({
            "thinker_id": thinker_id,
            "max_works": request.max_works or 100,
        }),
        created_at=datetime.utcnow(),
    )
    db.add(job)
    await db.commit()

    return HarvestCitationsResponse(
        job_id=job.id,
        thinker_id=thinker_id,
        works_pending=pending_works,
        message=f"Citation harvest job queued for {thinker.canonical_name}",
    )


# ============== Citation to Seed Conversion ==============

@app.post("/api/citations/{citation_id}/make-seed", response_model=CitationMakeSeedResponse)
async def make_citation_seed(
    citation_id: int,
    request: CitationMakeSeedRequest = None,
    db: AsyncSession = Depends(get_db)
):
    """
    Convert a citation into a new seed paper for harvesting.

    Creates a new Paper from the citation's data (title, authors, year, venue).
    Supports dossier selection:
    - dossier_id: specific dossier to add to
    - create_new_dossier: create a new dossier first
    """
    if request is None:
        request = CitationMakeSeedRequest()

    # Get the citation
    result = await db.execute(select(Citation).where(Citation.id == citation_id))
    citation = result.scalar_one_or_none()
    if not citation:
        raise HTTPException(status_code=404, detail="Citation not found")

    # Check if a paper with this scholar_id already exists
    if citation.scholar_id:
        existing = await db.execute(
            select(Paper).where(Paper.scholar_id == citation.scholar_id)
        )
        existing_paper = existing.scalar_one_or_none()
        if existing_paper:
            return CitationMakeSeedResponse(
                paper_id=existing_paper.id,
                title=existing_paper.title,
                dossier_id=existing_paper.dossier_id,
                dossier_name=None,
                message=f"Paper already exists as seed (ID: {existing_paper.id})"
            )

    # Determine target dossier
    target_dossier_id = None
    target_dossier_name = None
    target_collection_id = None

    if request.create_new_dossier and request.new_dossier_name:
        # Create a new dossier
        if not request.collection_id:
            raise HTTPException(status_code=400, detail="Collection ID required when creating new dossier")

        # Verify collection exists
        coll_result = await db.execute(select(Collection).where(Collection.id == request.collection_id))
        collection = coll_result.scalar_one_or_none()
        if not collection:
            raise HTTPException(status_code=404, detail="Collection not found")

        new_dossier = Dossier(
            name=request.new_dossier_name,
            collection_id=request.collection_id,
        )
        db.add(new_dossier)
        await db.flush()
        await db.refresh(new_dossier)
        target_dossier_id = new_dossier.id
        target_dossier_name = new_dossier.name
        target_collection_id = request.collection_id

    elif request.dossier_id:
        # Use specified dossier
        dossier_result = await db.execute(select(Dossier).where(Dossier.id == request.dossier_id))
        dossier = dossier_result.scalar_one_or_none()
        if not dossier:
            raise HTTPException(status_code=404, detail="Dossier not found")
        target_dossier_id = dossier.id
        target_dossier_name = dossier.name
        target_collection_id = dossier.collection_id

    # Create the new Paper
    new_paper = Paper(
        scholar_id=citation.scholar_id,
        title=citation.title,
        authors=citation.authors,
        year=citation.year,
        venue=citation.venue,
        abstract=citation.abstract,
        link=citation.link,
        citation_count=citation.citation_count,
        dossier_id=target_dossier_id,
        collection_id=target_collection_id,
        status="pending",
    )
    db.add(new_paper)
    await db.commit()
    await db.refresh(new_paper)

    return CitationMakeSeedResponse(
        paper_id=new_paper.id,
        title=new_paper.title,
        dossier_id=target_dossier_id,
        dossier_name=target_dossier_name,
        message=f"Created seed paper from citation: {new_paper.title[:50]}..."
    )


@app.post("/api/thinker-works/{work_id}/make-seed", response_model=CitationMakeSeedResponse)
async def make_thinker_work_seed(
    work_id: int,
    request: CitationMakeSeedRequest = None,
    db: AsyncSession = Depends(get_db)
):
    """
    Convert a thinker's work into a seed paper for harvesting.

    This allows harvesting citations for works that don't yet have a linked Paper.
    If the work already has a paper_id, returns that paper.
    """
    if request is None:
        request = CitationMakeSeedRequest()

    # Get the thinker work
    result = await db.execute(select(ThinkerWork).where(ThinkerWork.id == work_id))
    work = result.scalar_one_or_none()
    if not work:
        raise HTTPException(status_code=404, detail="Thinker work not found")

    # If work already has a paper_id, return that
    if work.paper_id:
        paper_result = await db.execute(select(Paper).where(Paper.id == work.paper_id))
        existing_paper = paper_result.scalar_one_or_none()
        if existing_paper:
            return CitationMakeSeedResponse(
                paper_id=existing_paper.id,
                title=existing_paper.title,
                dossier_id=existing_paper.dossier_id,
                dossier_name=None,
                message=f"Work already linked to paper (ID: {existing_paper.id})"
            )

    # Check if a paper with this scholar_id already exists
    if work.scholar_id:
        existing = await db.execute(
            select(Paper).where(Paper.scholar_id == work.scholar_id)
        )
        existing_paper = existing.scalar_one_or_none()
        if existing_paper:
            # Link the work to this paper
            work.paper_id = existing_paper.id
            await db.commit()
            return CitationMakeSeedResponse(
                paper_id=existing_paper.id,
                title=existing_paper.title,
                dossier_id=existing_paper.dossier_id,
                dossier_name=None,
                message=f"Paper already exists, linked to work (ID: {existing_paper.id})"
            )

    # Determine target dossier
    target_dossier_id = None
    target_dossier_name = None
    target_collection_id = None

    if request.create_new_dossier and request.new_dossier_name:
        # Create a new dossier
        if not request.collection_id:
            raise HTTPException(status_code=400, detail="Collection ID required when creating new dossier")

        # Verify collection exists
        coll_result = await db.execute(select(Collection).where(Collection.id == request.collection_id))
        collection = coll_result.scalar_one_or_none()
        if not collection:
            raise HTTPException(status_code=404, detail="Collection not found")

        new_dossier = Dossier(
            name=request.new_dossier_name,
            collection_id=request.collection_id,
        )
        db.add(new_dossier)
        await db.flush()
        await db.refresh(new_dossier)
        target_dossier_id = new_dossier.id
        target_dossier_name = new_dossier.name
        target_collection_id = request.collection_id

    elif request.dossier_id:
        # Use specified dossier
        dossier_result = await db.execute(select(Dossier).where(Dossier.id == request.dossier_id))
        dossier = dossier_result.scalar_one_or_none()
        if not dossier:
            raise HTTPException(status_code=404, detail="Dossier not found")
        target_dossier_id = dossier.id
        target_dossier_name = dossier.name
        target_collection_id = dossier.collection_id

    # Create the new Paper from the work
    new_paper = Paper(
        scholar_id=work.scholar_id,
        title=work.title,
        authors=work.authors_raw,
        year=work.year,
        venue=work.venue,
        link=work.link,
        citation_count=work.citation_count,
        dossier_id=target_dossier_id,
        collection_id=target_collection_id,
        status="pending",
    )
    db.add(new_paper)
    await db.flush()
    await db.refresh(new_paper)

    # Link the work to the new paper
    work.paper_id = new_paper.id
    await db.commit()

    return CitationMakeSeedResponse(
        paper_id=new_paper.id,
        title=new_paper.title,
        dossier_id=target_dossier_id,
        dossier_name=target_dossier_name,
        message=f"Created seed paper from thinker work: {new_paper.title[:50]}..."
    )


# ============== Author Search ==============

@app.get("/api/search/papers-by-author", response_model=AuthorSearchResponse)
async def search_papers_by_author(
    author_name: str,
    current_thinker_id: Optional[int] = None,
    limit: int = 50,
    db: AsyncSession = Depends(get_db)
):
    """
    Search for all papers by an author across the entire database.

    Searches both:
    - Papers (seeds) table
    - Citations table

    Results are flagged if they belong to the current_thinker_id context.
    """
    # Normalize search term for ILIKE
    search_term = f"%{author_name}%"

    # Search Papers
    papers_query = (
        select(Paper, Dossier.name.label("dossier_name"))
        .outerjoin(Dossier, Paper.dossier_id == Dossier.id)
        .where(Paper.authors.ilike(search_term))
        .where(Paper.deleted_at.is_(None))
        .order_by(Paper.citation_count.desc())
        .limit(limit)
    )
    papers_result = await db.execute(papers_query)
    papers_rows = papers_result.all()

    paper_results = []
    for row in papers_rows:
        paper = row[0]
        paper_results.append(AuthorPaperResult(
            source="paper",
            id=paper.id,
            title=paper.title,
            authors=paper.authors,
            year=paper.year,
            venue=paper.venue,
            citation_count=paper.citation_count,
            link=paper.link,
            citing_thinker_id=None,
            citing_thinker_name=None,
            citing_paper_id=None,
            citing_paper_title=None,
            is_from_current_thinker=False,
        ))

    # Search Citations with thinker info
    citations_query = (
        select(
            Citation,
            Paper.id.label("paper_id"),
            Paper.title.label("paper_title"),
            ThinkerWork.thinker_id,
            Thinker.canonical_name.label("thinker_name")
        )
        .join(Paper, Citation.paper_id == Paper.id)
        .outerjoin(ThinkerWork, Paper.id == ThinkerWork.paper_id)
        .outerjoin(Thinker, ThinkerWork.thinker_id == Thinker.id)
        .where(Citation.authors.ilike(search_term))
        .order_by(Citation.citation_count.desc())
        .limit(limit)
    )
    citations_result = await db.execute(citations_query)
    citations_rows = citations_result.all()

    citation_results = []
    for row in citations_rows:
        citation = row[0]
        paper_id = row[1]
        paper_title = row[2]
        thinker_id = row[3]
        thinker_name = row[4]

        citation_results.append(AuthorPaperResult(
            source="citation",
            id=citation.id,
            title=citation.title,
            authors=citation.authors,
            year=citation.year,
            venue=citation.venue,
            citation_count=citation.citation_count,
            link=citation.link,
            citing_thinker_id=thinker_id,
            citing_thinker_name=thinker_name,
            citing_paper_id=paper_id,
            citing_paper_title=paper_title,
            is_from_current_thinker=(thinker_id == current_thinker_id) if current_thinker_id and thinker_id else False,
        ))

    return AuthorSearchResponse(
        query=author_name,
        total_results=len(paper_results) + len(citation_results),
        papers=paper_results,
        citations=citation_results,
    )
