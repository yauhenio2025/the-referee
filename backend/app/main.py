"""
The Referee - Citation Analysis API

A robust API for discovering editions and extracting citations from academic papers.
"""
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from typing import List, Optional
from pydantic import BaseModel
import json

from .config import get_settings
from .database import init_db, get_db
from .models import Paper, Edition, Citation, Job, RawSearchResult, Collection
from .schemas import (
    PaperCreate, PaperResponse, PaperDetail, PaperSubmitBatch,
    EditionResponse, EditionDiscoveryRequest, EditionDiscoveryResponse, EditionSelectRequest,
    EditionUpdateConfidenceRequest, EditionFetchMoreRequest, EditionFetchMoreResponse,
    ManualEditionAddRequest, ManualEditionAddResponse,
    CitationResponse, CitationExtractionRequest, CitationExtractionResponse, CrossCitationResult,
    JobResponse, JobDetail, FetchMoreJobRequest, FetchMoreJobResponse,
    LanguageRecommendationRequest, LanguageRecommendationResponse, AvailableLanguagesResponse,
    CollectionCreate, CollectionUpdate, CollectionResponse, CollectionDetail,
    CanonicalEditionSummary,
    # Refresh/Auto-Updater schemas
    RefreshRequest, RefreshJobResponse, RefreshStatusResponse, StalenessReportResponse,
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize database on startup, start background worker"""
    await init_db()
    # Start background job worker
    from .services.job_worker import start_worker
    start_worker()
    yield
    # Stop worker on shutdown
    from .services.job_worker import stop_worker
    stop_worker()


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
    result = await db.execute(
        select(Collection).order_by(Collection.name)
    )
    collections = result.scalars().all()

    # Get paper counts for each collection
    responses = []
    for c in collections:
        count_result = await db.execute(
            select(func.count(Paper.id)).where(Paper.collection_id == c.id)
        )
        paper_count = count_result.scalar() or 0
        responses.append(CollectionResponse(
            id=c.id,
            name=c.name,
            description=c.description,
            color=c.color,
            created_at=c.created_at,
            updated_at=c.updated_at,
            paper_count=paper_count,
        ))
    return responses


@app.get("/api/collections/{collection_id}", response_model=CollectionDetail)
async def get_collection(collection_id: int, db: AsyncSession = Depends(get_db)):
    """Get collection details with papers (including edition stats)"""
    result = await db.execute(select(Collection).where(Collection.id == collection_id))
    collection = result.scalar_one_or_none()
    if not collection:
        raise HTTPException(status_code=404, detail="Collection not found")

    # Get papers in collection
    papers_result = await db.execute(
        select(Paper).where(Paper.collection_id == collection_id).order_by(Paper.created_at.desc())
    )
    papers = papers_result.scalars().all()

    # Build paper responses with edition stats
    paper_responses = []
    for paper in papers:
        paper_response = await build_paper_response_with_editions(paper, db)
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


def paper_to_response(paper: Paper) -> dict:
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
    return data


@app.get("/api/papers", response_model=List[PaperResponse])
async def list_papers(
    skip: int = 0,
    limit: int = 100,
    status: str = None,
    collection_id: Optional[int] = None,
    db: AsyncSession = Depends(get_db)
):
    """List all papers, optionally filtered by status or collection"""
    query = select(Paper).offset(skip).limit(limit).order_by(Paper.created_at.desc())
    if status:
        query = query.where(Paper.status == status)
    if collection_id is not None:
        query = query.where(Paper.collection_id == collection_id)
    result = await db.execute(query)
    papers = result.scalars().all()

    return [PaperResponse(**paper_to_response(p)) for p in papers]


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
async def delete_paper(paper_id: int, db: AsyncSession = Depends(get_db)):
    """Delete a paper and all related data"""
    result = await db.execute(select(Paper).where(Paper.id == paper_id))
    paper = result.scalar_one_or_none()
    if not paper:
        raise HTTPException(status_code=404, detail="Paper not found")

    await db.delete(paper)
    return {"deleted": True, "paper_id": paper_id}


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

    # Build response with harvested counts and staleness
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

    # Check if it's a Google Scholar URL - extract info directly
    scholar_url_match = re.search(r'scholar\.google\.[^/]+/scholar\?.*cites=(\d+)', input_text)
    cluster_id_match = re.search(r'cluster=(\d+)', input_text)

    resolution_details = {"input_type": "unknown", "llm_used": False}
    search_query = None
    expected_language = request.language_hint

    if scholar_url_match or cluster_id_match:
        # Direct Scholar link - we can fetch it directly
        resolution_details["input_type"] = "scholar_url"
        cluster_id = cluster_id_match.group(1) if cluster_id_match else None
        cites_id = scholar_url_match.group(1) if scholar_url_match else None
        resolution_details["cluster_id"] = cluster_id
        resolution_details["cites_id"] = cites_id
        # For now, we'll still use title-based search as we need scholar_id
        # Extract any title from URL params
        title_match = re.search(r'[?&]q=([^&]+)', input_text)
        if title_match:
            from urllib.parse import unquote
            search_query = unquote(title_match.group(1))

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
    limit: int = 500,
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

    query = query.order_by(Citation.citation_count.desc()).offset(skip).limit(limit)

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
    """List jobs with parsed params"""
    query = select(Job).order_by(Job.created_at.desc()).limit(limit)
    if status:
        query = query.where(Job.status == status)
    if job_type:
        query = query.where(Job.job_type == job_type)
    if paper_id:
        query = query.where(Job.paper_id == paper_id)
    result = await db.execute(query)
    jobs = result.scalars().all()

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

    if existing_job:
        return QuickHarvestResponse(
            job_id=existing_job.id,
            paper_id=paper_id,
            edition_id=edition.id,
            edition_created=edition_created,
            estimated_citations=edition.citation_count,
            message=f"Citation extraction already in progress (job {existing_job.id})"
        )

    # Queue citation extraction job
    job_id = await create_extract_citations_job(
        db=db,
        paper_id=paper_id,
        edition_ids=[edition.id],
        max_citations_per_edition=1000,
        skip_threshold=10000,
    )

    return QuickHarvestResponse(
        job_id=job_id,
        paper_id=paper_id,
        edition_id=edition.id,
        edition_created=edition_created,
        estimated_citations=edition.citation_count,
        message=f"Queued citation extraction for {edition.citation_count:,} citations"
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
    job = await create_extract_citations_job(
        db=db,
        paper_id=paper_id,
        edition_ids=[e.id for e in editions],
        max_citations_per_edition=request.max_citations_per_edition,
        skip_threshold=request.skip_threshold,
        is_refresh=True,
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
