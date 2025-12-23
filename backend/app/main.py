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
from sqlalchemy import select, func, update
from typing import List, Optional
from pydantic import BaseModel
import json

from .config import get_settings
from .database import init_db, get_db
from .models import Paper, Edition, Citation, Job, RawSearchResult, Collection, Dossier, PaperAdditionalDossier, FailedFetch, HarvestTarget
from .schemas import (
    PaperCreate, PaperResponse, PaperDetail, PaperSubmitBatch,
    EditionResponse, EditionDiscoveryRequest, EditionDiscoveryResponse, EditionSelectRequest,
    EditionUpdateConfidenceRequest, EditionFetchMoreRequest, EditionFetchMoreResponse,
    EditionExcludeRequest, EditionAddAsSeedRequest, EditionAddAsSeedResponse,
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

    # Get paper counts for each collection (excluding soft-deleted)
    responses = []
    for c in collections:
        count_result = await db.execute(
            select(func.count(Paper.id))
            .where(Paper.collection_id == c.id)
            .where(Paper.deleted_at.is_(None))
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

    # Get papers in collection (excluding soft-deleted)
    papers_result = await db.execute(
        select(Paper)
        .where(Paper.collection_id == collection_id)
        .where(Paper.deleted_at.is_(None))
        .order_by(Paper.created_at.desc())
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
    query = select(Dossier).order_by(Dossier.name)
    if collection_id is not None:
        query = query.where(Dossier.collection_id == collection_id)
    result = await db.execute(query)
    dossiers = result.scalars().all()

    # Get paper counts for each dossier
    responses = []
    for d in dossiers:
        count_result = await db.execute(
            select(func.count(Paper.id)).where(Paper.dossier_id == d.id)
        )
        paper_count = count_result.scalar() or 0
        responses.append(DossierResponse(
            id=d.id,
            name=d.name,
            description=d.description,
            color=d.color,
            collection_id=d.collection_id,
            created_at=d.created_at,
            updated_at=d.updated_at,
            paper_count=paper_count,
        ))
    return responses


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

    # Build paper responses with edition stats
    paper_responses = []
    for paper in papers:
        paper_response = await build_paper_response_with_editions(paper, db)
        paper_responses.append(paper_response)

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
    include_deleted: bool = False,
    db: AsyncSession = Depends(get_db)
):
    """List all papers, optionally filtered by status or collection"""
    query = select(Paper).offset(skip).limit(limit).order_by(Paper.created_at.desc())
    if status:
        query = query.where(Paper.status == status)
    if collection_id is not None:
        query = query.where(Paper.collection_id == collection_id)
    # Exclude soft-deleted papers by default
    if not include_deleted:
        query = query.where(Paper.deleted_at.is_(None))
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


@app.post("/api/citations/mark-reviewed")
async def mark_citations_reviewed(
    request: CitationMarkReviewedRequest,
    db: AsyncSession = Depends(get_db)
):
    """Bulk mark citations as reviewed/unseen"""
    if not request.citation_ids:
        return {"updated": 0}

    stmt = (
        update(Citation)
        .where(Citation.id.in_(request.citation_ids))
        .values(reviewed=request.reviewed)
    )
    result = await db.execute(stmt)
    await db.commit()

    return {"updated": result.rowcount, "reviewed": request.reviewed}


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
