"""
The Referee - Citation Analysis API

A robust API for discovering editions and extracting citations from academic papers.
"""
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from typing import List
from pydantic import BaseModel
import json

from .config import get_settings
from .database import init_db, get_db
from .models import Paper, Edition, Citation, Job, RawSearchResult
from .schemas import (
    PaperCreate, PaperResponse, PaperDetail, PaperSubmitBatch,
    EditionResponse, EditionDiscoveryRequest, EditionDiscoveryResponse, EditionSelectRequest,
    EditionFetchMoreRequest, EditionFetchMoreResponse,
    CitationResponse, CitationExtractionRequest, CitationExtractionResponse, CrossCitationResult,
    JobResponse, JobDetail, FetchMoreJobRequest, FetchMoreJobResponse,
    LanguageRecommendationRequest, LanguageRecommendationResponse, AvailableLanguagesResponse,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

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
        db_paper = Paper(
            title=paper_data.title,
            authors=paper_data.authors,
            year=paper_data.year,
            venue=paper_data.venue,
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


@app.get("/api/papers", response_model=List[PaperResponse])
async def list_papers(
    skip: int = 0,
    limit: int = 100,
    status: str = None,
    db: AsyncSession = Depends(get_db)
):
    """List all papers"""
    query = select(Paper).offset(skip).limit(limit).order_by(Paper.created_at.desc())
    if status:
        query = query.where(Paper.status == status)
    result = await db.execute(query)
    return result.scalars().all()


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

    return PaperDetail(
        **{k: v for k, v in paper.__dict__.items() if not k.startswith('_')},
        editions=[EditionResponse(**{k: v for k, v in e.__dict__.items() if not k.startswith('_')}) for e in editions],
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
            editions=[EditionResponse(**{k: v for k, v in e.__dict__.items() if not k.startswith('_')}) for e in editions],
            queries_used=discovery_result.get("summary", {}).get("queriesGenerated", []),
        )

    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Edition discovery failed: {str(e)}")


@app.get("/api/papers/{paper_id}/editions", response_model=List[EditionResponse])
async def get_paper_editions(paper_id: int, db: AsyncSession = Depends(get_db)):
    """Get all editions of a paper"""
    result = await db.execute(
        select(Edition).where(Edition.paper_id == paper_id).order_by(Edition.citation_count.desc())
    )
    return result.scalars().all()


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


# ============== Citation Extraction Endpoints ==============

@app.post("/api/citations/extract", response_model=CitationExtractionResponse)
async def extract_citations(
    request: CitationExtractionRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db)
):
    """Extract citations for a paper (from selected editions)"""
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
    editions = editions_result.scalars().all()

    if not editions:
        raise HTTPException(status_code=400, detail="No editions selected for extraction")

    # Create extraction job
    job = Job(
        paper_id=paper.id,
        job_type="extract_citations",
        status="pending",
    )
    db.add(job)
    await db.flush()
    await db.refresh(job)

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
    limit: int = 100,
    db: AsyncSession = Depends(get_db)
):
    """Get citations for a paper"""
    result = await db.execute(
        select(Citation)
        .where(Citation.paper_id == paper_id)
        .order_by(Citation.citation_count.desc())
        .offset(skip)
        .limit(limit)
    )
    return result.scalars().all()


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

    response = JobDetail(
        **{k: v for k, v in job.__dict__.items() if not k.startswith('_') and k != 'result'},
        result=json.loads(job.result) if job.result else None,
    )
    return response


@app.post("/api/jobs/{job_id}/cancel")
async def cancel_job(job_id: int, db: AsyncSession = Depends(get_db)):
    """Cancel a pending job"""
    result = await db.execute(select(Job).where(Job.id == job_id))
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status not in ["pending", "running"]:
        raise HTTPException(status_code=400, detail=f"Cannot cancel job with status: {job.status}")

    job.status = "cancelled"
    return {"cancelled": True, "job_id": job_id}


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
    papers_count = await db.execute(select(func.count(Paper.id)))
    editions_count = await db.execute(select(func.count(Edition.id)))
    citations_count = await db.execute(select(func.count(Citation.id)))
    pending_jobs = await db.execute(select(func.count(Job.id)).where(Job.status == "pending"))
    running_jobs = await db.execute(select(func.count(Job.id)).where(Job.status == "running"))

    return {
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
