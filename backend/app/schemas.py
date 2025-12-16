"""
Pydantic schemas for API request/response
"""
from datetime import datetime
from typing import Optional, List, Any
from pydantic import BaseModel, Field


# ============== Paper Schemas ==============

class PaperBase(BaseModel):
    title: str
    authors: Optional[str] = None
    year: Optional[int] = None
    venue: Optional[str] = None


class PaperCreate(PaperBase):
    """Submit a paper for analysis"""
    pass


class PaperSubmitBatch(BaseModel):
    """Submit multiple papers for analysis"""
    papers: List[PaperCreate]
    auto_discover_editions: bool = True
    language_strategy: str = "major_languages"
    custom_languages: List[str] = []


class ScholarCandidate(BaseModel):
    """A candidate paper from Google Scholar"""
    scholar_id: Optional[str] = None
    cluster_id: Optional[str] = None
    title: str
    authors: Optional[str] = None
    authors_raw: Optional[str] = None
    year: Optional[int] = None
    venue: Optional[str] = None
    abstract: Optional[str] = None
    link: Optional[str] = None
    citation_count: int = 0


class PaperResponse(PaperBase):
    id: int
    scholar_id: Optional[str] = None
    citation_count: int = 0
    language: Optional[str] = None
    status: str
    abstract: Optional[str] = None
    link: Optional[str] = None
    created_at: datetime
    candidates: Optional[List[ScholarCandidate]] = None  # For reconciliation

    class Config:
        from_attributes = True


class PaperDetail(PaperResponse):
    editions: List["EditionResponse"] = []
    citations_count: int = 0


# ============== Edition Schemas ==============

class EditionResponse(BaseModel):
    id: int
    scholar_id: Optional[str] = None
    title: str
    authors: Optional[str] = None
    year: Optional[int] = None
    venue: Optional[str] = None
    abstract: Optional[str] = None
    link: Optional[str] = None
    citation_count: int = 0
    language: Optional[str] = None
    confidence: str
    auto_selected: bool
    selected: bool

    class Config:
        from_attributes = True


class EditionDiscoveryRequest(BaseModel):
    paper_id: int
    language_strategy: str = "major_languages"
    custom_languages: List[str] = []


class EditionDiscoveryResponse(BaseModel):
    paper_id: int
    total_found: int
    high_confidence: int
    uncertain: int
    rejected: int
    editions: List[EditionResponse]
    queries_used: List[dict]


class EditionSelectRequest(BaseModel):
    edition_ids: List[int]
    selected: bool


# ============== Citation Schemas ==============

class CitationResponse(BaseModel):
    id: int
    scholar_id: Optional[str] = None
    title: str
    authors: Optional[str] = None
    year: Optional[int] = None
    venue: Optional[str] = None
    citation_count: int = 0
    intersection_count: int = 1

    class Config:
        from_attributes = True


class CitationExtractionRequest(BaseModel):
    paper_id: int
    edition_ids: List[int] = []  # If empty, use all selected editions
    max_citations_threshold: int = 10000


class CitationExtractionResponse(BaseModel):
    job_id: int
    paper_id: int
    editions_to_process: int
    estimated_time_minutes: int


class CrossCitationResult(BaseModel):
    paper_id: int
    total_unique_citations: int
    intersections: List[CitationResponse]
    by_intersection_count: dict


# ============== Job Schemas ==============

class JobResponse(BaseModel):
    id: int
    job_type: str
    status: str
    progress: float
    progress_message: Optional[str] = None
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error: Optional[str] = None

    class Config:
        from_attributes = True


class JobDetail(JobResponse):
    result: Optional[Any] = None


# ============== Language Schemas ==============

class LanguageRecommendationRequest(BaseModel):
    title: str
    author: Optional[str] = None
    year: Optional[int] = None


class LanguageRecommendationResponse(BaseModel):
    recommended: List[str]
    reasoning: str
    author_language: Optional[str] = None
    primary_markets: List[str]


class AvailableLanguagesResponse(BaseModel):
    languages: List[dict]  # {code, name, icon}


# Update forward references
PaperDetail.model_rebuild()
