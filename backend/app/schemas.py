"""
Pydantic schemas for API request/response
"""
from datetime import datetime
from typing import Optional, List, Any
from pydantic import BaseModel, Field


# ============== Collection Schemas ==============

class CollectionBase(BaseModel):
    name: str
    description: Optional[str] = None
    color: Optional[str] = None


class CollectionCreate(CollectionBase):
    """Create a new collection"""
    pass


class CollectionUpdate(BaseModel):
    """Update collection fields (all optional)"""
    name: Optional[str] = None
    description: Optional[str] = None
    color: Optional[str] = None


class CollectionResponse(CollectionBase):
    id: int
    created_at: datetime
    updated_at: datetime
    paper_count: int = 0

    class Config:
        from_attributes = True


class CollectionDetail(CollectionResponse):
    papers: List["PaperResponse"] = []


# ============== Paper Schemas ==============

class PaperBase(BaseModel):
    title: str
    authors: Optional[str] = None
    year: Optional[int] = None
    venue: Optional[str] = None


class PaperCreate(PaperBase):
    """Submit a paper for analysis"""
    collection_id: Optional[int] = None


class PaperSubmitBatch(BaseModel):
    """Submit multiple papers for analysis"""
    papers: List[PaperCreate]
    collection_id: Optional[int] = None  # Default collection for all papers
    auto_discover_editions: bool = True
    language_strategy: str = "major_languages"
    custom_languages: List[str] = []


class ScholarCandidate(BaseModel):
    """A candidate paper from Google Scholar"""
    scholar_id: Optional[str] = Field(None, alias="scholarId")
    cluster_id: Optional[str] = Field(None, alias="clusterId")
    title: str
    authors: Optional[str] = None
    authors_raw: Optional[str] = Field(None, alias="authorsRaw")
    year: Optional[int] = None
    venue: Optional[str] = None
    abstract: Optional[str] = None
    link: Optional[str] = None
    citation_count: int = Field(0, alias="citationCount")

    class Config:
        populate_by_name = True  # Allow both snake_case and camelCase


class CanonicalEditionSummary(BaseModel):
    """Summary of the canonical (highest-cited) edition"""
    id: int
    title: str
    citation_count: int
    language: Optional[str] = None


class PaperResponse(PaperBase):
    id: int
    collection_id: Optional[int] = None
    scholar_id: Optional[str] = None
    citation_count: int = 0
    language: Optional[str] = None
    status: str
    abstract: Optional[str] = None
    link: Optional[str] = None
    created_at: datetime
    candidates: Optional[List[Any]] = None  # For reconciliation (raw JSON from Scholar)
    # Edition aggregation stats
    edition_count: int = 0
    total_edition_citations: int = 0  # Sum of citations across all editions
    canonical_edition: Optional[CanonicalEditionSummary] = None  # Highest-cited edition
    # Harvest freshness tracking (auto-updater feature)
    any_edition_harvested_at: Optional[datetime] = None
    total_harvested_citations: int = 0
    is_stale: bool = False  # Computed: null or >90 days since any edition harvest
    days_since_harvest: Optional[int] = None  # Computed

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
    harvested_citations: int = 0  # Number of citations actually harvested from this edition
    language: Optional[str] = None
    confidence: str
    auto_selected: bool
    selected: bool
    is_supplementary: bool = False
    added_by_job_id: Optional[int] = None  # Non-null = NEW (from recent fetch job)
    # Harvest freshness tracking (auto-updater feature)
    last_harvested_at: Optional[datetime] = None
    last_harvest_year: Optional[int] = None
    harvested_citation_count: int = 0
    is_stale: bool = False  # Computed: null or >90 days since harvest
    days_since_harvest: Optional[int] = None  # Computed

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


class EditionUpdateConfidenceRequest(BaseModel):
    """Mark editions as high/uncertain/rejected"""
    edition_ids: List[int]
    confidence: str  # "high", "uncertain", "rejected"


class EditionFetchMoreRequest(BaseModel):
    """Request to fetch more editions in a specific language"""
    paper_id: int
    language: str  # e.g., "italian", "arabic", "chinese"
    max_results: int = 50


class ManualEditionAddRequest(BaseModel):
    """Request to manually add an edition via LLM resolution"""
    paper_id: int
    input_text: str  # Can be: Google Scholar URL, title, or pasted Scholar entry
    language_hint: Optional[str] = None  # Optional hint about expected language


class ManualEditionAddResponse(BaseModel):
    """Response from manual edition addition"""
    success: bool
    edition: Optional[EditionResponse] = None
    message: str
    resolution_details: Optional[dict] = None  # How LLM resolved the input


class EditionFetchMoreResponse(BaseModel):
    """Response from fetching more editions in a language"""
    paper_id: int
    language: str
    new_editions_found: int
    total_results_searched: int
    queries_used: List[str]


# ============== Citation Schemas ==============

class CitationResponse(BaseModel):
    id: int
    scholar_id: Optional[str] = None
    title: str
    authors: Optional[str] = None
    year: Optional[int] = None
    venue: Optional[str] = None
    link: Optional[str] = None
    citation_count: int = 0
    intersection_count: int = 1
    edition_id: Optional[int] = None
    edition_language: Optional[str] = None
    edition_title: Optional[str] = None  # For edition-specific filtering

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
    paper_id: Optional[int] = None
    job_type: str
    status: str
    progress: float
    progress_message: Optional[str] = None
    params: Optional[dict] = None
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error: Optional[str] = None

    class Config:
        from_attributes = True


class JobDetail(JobResponse):
    result: Optional[Any] = None


class FetchMoreJobRequest(BaseModel):
    """Request to queue a fetch-more job"""
    paper_id: int
    language: str
    max_results: int = 50


class FetchMoreJobResponse(BaseModel):
    """Response from queueing a fetch-more job"""
    job_id: int
    paper_id: int
    language: str
    status: str
    message: str


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


# ============== Refresh/Auto-Updater Schemas ==============

class RefreshRequest(BaseModel):
    """Request to refresh citations for paper/collection/global"""
    force_full_refresh: bool = False  # If True, ignore year_low optimization
    max_citations_per_edition: int = 1000
    skip_threshold: int = 10000  # Skip editions with more citations than this


class RefreshJobResponse(BaseModel):
    """Response when queueing refresh jobs"""
    jobs_created: int
    papers_included: int
    editions_included: int
    job_ids: List[int]
    batch_id: str  # UUID to track collection/global refreshes


class RefreshStatusResponse(BaseModel):
    """Status of refresh operation"""
    batch_id: str
    total_jobs: int
    completed_jobs: int
    failed_jobs: int
    running_jobs: int
    pending_jobs: int
    new_citations_added: int
    is_complete: bool


class StalenessReportResponse(BaseModel):
    """Report on stale papers and editions"""
    total_papers: int
    stale_papers: int
    never_harvested_papers: int
    total_editions: int
    stale_editions: int
    never_harvested_editions: int
    oldest_harvest_date: Optional[datetime] = None
    staleness_threshold_days: int = 90


# Update forward references
PaperDetail.model_rebuild()
CollectionDetail.model_rebuild()
