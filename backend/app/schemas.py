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
    dossier_count: int = 0

    class Config:
        from_attributes = True


class CollectionDetail(CollectionResponse):
    papers: List["PaperResponse"] = []
    dossiers: List["DossierResponse"] = []


# ============== Dossier Schemas ==============

class DossierBase(BaseModel):
    name: str
    description: Optional[str] = None
    color: Optional[str] = None


class DossierCreate(DossierBase):
    """Create a new dossier within a collection"""
    collection_id: int


class DossierUpdate(BaseModel):
    """Update dossier fields (all optional)"""
    name: Optional[str] = None
    description: Optional[str] = None
    color: Optional[str] = None
    collection_id: Optional[int] = None  # Allow moving dossier to another collection


class DossierResponse(DossierBase):
    id: int
    collection_id: int
    created_at: datetime
    updated_at: datetime
    paper_count: int = 0

    class Config:
        from_attributes = True


class DossierDetail(DossierResponse):
    """Dossier with its papers"""
    papers: List["PaperResponse"] = []
    collection_name: Optional[str] = None


# ============== Paper Schemas ==============

class PaperBase(BaseModel):
    title: str
    authors: Optional[str] = None
    year: Optional[int] = None
    venue: Optional[str] = None


class PaperCreate(PaperBase):
    """Submit a paper for analysis"""
    collection_id: Optional[int] = None
    dossier_id: Optional[int] = None  # Papers belong to dossiers, not directly to collections


class PaperSubmitBatch(BaseModel):
    """Submit multiple papers for analysis"""
    papers: List[PaperCreate]
    collection_id: Optional[int] = None  # Default collection for all papers (legacy)
    dossier_id: Optional[int] = None  # Default dossier for all papers
    auto_discover_editions: bool = True
    language_strategy: str = "major_languages"
    custom_languages: List[str] = []


class QuickAddRequest(BaseModel):
    """Quick-add a paper using Google Scholar ID or URL"""
    input: str  # Scholar ID or URL containing cites=ID or cluster=ID
    collection_id: Optional[int] = None
    dossier_id: Optional[int] = None
    start_harvest: bool = False  # Whether to immediately start harvesting


class QuickAddResponse(BaseModel):
    """Response from quick-add"""
    paper_id: int
    edition_id: int
    title: str
    authors: Optional[str] = None
    year: Optional[int] = None
    citation_count: int = 0
    scholar_id: str
    harvest_job_id: Optional[int] = None
    message: str


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
    collection_id: Optional[int] = None  # Legacy: for backward compatibility
    dossier_id: Optional[int] = None  # Papers belong to dossiers
    scholar_id: Optional[str] = None
    citation_count: int = 0
    language: Optional[str] = None
    status: str
    abstract: Optional[str] = None
    abstract_source: Optional[str] = None  # 'scholar_search', 'allintitle_scrape', 'manual'
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
    editions_finalized: bool = False  # User has finalized edition selection
    # Foreign edition tracking
    foreign_edition_needed: bool = False  # Mark papers needing foreign edition lookup
    # Harvest progress (for UI breakdown)
    harvest_expected: int = 0  # Total expected citations across all editions
    harvest_actual: int = 0  # Total harvested citations
    harvest_percent: float = 0.0  # Completion percentage

    class Config:
        from_attributes = True


class PapersPaginatedResponse(BaseModel):
    """Paginated list of papers with metadata"""
    papers: List["PaperResponse"]
    total: int
    page: int
    per_page: int
    total_pages: int
    has_next: bool
    has_prev: bool


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
    excluded: bool = False
    is_supplementary: bool = False
    merged_into_edition_id: Optional[int] = None  # Non-null = merged into another edition
    redirected_harvest_count: int = 0  # For merged editions: citations harvested from this scholar_id
    added_by_job_id: Optional[int] = None  # Non-null = NEW (from recent fetch job)
    # Harvest freshness tracking (auto-updater feature)
    last_harvested_at: Optional[datetime] = None
    last_harvest_year: Optional[int] = None
    harvested_citation_count: int = 0
    is_stale: bool = False  # Computed: null or >90 days since harvest
    days_since_harvest: Optional[int] = None  # Computed
    is_incomplete: bool = False  # Computed: harvested < total AND gap is significant
    missing_citations: int = 0  # Computed: citation_count - harvested_citation_count

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


class EditionExcludeRequest(BaseModel):
    """Exclude/unexclude editions from view"""
    edition_ids: List[int]
    excluded: bool  # True to exclude, False to un-exclude


class EditionAddAsSeedRequest(BaseModel):
    """Convert an edition into a new independent seed paper"""
    exclude_from_current: bool = True  # Also exclude this edition from current paper
    dossier_id: Optional[int] = None  # Target dossier (if None, uses parent paper's dossier)
    collection_id: Optional[int] = None  # Target collection (for creating new dossiers)
    create_new_dossier: bool = False  # If True, create a new dossier
    new_dossier_name: Optional[str] = None  # Name for new dossier


class EditionAddAsSeedResponse(BaseModel):
    """Response from adding edition as seed"""
    new_paper_id: int
    title: str
    message: str
    dossier_id: Optional[int] = None
    dossier_name: Optional[str] = None


class EditionMergeRequest(BaseModel):
    """Merge one edition into another (canonical) edition.

    Use case: Same work appears under different URLs/scholar_ids (e.g., JSTOR + marcuse.org).
    The merged edition's citations are pooled into the canonical edition.
    Both scholar_ids are preserved for future harvesting.
    """
    source_edition_id: int  # Edition to merge (will be marked as merged)
    target_edition_id: int  # Canonical edition (receives citations)
    copy_metadata: bool = False  # Copy target's metadata to source


class EditionMergeResponse(BaseModel):
    """Response from merging editions"""
    success: bool
    message: str
    citations_moved: int = 0
    source_edition_id: int
    target_edition_id: int


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
    # reviewed: bool = False  # TODO: add column to production DB first

    class Config:
        from_attributes = True


class CitationMarkReviewedRequest(BaseModel):
    """Request to mark citations as reviewed/seen"""
    citation_ids: List[int]
    reviewed: bool = True


class CitationExtractionRequest(BaseModel):
    paper_id: int
    edition_ids: List[int] = []  # If empty, use all selected editions
    max_citations_threshold: int = 50000


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
    skip_threshold: int = 50000  # Skip editions with more citations than this


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


# ============== Harvest Completeness Schemas ==============

class HarvestTargetResponse(BaseModel):
    """A single harvest target (expected vs actual for a year)"""
    id: int
    edition_id: int
    year: Optional[int] = None  # null = all years combined
    expected_count: int
    actual_count: int
    status: str  # harvesting, complete, incomplete
    pages_attempted: int
    pages_succeeded: int
    pages_failed: int
    created_at: datetime
    completed_at: Optional[datetime] = None
    # Computed fields
    missing_count: int = 0
    completion_percent: float = 0.0

    class Config:
        from_attributes = True


class FailedFetchResponse(BaseModel):
    """A failed page fetch record"""
    id: int
    edition_id: int
    url: str
    year: Optional[int] = None
    page_number: int
    retry_count: int
    last_retry_at: Optional[datetime] = None
    last_error: Optional[str] = None
    status: str  # pending, retrying, succeeded, abandoned
    recovered_citations: int = 0
    created_at: datetime
    resolved_at: Optional[datetime] = None
    # Joined data
    edition_title: Optional[str] = None
    paper_id: Optional[int] = None

    class Config:
        from_attributes = True


class HarvestCompletenessResponse(BaseModel):
    """Report on harvest completeness for an edition or paper"""
    edition_id: Optional[int] = None
    paper_id: Optional[int] = None
    total_expected: int
    total_actual: int
    total_missing: int
    completion_percent: float
    targets: List[HarvestTargetResponse] = []
    failed_fetches: List[FailedFetchResponse] = []
    incomplete_years: List[int] = []


class FailedFetchesSummary(BaseModel):
    """Summary of all failed fetches in the system"""
    total_pending: int
    total_retrying: int
    total_succeeded: int
    total_abandoned: int
    total_recovered_citations: int
    failed_fetches: List[FailedFetchResponse] = []


# ============== AI Gap Analysis Schemas ==============

class GapDetail(BaseModel):
    """Details of a single gap in the harvest"""
    gap_type: str  # "missing_year", "incomplete_year", "failed_pages", "never_harvested"
    year: Optional[int] = None
    edition_id: Optional[int] = None
    edition_title: Optional[str] = None
    expected_count: int = 0
    actual_count: int = 0
    missing_count: int = 0
    failed_pages: List[int] = []
    description: str = ""
    severity: str = "medium"  # "low", "medium", "high", "critical"


class GapFix(BaseModel):
    """A recommended fix for a gap"""
    fix_type: str  # "harvest_year", "retry_failed_pages", "full_harvest", "partition_harvest"
    priority: int = 1  # 1 = highest priority
    year: Optional[int] = None
    edition_id: Optional[int] = None
    edition_title: Optional[str] = None
    estimated_citations: int = 0
    description: str = ""
    action_url: Optional[str] = None  # API endpoint to call


class AIGapAnalysisResponse(BaseModel):
    """Response from AI gap analysis for a paper"""
    paper_id: int
    paper_title: str
    analysis_timestamp: datetime
    # Edition scope - null means all editions, otherwise specific edition
    edition_id: Optional[int] = None
    edition_title: Optional[str] = None
    edition_language: Optional[str] = None
    # Summary stats
    total_editions: int = 0
    selected_editions: int = 0
    total_expected_citations: int = 0
    total_harvested_citations: int = 0
    total_missing_citations: int = 0
    completion_percent: float = 0.0
    # Gap details
    gaps: List[GapDetail] = []
    # Recommended fixes
    recommended_fixes: List[GapFix] = []
    # LLM-generated summary
    ai_summary: str = ""
    ai_recommendations: str = ""


# ============== External API Schemas ==============

class BatchCrossRequest(BaseModel):
    """Request for batch cross-citation analysis across multiple papers"""
    paper_ids: List[int]
    min_intersection: int = 2  # Only return citations citing at least this many papers


class CrossCitationItem(BaseModel):
    """A citation that appears across multiple seed papers"""
    scholar_id: Optional[str] = None
    title: str
    authors: Optional[str] = None
    year: Optional[int] = None
    venue: Optional[str] = None
    link: Optional[str] = None
    cites_count: int  # How many of our papers this citation cites
    cites_papers: List[int]  # Which paper_ids it cites
    own_citation_count: int  # How popular is this paper itself


class BatchCrossResult(BaseModel):
    """Response from batch cross-citation analysis"""
    paper_ids: List[int]
    total_unique_citations: int
    cross_citations: List[CrossCitationItem]


class ExternalPaperInput(BaseModel):
    """Paper input for external API (simplified)"""
    title: str
    authors: Optional[str] = None
    year: Optional[int] = None


class ExternalAnalyzeRequest(BaseModel):
    """Request to analyze papers via external API"""
    papers: List[ExternalPaperInput]
    callback_url: Optional[str] = None  # Webhook URL to call when done
    callback_secret: Optional[str] = None  # Secret for HMAC signing
    options: Optional[dict] = None  # {discover_editions: bool, harvest_citations: bool, compute_cross_citations: bool}
    collection_name: Optional[str] = None  # Optional collection to add papers to
    dossier_name: Optional[str] = None  # Optional dossier name


class ExternalAnalyzeResponse(BaseModel):
    """Response from external analyze request"""
    job_id: int
    paper_ids: List[int]
    status: str
    message: str
    collection_id: Optional[int] = None
    dossier_id: Optional[int] = None


class WebhookPayload(BaseModel):
    """Payload sent to webhook callback URL"""
    event: str  # "job.completed", "job.failed", "job.progress"
    job_id: int
    job_type: str
    status: str
    paper_id: Optional[int] = None
    result: Optional[dict] = None
    error: Optional[str] = None
    progress: Optional[float] = None
    timestamp: datetime


# ============== Batch Operations Schemas ==============

class BatchCollectionAssignment(BaseModel):
    """Assign multiple papers to a collection/dossier at once"""
    paper_ids: List[int]
    collection_id: Optional[int] = None
    dossier_id: Optional[int] = None
    create_new_dossier: bool = False
    new_dossier_name: Optional[str] = None


class BatchForeignEditionRequest(BaseModel):
    """Mark multiple papers as needing foreign editions"""
    paper_ids: List[int]
    foreign_edition_needed: bool = True


class BatchForeignEditionResponse(BaseModel):
    """Response from batch foreign edition marking"""
    updated: int
    paper_ids: List[int]


# Update forward references
PaperDetail.model_rebuild()
PapersPaginatedResponse.model_rebuild()
CollectionDetail.model_rebuild()
DossierDetail.model_rebuild()
