"""
Pydantic schemas for API request/response
"""
from datetime import datetime
from typing import Optional, List, Any, Dict
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
    priority: int = 0  # Higher = runs first (thinker_harvest uses 100)
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


# ============== Dashboard Schemas ==============

class JobHistorySummary(BaseModel):
    """Summary of job outcomes in a time period"""
    completed: int = 0
    failed: int = 0
    cancelled: int = 0


class SystemHealthStats(BaseModel):
    """System health metrics"""
    active_jobs: int
    max_concurrent_jobs: int
    citations_last_hour: int
    papers_with_active_jobs: int
    jobs_24h: JobHistorySummary
    avg_duplicate_rate_1h: float = 0.0


class ActiveHarvestInfo(BaseModel):
    """Information about a currently running harvest"""
    paper_id: int
    paper_title: str
    job_id: int
    job_progress: float
    current_year: Optional[int] = None
    current_page: Optional[int] = None
    citations_saved_job: int = 0
    citations_saved_hour: int = 0
    duplicates_job: int = 0
    duplicate_rate: float = 0.0
    gap_remaining: int = 0
    expected_total: int = 0
    harvested_total: int = 0
    running_minutes: int = 0
    stall_count: int = 0
    edition_count: int = 1


class RecentlyCompletedPaper(BaseModel):
    """Paper that recently completed harvesting"""
    paper_id: int
    paper_title: str
    total_harvested: int
    expected_total: int
    gap_percent: float  # 1.0 = 100% complete
    completed_at: Optional[datetime] = None


class DashboardAlert(BaseModel):
    """Alert for a problem that needs attention"""
    type: str  # high_duplicate_rate, stalled_paper, repeated_failures, etc.
    paper_id: Optional[int] = None
    paper_title: Optional[str] = None
    edition_id: Optional[int] = None
    job_id: Optional[int] = None
    value: Optional[float] = None
    message: str
    # Harvest stats for stalled papers
    harvested_count: Optional[int] = None
    expected_count: Optional[int] = None
    gap_remaining: Optional[int] = None
    stall_count: Optional[int] = None
    # Year completion diagnosis - distinguishes GS fault from our fault
    years_complete: Optional[int] = None  # Years where we've scraped all pages
    years_incomplete: Optional[int] = None  # Years that need more scraping
    years_harvesting: Optional[int] = None  # Years still in progress
    years_total: Optional[int] = None  # Total years with HarvestTargets
    has_overflow_years: Optional[bool] = None  # Any year with >1000 citations?
    # Diagnosis: "gs_fault" (all complete, gap is GS data issue) or "needs_scraping" (incomplete years exist)
    diagnosis: Optional[str] = None


class HarvestDashboardResponse(BaseModel):
    """Complete dashboard data"""
    system_health: SystemHealthStats
    active_harvests: List[ActiveHarvestInfo]
    recently_completed: List[RecentlyCompletedPaper]
    alerts: List[DashboardAlert]
    job_history_summary: dict  # {"last_hour": {...}, "last_6h": {...}, "last_24h": {...}}


class JobHistoryItem(BaseModel):
    """A job in the history view"""
    id: int
    paper_id: Optional[int] = None
    paper_title: Optional[str] = None
    job_type: str
    status: str
    citations_saved: int = 0
    duplicates_found: int = 0
    duration_seconds: Optional[int] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error: Optional[str] = None


class JobHistoryResponse(BaseModel):
    """Paginated job history"""
    jobs: List[JobHistoryItem]
    total: int
    has_more: bool


# ============== AI Diagnosis Schemas ==============

class AIDiagnosisRecommendedAction(BaseModel):
    """Specific recommended action from AI diagnosis"""
    action_type: str  # RESUME, PARTITION, RESET, MARK_COMPLETE, WAIT, MANUAL_REVIEW
    action_description: str
    specific_params: Optional[dict] = None  # {start_year, start_page, skip_years, partition_years}


class AIDiagnosisAnalysis(BaseModel):
    """Parsed AI analysis results"""
    root_cause: Optional[str] = None  # RESUME_BUG, RATE_LIMITING, OVERFLOW_YEAR, etc.
    root_cause_explanation: Optional[str] = None
    gap_recoverable: Optional[bool] = None
    gap_recoverable_explanation: Optional[str] = None
    recommended_action: Optional[AIDiagnosisRecommendedAction] = None
    confidence: Optional[str] = None  # HIGH, MEDIUM, LOW
    additional_notes: Optional[str] = None
    thinking_summary: Optional[str] = None  # Summary of Claude's thinking process
    parse_error: Optional[bool] = None
    raw_response: Optional[str] = None


class AIDiagnosisContextSummary(BaseModel):
    """Summary of context used for diagnosis"""
    expected: int
    harvested: int
    gap: int
    gap_percent: float
    years_total: int
    years_complete: int
    recent_jobs: int


class AIDiagnosisResponse(BaseModel):
    """Response from AI diagnosis endpoint"""
    success: bool
    edition_id: int
    paper_title: Optional[str] = None
    edition_title: Optional[str] = None
    context_summary: Optional[AIDiagnosisContextSummary] = None
    analysis: Optional[AIDiagnosisAnalysis] = None
    raw_thinking: Optional[str] = None  # Truncated thinking for debugging
    error: Optional[str] = None


# ============== Thinker Bibliographies Schemas ==============

class ThinkerCreate(BaseModel):
    """Create a new thinker for bibliography harvesting"""
    name: str  # User input like "Marcuse" or "Herbert Marcuse"
    scholar_profile_url: Optional[str] = None  # e.g., https://scholar.google.com/citations?user=zKHBVTkAAAAJ


class ThinkerUpdate(BaseModel):
    """Update thinker fields"""
    status: Optional[str] = None  # pending, disambiguated, harvesting, complete
    canonical_name: Optional[str] = None
    bio: Optional[str] = None
    domains: Optional[List[str]] = None


class ThinkerCandidate(BaseModel):
    """A candidate thinker from disambiguation"""
    canonical_name: str
    birth_death: Optional[str] = None  # e.g., "1898-1979"
    bio: Optional[str] = None
    domains: List[str] = []  # ["critical theory", "Marxism", "Frankfurt School"]
    notable_works: List[str] = []  # ["One-Dimensional Man", "Eros and Civilization"]
    confidence: float = 0.0


class DisambiguationResponse(BaseModel):
    """Response from thinker disambiguation"""
    is_ambiguous: bool
    primary_candidate: ThinkerCandidate
    alternatives: List[ThinkerCandidate] = []
    confidence: float
    requires_confirmation: bool = False


class ThinkerConfirmRequest(BaseModel):
    """Confirm disambiguation choice"""
    candidate_index: int = 0  # 0 = primary candidate
    custom_domains: Optional[List[str]] = None  # Override domains if needed


class NameVariant(BaseModel):
    """A search query variant for a thinker"""
    query: str  # e.g., 'author:"h marcuse"'
    variant_type: str  # full_name, initial_surname, transliteration, misspelling
    language: Optional[str] = None  # For transliterations


class NameVariantsResponse(BaseModel):
    """Response from name variant generation"""
    thinker_id: int
    canonical_name: str
    variants: List[NameVariant]


class ThinkerResponse(BaseModel):
    """Basic thinker information"""
    id: int
    canonical_name: str
    birth_death: Optional[str] = None
    bio: Optional[str] = None
    domains: List[str] = []
    notable_works: List[str] = []
    name_variants: List[str] = []
    status: str  # pending, disambiguated, harvesting, complete
    works_discovered: int = 0
    works_harvested: int = 0
    total_citations: int = 0
    created_at: datetime
    disambiguated_at: Optional[datetime] = None
    harvest_started_at: Optional[datetime] = None
    harvest_completed_at: Optional[datetime] = None
    # Profile pre-fetch status (automatic after harvest completes)
    profiles_prefetch_status: Optional[str] = None  # null, pending, running, completed, failed
    profiles_prefetch_count: int = 0
    profiles_prefetched_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class ThinkerWorkResponse(BaseModel):
    """A work (paper/book) authored by a thinker"""
    id: int
    thinker_id: int
    paper_id: Optional[int] = None  # Link to Papers table if converted
    scholar_id: Optional[str] = None
    title: str
    authors_raw: Optional[str] = None
    year: Optional[int] = None
    citation_count: int = 0
    # Classification
    decision: str = "accepted"  # accepted, rejected, uncertain
    confidence: float = 0.8
    reason: Optional[str] = None
    # Translation detection
    is_translation: bool = False
    canonical_work_id: Optional[int] = None
    original_language: Optional[str] = None
    detected_language: Optional[str] = None
    # Harvest status
    citations_harvested: bool = False
    harvest_job_id: Optional[int] = None
    created_at: datetime

    class Config:
        from_attributes = True


class ThinkerWorkGroup(BaseModel):
    """A canonical work with its translations"""
    canonical_work: ThinkerWorkResponse
    translations: List[ThinkerWorkResponse] = []
    total_citation_count: int = 0


class ThinkerHarvestRunResponse(BaseModel):
    """A harvest run for a specific name variant query"""
    id: int
    thinker_id: int
    query_used: str
    variant_type: str
    pages_fetched: int = 0
    results_processed: int = 0
    results_accepted: int = 0
    results_rejected: int = 0
    results_uncertain: int = 0
    status: str  # pending, running, completed, failed
    started_at: datetime
    completed_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class ThinkerLLMCallResponse(BaseModel):
    """Audit trail for LLM calls in thinker workflows"""
    id: int
    thinker_id: int
    workflow: str  # disambiguation, variant_generation, page_filtering, translation_detection, retrospective_matching
    model: str
    status: str
    input_tokens: Optional[int] = None
    output_tokens: Optional[int] = None
    thinking_tokens: Optional[int] = None
    latency_ms: Optional[int] = None
    started_at: datetime
    completed_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class ThinkerDetail(ThinkerResponse):
    """Thinker with works and harvest runs"""
    works: List[ThinkerWorkResponse] = []
    work_groups: List[ThinkerWorkGroup] = []  # Works grouped by translation
    harvest_runs: List[ThinkerHarvestRunResponse] = []
    recent_llm_calls: List[ThinkerLLMCallResponse] = []


class StartWorkDiscoveryRequest(BaseModel):
    """Request to start discovering works by a thinker"""
    variant_types: List[str] = []  # If empty, use all generated variants
    max_pages_per_variant: int = 100  # Safety limit


class StartWorkDiscoveryResponse(BaseModel):
    """Response from starting work discovery"""
    thinker_id: int
    job_id: int
    variants_to_search: int
    status: str
    message: str


class DetectTranslationsRequest(BaseModel):
    """Request to run translation detection on discovered works"""
    force_rerun: bool = False  # Re-analyze even if already done


class DetectTranslationsResponse(BaseModel):
    """Response from translation detection"""
    thinker_id: int
    total_works: int
    groups_identified: int
    translations_found: int
    llm_call_id: int


class HarvestCitationsRequest(BaseModel):
    """Request to harvest citations for all discovered works"""
    work_ids: Optional[List[int]] = None  # If None, harvest all accepted works
    skip_existing: bool = True  # Skip works already converted to Papers
    max_works: Optional[int] = None  # Maximum number of works to process per batch


class HarvestCitationsResponse(BaseModel):
    """Response from starting citation harvest job"""
    job_id: int
    thinker_id: int
    works_pending: int  # Number of accepted works pending harvest
    message: str


class RetrospectiveMatchRequest(BaseModel):
    """Request to match existing papers to thinkers"""
    thinker_ids: Optional[List[int]] = None  # If None, match all thinkers
    paper_ids: Optional[List[int]] = None  # If None, scan all papers


class RetrospectiveMatchResponse(BaseModel):
    """Response from retrospective matching"""
    matches_found: int
    papers_scanned: int
    thinkers_checked: int
    llm_call_id: int
    matches: List[dict] = []  # [{paper_id, thinker_id, confidence, reason}]


class ThinkerQuickAddRequest(BaseModel):
    """Quick-add a thinker: 'harvest works by Marcuse'"""
    input: str  # Natural language input


class ThinkerQuickAddResponse(BaseModel):
    """Response from quick-add"""
    thinker_id: int
    canonical_name: str
    disambiguation_required: bool
    disambiguation: Optional[DisambiguationResponse] = None
    message: str


# ============== Thinker Analytics Schemas ==============

class CitingPaper(BaseModel):
    """A paper that cites one of the thinker's works"""
    citation_id: int  # ID in citations table
    scholar_id: Optional[str] = None  # Scholar ID for matching
    title: Optional[str] = None
    authors: Optional[str] = None
    author_profiles: Optional[List[Dict[str, Any]]] = None  # [{name, profile_url}, ...]
    year: Optional[int] = None
    venue: Optional[str] = None
    link: Optional[str] = None  # URL to the paper
    citation_count: int = 0  # How cited is this paper itself
    cites_works: int = 1  # How many of thinker's works it cites
    existing_paper_id: Optional[int] = None  # Paper ID if already seeded


class CitingAuthor(BaseModel):
    """An author who cites the thinker's work"""
    author: str
    citation_count: int  # Total citations from this author
    papers_count: int  # Number of distinct papers
    is_self_citation: bool = False  # Is this author the thinker themselves?
    confidence: float = 1.0  # LLM confidence in the is_self_citation determination
    citation_ids: List[int] = []  # Citation IDs for fetching this author's papers
    profile_url: Optional[str] = None  # Google Scholar profile URL if known
    # Enriched profile data (fetched from Scholar profile page)
    full_name: Optional[str] = None  # Full name from Scholar profile
    affiliation: Optional[str] = None  # Institution/university
    homepage_url: Optional[str] = None  # Personal homepage
    topics: Optional[List[str]] = None  # Research interests
    publications_count: int = 0  # Number of publications on their profile


class AuthorPublication(BaseModel):
    """A publication from an author's Google Scholar profile"""
    title: str
    authors: Optional[str] = None
    venue: Optional[str] = None
    year: Optional[int] = None
    citations: int = 0
    link: Optional[str] = None
    scholar_id: Optional[str] = None  # Citation ID within Scholar


class ScholarAuthorProfileResponse(BaseModel):
    """Google Scholar author profile data"""
    scholar_user_id: str
    profile_url: str
    full_name: Optional[str] = None
    affiliation: Optional[str] = None
    homepage_url: Optional[str] = None
    topics: List[str] = []
    publications: List[AuthorPublication] = []  # Author's publications
    publications_count: int = 0  # Total publications found
    fetched_at: Optional[datetime] = None


class MostCitedWork(BaseModel):
    """One of the thinker's most cited works"""
    work_id: int
    paper_id: Optional[int] = None  # Associated Paper ID if harvested
    scholar_id: Optional[str] = None  # Scholar ID for seeding
    title: str
    authors: Optional[str] = None  # Co-authors
    year: Optional[int] = None
    citations_received: int = 0
    link: Optional[str] = None  # Link to Google Scholar


class TopVenue(BaseModel):
    """A venue where the thinker's work is cited"""
    venue: str
    citation_count: int
    papers_count: int


class YearCitations(BaseModel):
    """Citation count for a specific year"""
    year: int
    count: int


class ThinkerAnalyticsResponse(BaseModel):
    """Comprehensive analytics for a thinker's scholarly impact"""
    thinker_id: int
    thinker_name: str
    total_citations: int
    total_works: int
    unique_citing_papers: int
    unique_citing_authors: int
    unique_venues: int

    top_citing_papers: List[CitingPaper] = []
    top_citing_authors: List[CitingAuthor] = []
    most_cited_works: List[MostCitedWork] = []
    top_venues: List[TopVenue] = []
    citations_by_year: List[YearCitations] = []

    # Debug info for author LLM processing
    debug_llm_processed: Optional[bool] = None
    debug_llm_error: Optional[str] = None


# ============== Citation to Seed Schemas ==============

class CitationMakeSeedRequest(BaseModel):
    """Request to convert a citation into a seed paper"""
    dossier_id: Optional[int] = None  # Target dossier (optional)
    create_new_dossier: bool = False
    new_dossier_name: Optional[str] = None
    collection_id: Optional[int] = None  # Required if creating new dossier


class CitationMakeSeedResponse(BaseModel):
    """Response after converting citation to seed"""
    paper_id: int
    title: str
    dossier_id: Optional[int] = None
    dossier_name: Optional[str] = None
    message: str


# ============== Author Search Schemas ==============

class AuthorPaperResult(BaseModel):
    """A paper result from author search"""
    source: str  # 'citation' or 'paper'
    id: int
    title: str
    authors: Optional[str] = None
    year: Optional[int] = None
    venue: Optional[str] = None
    citation_count: int = 0
    link: Optional[str] = None
    # For citations: which thinker/paper it cites
    citing_thinker_id: Optional[int] = None
    citing_thinker_name: Optional[str] = None
    citing_paper_id: Optional[int] = None
    citing_paper_title: Optional[str] = None
    # For flagging papers from current context
    is_from_current_thinker: bool = False


class AuthorSearchResponse(BaseModel):
    """Response from author search"""
    query: str
    total_results: int
    papers: List[AuthorPaperResult] = []
    citations: List[AuthorPaperResult] = []


# ============== Edition Analysis Schemas ==============
# Phase 6: API schemas for exhaustive edition analysis


class WorkResponse(BaseModel):
    """An abstract intellectual work (e.g., 'The Spirit of Utopia')"""
    id: int
    thinker_name: str
    canonical_title: str
    original_language: Optional[str] = None
    original_title: Optional[str] = None
    original_year: Optional[int] = None
    work_type: Optional[str] = None  # book, article, essay, lecture
    importance: Optional[str] = None  # major, minor, peripheral
    notes: Optional[str] = None
    created_at: datetime
    # Computed fields
    edition_count: int = 0
    languages_available: List[str] = []

    class Config:
        from_attributes = True


class WorkEditionResponse(BaseModel):
    """Links a Work to a Paper/Edition"""
    id: int
    work_id: int
    paper_id: Optional[int] = None
    edition_id: Optional[int] = None
    language: str
    edition_type: Optional[str] = None  # original, translation, abridged, anthology_excerpt
    year: Optional[int] = None
    verified: bool = False
    auto_linked: bool = True
    confidence: Optional[float] = None
    created_at: datetime
    # Joined data
    title: Optional[str] = None
    citation_count: int = 0

    class Config:
        from_attributes = True


class MissingEditionResponse(BaseModel):
    """A gap identified in a thinker's bibliography"""
    id: int
    work_id: int
    language: str
    expected_title: Optional[str] = None
    expected_year: Optional[int] = None
    source: Optional[str] = None  # llm_knowledge, web_search, google_scholar
    source_url: Optional[str] = None
    priority: Optional[str] = None  # high, medium, low
    status: str = "pending"  # pending, job_created, found, dismissed
    job_id: Optional[int] = None
    notes: Optional[str] = None
    created_at: datetime
    # Joined data from Work
    work_canonical_title: Optional[str] = None
    work_original_language: Optional[str] = None

    class Config:
        from_attributes = True


class EditionAnalysisRunResponse(BaseModel):
    """An edition analysis run (audit trail)"""
    id: int
    dossier_id: int
    thinker_name: str
    status: str  # pending, analyzing, web_searching, verifying, completed, failed
    phase: Optional[str] = None
    phase_progress: float = 0.0
    papers_analyzed: int = 0
    editions_analyzed: int = 0
    works_identified: int = 0
    links_created: int = 0
    gaps_found: int = 0
    jobs_created: int = 0
    llm_calls_count: int = 0
    web_searches_count: int = 0
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    thinking_tokens: int = 0
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error: Optional[str] = None
    error_phase: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True


class EditionAnalysisLLMCallResponse(BaseModel):
    """Audit trail for LLM calls during edition analysis"""
    id: int
    run_id: int
    phase: Optional[str] = None  # inventory, bibliographic_research, gap_analysis, verification
    model: Optional[str] = None
    prompt: Optional[str] = None
    context_json: Optional[dict] = None
    raw_response: Optional[str] = None
    parsed_result: Optional[dict] = None
    thinking_text: Optional[str] = None
    thinking_tokens: Optional[int] = None
    input_tokens: Optional[int] = None
    output_tokens: Optional[int] = None
    latency_ms: Optional[int] = None
    web_search_used: bool = False
    status: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True


class StartEditionAnalysisRequest(BaseModel):
    """Request to start edition analysis for a dossier"""
    force_rerun: bool = False  # Re-analyze even if recently completed


class StartEditionAnalysisResponse(BaseModel):
    """Response from starting edition analysis"""
    run_id: int
    dossier_id: int
    thinker_name: str
    status: str
    message: str


class WorkWithEditionsResponse(BaseModel):
    """A work with all its linked editions"""
    id: int
    thinker_name: str
    canonical_title: str
    original_language: Optional[str] = None
    original_title: Optional[str] = None
    original_year: Optional[int] = None
    work_type: Optional[str] = None
    importance: Optional[str] = None
    notes: Optional[str] = None
    created_at: datetime
    updated_at: Optional[datetime] = None
    editions: List[WorkEditionResponse] = []
    missing_editions: List[MissingEditionResponse] = []


class EditionAnalysisResultResponse(BaseModel):
    """Full results of an edition analysis"""
    dossier_id: int
    thinker_name: str
    run: Optional[EditionAnalysisRunResponse] = None
    works: List[WorkWithEditionsResponse] = []
    total_works: int = 0
    total_editions: int = 0
    total_gaps: int = 0
    pending_gaps: int = 0


class CreateJobFromGapRequest(BaseModel):
    """Request to create a scraper job for a specific gap"""
    priority: int = 10  # Default priority


class CreateJobFromGapResponse(BaseModel):
    """Response from creating a job for a gap"""
    job_id: int
    missing_edition_id: int
    message: str


class DismissGapRequest(BaseModel):
    """Request to dismiss a gap (mark as not actually missing)"""
    reason: Optional[str] = None


class ThinkerBibliographyResponse(BaseModel):
    """Full bibliography for a thinker from edition analysis"""
    thinker_name: str
    works: List[WorkWithEditionsResponse] = []
    total_works: int = 0
    total_editions: int = 0
    total_missing: int = 0
    analysis_run: Optional[EditionAnalysisRunResponse] = None


# Update forward references
PaperDetail.model_rebuild()
PapersPaginatedResponse.model_rebuild()
CollectionDetail.model_rebuild()
DossierDetail.model_rebuild()
ThinkerDetail.model_rebuild()
