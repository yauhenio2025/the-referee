"""
Database models for The Referee
"""
from datetime import datetime
from typing import Optional, List
from sqlalchemy import String, Integer, Text, DateTime, Boolean, ForeignKey, JSON, Float, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Collection(Base):
    """A collection of related papers for grouped analysis"""
    __tablename__ = "collections"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(200), unique=True)
    description: Mapped[Optional[str]] = mapped_column(Text)
    color: Mapped[Optional[str]] = mapped_column(String(20))  # For UI display (hex color)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    dossiers: Mapped[List["Dossier"]] = relationship(back_populates="collection", cascade="all, delete-orphan")
    # Legacy: papers relationship kept for migration, but papers now belong to dossiers
    papers: Mapped[List["Paper"]] = relationship(back_populates="collection")


class Dossier(Base):
    """A dossier within a collection - papers belong to dossiers, not directly to collections"""
    __tablename__ = "dossiers"

    id: Mapped[int] = mapped_column(primary_key=True)
    collection_id: Mapped[int] = mapped_column(ForeignKey("collections.id", ondelete="CASCADE"), index=True)
    name: Mapped[str] = mapped_column(String(200))
    description: Mapped[Optional[str]] = mapped_column(Text)
    color: Mapped[Optional[str]] = mapped_column(String(20))  # For UI display (hex color)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    collection: Mapped["Collection"] = relationship(back_populates="dossiers")
    papers: Mapped[List["Paper"]] = relationship(back_populates="dossier")
    # Edition analysis runs for this dossier
    edition_analysis_runs: Mapped[List["EditionAnalysisRun"]] = relationship(
        "EditionAnalysisRun",
        back_populates="dossier",
        cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("ix_dossiers_collection", "collection_id"),
    )


class PaperAdditionalDossier(Base):
    """Junction table for papers that belong to multiple dossiers.

    Papers have a primary dossier (Paper.dossier_id) and can additionally
    belong to other dossiers through this junction table.
    """
    __tablename__ = "paper_additional_dossiers"

    id: Mapped[int] = mapped_column(primary_key=True)
    paper_id: Mapped[int] = mapped_column(ForeignKey("papers.id", ondelete="CASCADE"), index=True)
    dossier_id: Mapped[int] = mapped_column(ForeignKey("dossiers.id", ondelete="CASCADE"), index=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_paper_additional_dossiers_paper", "paper_id"),
        Index("ix_paper_additional_dossiers_dossier", "dossier_id"),
        Index("ix_paper_additional_dossiers_unique", "paper_id", "dossier_id", unique=True),
    )


class Paper(Base):
    """A seed paper to analyze for citations"""
    __tablename__ = "papers"

    id: Mapped[int] = mapped_column(primary_key=True)
    # Legacy: collection_id kept for backward compatibility, but papers now belong to dossiers
    collection_id: Mapped[Optional[int]] = mapped_column(ForeignKey("collections.id", ondelete="SET NULL"), index=True)
    dossier_id: Mapped[Optional[int]] = mapped_column(ForeignKey("dossiers.id", ondelete="SET NULL"), index=True)
    scholar_id: Mapped[Optional[str]] = mapped_column(String(50), unique=True, index=True)
    cluster_id: Mapped[Optional[str]] = mapped_column(String(50), index=True)

    title: Mapped[str] = mapped_column(Text)
    authors: Mapped[Optional[str]] = mapped_column(Text)  # JSON array as string
    year: Mapped[Optional[int]] = mapped_column(Integer)
    venue: Mapped[Optional[str]] = mapped_column(String(500))
    abstract: Mapped[Optional[str]] = mapped_column(Text)
    abstract_source: Mapped[Optional[str]] = mapped_column(String(50))  # 'scholar_search', 'allintitle_scrape', 'manual'
    link: Mapped[Optional[str]] = mapped_column(Text)

    citation_count: Mapped[int] = mapped_column(Integer, default=0)
    language: Mapped[Optional[str]] = mapped_column(String(50))

    # Status: pending, needs_reconciliation, resolved, error
    status: Mapped[str] = mapped_column(String(50), default="pending")
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Candidate papers for reconciliation (JSON array)
    candidates: Mapped[Optional[str]] = mapped_column(Text)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Aggregate harvest tracking (computed from editions, for quick staleness checks)
    any_edition_harvested_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True, default=None)
    total_harvested_citations: Mapped[int] = mapped_column(Integer, default=0)

    # Edition management
    editions_finalized: Mapped[bool] = mapped_column(Boolean, default=False)  # User finalized edition selection

    # Job control
    harvest_paused: Mapped[bool] = mapped_column(Boolean, default=False)  # Pause auto-resume for this paper

    # Soft delete
    deleted_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True, default=None)

    # Foreign edition tracking - mark papers that need foreign editions to be looked up
    foreign_edition_needed: Mapped[bool] = mapped_column(Boolean, default=False)

    # Relationships
    collection: Mapped[Optional["Collection"]] = relationship(back_populates="papers")
    dossier: Mapped[Optional["Dossier"]] = relationship(back_populates="papers")
    editions: Mapped[List["Edition"]] = relationship(back_populates="paper", cascade="all, delete-orphan")
    citations: Mapped[List["Citation"]] = relationship(back_populates="paper", cascade="all, delete-orphan")
    jobs: Mapped[List["Job"]] = relationship(back_populates="paper", cascade="all, delete-orphan")
    # Additional dossiers (many-to-many via junction table)
    additional_dossiers: Mapped[List["PaperAdditionalDossier"]] = relationship(cascade="all, delete-orphan")
    # Work edition link (if this paper is linked to a Work)
    work_edition: Mapped[Optional["WorkEdition"]] = relationship(
        "WorkEdition",
        foreign_keys="WorkEdition.paper_id",
        uselist=False
    )

    __table_args__ = (
        Index("ix_papers_title", "title"),  # Regular index for title lookups
    )


class Edition(Base):
    """An edition/translation of a paper found via edition discovery"""
    __tablename__ = "editions"

    id: Mapped[int] = mapped_column(primary_key=True)
    paper_id: Mapped[int] = mapped_column(ForeignKey("papers.id", ondelete="CASCADE"))

    scholar_id: Mapped[Optional[str]] = mapped_column(String(50), index=True)
    cluster_id: Mapped[Optional[str]] = mapped_column(String(50))

    title: Mapped[str] = mapped_column(Text)
    authors: Mapped[Optional[str]] = mapped_column(Text)
    year: Mapped[Optional[int]] = mapped_column(Integer)
    venue: Mapped[Optional[str]] = mapped_column(String(500))
    abstract: Mapped[Optional[str]] = mapped_column(Text)
    link: Mapped[Optional[str]] = mapped_column(Text)

    citation_count: Mapped[int] = mapped_column(Integer, default=0)
    language: Mapped[Optional[str]] = mapped_column(String(50))

    # Discovery metadata
    confidence: Mapped[str] = mapped_column(String(20), default="uncertain")  # high, uncertain
    auto_selected: Mapped[bool] = mapped_column(Boolean, default=False)
    found_by_query: Mapped[Optional[str]] = mapped_column(Text)

    # Selection status
    selected: Mapped[bool] = mapped_column(Boolean, default=False)  # User selected for citation extraction
    excluded: Mapped[bool] = mapped_column(Boolean, default=False)  # User excluded from view

    # Supplementary flag - True if added via "Fetch more" button
    is_supplementary: Mapped[bool] = mapped_column(Boolean, default=False)

    # Track which job added this edition (for NEW badge - null means not new)
    added_by_job_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, default=None)

    # Citation harvest tracking (for auto-updater feature)
    last_harvested_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True, default=None)
    last_harvest_year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, default=None)
    harvested_citation_count: Mapped[int] = mapped_column(Integer, default=0)

    # Year-by-year harvest resume state (JSON: {mode, current_year, current_page, completed_years})
    # Allows proper resume without re-fetching already-processed years
    harvest_resume_state: Mapped[Optional[str]] = mapped_column(Text, nullable=True, default=None)

    # Stall detection - consecutive jobs with zero new citations
    # Used to prevent infinite auto-resume loops when harvest can't progress further
    harvest_stall_count: Mapped[int] = mapped_column(Integer, default=0)

    # Harvest completion tracking - when we've verified we can't get more citations
    # This stops auto-resume even if there's a gap (the gap is GS's fault, not ours)
    harvest_complete: Mapped[bool] = mapped_column(Boolean, default=False)
    # Reason: "exhausted" (all years complete), "manual" (user marked), "gs_inaccuracy" (verified gap is GS fault)
    harvest_complete_reason: Mapped[Optional[str]] = mapped_column(String(50), nullable=True, default=None)

    # Stall tracking for diagnostics
    harvest_reset_count: Mapped[int] = mapped_column(Integer, default=0)  # How many times stall was reset
    last_stall_year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # Year that caused last stall
    last_stall_offset: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # Page offset at stall
    last_stall_reason: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)  # Reason: "zero_new", "rate_limit", "parse_error"
    last_stall_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)  # When the stall occurred

    # Edition merging - when editions are duplicates (same work, different URLs/scholar_ids)
    # The merged edition's citations are pooled into the canonical edition
    # But we keep both scholar_ids for harvesting from both Google Scholar entries
    merged_into_edition_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("editions.id", ondelete="SET NULL"), nullable=True, default=None
    )
    # For merged editions: how many citations were harvested FROM this scholar_id
    # (these citations are assigned to the canonical edition, but we track the contribution)
    redirected_harvest_count: Mapped[int] = mapped_column(Integer, default=0)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # Relationships
    paper: Mapped["Paper"] = relationship(back_populates="editions")
    merged_editions: Mapped[List["Edition"]] = relationship(
        "Edition",
        backref="canonical_edition",
        remote_side="Edition.id",
        foreign_keys="Edition.merged_into_edition_id"
    )
    # Work edition link (if this edition is linked to a Work)
    work_edition: Mapped[Optional["WorkEdition"]] = relationship(
        "WorkEdition",
        foreign_keys="WorkEdition.edition_id",
        uselist=False
    )


class Citation(Base):
    """A paper that cites a seed paper (or one of its editions)"""
    __tablename__ = "citations"

    id: Mapped[int] = mapped_column(primary_key=True)
    paper_id: Mapped[int] = mapped_column(ForeignKey("papers.id", ondelete="CASCADE"))
    edition_id: Mapped[Optional[int]] = mapped_column(ForeignKey("editions.id", ondelete="SET NULL"))

    scholar_id: Mapped[Optional[str]] = mapped_column(String(50), index=True)

    title: Mapped[str] = mapped_column(Text)
    authors: Mapped[Optional[str]] = mapped_column(Text)
    # JSON array of author profiles: [{"name": "S Brammer", "profile_url": "https://scholar..."}]
    author_profiles: Mapped[Optional[str]] = mapped_column(Text)
    year: Mapped[Optional[int]] = mapped_column(Integer)
    venue: Mapped[Optional[str]] = mapped_column(String(500))
    abstract: Mapped[Optional[str]] = mapped_column(Text)
    link: Mapped[Optional[str]] = mapped_column(Text)

    citation_count: Mapped[int] = mapped_column(Integer, default=0)

    # For cross-citation analysis
    intersection_count: Mapped[int] = mapped_column(Integer, default=1)  # How many seeds this cites

    # Duplicate encounter tracking - increments each time we see this paper in GS results
    # Helps reconcile our count vs GS count (GS tolerates duplicates, we don't)
    # SUM(encounter_count) = GS-equivalent count, COUNT(*) = our deduplicated count
    encounter_count: Mapped[int] = mapped_column(Integer, default=1)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # User review tracking - TODO: add column to production DB first
    # reviewed: Mapped[bool] = mapped_column(Boolean, default=False)

    # Relationships
    paper: Mapped["Paper"] = relationship(back_populates="citations")

    __table_args__ = (
        # Unique constraint required for ON CONFLICT (paper_id, scholar_id) DO NOTHING
        Index("ix_citations_paper_scholar_unique", "paper_id", "scholar_id", unique=True),
    )


class ScholarAuthorProfile(Base):
    """Cached Google Scholar author profile data"""
    __tablename__ = "scholar_author_profiles"

    id: Mapped[int] = mapped_column(primary_key=True)
    # The user ID from Google Scholar URL (e.g., "1X4qGg4AAAAJ" from citations?user=1X4qGg4AAAAJ)
    scholar_user_id: Mapped[str] = mapped_column(String(50), unique=True, index=True)
    profile_url: Mapped[str] = mapped_column(Text)

    # Profile data
    full_name: Mapped[Optional[str]] = mapped_column(String(255))
    affiliation: Mapped[Optional[str]] = mapped_column(String(500))
    homepage_url: Mapped[Optional[str]] = mapped_column(Text)
    # JSON array of topics: ["Business Ethics", "Corporate Social Responsibility", ...]
    topics: Mapped[Optional[str]] = mapped_column(Text)
    # JSON array of publications: [{title, authors, venue, year, citations, scholar_id, link}, ...]
    publications: Mapped[Optional[str]] = mapped_column(Text)
    publications_count: Mapped[int] = mapped_column(Integer, default=0)

    # Timestamps
    fetched_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class Job(Base):
    """Background processing job"""
    __tablename__ = "jobs"

    id: Mapped[int] = mapped_column(primary_key=True)
    paper_id: Mapped[Optional[int]] = mapped_column(ForeignKey("papers.id", ondelete="CASCADE"))

    job_type: Mapped[str] = mapped_column(String(50))  # resolve, discover_editions, extract_citations, fetch_more_editions
    status: Mapped[str] = mapped_column(String(20), default="pending")  # pending, running, completed, failed
    priority: Mapped[int] = mapped_column(Integer, default=0)

    # Job parameters (JSON) - e.g., {"language": "italian", "max_results": 50}
    params: Mapped[Optional[str]] = mapped_column(Text)

    # Progress
    progress: Mapped[float] = mapped_column(Float, default=0.0)
    progress_message: Mapped[Optional[str]] = mapped_column(Text)

    # Results
    result: Mapped[Optional[str]] = mapped_column(Text)  # JSON
    error: Mapped[Optional[str]] = mapped_column(Text)

    # Timing
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Webhook callback (for external API integration)
    callback_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True, default=None)
    callback_secret: Mapped[Optional[str]] = mapped_column(String(256), nullable=True, default=None)
    callback_sent_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    callback_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True, default=None)

    # Relationships
    paper: Mapped[Optional["Paper"]] = relationship(back_populates="jobs")

    __table_args__ = (
        Index("ix_jobs_status_priority", "status", "priority"),
    )


class RawSearchResult(Base):
    """Raw search results before LLM processing - for debugging/auditing"""
    __tablename__ = "raw_search_results"

    id: Mapped[int] = mapped_column(primary_key=True)
    paper_id: Mapped[int] = mapped_column(ForeignKey("papers.id", ondelete="CASCADE"))
    job_id: Mapped[Optional[int]] = mapped_column(ForeignKey("jobs.id", ondelete="SET NULL"))

    # Search context
    search_type: Mapped[str] = mapped_column(String(50))  # discover_editions, fetch_more
    target_language: Mapped[Optional[str]] = mapped_column(String(50))
    query: Mapped[str] = mapped_column(Text)

    # Raw results from Scholar (before LLM)
    raw_results: Mapped[str] = mapped_column(Text)  # JSON array
    result_count: Mapped[int] = mapped_column(Integer, default=0)

    # LLM classification results
    llm_classification: Mapped[Optional[str]] = mapped_column(Text)  # JSON with high/uncertain/rejected

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_raw_search_paper", "paper_id"),
        Index("ix_raw_search_job", "job_id"),
    )


class SearchCache(Base):
    """Cache for Google Scholar search results"""
    __tablename__ = "search_cache"

    id: Mapped[int] = mapped_column(primary_key=True)
    query_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    query: Mapped[str] = mapped_column(Text)

    results: Mapped[str] = mapped_column(Text)  # JSON array of papers
    result_count: Mapped[int] = mapped_column(Integer, default=0)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    expires_at: Mapped[datetime] = mapped_column(DateTime)

    __table_args__ = (
        Index("ix_cache_expires", "expires_at"),
    )


class FailedFetch(Base):
    """Track failed page fetches for retry later.

    When a page fetch fails after all retries, store it here instead of skipping.
    A background job will periodically retry these until successful.
    """
    __tablename__ = "failed_fetches"

    id: Mapped[int] = mapped_column(primary_key=True)
    edition_id: Mapped[int] = mapped_column(ForeignKey("editions.id", ondelete="CASCADE"), index=True)

    # The full URL that failed
    url: Mapped[str] = mapped_column(Text)

    # Context for retry
    year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # Year filter if applicable
    page_number: Mapped[int] = mapped_column(Integer)  # Which page (0-indexed)

    # Retry tracking
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    last_retry_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Status: pending, retrying, succeeded, abandoned (after max retries)
    status: Mapped[str] = mapped_column(String(20), default="pending")

    # When successfully retried, how many citations were recovered
    recovered_citations: Mapped[int] = mapped_column(Integer, default=0)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    __table_args__ = (
        Index("ix_failed_fetches_status", "status"),
        Index("ix_failed_fetches_edition", "edition_id"),
    )


class HarvestTarget(Base):
    """Track expected citation counts per partition for an edition.

    Partitions can be:
    - year: Traditional year-by-year harvesting (legacy, still used for tracking)
    - letter: Author-letter partitioning (e.g., 'a', 'b', ... 'z', or '_' for no-letter)
    - Combined: Both year and letter can be set for fine-grained tracking

    When we start harvesting, we record the total count Scholar reports for each partition.
    This lets us verify completeness and identify gaps.
    """
    __tablename__ = "harvest_targets"

    id: Mapped[int] = mapped_column(primary_key=True)
    edition_id: Mapped[int] = mapped_column(ForeignKey("editions.id", ondelete="CASCADE"), index=True)

    year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # null = all years combined
    letter: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)  # partition key: 'a'-'z', '_', 'lang_zh-CN', 'a_excl', etc.

    # What Scholar reported as the total count
    expected_count: Mapped[int] = mapped_column(Integer)

    # What we actually harvested
    actual_count: Mapped[int] = mapped_column(Integer, default=0)

    # Status: harvesting, complete, incomplete
    status: Mapped[str] = mapped_column(String(20), default="harvesting")

    # Track pages harvested for this year
    pages_attempted: Mapped[int] = mapped_column(Integer, default=0)
    pages_succeeded: Mapped[int] = mapped_column(Integer, default=0)
    pages_failed: Mapped[int] = mapped_column(Integer, default=0)

    # Gap tracking for diagnostics
    # Original count GS showed on page 1
    original_expected: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    # Last count GS showed (may differ from original as we paginate)
    final_gs_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    # Reason for gap: gs_estimate_changed, rate_limit, parse_error, max_pages_reached,
    #                 blocked, captcha, empty_page, pagination_ended, unknown
    gap_reason: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    # Additional context as JSON
    gap_details: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    # Page number where scraping stopped
    last_scraped_page: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    # Manual review tracking
    gap_reviewed: Mapped[bool] = mapped_column(Boolean, default=False)
    gap_review_notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    __table_args__ = (
        Index("ix_harvest_targets_edition_year", "edition_id", "year", unique=True),
    )


class HarvestQuery(Base):
    """Universal query logging for ALL harvesting operations.

    Records every Google Scholar query we execute, providing full traceability
    for both standard (<1000) and overflow (>1000) harvesting.

    This enables:
    - Debugging why certain citations weren't captured
    - Analyzing query patterns and success rates
    - Resuming from specific queries on failure
    - Auditing API usage and costs
    """
    __tablename__ = "harvest_queries"

    id: Mapped[int] = mapped_column(primary_key=True)
    edition_id: Mapped[int] = mapped_column(ForeignKey("editions.id", ondelete="CASCADE"), index=True)
    job_id: Mapped[Optional[int]] = mapped_column(ForeignKey("jobs.id", ondelete="SET NULL"), nullable=True, index=True)

    # The actual query sent to Google Scholar
    query_string: Mapped[str] = mapped_column(Text, nullable=False)

    # Partition type: 'standard', 'year', 'letter', 'lang', 'subdivision'
    partition_type: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    # Partition value: year (2020), letter (a), lang code (zh-CN), or combined (a_excl)
    partition_value: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    # Pagination
    page_number: Mapped[int] = mapped_column(Integer, default=0)

    # Results
    results_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # Citations returned
    success: Mapped[bool] = mapped_column(Boolean, default=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_harvest_queries_edition", "edition_id"),
        Index("ix_harvest_queries_job", "job_id"),
        Index("ix_harvest_queries_created", "created_at"),
    )


# ============== PARTITION HARVEST TRACEABILITY ==============
# Complete tracking of overflow year harvesting using partition strategy


class PartitionRun(Base):
    """
    Master record for a partition harvest attempt on an overflow year.

    Tracks the complete lifecycle of partitioning a year with >1000 citations:
    1. Initial detection of overflow
    2. Term discovery phase (finding exclusions)
    3. Exclusion set harvest
    4. Inclusion set harvest (or recursive partition)
    5. Final results
    """
    __tablename__ = "partition_runs"

    id: Mapped[int] = mapped_column(primary_key=True)

    # What we're partitioning
    edition_id: Mapped[int] = mapped_column(ForeignKey("editions.id", ondelete="CASCADE"), index=True)
    job_id: Mapped[Optional[int]] = mapped_column(ForeignKey("jobs.id", ondelete="SET NULL"), nullable=True, index=True)
    year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)  # NULL for author-letter partitions
    letter: Mapped[Optional[str]] = mapped_column(String(20), nullable=True, index=True)  # partition key: 'a'-'z', '_', 'lang_zh-CN', 'a_excl', etc.

    # Parent partition (for recursive partitioning)
    parent_partition_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("partition_runs.id", ondelete="CASCADE"), nullable=True, index=True
    )
    depth: Mapped[int] = mapped_column(Integer, default=0)  # 0 = top level, 1+ = recursive

    # Base query constraint (for recursive partitions, this is the inclusion query from parent)
    base_query: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Initial state
    initial_count: Mapped[int] = mapped_column(Integer)  # What Scholar reported before partitioning
    target_threshold: Mapped[int] = mapped_column(Integer, default=950)  # Target to get below

    # Status tracking
    status: Mapped[str] = mapped_column(String(30), default="pending", index=True)
    # Statuses: pending, finding_terms, terms_found, terms_failed,
    #           harvesting_exclusion, harvesting_inclusion,
    #           needs_recursive, completed, failed, aborted

    # Term discovery results
    terms_tried_count: Mapped[int] = mapped_column(Integer, default=0)
    terms_kept_count: Mapped[int] = mapped_column(Integer, default=0)
    final_exclusion_terms: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON array
    final_exclusion_query: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    exclusion_set_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # Count after exclusions

    # Inclusion set info
    final_inclusion_query: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    inclusion_set_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Harvest results
    exclusion_harvested: Mapped[int] = mapped_column(Integer, default=0)
    inclusion_harvested: Mapped[int] = mapped_column(Integer, default=0)
    total_harvested: Mapped[int] = mapped_column(Integer, default=0)
    total_new_unique: Mapped[int] = mapped_column(Integer, default=0)  # After dedup

    # Error tracking
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error_stage: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    # Timing
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    terms_started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    terms_completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    exclusion_started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    exclusion_completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    inclusion_started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    inclusion_completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    # Relationships
    term_attempts: Mapped[List["PartitionTermAttempt"]] = relationship(
        "PartitionTermAttempt", back_populates="partition_run", cascade="all, delete-orphan"
    )
    queries: Mapped[List["PartitionQuery"]] = relationship(
        "PartitionQuery", back_populates="partition_run", cascade="all, delete-orphan"
    )
    llm_calls: Mapped[List["PartitionLLMCall"]] = relationship(
        "PartitionLLMCall", back_populates="partition_run", cascade="all, delete-orphan"
    )
    child_partitions: Mapped[List["PartitionRun"]] = relationship(
        "PartitionRun",
        back_populates="parent_partition",
        primaryjoin="PartitionRun.parent_partition_id == PartitionRun.id",
        foreign_keys="[PartitionRun.parent_partition_id]"
    )
    parent_partition: Mapped[Optional["PartitionRun"]] = relationship(
        "PartitionRun",
        back_populates="child_partitions",
        primaryjoin="PartitionRun.id == foreign(PartitionRun.parent_partition_id)",
        remote_side="[PartitionRun.id]"
    )

    # NOTE: Indexes ix_partition_runs_edition_year and ix_partition_runs_status
    # already exist in production - don't define in __table_args__


class PartitionTermAttempt(Base):
    """
    Record of every term we attempted to use as an exclusion.

    Tracks what terms were suggested by LLM, what count reduction they achieved,
    and whether we kept them in the final exclusion set.
    """
    __tablename__ = "partition_term_attempts"

    id: Mapped[int] = mapped_column(primary_key=True)
    partition_run_id: Mapped[int] = mapped_column(
        ForeignKey("partition_runs.id", ondelete="CASCADE"), index=True
    )

    # The term
    term: Mapped[str] = mapped_column(String(100), index=True)
    order_tried: Mapped[int] = mapped_column(Integer)  # 1, 2, 3... in order tried

    # Source of this term
    source: Mapped[str] = mapped_column(String(20))  # 'llm', 'fallback', 'manual', 'domain'
    llm_call_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("partition_llm_calls.id", ondelete="SET NULL"), nullable=True
    )

    # What query we used to test this term
    test_query: Mapped[str] = mapped_column(Text)  # Full query with all exclusions including this one

    # Results
    count_before: Mapped[int] = mapped_column(Integer)  # Count before adding this term
    count_after: Mapped[int] = mapped_column(Integer)  # Count after adding this term
    reduction: Mapped[int] = mapped_column(Integer)  # count_before - count_after
    reduction_percent: Mapped[float] = mapped_column(Float, default=0.0)

    # Decision
    kept: Mapped[bool] = mapped_column(Boolean, default=False)  # Did we keep this term?
    skip_reason: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)  # Why skipped if not kept

    # Timing
    tested_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    latency_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # Time to get count

    # Relationship
    partition_run: Mapped["PartitionRun"] = relationship(
        "PartitionRun", back_populates="term_attempts"
    )

    # NOTE: Index ix_partition_term_partition_term already exists in production


class PartitionQuery(Base):
    """
    Record of every Google Scholar query executed during partition harvesting.

    This gives us complete visibility into what queries were run, what they returned,
    and whether they succeeded or failed.
    """
    __tablename__ = "partition_queries"

    id: Mapped[int] = mapped_column(primary_key=True)
    partition_run_id: Mapped[int] = mapped_column(
        ForeignKey("partition_runs.id", ondelete="CASCADE"), index=True
    )

    # Query details
    query_type: Mapped[str] = mapped_column(String(30), index=True)
    # Types: 'initial_count', 'term_test', 'exclusion_harvest', 'inclusion_harvest', 'recursive_count'

    scholar_id: Mapped[str] = mapped_column(String(50))
    year: Mapped[int] = mapped_column(Integer)
    additional_query: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # The -intitle or OR query
    full_constructed_query: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # For reference

    # Purpose (human-readable)
    purpose: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    # e.g., "Testing exclusion of 'analysis'", "Harvesting exclusion set", etc.

    # Expected vs actual
    expected_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    actual_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # Total results Scholar reports

    # Harvest results (for harvest queries)
    pages_requested: Mapped[int] = mapped_column(Integer, default=0)
    pages_fetched: Mapped[int] = mapped_column(Integer, default=0)
    pages_succeeded: Mapped[int] = mapped_column(Integer, default=0)
    pages_failed: Mapped[int] = mapped_column(Integer, default=0)
    citations_harvested: Mapped[int] = mapped_column(Integer, default=0)
    citations_new: Mapped[int] = mapped_column(Integer, default=0)  # After dedup
    citations_duplicate: Mapped[int] = mapped_column(Integer, default=0)

    # Status
    status: Mapped[str] = mapped_column(String(20), default="pending")
    # Statuses: pending, running, completed, failed, partial

    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Timing
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    latency_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Relationship
    partition_run: Mapped["PartitionRun"] = relationship(
        "PartitionRun", back_populates="queries"
    )

    # NOTE: Indexes ix_partition_queries_type and ix_partition_queries_status
    # already exist in production


class PartitionLLMCall(Base):
    """
    Record of every LLM call made to suggest exclusion terms.

    Complete audit trail of what we asked the LLM and what it returned.
    """
    __tablename__ = "partition_llm_calls"

    id: Mapped[int] = mapped_column(primary_key=True)
    partition_run_id: Mapped[int] = mapped_column(
        ForeignKey("partition_runs.id", ondelete="CASCADE"), index=True
    )

    # Call details
    call_number: Mapped[int] = mapped_column(Integer)  # 1st, 2nd, 3rd call for this partition
    purpose: Mapped[str] = mapped_column(String(100))  # 'initial_suggestions', 'more_terms', etc.

    # Model info
    model: Mapped[str] = mapped_column(String(100))

    # Prompt (full text)
    prompt: Mapped[str] = mapped_column(Text)

    # Context provided to LLM
    edition_title: Mapped[str] = mapped_column(String(500))
    year: Mapped[int] = mapped_column(Integer)
    current_count: Mapped[int] = mapped_column(Integer)
    already_excluded_terms: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON array

    # Response
    raw_response: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    parsed_terms: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON array
    terms_count: Mapped[int] = mapped_column(Integer, default=0)

    # Status
    status: Mapped[str] = mapped_column(String(20), default="pending")
    # Statuses: pending, completed, failed, parse_error

    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Usage stats
    input_tokens: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    output_tokens: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Timing
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    latency_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Relationship
    partition_run: Mapped["PartitionRun"] = relationship(
        "PartitionRun", back_populates="llm_calls"
    )

    # NOTE: Index ix_partition_llm_calls_partition already exists in production


class ApiCallLog(Base):
    """
    Log of API calls (Oxylabs) and page fetches for statistics tracking.

    Enables dashboard to show activity stats for 15min, 1hr, 6hr, 24hr periods.
    """
    __tablename__ = "api_call_logs"

    id: Mapped[int] = mapped_column(primary_key=True)

    # What type of call: 'oxylabs', 'page_fetch', 'citation_save'
    call_type: Mapped[str] = mapped_column(String(30), index=True)

    # Related job (optional)
    job_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)

    # Related edition (optional)
    edition_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Count (for batch operations like citation saves)
    count: Mapped[int] = mapped_column(Integer, default=1)

    # Success/failure
    success: Mapped[bool] = mapped_column(Boolean, default=True)

    # Optional extra info (e.g., page number, year, error message)
    extra_info: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Timestamp for time-based queries
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)

    __table_args__ = (
        # Composite index for efficient time-range + type queries
        Index('ix_api_call_logs_type_created', 'call_type', 'created_at'),
    )


class HealthMonitorLog(Base):
    """
    Log of health monitor diagnoses and actions taken.

    Tracks when the LLM-powered health monitor detected issues,
    what it diagnosed, and what actions were executed.
    """
    __tablename__ = "health_monitor_logs"

    id: Mapped[int] = mapped_column(primary_key=True)

    # Trigger info
    trigger_reason: Mapped[str] = mapped_column(String(100))  # e.g., "zero_citations_15min"
    active_jobs_count: Mapped[int] = mapped_column(Integer, default=0)
    citations_15min: Mapped[int] = mapped_column(Integer, default=0)

    # Diagnostic data sent to LLM (JSON snapshot)
    diagnostic_data: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # LLM response
    llm_model: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    llm_diagnosis: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # Brief explanation
    llm_root_cause: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)  # RATE_LIMIT, ZOMBIE_JOBS, etc.
    llm_confidence: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)  # HIGH, MEDIUM, LOW
    llm_raw_response: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # Full JSON response

    # Action taken
    action_type: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)  # RESTART_ZOMBIE_JOBS, etc.
    action_params: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON
    action_executed: Mapped[bool] = mapped_column(Boolean, default=False)
    action_result: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # Success/failure details
    action_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Timing
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    llm_call_duration_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    action_duration_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    __table_args__ = (
        Index('ix_health_monitor_logs_created', 'created_at'),
    )


# ============== THINKER BIBLIOGRAPHIES ==============
# Track complete bibliographies of individual thinkers (philosophers, theorists, etc.)


class Thinker(Base):
    """
    A canonical thinker whose complete bibliography we're harvesting.

    Thinkers are top-level entities (parallel to Collections) representing
    an author/philosopher whose works we want to systematically discover
    and harvest citations for.
    """
    __tablename__ = "thinkers"

    id: Mapped[int] = mapped_column(primary_key=True)
    canonical_name: Mapped[str] = mapped_column(String(200), unique=True, index=True)

    # LLM disambiguation context
    birth_death: Mapped[Optional[str]] = mapped_column(String(50))  # e.g., "1898-1979"
    bio: Mapped[Optional[str]] = mapped_column(Text)  # Brief biographical note
    domains: Mapped[Optional[str]] = mapped_column(Text)  # JSON array: ["critical theory", "Marxism"]
    notable_works: Mapped[Optional[str]] = mapped_column(Text)  # JSON array of major works

    # Name variants for search queries (JSON array of query strings)
    # e.g., ['author:"Herbert Marcuse"', 'author:"H Marcuse"', 'マルクーゼ']
    name_variants: Mapped[Optional[str]] = mapped_column(Text)

    # Google Scholar author profile URL (optional)
    # If provided, publications are pre-seeded from profile before discovery
    scholar_profile_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    scholar_user_id: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    # Status: pending, disambiguated, generating_variants, harvesting, complete
    status: Mapped[str] = mapped_column(String(50), default="pending", index=True)

    # Progress tracking
    works_discovered: Mapped[int] = mapped_column(Integer, default=0)
    works_harvested: Mapped[int] = mapped_column(Integer, default=0)
    total_citations: Mapped[int] = mapped_column(Integer, default=0)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    disambiguated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    variants_generated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    harvest_started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    harvest_completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    # Harvest batch tracking for profile pre-fetching
    # Tracks completion of all extract_citations jobs to trigger automatic profile fetching
    harvest_batch_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)  # UUID
    harvest_batch_jobs_total: Mapped[int] = mapped_column(Integer, default=0)
    harvest_batch_jobs_completed: Mapped[int] = mapped_column(Integer, default=0)
    harvest_batch_jobs_failed: Mapped[int] = mapped_column(Integer, default=0)

    # Profile pre-fetch status: null, pending, running, completed, failed
    profiles_prefetch_status: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    profiles_prefetch_count: Mapped[int] = mapped_column(Integer, default=0)
    profiles_prefetched_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    # Relationships
    works: Mapped[List["ThinkerWork"]] = relationship(
        "ThinkerWork", back_populates="thinker", cascade="all, delete-orphan"
    )
    harvest_runs: Mapped[List["ThinkerHarvestRun"]] = relationship(
        "ThinkerHarvestRun", back_populates="thinker", cascade="all, delete-orphan"
    )
    llm_calls: Mapped[List["ThinkerLLMCall"]] = relationship(
        "ThinkerLLMCall", back_populates="thinker", cascade="all, delete-orphan"
    )


class ThinkerWork(Base):
    """
    A work (paper/book) authored by a thinker.

    Links a Thinker to their discovered works. Works may or may not
    be converted to Papers (for citation harvesting). Also tracks
    translation relationships between works.
    """
    __tablename__ = "thinker_works"

    id: Mapped[int] = mapped_column(primary_key=True)
    thinker_id: Mapped[int] = mapped_column(ForeignKey("thinkers.id", ondelete="CASCADE"), index=True)

    # Link to Paper if created for citation harvesting
    paper_id: Mapped[Optional[int]] = mapped_column(ForeignKey("papers.id", ondelete="SET NULL"), index=True)

    # Work metadata (from Scholar discovery)
    scholar_id: Mapped[Optional[str]] = mapped_column(String(50), index=True)
    # Cluster ID for citation harvesting (from "Cited by" link on profile)
    # This is the numeric ID needed for cites= queries, different from scholar_id
    cluster_id: Mapped[Optional[str]] = mapped_column(String(50), index=True)
    title: Mapped[str] = mapped_column(Text)
    authors_raw: Mapped[Optional[str]] = mapped_column(Text)  # Raw author string from Scholar
    year: Mapped[Optional[int]] = mapped_column(Integer)
    venue: Mapped[Optional[str]] = mapped_column(String(500))
    citation_count: Mapped[int] = mapped_column(Integer, default=0)
    link: Mapped[Optional[str]] = mapped_column(Text)

    # LLM classification decision
    decision: Mapped[str] = mapped_column(String(20), default="accepted")  # accepted, rejected, uncertain
    confidence: Mapped[float] = mapped_column(Float, default=0.8)
    reason: Mapped[Optional[str]] = mapped_column(Text)  # Why accepted/rejected

    # Translation detection
    is_translation: Mapped[bool] = mapped_column(Boolean, default=False)
    canonical_work_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("thinker_works.id", ondelete="SET NULL"), nullable=True
    )
    original_language: Mapped[Optional[str]] = mapped_column(String(50))
    detected_language: Mapped[Optional[str]] = mapped_column(String(50))

    # Harvest status
    citations_harvested: Mapped[bool] = mapped_column(Boolean, default=False)
    harvest_job_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Discovery context
    found_by_variant: Mapped[Optional[str]] = mapped_column(Text)  # Which name variant found this work
    harvest_run_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("thinker_harvest_runs.id", ondelete="SET NULL"), nullable=True
    )

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # Relationships
    thinker: Mapped["Thinker"] = relationship("Thinker", back_populates="works")
    translations: Mapped[List["ThinkerWork"]] = relationship(
        "ThinkerWork",
        backref="canonical_work",
        remote_side="ThinkerWork.id",
        foreign_keys="ThinkerWork.canonical_work_id"
    )

    __table_args__ = (
        # Unique composite index for deduplication - single-column indexes created by index=True on columns
        Index("ix_thinker_works_thinker_scholar", "thinker_id", "scholar_id", unique=True),
    )


class ThinkerHarvestRun(Base):
    """
    A harvest run for a specific name variant query.

    Each run represents paginating through all results for one
    author search query variant.
    """
    __tablename__ = "thinker_harvest_runs"

    id: Mapped[int] = mapped_column(primary_key=True)
    thinker_id: Mapped[int] = mapped_column(ForeignKey("thinkers.id", ondelete="CASCADE"), index=True)

    # Query details
    query_used: Mapped[str] = mapped_column(Text)  # Full query string
    variant_type: Mapped[str] = mapped_column(String(50))  # full_name, initial_surname, transliteration, etc.

    # Progress tracking
    total_results_reported: Mapped[int] = mapped_column(Integer, default=0)  # What Scholar reports
    pages_fetched: Mapped[int] = mapped_column(Integer, default=0)
    results_processed: Mapped[int] = mapped_column(Integer, default=0)
    results_accepted: Mapped[int] = mapped_column(Integer, default=0)
    results_rejected: Mapped[int] = mapped_column(Integer, default=0)
    results_uncertain: Mapped[int] = mapped_column(Integer, default=0)

    # Status: pending, running, completed, failed
    status: Mapped[str] = mapped_column(String(20), default="pending", index=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Timing
    started_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    # Relationship
    thinker: Mapped["Thinker"] = relationship("Thinker", back_populates="harvest_runs")

    # Note: Single-column indexes created by index=True on thinker_id and status columns


class ThinkerLLMCall(Base):
    """
    Audit trail for all LLM calls in thinker workflows.

    Provides complete traceability of:
    - Disambiguation calls
    - Name variant generation
    - Per-page filtering decisions
    - Translation detection
    - Retrospective matching
    """
    __tablename__ = "thinker_llm_calls"

    id: Mapped[int] = mapped_column(primary_key=True)
    # Nullable to allow LLM calls during disambiguation before thinker is created
    thinker_id: Mapped[Optional[int]] = mapped_column(ForeignKey("thinkers.id", ondelete="SET NULL"), index=True, nullable=True)

    # Workflow type
    workflow: Mapped[str] = mapped_column(String(50), index=True)
    # Values: disambiguation, variant_generation, page_filtering, translation_detection, retrospective_matching

    call_number: Mapped[int] = mapped_column(Integer, default=1)  # 1st, 2nd, 3rd call for this workflow

    # Model info
    model: Mapped[str] = mapped_column(String(100))

    # Prompt
    prompt: Mapped[str] = mapped_column(Text)

    # Context provided (JSON)
    context_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Response
    raw_response: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    parsed_result: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON

    # Extended thinking (for Opus calls)
    thinking_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    thinking_tokens: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Status: pending, completed, failed, parse_error
    status: Mapped[str] = mapped_column(String(20), default="pending")
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Usage stats
    input_tokens: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    output_tokens: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    latency_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Timing
    started_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    # Relationship
    thinker: Mapped["Thinker"] = relationship("Thinker", back_populates="llm_calls")

    # Note: Single-column indexes created by index=True on thinker_id and workflow columns


# ============== EXHAUSTIVE EDITION ANALYSIS ==============
# Work-centric model for analyzing and linking editions across languages
# Used for comprehensive bibliographic analysis of thinker dossiers


class Work(Base):
    """
    An abstract intellectual work (book, essay, article, etc.).

    A Work is the abstract entity that can have multiple editions/translations.
    For example, "The Spirit of Utopia" is a Work that has German original
    "Geist der Utopie" and English translation "The Spirit of Utopia".

    Works are identified by thinker + canonical_title, which is typically
    the English title or the most commonly referenced title.
    """
    __tablename__ = "works"

    id: Mapped[int] = mapped_column(primary_key=True)

    # Thinker identification (text, not FK - works can exist for thinkers without Thinker record)
    thinker_name: Mapped[str] = mapped_column(String(255), index=True)

    # Canonical identification
    canonical_title: Mapped[str] = mapped_column(String(500))  # Usually English or most common title

    # Original work details
    original_language: Mapped[Optional[str]] = mapped_column(String(50))  # e.g., "german"
    original_title: Mapped[Optional[str]] = mapped_column(String(500))  # e.g., "Geist der Utopie"
    original_year: Mapped[Optional[int]] = mapped_column(Integer)

    # Classification
    work_type: Mapped[Optional[str]] = mapped_column(String(50))  # book, article, essay, lecture, anthology
    importance: Mapped[Optional[str]] = mapped_column(String(20))  # major, minor, peripheral

    # Additional context
    notes: Mapped[Optional[str]] = mapped_column(Text)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    editions: Mapped[List["WorkEdition"]] = relationship(
        "WorkEdition", back_populates="work", cascade="all, delete-orphan"
    )
    missing_editions: Mapped[List["MissingEdition"]] = relationship(
        "MissingEdition", back_populates="work", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("ix_works_thinker_title", "thinker_name", "canonical_title", unique=True),
        Index("ix_works_thinker", "thinker_name"),
    )


class WorkEdition(Base):
    """
    Links a Work to a specific Paper/Edition in the database.

    Represents a concrete manifestation of a Work - either the original
    or a translation in a specific language.

    A Work can have multiple WorkEditions (one per language/translation).
    A Paper or Edition can only link to one Work (enforced by unique constraint).
    """
    __tablename__ = "work_editions"

    id: Mapped[int] = mapped_column(primary_key=True)
    work_id: Mapped[int] = mapped_column(ForeignKey("works.id", ondelete="CASCADE"), index=True)

    # Link to actual database record (one of these should be set)
    paper_id: Mapped[Optional[int]] = mapped_column(ForeignKey("papers.id", ondelete="SET NULL"), index=True)
    edition_id: Mapped[Optional[int]] = mapped_column(ForeignKey("editions.id", ondelete="SET NULL"), index=True)

    # Edition details
    language: Mapped[str] = mapped_column(String(50))  # e.g., "english", "german", "french"
    edition_type: Mapped[Optional[str]] = mapped_column(String(50))  # original, translation, abridged, anthology_excerpt
    year: Mapped[Optional[int]] = mapped_column(Integer)
    translator: Mapped[Optional[str]] = mapped_column(String(255))  # For translations
    publisher: Mapped[Optional[str]] = mapped_column(String(255))

    # Verification status
    verified: Mapped[bool] = mapped_column(Boolean, default=False)  # Manually verified link
    auto_linked: Mapped[bool] = mapped_column(Boolean, default=True)  # Linked by LLM/algorithm
    confidence: Mapped[Optional[float]] = mapped_column(Float)  # 0.0 to 1.0
    link_reason: Mapped[Optional[str]] = mapped_column(Text)  # Why this link was made

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # Relationships
    work: Mapped["Work"] = relationship("Work", back_populates="editions")

    __table_args__ = (
        # A specific edition can only be linked to one work
        Index("ix_work_editions_edition_unique", "edition_id", unique=True, postgresql_where="edition_id IS NOT NULL"),
        # A specific paper can only be linked to one work
        Index("ix_work_editions_paper_unique", "paper_id", unique=True, postgresql_where="paper_id IS NOT NULL"),
        Index("ix_work_editions_work", "work_id"),
        Index("ix_work_editions_language", "work_id", "language"),
    )


class MissingEdition(Base):
    """
    A gap identified in the edition coverage for a Work.

    When bibliographic analysis determines that a translation exists
    but isn't in our database, we record it here. This can then be
    used to generate scraper jobs to find the missing edition.
    """
    __tablename__ = "missing_editions"

    id: Mapped[int] = mapped_column(primary_key=True)
    work_id: Mapped[int] = mapped_column(ForeignKey("works.id", ondelete="CASCADE"), index=True)

    # What's missing
    language: Mapped[str] = mapped_column(String(50))  # Missing language
    expected_title: Mapped[Optional[str]] = mapped_column(String(500))  # Expected title in that language
    expected_year: Mapped[Optional[int]] = mapped_column(Integer)
    expected_translator: Mapped[Optional[str]] = mapped_column(String(255))
    expected_publisher: Mapped[Optional[str]] = mapped_column(String(255))

    # How we know it exists
    source: Mapped[Optional[str]] = mapped_column(String(100))  # llm_knowledge, web_search, google_scholar
    source_url: Mapped[Optional[str]] = mapped_column(Text)  # Verification URL if found
    source_details: Mapped[Optional[str]] = mapped_column(Text)  # Additional source info (JSON)

    # Priority and status
    priority: Mapped[str] = mapped_column(String(20), default="medium")  # high, medium, low
    status: Mapped[str] = mapped_column(String(20), default="pending")  # pending, job_created, found, dismissed

    # Link to scraper job if created
    job_id: Mapped[Optional[int]] = mapped_column(ForeignKey("jobs.id", ondelete="SET NULL"))

    # Resolution tracking
    dismissed_reason: Mapped[Optional[str]] = mapped_column(Text)  # Why dismissed if status=dismissed
    found_edition_id: Mapped[Optional[int]] = mapped_column(ForeignKey("editions.id", ondelete="SET NULL"))

    notes: Mapped[Optional[str]] = mapped_column(Text)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Relationships
    work: Mapped["Work"] = relationship("Work", back_populates="missing_editions")

    __table_args__ = (
        Index("ix_missing_editions_work", "work_id"),
        Index("ix_missing_editions_status", "status"),
        Index("ix_missing_editions_priority", "priority"),
        Index("ix_missing_editions_work_lang", "work_id", "language", unique=True),
    )


class EditionAnalysisRun(Base):
    """
    Audit trail for a complete edition analysis run on a dossier.

    Tracks the full lifecycle of analyzing a thinker's dossier:
    1. Inventory of existing papers/editions
    2. Bibliographic research via Claude
    3. Linking editions to Works
    4. Gap analysis
    5. Job generation

    Provides cost tracking and status updates for the UI.
    """
    __tablename__ = "edition_analysis_runs"

    id: Mapped[int] = mapped_column(primary_key=True)
    dossier_id: Mapped[int] = mapped_column(ForeignKey("dossiers.id", ondelete="CASCADE"), index=True)

    # Thinker being analyzed
    thinker_name: Mapped[str] = mapped_column(String(255))

    # Status tracking
    status: Mapped[str] = mapped_column(String(30), default="pending", index=True)
    # Statuses: pending, inventorying, researching, linking, analyzing_gaps, generating_jobs, completed, failed
    phase: Mapped[Optional[str]] = mapped_column(String(50))  # Current phase description
    phase_progress: Mapped[float] = mapped_column(Float, default=0.0)  # 0.0 to 1.0

    # Progress counters
    papers_analyzed: Mapped[int] = mapped_column(Integer, default=0)
    editions_analyzed: Mapped[int] = mapped_column(Integer, default=0)
    works_identified: Mapped[int] = mapped_column(Integer, default=0)
    links_created: Mapped[int] = mapped_column(Integer, default=0)
    gaps_found: Mapped[int] = mapped_column(Integer, default=0)
    jobs_created: Mapped[int] = mapped_column(Integer, default=0)

    # LLM usage stats
    llm_calls_count: Mapped[int] = mapped_column(Integer, default=0)
    web_searches_count: Mapped[int] = mapped_column(Integer, default=0)
    total_input_tokens: Mapped[int] = mapped_column(Integer, default=0)
    total_output_tokens: Mapped[int] = mapped_column(Integer, default=0)
    thinking_tokens: Mapped[int] = mapped_column(Integer, default=0)

    # Results summary (JSON)
    results_summary: Mapped[Optional[str]] = mapped_column(Text)  # JSON summary for quick display

    # Error tracking
    error: Mapped[Optional[str]] = mapped_column(Text)
    error_phase: Mapped[Optional[str]] = mapped_column(String(50))

    # Timing
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Relationships
    dossier: Mapped["Dossier"] = relationship("Dossier", back_populates="edition_analysis_runs")
    llm_calls: Mapped[List["EditionAnalysisLLMCall"]] = relationship(
        "EditionAnalysisLLMCall", back_populates="run", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("ix_edition_analysis_runs_dossier", "dossier_id"),
        Index("ix_edition_analysis_runs_status", "status"),
    )


class EditionAnalysisLLMCall(Base):
    """
    Detailed audit trail for each LLM call in edition analysis.

    Provides complete traceability for:
    - Bibliographic research queries
    - Edition verification
    - Gap analysis reasoning
    - Job generation decisions
    """
    __tablename__ = "edition_analysis_llm_calls"

    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("edition_analysis_runs.id", ondelete="CASCADE"), index=True)

    # Call context
    phase: Mapped[str] = mapped_column(String(50), index=True)
    # Phases: inventory, bibliographic_research, gap_analysis, verification, job_generation
    call_number: Mapped[int] = mapped_column(Integer, default=1)  # Sequence within phase
    purpose: Mapped[Optional[str]] = mapped_column(String(200))  # Human-readable purpose

    # Model info
    model: Mapped[str] = mapped_column(String(100))

    # Request
    prompt: Mapped[str] = mapped_column(Text)
    context_json: Mapped[Optional[str]] = mapped_column(Text)  # JSON context provided

    # Response
    raw_response: Mapped[Optional[str]] = mapped_column(Text)
    parsed_result: Mapped[Optional[str]] = mapped_column(Text)  # JSON parsed result

    # Extended thinking (for Opus calls)
    thinking_text: Mapped[Optional[str]] = mapped_column(Text)
    thinking_tokens: Mapped[Optional[int]] = mapped_column(Integer)

    # Web search tracking
    web_search_used: Mapped[bool] = mapped_column(Boolean, default=False)
    web_search_queries: Mapped[Optional[str]] = mapped_column(Text)  # JSON array of queries
    web_sources_cited: Mapped[Optional[str]] = mapped_column(Text)  # JSON array of URLs

    # Usage stats
    input_tokens: Mapped[Optional[int]] = mapped_column(Integer)
    output_tokens: Mapped[Optional[int]] = mapped_column(Integer)
    latency_ms: Mapped[Optional[int]] = mapped_column(Integer)

    # Status
    status: Mapped[str] = mapped_column(String(20), default="pending")
    # Statuses: pending, streaming, completed, failed, parse_error
    error_message: Mapped[Optional[str]] = mapped_column(Text)

    # Timing
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Relationships
    run: Mapped["EditionAnalysisRun"] = relationship("EditionAnalysisRun", back_populates="llm_calls")

    __table_args__ = (
        Index("ix_edition_analysis_llm_calls_run", "run_id"),
        Index("ix_edition_analysis_llm_calls_phase", "phase"),
    )
