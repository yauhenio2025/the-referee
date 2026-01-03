"""
Database models for The Referee
"""
from datetime import datetime
from typing import Optional, List
from sqlalchemy import String, Integer, Text, DateTime, Boolean, ForeignKey, JSON, Float, Index
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


class Citation(Base):
    """A paper that cites a seed paper (or one of its editions)"""
    __tablename__ = "citations"

    id: Mapped[int] = mapped_column(primary_key=True)
    paper_id: Mapped[int] = mapped_column(ForeignKey("papers.id", ondelete="CASCADE"))
    edition_id: Mapped[Optional[int]] = mapped_column(ForeignKey("editions.id", ondelete="SET NULL"))

    scholar_id: Mapped[Optional[str]] = mapped_column(String(50), index=True)

    title: Mapped[str] = mapped_column(Text)
    authors: Mapped[Optional[str]] = mapped_column(Text)
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
    """Track expected citation counts per year for an edition.

    When we start harvesting, we record the total count Scholar reports for each year.
    This lets us verify completeness and identify gaps.
    """
    __tablename__ = "harvest_targets"

    id: Mapped[int] = mapped_column(primary_key=True)
    edition_id: Mapped[int] = mapped_column(ForeignKey("editions.id", ondelete="CASCADE"), index=True)

    year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # null = all years combined

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

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    __table_args__ = (
        Index("ix_harvest_targets_edition_year", "edition_id", "year", unique=True),
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
    year: Mapped[int] = mapped_column(Integer, index=True)

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
